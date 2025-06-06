/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <map>

#include "errors.h"
#include "execution/execution.h"
#include "parser/parser.h"
#include "system/sm.h"
#include "common/context.h"
#include "transaction/transaction_manager.h"
#include "planner.h"
#include "plan.h"
#include "execution/execution_manager.h"
#include "record/rm_file_handle.h"
#include <memory>
#include <vector>

// 添加TabCol的比较运算符
bool operator==(const TabCol &lhs, const TabCol &rhs)
{
    return lhs.tab_name == rhs.tab_name && lhs.col_name == rhs.col_name;
}

class Optimizer
{
private:
    SmManager *sm_manager_;
    Planner *planner_;
    std::map<std::string, std::unique_ptr<RmFileHandle>> fhs_;

    // 获取表的基数（行数）
    size_t get_table_cardinality(const std::string &table_name)
    {
        auto fh = sm_manager_->fhs_.at(table_name).get();
        size_t num_records = 0;
        for (int page_no = 1; page_no < fh->get_file_hdr().num_pages; page_no++)
        {
            auto page_handle = fh->fetch_page_handle(page_no);
            num_records += page_handle.page_hdr->num_records;
        }
        return num_records;
    }

    // 估算连接结果的基数
    size_t estimate_join_cardinality(const std::vector<std::string> &tables)
    {
        size_t result = 1;
        for (const auto &table : tables)
        {
            result *= get_table_cardinality(table);
        }
        return result;
    }

    // 选择运算下推
    std::shared_ptr<Plan> push_down_select(std::shared_ptr<Plan> plan, std::vector<Condition> &conditions)
    {
        if (!plan)
            return nullptr;

        switch (plan->tag)
        {
        case T_Projection:
        {
            auto proj_plan = std::dynamic_pointer_cast<ProjectionPlan>(plan);
            proj_plan->subplan_ = push_down_select(proj_plan->subplan_, conditions);
            return plan;
        }
        case T_NestLoop:
        {
            auto join_plan = std::dynamic_pointer_cast<JoinPlan>(plan);

            // 分离连接条件和过滤条件
            std::vector<Condition> join_conds;
            std::vector<Condition> left_conds;
            std::vector<Condition> right_conds;

            for (const auto &cond : conditions)
            {
                if (!cond.is_rhs_val)
                {
                    // 连接条件
                    join_conds.push_back(cond);
                }
                else
                {
                    // 过滤条件，根据列所属表分配
                    auto left_tables = get_tables_in_plan(join_plan->left_);
                    if (std::find(left_tables.begin(), left_tables.end(), cond.lhs_col.tab_name) != left_tables.end())
                    {
                        left_conds.push_back(cond);
                    }
                    else
                    {
                        right_conds.push_back(cond);
                    }
                }
            }

            // 递归下推过滤条件
            join_plan->left_ = push_down_select(join_plan->left_, left_conds);
            join_plan->right_ = push_down_select(join_plan->right_, right_conds);
            join_plan->conds_ = join_conds;

            return plan;
        }
        case T_SeqScan:
        {
            auto scan_plan = std::dynamic_pointer_cast<ScanPlan>(plan);
            // 将适用于此表的过滤条件添加到扫描节点
            std::vector<Condition> applicable_conds;
            for (const auto &cond : conditions)
            {
                if (cond.lhs_col.tab_name == scan_plan->tab_name_)
                {
                    applicable_conds.push_back(cond);
                }
            }
            scan_plan->fed_conds_ = applicable_conds;
            return plan;
        }
        default:
            return plan;
        }
    }

    // 投影运算下推
    std::shared_ptr<Plan> push_down_project(std::shared_ptr<Plan> plan, const std::vector<TabCol> &required_cols)
    {
        if (!plan)
            return nullptr;

        switch (plan->tag)
        {
        case T_Projection:
        {
            auto proj_plan = std::dynamic_pointer_cast<ProjectionPlan>(plan);
            // 只保留所需的列
            std::vector<TabCol> new_cols;
            for (const auto &col : proj_plan->sel_cols_)
            {
                if (std::find(required_cols.begin(), required_cols.end(), col) != required_cols.end())
                {
                    new_cols.push_back(col);
                }
            }
            proj_plan->sel_cols_ = new_cols;
            proj_plan->subplan_ = push_down_project(proj_plan->subplan_, required_cols);
            return plan;
        }
        case T_NestLoop:
        {
            auto join_plan = std::dynamic_pointer_cast<JoinPlan>(plan);

            // 收集连接条件中用到的列
            std::vector<TabCol> join_cols;
            for (const auto &cond : join_plan->conds_)
            {
                join_cols.push_back(cond.lhs_col);
                if (!cond.is_rhs_val)
                {
                    join_cols.push_back(cond.rhs_col);
                }
            }

            // 合并所需列和连接列
            std::vector<TabCol> all_required_cols = required_cols;
            all_required_cols.insert(all_required_cols.end(), join_cols.begin(), join_cols.end());

            // 递归下推到子节点
            join_plan->left_ = push_down_project(join_plan->left_, all_required_cols);
            join_plan->right_ = push_down_project(join_plan->right_, all_required_cols);

            return plan;
        }
        default:
            return plan;
        }
    }

    // 获取计划中涉及的所有表名
    std::vector<std::string> get_tables_in_plan(const std::shared_ptr<Plan> &plan)
    {
        std::vector<std::string> tables;
        if (!plan)
            return tables;

        switch (plan->tag)
        {
        case T_SeqScan:
        {
            auto scan_plan = std::dynamic_pointer_cast<ScanPlan>(plan);
            tables.push_back(scan_plan->tab_name_);
            break;
        }
        case T_NestLoop:
        {
            auto join_plan = std::dynamic_pointer_cast<JoinPlan>(plan);
            auto left_tables = get_tables_in_plan(join_plan->left_);
            auto right_tables = get_tables_in_plan(join_plan->right_);
            tables.insert(tables.end(), left_tables.begin(), left_tables.end());
            tables.insert(tables.end(), right_tables.begin(), right_tables.end());
            break;
        }
        case T_Projection:
        {
            auto proj_plan = std::dynamic_pointer_cast<ProjectionPlan>(plan);
            return get_tables_in_plan(proj_plan->subplan_);
        }
        default:
            break;
        }
        return tables;
    }

public:
    Optimizer(SmManager *sm_manager, Planner *planner)
        : sm_manager_(sm_manager), planner_(planner)
    {
        // 从SmManager获取文件句柄
        for (const auto &[tab_name, fh] : sm_manager->fhs_)
        {
            fhs_[tab_name] = std::unique_ptr<RmFileHandle>(fh.get());
        }
    }

    std::shared_ptr<Plan> plan_query(std::shared_ptr<Query> query, Context *context)
    {
        switch (query->parse->Nodetype())
        {
        case ast::TreeNodeType::Help:
            return std::make_shared<OtherPlan>(T_Help, std::string());
        case ast::TreeNodeType::ShowTables:
            return std::make_shared<OtherPlan>(T_ShowTable, std::string());
        case ast::TreeNodeType::DescTable:
        {
            auto x = std::static_pointer_cast<ast::DescTable>(query->parse);
            return std::make_shared<OtherPlan>(T_DescTable, x->tab_name);
        }
        case ast::TreeNodeType::TxnBegin:
            return std::make_shared<OtherPlan>(T_Transaction_begin, std::string());
        case ast::TreeNodeType::TxnAbort:
            return std::make_shared<OtherPlan>(T_Transaction_abort, std::string());
        case ast::TreeNodeType::TxnCommit:
            return std::make_shared<OtherPlan>(T_Transaction_commit, std::string());
        case ast::TreeNodeType::TxnRollback:
            return std::make_shared<OtherPlan>(T_Transaction_rollback, std::string());
        case ast::TreeNodeType::SetStmt:
        {
            auto x = std::dynamic_pointer_cast<ast::SetStmt>(query->parse);
            return std::make_shared<SetKnobPlan>(x->set_knob_type_, x->bool_val_);
        }
        case ast::TreeNodeType::ExplainStmt:
        {
            auto x = std::dynamic_pointer_cast<ast::ExplainStmt>(query->parse);
            auto query_copy = std::make_shared<Query>();
            query_copy->parse = x->query;
            auto subplan = plan_query(query_copy, context);
            return std::make_shared<ExplainPlan>(T_Explain, subplan);
        }
        default:
            return planner_->do_planner(query, context);
        }
    }

    // 优化查询计划
    std::shared_ptr<Plan> optimize(std::shared_ptr<Plan> plan)
    {
        if (!plan)
            return nullptr;

        // 1. 如果是DML计划，获取其子计划
        if (plan->tag == T_select)
        {
            auto select_plan = std::dynamic_pointer_cast<DMLPlan>(plan);
            auto optimized_subplan = optimize(select_plan->subplan_);
            select_plan->subplan_ = optimized_subplan;
            return plan;
        }

        // 2. 收集所需的列和条件
        std::vector<TabCol> required_cols;
        std::vector<Condition> conditions;
        collect_cols_and_conditions(plan, required_cols, conditions);

        // 3. 应用优化规则
        auto optimized_plan = plan;

        // 3.1 选择运算下推
        optimized_plan = push_down_select(optimized_plan, conditions);

        // 3.2 投影运算下推
        optimized_plan = push_down_project(optimized_plan, required_cols);

        // 3.3 连接顺序优化（对于多表连接）
        if (optimized_plan->tag == T_NestLoop)
        {
            optimized_plan = optimize_join_order(optimized_plan);
        }

        return optimized_plan;
    }

    // 收集计划中的列和条件
    void collect_cols_and_conditions(const std::shared_ptr<Plan> &plan,
                                     std::vector<TabCol> &cols,
                                     std::vector<Condition> &conditions)
    {
        if (!plan)
            return;

        switch (plan->tag)
        {
        case T_Projection:
        {
            auto proj_plan = std::dynamic_pointer_cast<ProjectionPlan>(plan);
            cols.insert(cols.end(), proj_plan->sel_cols_.begin(), proj_plan->sel_cols_.end());
            collect_cols_and_conditions(proj_plan->subplan_, cols, conditions);
            break;
        }
        case T_NestLoop:
        {
            auto join_plan = std::dynamic_pointer_cast<JoinPlan>(plan);
            conditions.insert(conditions.end(), join_plan->conds_.begin(), join_plan->conds_.end());
            collect_cols_and_conditions(join_plan->left_, cols, conditions);
            collect_cols_and_conditions(join_plan->right_, cols, conditions);
            break;
        }
        case T_SeqScan:
        {
            auto scan_plan = std::dynamic_pointer_cast<ScanPlan>(plan);
            conditions.insert(conditions.end(), scan_plan->fed_conds_.begin(), scan_plan->fed_conds_.end());
            break;
        }
        default:
            break;
        }
    }

    // 优化连接顺序
    std::shared_ptr<Plan> optimize_join_order(std::shared_ptr<Plan> plan)
    {
        if (plan->tag != T_NestLoop)
            return plan;

        // 收集所有基表和连接条件
        std::vector<std::shared_ptr<Plan>> base_tables;
        std::vector<Condition> all_join_conditions;
        collect_base_tables_and_conditions(plan, base_tables, all_join_conditions);

        if (base_tables.size() <= 2)
            return plan;

        // 使用贪心算法选择连接顺序
        auto result = build_left_deep_tree(base_tables, all_join_conditions);
        return result;
    }

    // 收集基表和连接条件
    void collect_base_tables_and_conditions(const std::shared_ptr<Plan> &plan,
                                            std::vector<std::shared_ptr<Plan>> &tables,
                                            std::vector<Condition> &conditions)
    {
        if (!plan)
            return;

        switch (plan->tag)
        {
        case T_NestLoop:
        {
            auto join_plan = std::dynamic_pointer_cast<JoinPlan>(plan);
            conditions.insert(conditions.end(), join_plan->conds_.begin(), join_plan->conds_.end());
            collect_base_tables_and_conditions(join_plan->left_, tables, conditions);
            collect_base_tables_and_conditions(join_plan->right_, tables, conditions);
            break;
        }
        case T_SeqScan:
        {
            tables.push_back(plan);
            break;
        }
        default:
            break;
        }
    }

    // 构建左深树
    std::shared_ptr<Plan> build_left_deep_tree(std::vector<std::shared_ptr<Plan>> &tables,
                                               const std::vector<Condition> &all_conditions)
    {
        if (tables.empty())
            return nullptr;
        if (tables.size() == 1)
            return tables[0];

        // 找到基数最小的两个表
        size_t min_i = 0, min_j = 1;
        size_t min_card = SIZE_MAX;

        for (size_t i = 0; i < tables.size(); i++)
        {
            for (size_t j = i + 1; j < tables.size(); j++)
            {
                auto table_i = std::dynamic_pointer_cast<ScanPlan>(tables[i]);
                auto table_j = std::dynamic_pointer_cast<ScanPlan>(tables[j]);
                size_t card = get_table_cardinality(table_i->tab_name_) *
                              get_table_cardinality(table_j->tab_name_);
                if (card < min_card)
                {
                    min_card = card;
                    min_i = i;
                    min_j = j;
                }
            }
        }

        // 创建初始连接
        auto first_join = std::make_shared<JoinPlan>(
            T_NestLoop,
            tables[min_i],
            tables[min_j],
            get_join_conditions(tables[min_i], tables[min_j], all_conditions));

        // 移除已使用的表
        tables.erase(tables.begin() + std::max(min_i, min_j));
        tables.erase(tables.begin() + std::min(min_i, min_j));

        // 逐个添加剩余的表
        auto current_plan = first_join;
        while (!tables.empty())
        {
            // 找到添加后产生最小中间结果的表
            size_t best_idx = 0;
            size_t min_result_card = SIZE_MAX;

            for (size_t i = 0; i < tables.size(); i++)
            {
                auto current_tables = get_tables_in_plan(current_plan);
                current_tables.push_back(std::dynamic_pointer_cast<ScanPlan>(tables[i])->tab_name_);
                size_t card = estimate_join_cardinality(current_tables);

                if (card < min_result_card)
                {
                    min_result_card = card;
                    best_idx = i;
                }
            }

            // 添加选中的表
            current_plan = std::make_shared<JoinPlan>(
                T_NestLoop,
                current_plan,
                tables[best_idx],
                get_join_conditions(current_plan, tables[best_idx], all_conditions));

            tables.erase(tables.begin() + best_idx);
        }

        return current_plan;
    }

    // 获取两个计划节点之间的连接条件
    std::vector<Condition> get_join_conditions(const std::shared_ptr<Plan> &left,
                                               const std::shared_ptr<Plan> &right,
                                               const std::vector<Condition> &all_conditions)
    {
        std::vector<Condition> join_conditions;
        auto left_tables = get_tables_in_plan(left);
        auto right_tables = get_tables_in_plan(right);

        for (const auto &cond : all_conditions)
        {
            if (!cond.is_rhs_val)
            { // 只考虑连接条件
                bool left_has_lhs = std::find(left_tables.begin(), left_tables.end(),
                                              cond.lhs_col.tab_name) != left_tables.end();
                bool right_has_rhs = std::find(right_tables.begin(), right_tables.end(),
                                               cond.rhs_col.tab_name) != right_tables.end();

                if ((left_has_lhs && right_has_rhs) ||
                    (std::find(right_tables.begin(), right_tables.end(), cond.lhs_col.tab_name) != right_tables.end() &&
                     std::find(left_tables.begin(), left_tables.end(), cond.rhs_col.tab_name) != left_tables.end()))
                {
                    join_conditions.push_back(cond);
                }
            }
        }

        return join_conditions;
    }
};
