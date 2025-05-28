/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "planner.h"

#include <memory>

#include "execution/executor_delete.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_insert.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_update.h"
#include "index/ix.h"
#include "record_printer.h"
static const std::map<CompOp, CompOp> swap_op = {
    {OP_EQ, OP_EQ},
    {OP_NE, OP_NE},
    {OP_LT, OP_GT},
    {OP_GT, OP_LT},
    {OP_LE, OP_GE},
    {OP_GE, OP_LE},
};
// 目前的索引匹配规则为：完全匹配索引字段，且全部为单点查询，不会自动调整where条件的顺序
std::pair<IndexMeta*, int> Planner::get_index_cols(const std::string &tab_name, const std::vector<Condition> &curr_conds)
{
    // 获取表格对象
    auto &tab = sm_manager_->db_.get_table(tab_name);
    // 如果表格没有索引，返回false
    if (tab.indexes.empty())
        std::make_pair(nullptr, -1);

    // 用于存储条件列的集合
    std::unordered_map<std::string, bool> conds_cols_;
    // 遍历当前条件
    for (const auto &cond : curr_conds)
    {
        // 如果条件是列与值比较，并且操作符不是不等于，并且列属于当前表格
        if (cond.is_rhs_val/* && cond.lhs_col.tab_name == tab_name*/ && cond.op != CompOp::OP_NE)
        {
            // 将列名加入集合
            auto iter = conds_cols_.try_emplace(cond.lhs_col.col_name, false).first;
            iter->second = iter->second || (cond.op != CompOp::OP_EQ);
        }
    }

    // 初始化匹配到的索引号为-1，最大匹配列数为0
    int matched_index_number = -1;
    int max_match_col_count = 0;
    // 遍历表格的索引
    for (size_t idx_number = 0; idx_number < tab.indexes.size(); ++idx_number)
    {
        int match_col_num = 0;
        // 遍历索引的列
        for (auto &col : tab.indexes[idx_number].cols)
        {
            // 如果当前索引列在条件列集合中
            auto iter = conds_cols_.find(col.name);
            if (iter == conds_cols_.end())
                break;
            ++match_col_num;
            if (iter->second)
                break;
        }
        // 如果匹配列数大于最大匹配列数
        if (match_col_num > max_match_col_count)
        {
            max_match_col_count = match_col_num;
            matched_index_number = idx_number;
        }
    }

    if (matched_index_number != -1)
        return std::make_pair(&tab.indexes[matched_index_number], max_match_col_count);
    return std::make_pair(nullptr, -1);
}

/**
 * @brief 表算子条件谓词生成
 *
 * @param conds 条件
 * @param tab_names 表名
 * @return std::vector<Condition>
 */
std::vector<Condition> pop_conds(std::vector<Condition> &conds, std::string &tab_names)
{
    std::vector<Condition> solved_conds;
    solved_conds.reserve(conds.size());

    auto left = conds.begin();
    auto right = conds.end();
    auto check = [&](const Condition &cond)
    {
        // std::string s = cond.lhs_col.tab_name;
        return (tab_names.compare(cond.lhs_col.tab_name) == 0 && cond.is_rhs_val) ||
               (cond.lhs_col.tab_name.compare(cond.rhs_col.tab_name) == 0)/* || (tab_names == cond.lhs_col.tab_name && cond.is_subquery)*/;
    };
    while (left < right)
    {
        if (check(*left))
        {
            // 将右侧非目标元素换到左侧
            while (left < --right && check(*right))
                solved_conds.emplace_back(std::move(*right));
            solved_conds.emplace_back(std::move(*left));
            if (left >= right)
                break;
            *left = std::move(*right);
        }
        ++left;
    }
    conds.erase(left, conds.end());
    return solved_conds;
}

int push_conds(Condition *cond, std::shared_ptr<Plan> plan)
{
    switch (plan->tag)
    {
    case PlanTag::T_IndexScan:
    case PlanTag::T_SeqScan:
    {
        auto x = std::static_pointer_cast<ScanPlan>(plan);
        if (x->tab_name_.compare(cond->lhs_col.tab_name) == 0)
            return 1;
        else if (x->tab_name_.compare(cond->rhs_col.tab_name) == 0)
            return 2;
        return 0;
    }
    case PlanTag::T_NestLoop:
    case PlanTag::T_SortMerge:
    {
        auto x = std::static_pointer_cast<JoinPlan>(plan);
        int left_res = push_conds(cond, x->left_);
        // 条件已经下推到左子节点
        if (left_res == 3)
            return 3;
        int right_res = push_conds(cond, x->right_);
        // 条件已经下推到右子节点
        if (right_res == 3)
            return 3;
        // 左子节点或右子节点有一个没有匹配到条件的列
        if (left_res == 0 || right_res == 0)
            return left_res + right_res;
        // 左子节点匹配到条件的右边
        if (left_res == 2)
        {
            // 需要将左右两边的条件变换位置
            std::swap(cond->lhs_col, cond->rhs_col);
            cond->op = swap_op.at(cond->op);
        }
        x->conds_.emplace_back(std::move(*cond));
        return 3;
    }
    default:
        return false;
    }
}

std::shared_ptr<Plan> pop_scan(int *scantbl, std::string &table, std::unordered_set<std::string> &joined_tables,
                               std::vector<std::shared_ptr<Plan>> plans)
{
    for (size_t i = 0; i < plans.size(); ++i)
    {
        auto x = std::static_pointer_cast<ScanPlan>(plans[i]);
        if (x->tab_name_.compare(table) == 0)
        {
            scantbl[i] = 1;
            joined_tables.emplace(x->tab_name_);
            return plans[i];
        }
    }
    return nullptr;
}

std::shared_ptr<Query> Planner::logical_optimization(std::shared_ptr<Query> query, Context *context)
{

    // TODO 实现逻辑优化规则

    return query;
}

std::shared_ptr<Plan> Planner::physical_optimization(std::shared_ptr<Query> query, Context *context)
{
    std::shared_ptr<Plan> plan = make_one_rel(query, context);

    // 其他物理优化
    // 处理聚合函数
    plan = generate_agg_plan(query, std::move(plan));
    // 处理orderby
    plan = generate_sort_plan(query, std::move(plan));

    return plan;
}

std::shared_ptr<Plan> Planner::make_one_rel(std::shared_ptr<Query> query, Context *context)
{
    std::vector<std::string> tables = query->tables;

    // 处理where里面的不相关子查询
    // for (auto &cond : query->conds)
    // {
    //     if (!cond.is_subquery || cond.subQuery->stmt == nullptr)
    //         continue;
    //     cond.subQuery->plan = do_planner(cond.subQuery->query, context);
    // }

    // // Scan table , 生成表算子列表tab_nodes
    std::vector<std::shared_ptr<Plan>> table_scan_executors(tables.size());
    for (size_t i = 0; i < tables.size(); ++i)
    {
        auto curr_conds = pop_conds(query->conds, tables[i]);
        // int index_no = get_indexNo(tables[i], curr_conds);
        auto [index_meta, max_match_col_count] = get_index_cols(tables[i], curr_conds);
        if (index_meta == nullptr)
        { // 该表没有索引
            table_scan_executors[i] =
                std::make_shared<ScanPlan>(T_SeqScan, sm_manager_, tables[i], curr_conds);
        }
        else
        { // 存在索引
            table_scan_executors[i] =
                std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, tables[i], curr_conds, *index_meta, max_match_col_count);
        }
    }
    // 只有一个表，不需要join。
    if (tables.size() == 1)
    {
        return table_scan_executors[0];
    }
    // 获取where条件
    auto conds = std::move(query->conds);
    std::shared_ptr<Plan> table_join_executors;

    int scantbl[tables.size()];
    for (size_t i = 0; i < tables.size(); ++i)
    {
        scantbl[i] = -1;
    }
    // 假设在ast中已经添加了jointree，这里需要修改的逻辑是，先处理jointree，然后再考虑剩下的部分
    if (conds.size() >= 1)
    {
        // 有连接条件
        // 根据连接条件，生成第一层join
        std::unordered_set<std::string> joined_tables_set;
        std::transform(tables.begin(), tables.end(),
                       std::inserter(joined_tables_set, joined_tables_set.end()),
                       [](const auto &t)
                       { return t; });
        auto it = conds.begin();
        if (!conds.empty())
        {
            std::shared_ptr<Plan> left, right;
            left = pop_scan(scantbl, it->lhs_col.tab_name, joined_tables_set, table_scan_executors);
            right = pop_scan(scantbl, it->rhs_col.tab_name, joined_tables_set, table_scan_executors);
            std::vector<Condition> join_conds{std::move(*it)};
            // 建立join
            //  判断使用哪种join方式
            if (enable_nestedloop_join && enable_sortmerge_join)
            {
                // 默认nested loop join
                table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(left), std::move(right), join_conds);
            }
            else if (enable_nestedloop_join)
            {
                table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(left), std::move(right), join_conds);
            }
            else if (enable_sortmerge_join)
            {
                if (left->tag != T_IndexScan)
                {
                    left = std::make_shared<SortPlan>(T_Sort, std::move(left), join_conds[0].lhs_col, false);
                }
                if (right->tag != T_IndexScan)
                {
                    right = std::make_shared<SortPlan>(T_Sort, std::move(right), join_conds[0].rhs_col, false);
                }
                table_join_executors = std::make_shared<JoinPlan>(T_SortMerge, std::move(left), std::move(right), join_conds);
            }
            else
            {
                // error
                throw RMDBError("No join executor selected!");
            }

            // table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(left), std::move(right), join_conds);
            ++it;
        }
        // 根据连接条件，生成第2-n层join
        while (it != conds.end())
        {
            std::shared_ptr<Plan> left_need_to_join_executors = nullptr;
            std::shared_ptr<Plan> right_need_to_join_executors = nullptr;
            bool isneedreverse = false;
            if (!joined_tables_set.count(it->lhs_col.tab_name))
            {
                left_need_to_join_executors = pop_scan(scantbl, it->lhs_col.tab_name, joined_tables_set, table_scan_executors);
            }
            if (!joined_tables_set.count(it->rhs_col.tab_name))
            {
                right_need_to_join_executors = pop_scan(scantbl, it->rhs_col.tab_name, joined_tables_set, table_scan_executors);
                isneedreverse = true;
            }

            if (left_need_to_join_executors != nullptr && right_need_to_join_executors != nullptr)
            {
                std::vector<Condition> join_conds{std::move(*it)};
                std::shared_ptr<Plan> temp_join_executors = std::make_shared<JoinPlan>(T_NestLoop,
                                                                                       std::move(left_need_to_join_executors),
                                                                                       std::move(right_need_to_join_executors),
                                                                                       join_conds);
                table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(temp_join_executors),
                                                                  std::move(table_join_executors),
                                                                  std::vector<Condition>());
            }
            else if (left_need_to_join_executors != nullptr || right_need_to_join_executors != nullptr)
            {
                if (isneedreverse)
                {
                    std::swap(it->lhs_col, it->rhs_col);
                    it->op = swap_op.at(it->op);
                    left_need_to_join_executors = std::move(right_need_to_join_executors);
                }
                std::vector<Condition> join_conds{std::move(*it)};
                table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(left_need_to_join_executors),
                                                                  std::move(table_join_executors), join_conds);
            }
            else
            {
                push_conds(&(*it), table_join_executors);
            }
            ++it;
        }
    }
    else
    {
        table_join_executors = table_scan_executors[0];
        scantbl[0] = 1;
    }

    // 连接剩余表
    for (size_t i = 0; i < tables.size(); ++i)
    {
        if (scantbl[i] == -1)
        {
            table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(table_scan_executors[i]),
                                                              std::move(table_join_executors), std::vector<Condition>());
        }
    }

    return table_join_executors;
}
std::shared_ptr<Plan> Planner::generate_agg_plan(const std::shared_ptr<Query> &query, std::shared_ptr<Plan> plan)
{
    auto x = std::static_pointer_cast<ast::SelectStmt>(query->parse);
    if (!x->has_agg && !x->has_groupby)
    {
        return plan;
    }
    // 生成聚合计划
    plan = std::make_shared<AggPlan>(T_Agg, std::move(plan), query->cols, query->groupby, query->having_conds);
    return plan;
}
std::shared_ptr<Plan> Planner::generate_sort_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan)
{
    int limit = -1;
    auto x = std::static_pointer_cast<ast::SelectStmt>(query->parse);
    std::vector<std::string> &tables = query->tables;
    if (!x->has_sort || tables.size() > 1)
    {
        return plan;
    }
    std::vector<ColMeta> all_cols;
    for (auto &sel_tab_name : tables)
    {
        // 这里db_不能写成get_db(), 注意要传指针
        const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
        all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
    }
    TabCol sel_col;
    for (auto &col : all_cols)
    {
        if (col.name.compare(x->order->cols->col_name) == 0)
            sel_col = TabCol(col.tab_name, col.name);
    }
    if(x->has_limit){
        limit = x->limit;
    }
    return std::make_shared<SortPlan>(T_Sort, std::move(plan), sel_col,
                                      x->order->orderby_dir == ast::OrderBy_DESC, limit);
}

/**
 * @brief select plan 生成
 *
 * @param sel_cols select plan 选取的列
 * @param tab_names select plan 目标的表
 * @param conds select plan 选取条件
 */
std::shared_ptr<Plan> Planner::generate_select_plan(std::shared_ptr<Query> query, Context *context)
{
    // 逻辑优化
    query = logical_optimization(std::move(query), context);

    // 物理优化
    auto sel_cols = query->cols;
    std::shared_ptr<Plan> plannerRoot = physical_optimization(query, context);
    plannerRoot = std::make_shared<ProjectionPlan>(T_Projection, std::move(plannerRoot),
                                                   std::move(sel_cols));

    return plannerRoot;
}

// 生成DDL语句和DML语句的查询执行计划
std::shared_ptr<Plan> Planner::do_planner(std::shared_ptr<Query> query, Context *context)
{
    switch (query->parse->Nodetype())
    {
    case ast::TreeNodeType::CreateTable:
    {
        auto x = std::static_pointer_cast<ast::CreateTable>(query->parse);
        std::vector<ColDef> col_defs;
        col_defs.reserve(x->fields.size());
        for (auto &field : x->fields)
        {
            if (field->Nodetype() == ast::TreeNodeType::ColDef)
            {
                auto sv_col_def = std::static_pointer_cast<ast::ColDef>(field);
                ColDef col_def = {.name = std::move(sv_col_def->col_name),
                                  .type = interp_sv_type(sv_col_def->type_len->type),
                                  .len = sv_col_def->type_len->len};
                col_defs.emplace_back(std::move(col_def));
            }
            else
            {
                throw InternalError("Unexpected field type");
            }
        }
        return std::make_shared<DDLPlan>(T_CreateTable, x->tab_name, std::vector<std::string>(), col_defs);
    }
    case ast::TreeNodeType::DropTable:
    {
        auto x = std::static_pointer_cast<ast::DropTable>(query->parse);
        return std::make_shared<DDLPlan>(T_DropTable, x->tab_name, std::vector<std::string>(), std::vector<ColDef>());
    }
    case ast::TreeNodeType::CreateIndex:
    {
        auto x = std::static_pointer_cast<ast::CreateIndex>(query->parse);
        return std::make_shared<DDLPlan>(T_CreateIndex, x->tab_name, x->col_names, std::vector<ColDef>());
    }
    case ast::TreeNodeType::DropIndex:
    {
        auto x = std::static_pointer_cast<ast::DropIndex>(query->parse);
        return std::make_shared<DDLPlan>(T_DropIndex, x->tab_name, x->col_names, std::vector<ColDef>());
    }
    case ast::TreeNodeType::ShowIndex:
    {
        auto x = std::static_pointer_cast<ast::ShowIndex>(query->parse);
        return std::make_shared<DDLPlan>(T_ShowIndex, x->tab_name, std::vector<std::string>(), std::vector<ColDef>());
    }
    case ast::TreeNodeType::InsertStmt:
    {
        auto x = std::static_pointer_cast<ast::InsertStmt>(query->parse);
        return std::make_shared<DMLPlan>(T_Insert, std::shared_ptr<Plan>(), x->tab_name,
                                         query->values, std::vector<Condition>(), std::vector<SetClause>());
    }
    case ast::TreeNodeType::DeleteStmt:
    {
        auto x = std::static_pointer_cast<ast::DeleteStmt>(query->parse);
        // 生成表扫描方式
        std::shared_ptr<Plan> table_scan_executors;
        // 只有一张表，不需要进行物理优化了
        // int index_no = get_indexNo(x->tab_name, query->conds);
       auto [index_meta, max_match_col_count] = get_index_cols(x->tab_name, query->conds);

        if (index_meta == nullptr)
        { // 该表没有索引
            table_scan_executors =
                std::make_shared<ScanPlan>(T_SeqScan, sm_manager_, x->tab_name, query->conds);
        }
        else
        { // 存在索引
            table_scan_executors =
                std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, x->tab_name, query->conds, *index_meta, max_match_col_count);
        }

        return std::make_shared<DMLPlan>(T_Delete, table_scan_executors, x->tab_name,
                                         std::vector<Value>(), query->conds, std::vector<SetClause>());
    }
    case ast::TreeNodeType::UpdateStmt:
    {
        auto x = std::static_pointer_cast<ast::UpdateStmt>(query->parse);
        // 生成表扫描方式
        std::shared_ptr<Plan> table_scan_executors;
        // 只有一张表，不需要进行物理优化了
        // int index_no = get_indexNo(x->tab_name, query->conds);
        auto [index_meta, max_match_col_count] = get_index_cols(x->tab_name, query->conds);

        if (index_meta == nullptr)
        { // 该表没有索引
            table_scan_executors =
                std::make_shared<ScanPlan>(T_SeqScan, sm_manager_, x->tab_name, query->conds);
        }
        else
        { // 存在索引
            table_scan_executors =
                std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, x->tab_name, query->conds, *index_meta, max_match_col_count);
        }
        return std::make_shared<DMLPlan>(T_Update, table_scan_executors, x->tab_name,
                                         std::vector<Value>(), query->conds,
                                         query->set_clauses);
    }
    case ast::TreeNodeType::SelectStmt:
    {
        auto x = std::static_pointer_cast<ast::SelectStmt>(query->parse);
        std::shared_ptr<plannerInfo> root = std::make_shared<plannerInfo>(x);
        // 生成select语句的查询执行计划
        std::shared_ptr<Plan> projection = generate_select_plan(std::move(query), context);
        return std::make_shared<DMLPlan>(T_select, projection, std::string(), std::vector<Value>(),
                                         std::vector<Condition>(), std::vector<SetClause>());
    }
    default:
        throw InternalError("Unexpected AST root");
        break;
    }
    return nullptr;
}