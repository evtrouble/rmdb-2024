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
#include <iostream>

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
std::pair<IndexMeta *, int> Planner::get_index_cols(const std::string &tab_name, const std::vector<Condition> &curr_conds)
{
    // 获取表格对象
    auto &tab = sm_manager_->db_.get_table(tab_name);
    // 如果表格没有索引，返回nullptr和-1
    if (tab.indexes.empty())
        return std::make_pair(nullptr, -1);

    // 用于存储条件列的集合
    std::unordered_map<std::string, bool> conds_cols_;
    // 遍历当前条件
    for (const auto &cond : curr_conds)
    {
        // 如果条件是列与值比较，并且操作符不是不等于，并且列属于当前表格
        if (cond.is_rhs_val /* && cond.lhs_col.tab_name == tab_name*/ && cond.op != CompOp::OP_NE)
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
               (cond.lhs_col.tab_name.compare(cond.rhs_col.tab_name) == 0) /* || (tab_names == cond.lhs_col.tab_name && cond.is_subquery)*/;
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
    if (auto select = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse))
    {
        std::cout << "[Debug] 开始逻辑优化..." << std::endl;

        // 1. 收集所有选择条件
        std::vector<Condition> &all_conds = query->conds;
        std::cout << "[Debug] 总条件数: " << all_conds.size() << std::endl;

        std::vector<Condition> remaining_conds;
        std::vector<Condition> join_conds;

        // 2. 对每个选择条件进行分类和下推
        for (auto it = all_conds.begin(); it != all_conds.end();)
        {
            std::cout << "[Debug] 处理条件: " << it->lhs_col.tab_name << "."
                      << it->lhs_col.col_name;
            if (!it->is_rhs_val)
            {
                std::cout << " = " << it->rhs_col.tab_name << "."
                          << it->rhs_col.col_name << std::endl;
            }
            else
            {
                std::cout << " (与值比较)" << std::endl;
            }

            // 判断是否可以下推(只涉及一个表的条件可以下推)
            if (it->is_rhs_val)
            {
                std::cout << "[Debug] 单表条件，可以下推" << std::endl;
                remaining_conds.push_back(*it);
                it = all_conds.erase(it);
            }
            else if (it->lhs_col.tab_name == it->rhs_col.tab_name)
            {
                std::cout << "[Debug] 同一表的多列条件，可以下推" << std::endl;
                remaining_conds.push_back(*it);
                it = all_conds.erase(it);
            }
            else
            {
                std::cout << "[Debug] 多表连接条件，保留在join节点" << std::endl;
                join_conds.push_back(*it);
                it = all_conds.erase(it);
            }
        }

        std::cout << "[Debug] 分类结果:" << std::endl;
        std::cout << "  - 连接条件数: " << join_conds.size() << std::endl;
        std::cout << "  - 可下推条件数: " << remaining_conds.size() << std::endl;

        // 3. 按表分组条件
        std::map<std::string, std::vector<Condition>> table_conds;
        for (const auto &cond : remaining_conds)
        {
            std::cout << "[Debug] 将条件分配给表 " << cond.lhs_col.tab_name << std::endl;
            table_conds[cond.lhs_col.tab_name].push_back(cond);
        }

        // 4. 将分组后的条件存储回query对象
        query->conds = join_conds;      // 保存连接条件
        query->tab_conds = table_conds; // 保存下推的单表条件

        std::cout << "[Debug] 逻辑优化完成" << std::endl;
        std::cout << "  - 最终连接条件数: " << query->conds.size() << std::endl;
        std::cout << "  - 最终表条件数: " << table_conds.size() << std::endl;

        // 5. 如果是单表查询，将所有条件放回conds中
        if (query->tables.size() == 1)
        {
            std::cout << "[Debug] 单表查询，所有条件放回conds" << std::endl;
            query->conds = remaining_conds;
        }
    }
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
    std::cout << "[Debug] 开始生成物理计划..." << std::endl;

    // 存储每个表的扫描计划
    std::vector<std::shared_ptr<Plan>> table_scan_executors;
    // 存储每个表的大小（用于连接顺序优化）
    std::vector<std::pair<std::string, size_t>> table_sizes;

    // 检查是否有表需要处理
    if (query->tables.empty())
    {
        throw RMDBError("No tables in query");
    }

    std::cout << "[Debug] 处理的表数量: " << query->tables.size() << std::endl;

    // 为每个表创建扫描计划
    for (const auto &table_name : query->tables)
    {
        std::cout << "[Debug] 处理表: " << table_name << std::endl;

        // 获取表的条件
        std::vector<Condition> table_conditions;
        if (query->tab_conds.find(table_name) != query->tab_conds.end())
        {
            table_conditions = query->tab_conds[table_name];
        }
        std::cout << "[Debug] 表 " << table_name << " 的条件数: "
                  << table_conditions.size() << std::endl;

        // 获取表的大小
        size_t table_size = get_table_cardinality(table_name);
        table_sizes.emplace_back(table_name, table_size);
        std::cout << "[Debug] 表 " << table_name << " 的大小: " << table_size << std::endl;

        // 检查是否可以使用索引
        auto [index_meta, max_match_col_count] = get_index_cols(table_name, table_conditions);
        std::shared_ptr<Plan> scan_plan;

        if (index_meta != nullptr)
        {
            std::cout << "[Debug] 使用索引扫描" << std::endl;
            scan_plan = std::make_shared<ScanPlan>(PlanTag::T_IndexScan,
                                                   sm_manager_,
                                                   table_name,
                                                   table_conditions,
                                                   *index_meta,
                                                   max_match_col_count);
        }
        else
        {
            std::cout << "[Debug] 使用顺序扫描" << std::endl;
            scan_plan = std::make_shared<ScanPlan>(PlanTag::T_SeqScan,
                                                   sm_manager_,
                                                   table_name,
                                                   table_conditions);
        }

        table_scan_executors.push_back(scan_plan);
    }

    // 如果只有一个表，直接返回扫描计划
    if (table_scan_executors.size() == 1)
    {
        std::cout << "[Debug] 单表查询，直接返回扫描计划" << std::endl;
        return table_scan_executors[0];
    }

    std::cout << "[Debug] 开始处理多表连接..." << std::endl;
    std::cout << "[Debug] 连接条件数: " << query->conds.size() << std::endl;

    // 按表大小排序
    std::sort(table_sizes.begin(), table_sizes.end(),
              [](const auto &a, const auto &b)
              { return a.second < b.second; });

    // 从最小的两个表开始连接
    std::shared_ptr<Plan> join_plan;
    std::vector<Condition> join_conds;

    // 找到第一个表的索引
    size_t curr_idx = 0;
    for (size_t i = 0; i < table_scan_executors.size(); i++)
    {
        auto scan = std::static_pointer_cast<ScanPlan>(table_scan_executors[i]);
        if (scan->tab_name_ == table_sizes[0].first)
        {
            curr_idx = i;
            break;
        }
    }

    // 找到第二个表的索引
    size_t next_idx = 0;
    for (size_t i = 0; i < table_scan_executors.size(); i++)
    {
        auto scan = std::static_pointer_cast<ScanPlan>(table_scan_executors[i]);
        if (scan->tab_name_ == table_sizes[1].first)
        {
            next_idx = i;
            break;
        }
    }

    std::cout << "[Debug] 初始连接: " << table_sizes[0].first
              << " 和 " << table_sizes[1].first << std::endl;

    // 收集这两个表之间的连接条件
    for (const auto &cond : query->conds)
    {
        if (!cond.is_rhs_val)
        {
            if ((cond.lhs_col.tab_name == table_sizes[0].first &&
                 cond.rhs_col.tab_name == table_sizes[1].first) ||
                (cond.lhs_col.tab_name == table_sizes[1].first &&
                 cond.rhs_col.tab_name == table_sizes[0].first))
            {
                std::cout << "[Debug] 添加连接条件: " << cond.lhs_col.tab_name
                          << "." << cond.lhs_col.col_name << " = "
                          << cond.rhs_col.tab_name << "."
                          << cond.rhs_col.col_name << std::endl;
                join_conds.push_back(cond);
            }
        }
    }

    // 创建第一个连接计划
    join_plan = std::make_shared<JoinPlan>(PlanTag::T_NestLoop,
                                           table_scan_executors[curr_idx],
                                           table_scan_executors[next_idx],
                                           join_conds);

    // 依次加入剩余的表
    for (size_t i = 2; i < table_sizes.size(); i++)
    {
        std::cout << "[Debug] 添加表: " << table_sizes[i].first << std::endl;

        // 找到下一个要连接的表
        for (size_t j = 0; j < table_scan_executors.size(); j++)
        {
            auto scan = std::static_pointer_cast<ScanPlan>(table_scan_executors[j]);
            if (scan->tab_name_ == table_sizes[i].first)
            {
                curr_idx = j;
                break;
            }
        }

        // 收集与新表相关的连接条件
        join_conds.clear();
        for (const auto &cond : query->conds)
        {
            if (!cond.is_rhs_val)
            {
                bool is_join_cond = false;
                for (size_t j = 0; j < i; j++)
                {
                    if ((cond.lhs_col.tab_name == table_sizes[j].first &&
                         cond.rhs_col.tab_name == table_sizes[i].first) ||
                        (cond.lhs_col.tab_name == table_sizes[i].first &&
                         cond.rhs_col.tab_name == table_sizes[j].first))
                    {
                        is_join_cond = true;
                        break;
                    }
                }
                if (is_join_cond)
                {
                    std::cout << "[Debug] 添加连接条件: " << cond.lhs_col.tab_name
                              << "." << cond.lhs_col.col_name << " = "
                              << cond.rhs_col.tab_name << "."
                              << cond.rhs_col.col_name << std::endl;
                    join_conds.push_back(cond);
                }
            }
        }

        // 创建新的连接计划
        join_plan = std::make_shared<JoinPlan>(PlanTag::T_NestLoop,
                                               join_plan,
                                               table_scan_executors[curr_idx],
                                               join_conds);
    }

    std::cout << "[Debug] 物理计划生成完成" << std::endl;
    return join_plan;
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
    // 准备多列排序参数
    std::vector<TabCol> sort_cols;
    std::vector<bool> is_desc_orders; // 每列对应的排序方向

    // 遍历所有排序列及其排序方向
    for (size_t i = 0; i < x->order->cols.size(); ++i)
    {
        auto &order_col = x->order->cols[i];
        auto &order_dir = x->order->dirs[i];

        // 查找匹配的列
        bool found = false;
        for (auto &col : all_cols)
        {
            if (col.name.compare(order_col->col_name) == 0)
            {
                sort_cols.emplace_back(col.tab_name, col.name);
                is_desc_orders.emplace_back(order_dir == ast::OrderByDir::OrderBy_DESC);
                found = true;
                break;
            }
        }

        if (!found)
        {
            throw RMDBError("Sort column not found: " + order_col->col_name);
        }
    }
    if (x->has_limit)
    {
        limit = x->limit;
    }
    // 创建排序计划
    return std::make_shared<SortPlan>(T_Sort, std::move(plan), sort_cols, is_desc_orders, limit);
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
    // 检查Query对象的有效性
    if (!query || !query->parse)
    {
        throw RMDBError("Invalid query object");
    }

    auto select_stmt = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);
    if (!select_stmt)
    {
        throw RMDBError("Not a SELECT statement");
    }

    // 确保query对象包含表信息
    if (query->tables.empty())
    {
        // 从SELECT语句中获取表信息
        query->tables.clear();
        for (const auto &tab_name : select_stmt->tabs)
        {
            query->tables.push_back(tab_name);
        }
    }

    // 确保query对象包含列信息
    if (query->cols.empty() && !select_stmt->cols.empty())
    {
        for (const auto &col : select_stmt->cols)
        {
            TabCol tab_col;
            tab_col.tab_name = query->tables[0]; // 假设只有一个表
            tab_col.col_name = col->col_name;
            query->cols.push_back(tab_col);
        }
    }

    // 确保query对象包含条件信息
    if (query->conds.empty() && !select_stmt->conds.empty())
    {
        for (const auto &cond : select_stmt->conds)
        {
            Condition condition;
            condition.lhs_col.tab_name = query->tables[0];
            condition.lhs_col.col_name = cond->lhs->col_name;

            switch (cond->op)
            {
            case ast::SvCompOp::SV_OP_EQ:
                condition.op = CompOp::OP_EQ;
                break;
            case ast::SvCompOp::SV_OP_NE:
                condition.op = CompOp::OP_NE;
                break;
            case ast::SvCompOp::SV_OP_LT:
                condition.op = CompOp::OP_LT;
                break;
            case ast::SvCompOp::SV_OP_GT:
                condition.op = CompOp::OP_GT;
                break;
            case ast::SvCompOp::SV_OP_LE:
                condition.op = CompOp::OP_LE;
                break;
            case ast::SvCompOp::SV_OP_GE:
                condition.op = CompOp::OP_GE;
                break;
            default:
                throw InternalError("Unsupported operator type");
            }

            if (auto col = std::dynamic_pointer_cast<ast::Col>(cond->rhs))
            {
                condition.is_rhs_val = false;
                condition.rhs_col.tab_name = query->tables[0];
                condition.rhs_col.col_name = col->col_name;
            }
            else if (auto int_lit = std::dynamic_pointer_cast<ast::IntLit>(cond->rhs))
            {
                condition.is_rhs_val = true;
                condition.rhs_val.type = TYPE_INT;
                condition.rhs_val.int_val = int_lit->val;
            }
            else if (auto float_lit = std::dynamic_pointer_cast<ast::FloatLit>(cond->rhs))
            {
                condition.is_rhs_val = true;
                condition.rhs_val.type = TYPE_FLOAT;
                condition.rhs_val.float_val = float_lit->val;
            }
            else if (auto str_lit = std::dynamic_pointer_cast<ast::StringLit>(cond->rhs))
            {
                condition.is_rhs_val = true;
                condition.rhs_val.type = TYPE_STRING;
                condition.rhs_val.str_val = str_lit->val;
            }
            else
            {
                throw InternalError("Unsupported value type");
            }
            query->conds.push_back(condition);
        }
    }

    // 逻辑优化
    query = logical_optimization(std::move(query), context);

    // 物理优化
    auto sel_cols = query->cols;
    std::shared_ptr<Plan> plannerRoot = physical_optimization(query, context);

    // 检查是否成功生成了计划
    if (!plannerRoot)
    {
        throw RMDBError("Failed to generate physical plan");
    }

    plannerRoot = std::make_shared<ProjectionPlan>(T_Projection, std::move(plannerRoot),
                                                   std::move(sel_cols));

    return plannerRoot;
}

// 生成DDL语句和DML语句的查询执行计划
std::shared_ptr<Plan> Planner::do_planner(std::shared_ptr<Query> query, Context *context)
{
    switch (query->parse->Nodetype())
    {
    case ast::TreeNodeType::ExplainStmt:
    {
        auto explain_stmt = std::static_pointer_cast<ast::ExplainStmt>(query->parse);
        std::cout << "[Debug] Processing EXPLAIN statement" << std::endl;

        // 递归处理内部的查询
        auto inner_query = std::make_shared<Query>();

        // 从原始SELECT语句中获取列和表信息
        if (auto select_stmt = std::dynamic_pointer_cast<ast::SelectStmt>(explain_stmt->query))
        {
            std::cout << "[Debug] Found SELECT statement" << std::endl;
            std::cout << "[Debug] Number of conditions in select_stmt: " << select_stmt->conds.size() << std::endl;

            // 设置解析树
            inner_query->parse = explain_stmt->query;

            // 设置表信息
            inner_query->tables = select_stmt->tabs;
            std::cout << "[Debug] Tables: " << inner_query->tables.size() << std::endl;

            // 设置列信息
            for (const auto &col : select_stmt->cols)
            {
                std::cout << "[Debug] Adding column: " << col->col_name << std::endl;
                TabCol tab_col;
                tab_col.tab_name = inner_query->tables[0]; // 假设只有一个表
                tab_col.col_name = col->col_name;
                inner_query->cols.push_back(tab_col);
            }

            // 设置条件信息
            std::cout << "[Debug] Processing conditions from select statement" << std::endl;
            for (const auto &cond : select_stmt->conds)
            {
                std::cout << "[Debug] Found condition in select statement" << std::endl;
                std::cout << "[Debug] LHS column: " << cond->lhs->col_name << std::endl;

                Condition condition;
                condition.lhs_col.tab_name = inner_query->tables[0]; // 假设只有一个表
                condition.lhs_col.col_name = cond->lhs->col_name;

                // 转换操作符
                switch (cond->op)
                {
                case ast::SvCompOp::SV_OP_EQ:
                    condition.op = CompOp::OP_EQ;
                    std::cout << "[Debug] Operator: EQ" << std::endl;
                    break;
                case ast::SvCompOp::SV_OP_NE:
                    condition.op = CompOp::OP_NE;
                    std::cout << "[Debug] Operator: NE" << std::endl;
                    break;
                case ast::SvCompOp::SV_OP_LT:
                    condition.op = CompOp::OP_LT;
                    std::cout << "[Debug] Operator: LT" << std::endl;
                    break;
                case ast::SvCompOp::SV_OP_GT:
                    condition.op = CompOp::OP_GT;
                    std::cout << "[Debug] Operator: GT" << std::endl;
                    break;
                case ast::SvCompOp::SV_OP_LE:
                    condition.op = CompOp::OP_LE;
                    std::cout << "[Debug] Operator: LE" << std::endl;
                    break;
                case ast::SvCompOp::SV_OP_GE:
                    condition.op = CompOp::OP_GE;
                    std::cout << "[Debug] Operator: GE" << std::endl;
                    break;
                default:
                    throw InternalError("Unsupported operator type");
                }

                // 处理右侧表达式
                if (auto col = std::dynamic_pointer_cast<ast::Col>(cond->rhs))
                {
                    std::cout << "[Debug] RHS is column: " << col->col_name << std::endl;
                    condition.is_rhs_val = false;
                    condition.rhs_col.tab_name = inner_query->tables[0]; // 假设只有一个表
                    condition.rhs_col.col_name = col->col_name;
                }
                else if (auto int_lit = std::dynamic_pointer_cast<ast::IntLit>(cond->rhs))
                {
                    std::cout << "[Debug] RHS is integer: " << int_lit->val << std::endl;
                    condition.is_rhs_val = true;
                    condition.rhs_val.type = TYPE_INT;
                    condition.rhs_val.int_val = int_lit->val;
                }
                else if (auto float_lit = std::dynamic_pointer_cast<ast::FloatLit>(cond->rhs))
                {
                    std::cout << "[Debug] RHS is float: " << float_lit->val << std::endl;
                    condition.is_rhs_val = true;
                    condition.rhs_val.type = TYPE_FLOAT;
                    condition.rhs_val.float_val = float_lit->val;
                }
                else if (auto str_lit = std::dynamic_pointer_cast<ast::StringLit>(cond->rhs))
                {
                    std::cout << "[Debug] RHS is string: " << str_lit->val << std::endl;
                    condition.is_rhs_val = true;
                    condition.rhs_val.type = TYPE_STRING;
                    condition.rhs_val.str_val = str_lit->val;
                }
                else
                {
                    std::cout << "[Debug] Unknown RHS type" << std::endl;
                    throw InternalError("Unsupported value type");
                }
                inner_query->conds.push_back(condition);
                std::cout << "[Debug] Added condition to inner_query" << std::endl;
            }
        }

        std::cout << "[Debug] Creating inner plan" << std::endl;
        std::cout << "[Debug] Number of conditions in inner_query before analyze: " << inner_query->conds.size() << std::endl;

        // 创建一个新的Analyze对象来处理内部查询
        Analyze analyze(sm_manager_);
        auto analyzed_query = analyze.do_analyze(inner_query->parse);

        // 保持原始的列和条件信息
        analyzed_query->cols = inner_query->cols;
        analyzed_query->conds = inner_query->conds;
        analyzed_query->tables = inner_query->tables;

        std::cout << "[Debug] Number of conditions in analyzed_query: " << analyzed_query->conds.size() << std::endl;

        auto inner_plan = do_planner(analyzed_query, context);
        std::cout << "[Debug] Creating explain plan" << std::endl;
        return std::make_shared<ExplainPlan>(T_Explain, inner_plan);
    }
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
        auto dml_plan = std::make_shared<DMLPlan>(T_select, projection, std::string(), std::vector<Value>(),
                                                  std::vector<Condition>(), std::vector<SetClause>());
        dml_plan->parse = x; // 设置parse成员
        return dml_plan;
    }
    default:
        throw InternalError("Unexpected AST root");
        break;
    }
    return nullptr;
}