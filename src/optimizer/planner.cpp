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
            std::cout << "[Debug] 处理条件: ";
            if (it->is_rhs_val)
            {
                std::cout << it->lhs_col.tab_name << "." << it->lhs_col.col_name
                          << " " << get_op_string(it->op) << " " << "值比较" << std::endl;
            }
            else
            {
                std::cout << it->lhs_col.tab_name << "." << it->lhs_col.col_name
                          << " " << get_op_string(it->op) << " "
                          << it->rhs_col.tab_name << "." << it->rhs_col.col_name << std::endl;
            }

            // 判断是否可以下推(只涉及一个表的条件可以下推)
            if (it->is_rhs_val)
            {
                std::cout << "[Debug] 单表条件，可以下推到表 " << it->lhs_col.tab_name << std::endl;
                remaining_conds.push_back(*it);
                it = all_conds.erase(it);
            }
            else if (it->lhs_col.tab_name == it->rhs_col.tab_name)
            {
                std::cout << "[Debug] 同一表的多列条件，可以下推到表 " << it->lhs_col.tab_name << std::endl;
                remaining_conds.push_back(*it);
                it = all_conds.erase(it);
            }
            else
            {
                // 检查是否是有效的连接条件
                bool is_valid_join = false;
                for (const auto &tab1 : query->tables)
                {
                    for (const auto &tab2 : query->tables)
                    {
                        if (tab1 != tab2 &&
                            ((it->lhs_col.tab_name == tab1 && it->rhs_col.tab_name == tab2) ||
                             (it->lhs_col.tab_name == tab2 && it->rhs_col.tab_name == tab1)))
                        {
                            is_valid_join = true;
                            break;
                        }
                    }
                    if (is_valid_join)
                        break;
                }

                if (is_valid_join)
                {
                    std::cout << "[Debug] 多表连接条件: "
                              << it->lhs_col.tab_name << " 和 "
                              << it->rhs_col.tab_name << std::endl;
                    join_conds.push_back(*it);
                }
                else
                {
                    std::cout << "[Debug] 无效的连接条件，跳过" << std::endl;
                }
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

bool Planner::is_select_star_query(const std::shared_ptr<ast::SelectStmt> &select_stmt)
{
    if (!select_stmt)
        return false;

    // 情况1：cols 向量为空（隐式的 SELECT *）
    if (select_stmt->cols.empty())
        return true;

    // 情况2：只有一个列且列名为 "*"
    if (select_stmt->cols.size() == 1 && select_stmt->cols[0]->col_name == "*")
        return true;

    return false;
}

// 修复后的 compute_required_columns 函数
std::vector<TabCol> Planner::compute_required_columns(const std::string &table_name, const std::shared_ptr<Query> &query)
{
    std::set<std::string> required_cols;

    // 建立别名映射
    std::map<std::string, std::string> tab_to_alias;
    std::map<std::string, std::string> alias_to_tab;
    auto select_stmt = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);
    if (select_stmt)
    {
        for (size_t i = 0; i < select_stmt->tabs.size(); i++)
        {
            if (i < select_stmt->tab_aliases.size() && !select_stmt->tab_aliases[i].empty())
            {
                tab_to_alias[select_stmt->tabs[i]] = select_stmt->tab_aliases[i];
                alias_to_tab[select_stmt->tab_aliases[i]] = select_stmt->tabs[i];
            }
        }
    }

    // 1. 添加 SELECT 子句中的列
    for (const auto &col : query->cols)
    {
        if (table_matches(col.tab_name, table_name, alias_to_tab, tab_to_alias) && col.col_name != "*")
        {
            required_cols.insert(col.col_name);
        }
    }

    // 2. 添加连接条件中的列
    for (const auto &cond : query->conds)
    {
        if (!cond.is_rhs_val)
        {
            if (table_matches(cond.lhs_col.tab_name, table_name, alias_to_tab, tab_to_alias))
            {
                required_cols.insert(cond.lhs_col.col_name);
            }
            if (table_matches(cond.rhs_col.tab_name, table_name, alias_to_tab, tab_to_alias))
            {
                required_cols.insert(cond.rhs_col.col_name);
            }
        }
    }

    // 3. 添加 WHERE 条件中的列
    if (query->tab_conds.find(table_name) != query->tab_conds.end())
    {
        for (const auto &cond : query->tab_conds.at(table_name))
        {
            if (table_matches(cond.lhs_col.tab_name, table_name, alias_to_tab, tab_to_alias))
            {
                required_cols.insert(cond.lhs_col.col_name);
            }
        }
    }

    // 转换为 TabCol 格式
    std::vector<TabCol> result;
    for (const auto &col_name : required_cols)
    {
        result.push_back({table_name, col_name});
    }

    return result;
}

std::shared_ptr<Plan> Planner::add_leaf_projections(std::shared_ptr<Plan> plan, const std::shared_ptr<Query> &query)
{
    if (!plan)
        return nullptr;

    auto select_stmt = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);
    if (select_stmt && is_select_star_query(select_stmt))
    {
        // 对于 SELECT * 查询，不添加任何中间投影节点
        switch (plan->tag)
        {
        case PlanTag::T_NestLoop:
        {
            auto join_plan = std::dynamic_pointer_cast<JoinPlan>(plan);
            join_plan->left_ = add_leaf_projections(join_plan->left_, query);
            join_plan->right_ = add_leaf_projections(join_plan->right_, query);
            return plan;
        }
        default:
            return plan;
        }
    }

    // 对于非 SELECT * 查询，添加必要的投影节点
    switch (plan->tag)
    {
    case PlanTag::T_SeqScan:
    case PlanTag::T_IndexScan:
    {
        auto scan_plan = std::dynamic_pointer_cast<ScanPlan>(plan);

        // 计算该表需要的列集合
        std::vector<TabCol> required_cols = compute_required_columns(scan_plan->tab_name_, query);

        if (!required_cols.empty())
        {
            return std::make_shared<ProjectionPlan>(PlanTag::T_Projection, plan, required_cols);
        }
        return plan;
    }
    case PlanTag::T_NestLoop:
    {
        auto join_plan = std::dynamic_pointer_cast<JoinPlan>(plan);
        join_plan->left_ = add_leaf_projections(join_plan->left_, query);
        join_plan->right_ = add_leaf_projections(join_plan->right_, query);
        return plan;
    }
    default:
        return plan;
    }
}

std::shared_ptr<Plan> Planner::apply_projection_pushdown(std::shared_ptr<Plan> plan, const std::shared_ptr<Query> &query)
{
    auto select_stmt = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);

    // 对于 SELECT * 查询，我们只在最顶层添加投影节点
    if (select_stmt && is_select_star_query(select_stmt))
    {
        return plan;
    }

    // 对于非 SELECT * 查询，在叶子节点添加投影
    return add_leaf_projections(plan, query);
}

std::shared_ptr<Plan> Planner::physical_optimization(std::shared_ptr<Query> query, Context *context)
{
    std::shared_ptr<Plan> plan = make_one_rel(query, context);

    // 应用投影下推优化
    plan = apply_projection_pushdown(plan, query);

    // 处理聚合函数
    plan = generate_agg_plan(query, std::move(plan));

    // 处理orderby
    plan = generate_sort_plan(query, std::move(plan));

    // **关键修改：添加最终的投影节点**
    auto select_stmt = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);
    if (select_stmt)
    {
        if (is_select_star_query(select_stmt))
        {
            // **修改：对于 SELECT * 情况，创建一个包含 "*" 标记的投影节点**
            std::cout << "[Debug] 添加 SELECT * 投影节点" << std::endl;

            // 创建一个特殊的 TabCol 来表示 SELECT *
            std::vector<TabCol> star_cols;
            star_cols.push_back(TabCol{"", "*"}); // 使用空表名和 "*" 列名来表示 SELECT *

            plan = std::make_shared<ProjectionPlan>(PlanTag::T_Projection, plan, star_cols);
        }
        else
        {
            // SELECT 具体列情况：使用查询中指定的列
            std::cout << "[Debug] 添加具体列投影节点" << std::endl;
            plan = std::make_shared<ProjectionPlan>(PlanTag::T_Projection, plan, query->cols);
        }
    }

    return plan;
}

std::shared_ptr<Plan> Planner::make_one_rel(std::shared_ptr<Query> query, Context *context)
{
    std::cout << "[Debug] 开始生成物理计划..." << std::endl;

    // 建立表名到别名的映射
    std::map<std::string, std::string> tab_to_alias;
    std::map<std::string, std::string> alias_to_tab;
    auto select_stmt = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);
    if (select_stmt)
    {
        for (size_t i = 0; i < select_stmt->tabs.size(); i++)
        {
            if (i < select_stmt->tab_aliases.size() && !select_stmt->tab_aliases[i].empty())
            {
                tab_to_alias[select_stmt->tabs[i]] = select_stmt->tab_aliases[i];
                alias_to_tab[select_stmt->tab_aliases[i]] = select_stmt->tabs[i];
            }
        }
    }

    // 收集每个表需要的列
    std::map<std::string, std::set<std::string>> required_cols;

    // 1. 收集最终查询需要的列
    for (const auto &col : query->cols)
    {
        if (col.col_name != "*")
        {
            std::string real_tab_name = col.tab_name;
            if (alias_to_tab.find(col.tab_name) != alias_to_tab.end())
            {
                real_tab_name = alias_to_tab[col.tab_name];
            }
            required_cols[real_tab_name].insert(col.col_name);
        }
    }

    // 2. 收集连接条件中用到的列
    for (const auto &cond : query->conds)
    {
        if (!cond.is_rhs_val)
        {
            std::string lhs_real_tab = cond.lhs_col.tab_name;
            if (alias_to_tab.find(cond.lhs_col.tab_name) != alias_to_tab.end())
            {
                lhs_real_tab = alias_to_tab[cond.lhs_col.tab_name];
            }
            required_cols[lhs_real_tab].insert(cond.lhs_col.col_name);

            std::string rhs_real_tab = cond.rhs_col.tab_name;
            if (alias_to_tab.find(cond.rhs_col.tab_name) != alias_to_tab.end())
            {
                rhs_real_tab = alias_to_tab[cond.rhs_col.tab_name];
            }
            required_cols[rhs_real_tab].insert(cond.rhs_col.col_name);
        }
    }

    // 为每个表创建基础扫描计划
    std::vector<std::shared_ptr<Plan>> table_scan_executors;
    std::map<std::string, std::shared_ptr<Plan>> table_plans;

    for (const auto &table : query->tables)
    {
        auto [index_meta, max_match_col_count] = get_index_cols(table, query->tab_conds[table]);
        std::shared_ptr<Plan> scan_plan;

        if (index_meta == nullptr)
        {
            scan_plan = std::make_shared<ScanPlan>(T_SeqScan, sm_manager_, table, std::vector<Condition>());
        }
        else
        {
            scan_plan = std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, table, std::vector<Condition>(), *index_meta, max_match_col_count);
        }

        // 如果有过滤条件，创建FilterPlan
        if (!query->tab_conds[table].empty())
        {
            scan_plan = std::make_shared<FilterPlan>(
                PlanTag::T_Filter,
                scan_plan,
                query->tab_conds[table]);
        }

        table_plans[table] = scan_plan;
    }

    // 如果只有一个表，直接返回其扫描计划
    if (query->tables.size() == 1)
    {
        return table_plans[query->tables[0]];
    }

    // 处理多表连接
    auto plan = table_plans[query->tables[0]];
    for (size_t i = 1; i < query->tables.size(); i++)
    {
        std::vector<Condition> join_conds;
        for (const auto &cond : query->conds)
        {
            if (!cond.is_rhs_val &&
                ((cond.lhs_col.tab_name == query->tables[i - 1] && cond.rhs_col.tab_name == query->tables[i]) ||
                 (cond.lhs_col.tab_name == query->tables[i] && cond.rhs_col.tab_name == query->tables[i - 1])))
            {
                join_conds.push_back(cond);
            }
        }

        plan = std::make_shared<JoinPlan>(
            enable_nestedloop_join ? T_NestLoop : T_SortMerge,
            plan,
            table_plans[query->tables[i]],
            join_conds);
    }

    return plan;
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
            // 使用列的原始表名或别名
            if (col->tab_name.empty())
            {
                // 如果列没有指定表名，使用第一个表名（这可能不是正确的做法）
                tab_col.tab_name = query->tables[0];
            }
            else
            {
                // 使用列的原始表名
                tab_col.tab_name = col->tab_name;
            }
            tab_col.col_name = col->col_name;
            query->cols.push_back(tab_col);
        }
    }

    // 确保query对象包含条件信息
    if (query->conds.empty())
    {
        std::cout << "[Debug] 处理SELECT语句中的条件" << std::endl;

        // 首先建立表名到别名的映射
        std::map<std::string, std::string> tab_aliases;
        std::map<std::string, std::string> alias_to_table; // 别名到表名的映射
        for (size_t i = 0; i < select_stmt->tabs.size(); ++i)
        {
            if (i < select_stmt->tab_aliases.size() && !select_stmt->tab_aliases[i].empty())
            {
                std::cout << "[Debug] 表别名映射: " << select_stmt->tabs[i]
                          << " -> " << select_stmt->tab_aliases[i] << std::endl;
                tab_aliases[select_stmt->tab_aliases[i]] = select_stmt->tabs[i];
                alias_to_table[select_stmt->tab_aliases[i]] = select_stmt->tabs[i];
            }
        }

        // 处理 JOIN 条件
        std::cout << "[Debug] 处理 JOIN 条件" << std::endl;
        for (const auto &join_expr : select_stmt->jointree)
        {
            std::cout << "[Debug] 处理 JOIN 表达式: " << join_expr->left << " JOIN " << join_expr->right << std::endl;

            // 处理 JOIN 表达式中的所有条件
            for (const auto &join_cond : join_expr->conds)
            {
                auto lhs_col = std::dynamic_pointer_cast<ast::Col>(join_cond->lhs);
                auto rhs_col = std::dynamic_pointer_cast<ast::Col>(join_cond->rhs);

                if (!lhs_col || !rhs_col)
                {
                    throw RMDBError("JOIN condition must be between two columns");
                }

                std::cout << "[Debug] 处理 JOIN 条件: " << lhs_col->col_name << std::endl;

                Condition condition;

                // 处理左侧列的表名
                if (lhs_col->tab_name.empty())
                {
                    // 如果列没有指定表名，使用 JOIN 表达式的左表名或其别名
                    condition.lhs_col.tab_name = join_expr->left_alias.empty() ? join_expr->left : join_expr->left_alias;
                }
                else
                {
                    // 如果使用了别名，查找实际的表名
                    auto it = alias_to_table.find(lhs_col->tab_name);
                    if (it != alias_to_table.end())
                    {
                        condition.lhs_col.tab_name = it->second;
                    }
                    else
                    {
                        condition.lhs_col.tab_name = lhs_col->tab_name;
                    }
                }
                condition.lhs_col.col_name = lhs_col->col_name;

                // JOIN 条件总是等值连接
                condition.op = CompOp::OP_EQ;
                condition.is_rhs_val = false;

                // 处理右侧列的表名
                if (rhs_col->tab_name.empty())
                {
                    // 如果列没有指定表名，使用 JOIN 表达式的右表名或其别名
                    condition.rhs_col.tab_name = join_expr->right_alias.empty() ? join_expr->right : join_expr->right_alias;
                }
                else
                {
                    auto it = alias_to_table.find(rhs_col->tab_name);
                    if (it != alias_to_table.end())
                    {
                        condition.rhs_col.tab_name = it->second;
                    }
                    else
                    {
                        condition.rhs_col.tab_name = rhs_col->tab_name;
                    }
                }
                condition.rhs_col.col_name = rhs_col->col_name;

                std::cout << "[Debug] 添加 JOIN 条件: "
                          << condition.lhs_col.tab_name << "." << condition.lhs_col.col_name
                          << " = "
                          << condition.rhs_col.tab_name << "." << condition.rhs_col.col_name
                          << std::endl;

                query->conds.push_back(condition);
            }
        }

        // 处理 WHERE 条件
        std::cout << "[Debug] 处理 WHERE 条件" << std::endl;
        for (const auto &cond : select_stmt->conds)
        {
            std::cout << "[Debug] 处理 WHERE 条件: " << cond->lhs->col_name << std::endl;

            Condition condition;

            // 处理左侧列的表名
            if (cond->lhs->tab_name.empty())
            {
                condition.lhs_col.tab_name = query->tables[0];
            }
            else
            {
                // 如果使用了别名，查找实际的表名
                auto it = alias_to_table.find(cond->lhs->tab_name);
                if (it != alias_to_table.end())
                {
                    condition.lhs_col.tab_name = it->second;
                }
                else
                {
                    condition.lhs_col.tab_name = cond->lhs->tab_name;
                }
            }
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
                // 处理右侧列的表名
                if (col->tab_name.empty())
                {
                    condition.rhs_col.tab_name = query->tables[0];
                }
                else
                {
                    auto it = alias_to_table.find(col->tab_name);
                    if (it != alias_to_table.end())
                    {
                        condition.rhs_col.tab_name = it->second;
                    }
                    else
                    {
                        condition.rhs_col.tab_name = col->tab_name;
                    }
                }
                condition.rhs_col.col_name = col->col_name;

                std::cout << "[Debug] WHERE 条件: "
                          << condition.lhs_col.tab_name << "." << condition.lhs_col.col_name
                          << " " << get_op_string(condition.op) << " "
                          << condition.rhs_col.tab_name << "." << condition.rhs_col.col_name
                          << std::endl;
            }
            else if (auto int_lit = std::dynamic_pointer_cast<ast::IntLit>(cond->rhs))
            {
                condition.is_rhs_val = true;
                condition.rhs_val.type = TYPE_INT;
                condition.rhs_val.int_val = int_lit->val;
                std::cout << "[Debug] WHERE 条件: "
                          << condition.lhs_col.tab_name << "." << condition.lhs_col.col_name
                          << " " << get_op_string(condition.op) << " "
                          << int_lit->val << " (INT)" << std::endl;
            }
            else if (auto float_lit = std::dynamic_pointer_cast<ast::FloatLit>(cond->rhs))
            {
                condition.is_rhs_val = true;
                condition.rhs_val.type = TYPE_FLOAT;
                condition.rhs_val.float_val = float_lit->val;
                std::cout << "[Debug] WHERE 条件: "
                          << condition.lhs_col.tab_name << "." << condition.lhs_col.col_name
                          << " " << get_op_string(condition.op) << " "
                          << float_lit->val << " (FLOAT)" << std::endl;
            }
            else if (auto str_lit = std::dynamic_pointer_cast<ast::StringLit>(cond->rhs))
            {
                condition.is_rhs_val = true;
                condition.rhs_val.type = TYPE_STRING;
                condition.rhs_val.str_val = str_lit->val;
                std::cout << "[Debug] WHERE 条件: "
                          << condition.lhs_col.tab_name << "." << condition.lhs_col.col_name
                          << " " << get_op_string(condition.op) << " "
                          << str_lit->val << " (STRING)" << std::endl;
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

    // 物理优化 - 这里已经包含了必要的Project节点
    std::shared_ptr<Plan> plannerRoot = physical_optimization(query, context);

    // 检查是否成功生成了计划
    if (!plannerRoot)
    {
        throw RMDBError("Failed to generate physical plan");
    }

    // 不再需要添加额外的Project节点，因为physical_optimization已经处理了
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