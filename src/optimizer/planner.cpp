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
#include <sstream>
#include <iomanip>

#include "execution/executor_delete.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_insert.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_update.h"
#include "index/ix.h"
#include "record_printer.h"
#include "execution/executor_explain.h"
std::unordered_map<std::string, std::string> Planner::empty_map_;

// 辅助函数：将操作符转换为字符串
std::string get_op_string(CompOp op)
{
    switch (op)
    {
    case CompOp::OP_EQ:
        return "=";
    case CompOp::OP_NE:
        return "!=";
    case CompOp::OP_LT:
        return "<";
    case CompOp::OP_GT:
        return ">";
    case CompOp::OP_LE:
        return "<=";
    case CompOp::OP_GE:
        return ">=";
    default:
        return "unknown";
    }
}

// 辅助函数：将值转换为字符串
std::string value_to_string(const Value &val)
{
    std::stringstream ss;
    switch (val.type)
    {
    case TYPE_INT:
        ss << val.int_val;
        break;
    case TYPE_FLOAT:
        ss << std::fixed << std::setprecision(2) << val.float_val;
        break;
    case TYPE_STRING:
    case TYPE_DATETIME:
        ss << "'" << val.str_val << "'";
        break;
    default:
        ss << "unknown_type";
        break;
    }
    return ss.str();
}

int get_plan_type_priority(const std::shared_ptr<Plan>& plan)
{
    switch (plan->tag)
    {
    case T_Filter:
        return 0;
    case T_NestLoop:
    case T_SortMerge:
        return 1;
    case T_Projection:
        return 2;
    case T_SeqScan:
    case T_IndexScan:
        return 3;
    default:
        return 4;
    }
}
// 获取Scan节点的表名
std::string get_scan_table_name(const std::shared_ptr<Plan>& plan)
{
    auto scan_plan = std::dynamic_pointer_cast<ScanPlan>(plan);
    if (scan_plan)
    {
        return scan_plan->tab_name_;
    }
    return "";
}
// 获取Join节点的所有表名（按字典序排序）
std::vector<std::string> get_join_table_names(const std::shared_ptr<Plan>& plan)
{
    std::set<std::string> tables_set;

    // 递归收集所有表名
    std::function<void(const std::shared_ptr<Plan>&)> collect_tables = [&](const std::shared_ptr<Plan>& p)
    {
        if (p->tag == T_SeqScan || p->tag == T_IndexScan)
        {
            auto scan_plan = std::dynamic_pointer_cast<ScanPlan>(p);
            if (scan_plan)
            {
                tables_set.insert(scan_plan->tab_name_);
            }
        }
        else if (p->tag == T_NestLoop || p->tag == T_SortMerge)
        {
            auto join_plan = std::dynamic_pointer_cast<JoinPlan>(p);
            if (join_plan)
            {
                collect_tables(join_plan->left_);
                collect_tables(join_plan->right_);
            }
        }
        else if (p->tag == T_Filter)
        {
            auto filter_plan = std::dynamic_pointer_cast<FilterPlan>(p);
            if (filter_plan)
            {
                collect_tables(filter_plan->subplan_);
            }
        }
        else if (p->tag == T_Projection)
        {
            auto proj_plan = std::dynamic_pointer_cast<ProjectionPlan>(p);
            if (proj_plan)
            {
                collect_tables(proj_plan->subplan_);
            }
        }
    };

    collect_tables(plan);
    std::vector<std::string> tables(tables_set.begin(), tables_set.end());
    return tables;
}
// 获取Filter节点的条件字符串（用于排序比较）
std::string get_filter_condition_string(const std::shared_ptr<Plan>& plan)
{
    auto filter_plan = std::dynamic_pointer_cast<FilterPlan>(plan);
    if (!filter_plan || filter_plan->conds_.empty())
    {
        return "";
    }

    // 将条件转换为字符串并排序
    std::set<std::string> cond_strs_set;
    for (const auto &cond : filter_plan->conds_)
    {
        std::string cond_str = cond.lhs_col.tab_name + "." + cond.lhs_col.col_name;
        cond_str += " " + get_op_string(cond.op) + " ";
        if (cond.is_rhs_val)
        {
            cond_str += value_to_string(cond.rhs_val);
        }
        else
        {
            cond_str += cond.rhs_col.tab_name + "." + cond.rhs_col.col_name;
        }
        cond_strs_set.insert(cond_str);
    }

    // 预分配容量优化字符串拼接
    std::string result;
    size_t total_size = 0;
    for (const auto& cond_str : cond_strs_set) {
        total_size += cond_str.size() + 1; // +1 for comma
    }
    if (total_size > 0) {
        result.reserve(total_size - 1); // -1 because last element doesn't need comma
    }

    bool first = true;
    for (const auto& cond_str : cond_strs_set)
    {
        if (!first)
            result += ",";
        result += cond_str;
        first = false;
    }
    return result;
}

// 获取Project节点的列名字符串（用于排序比较）
std::string get_project_columns_string(const std::shared_ptr<Plan>& plan)
{
    auto proj_plan = std::dynamic_pointer_cast<ProjectionPlan>(plan);
    if (!proj_plan || proj_plan->sel_cols_.empty())
    {
        return "";
    }

    std::set<std::string> col_strs_set;
    for (const auto &col : proj_plan->sel_cols_)
    {
        col_strs_set.insert(col.tab_name + "." + col.col_name);
    }

    // 预分配容量优化字符串拼接
    std::string result;
    size_t total_size = 0;
    for (const auto& col_str : col_strs_set) {
        total_size += col_str.size() + 1; // +1 for comma
    }
    if (total_size > 0) {
        result.reserve(total_size - 1); // -1 because last element doesn't need comma
    }

    bool first = true;
    for (const auto& col_str : col_strs_set)
    {
        if (!first)
            result += ",";
        result += col_str;
        first = false;
    }
    return result;
}

bool should_left_come_first(const std::shared_ptr<Plan>& left, const std::shared_ptr<Plan>& right)
{
    int left_priority = get_plan_type_priority(left);
    int right_priority = get_plan_type_priority(right);

    // 如果类型不同，按照Filter Join Project Scan的优先级排序
    if (left_priority != right_priority)
    {
        return left_priority < right_priority;
    }

    // 如果类型相同，按照具体规则排序
    switch (left->tag)
    {
    case T_SeqScan:
    case T_IndexScan:
    {
        // Scan节点按照表名升序
        std::string left_table = get_scan_table_name(left);
        std::string right_table = get_scan_table_name(right);
        return left_table < right_table;
    }

    case T_NestLoop:
    case T_SortMerge:
    {
        // Join节点按照表名集合升序
        auto left_tables = get_join_table_names(left);
        auto right_tables = get_join_table_names(right);
        return left_tables < right_tables;
    }

    case T_Filter:
    {
        // Filter节点按照条件字典序升序
        std::string left_conds = get_filter_condition_string(left);
        std::string right_conds = get_filter_condition_string(right);
        return left_conds < right_conds;
    }

    case T_Projection:
    {
        // Project节点按照列名升序
        std::string left_cols = get_project_columns_string(left);
        std::string right_cols = get_project_columns_string(right);
        return left_cols < right_cols;
    }

    default:
        return false;
    }
}
std::shared_ptr<ScanPlan> extract_scan_plan(const std::shared_ptr<Plan>& plan)
{
    if (!plan)
        return nullptr;

    // 如果是ScanPlan，直接返回
    auto scan_plan = std::dynamic_pointer_cast<ScanPlan>(plan);
    if (scan_plan)
    {
        return scan_plan;
    }

    // 如果是FilterPlan，递归提取其子计划
    auto filter_plan = std::dynamic_pointer_cast<FilterPlan>(plan);
    if (filter_plan && filter_plan->subplan_)
    {
        return extract_scan_plan(filter_plan->subplan_);
    }

    // 如果是ProjectionPlan，递归提取其子计划
    auto projection_plan = std::dynamic_pointer_cast<ProjectionPlan>(plan);
    if (projection_plan && projection_plan->subplan_)
    {
        return extract_scan_plan(projection_plan->subplan_);
    }

    return nullptr;
}
std::shared_ptr<Plan> create_ordered_join(
    PlanTag join_type,
    const std::shared_ptr<Plan>& plan1,
    const std::shared_ptr<Plan>& plan2,
    const std::vector<Condition> &join_conds)
{
    // 根据排序规则决定左右子树
    if (should_left_come_first(plan1, plan2))
    {
        return std::make_shared<JoinPlan>(join_type, plan1, plan2, join_conds);
    }
    else
    {
        return std::make_shared<JoinPlan>(join_type, plan2, plan1, join_conds);
    }
}
void QueryColumnRequirement::calculate_layered_requirements()
{
    // 1. 扫描层：需要所有涉及的列
    std::set<TabCol> all_cols;
    all_cols.insert(select_cols.begin(), select_cols.end());
    all_cols.insert(join_cols.begin(), join_cols.end());
    all_cols.insert(where_cols.begin(), where_cols.end());
    all_cols.insert(groupby_cols.begin(), groupby_cols.end());
    all_cols.insert(having_cols.begin(), having_cols.end());
    all_cols.insert(orderby_cols.begin(), orderby_cols.end());

    // 按表分组扫描层需要的列
    scan_level_cols.clear();
    for (const auto &col : all_cols)
    {
        scan_level_cols[col.tab_name].emplace(col);
    }

    // 2. 过滤后层：去掉只在WHERE中使用的列
    post_filter_cols.clear();
    for (const auto &[table_name, table_cols] : scan_level_cols)
    {
        for (const auto &col : table_cols)
        {
            if (!is_where_only_column(col))
            {
                post_filter_cols[table_name].insert(col);
            }
        }
    }

    // 3. 连接后层：当前与过滤后相同（可以后续扩展）
    // post_join_cols = post_filter_cols;

    // 4. 最终层：只保留SELECT、GROUP BY、HAVING、ORDER BY需要的列
    final_cols.clear();
    std::set<TabCol> final_needed_cols;
    final_needed_cols.insert(select_cols.begin(), select_cols.end());
    final_needed_cols.insert(groupby_cols.begin(), groupby_cols.end());
    final_needed_cols.insert(having_cols.begin(), having_cols.end());
    final_needed_cols.insert(orderby_cols.begin(), orderby_cols.end());

    for (const auto &col : final_needed_cols)
    {
        final_cols[col.tab_name].insert(col);
    }
}
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
// 目前只支持一个列的索引
std::pair<IndexMeta *, int> Planner::get_index_for_join(const std::string &tab_name, const TabCol &join_col)
{
    // 获取表格对象
    auto &tab = sm_manager_->db_.get_table(tab_name);

    // 如果表格没有索引，直接返回
    if (tab.indexes.empty())
        return std::make_pair(nullptr, -1);

    // 遍历所有索引寻找匹配
    for (size_t idx_number = 0; idx_number < tab.indexes.size(); ++idx_number)
    {
        const auto &index = tab.indexes[idx_number];

        // 检查索引的第一列是否匹配连接列
        if (!index.cols.empty() && index.cols[0].name == join_col.col_name)
        {
            // 找到匹配的索引
            return std::make_pair(&tab.indexes[idx_number], 1);
        }
    }

    // 没有找到匹配的索引
    return std::make_pair(nullptr, -1);
}

/**
 * @brief 表算子条件谓词生成
 *
 * @param conds 条件
 * @param tab_names 表名
 * @return std::vector<Condition>
 */

std::shared_ptr<Query> Planner::logical_optimization(std::shared_ptr<Query> query, Context *context)
{
    if (query->parse->Nodetype() == ast::TreeNodeType::SelectStmt)
    {
        // 1. 处理 WHERE 条件，将其分类
        auto &where_conds = query->conds;
        std::vector<Condition> where_join_conds; // WHERE 中的跨表条件
        where_join_conds.reserve(where_conds.size()); // 预分配最大可能容量

        for (auto &cond : where_conds)
        {
            // 判断是否可以下推(只涉及一个表的条件可以下推)
            if (cond.is_rhs_val || cond.lhs_col.tab_name == cond.rhs_col.tab_name)
            {
                // 单表条件，按表分组下推
                query->tab_conds[cond.lhs_col.tab_name].emplace_back(std::move(cond));
            }
            else
            {
                // 跨表条件，作为连接条件
                where_join_conds.emplace_back(std::move(cond));
            }
        }

        // 2. 遍历 jointree，提取 JOIN 条件
        std::vector<Condition> jointree_conds;
        for (const auto &join_expr : query->jointree)
        {
            // 将每个 JoinExpr 中的条件添加到 jointree_conds
            for (const auto &cond : join_expr.conds)
            {
                jointree_conds.emplace_back(cond);
            }
        }

        // 3. 合并所有连接条件到 query->join_conds
        query->join_conds.clear();
        query->join_conds.reserve(where_join_conds.size() + jointree_conds.size());

        // 添加 WHERE 中的跨表条件
        for (auto &cond : where_join_conds)
        {
            query->join_conds.emplace_back(std::move(cond));
        }

        // 添加 jointree 中的条件
        for (auto &cond : jointree_conds)
        {
            query->join_conds.emplace_back(std::move(cond));
        }

        // 4. 清空原始 WHERE 条件，因为已经分类处理了
        query->conds.clear();
    }
    return query;
}

std::shared_ptr<Plan> Planner::physical_optimization(std::shared_ptr<Query> query, Context *context)
{
    // 在顶层计算一次 QueryColumnRequirement
    QueryColumnRequirement column_requirements;
    analyze_column_requirements(query, context, column_requirements);

    // 将计算好的 column_requirements 传递给需要的函数
    std::shared_ptr<Plan> plan = make_one_rel(query, context, column_requirements);

    // 处理聚合函数
    plan = generate_agg_plan(query, std::move(plan));

    // 处理orderby
    plan = generate_sort_plan(query, std::move(plan));

    // 添加最终的投影节点
    if (query->parse->Nodetype() == ast::TreeNodeType::SelectStmt)
    {
        plan = std::make_shared<ProjectionPlan>(PlanTag::T_Projection, plan, query->cols);
    }

    return plan;
}
// 计算所有表的基数并缓存
std::unordered_map<std::string, size_t> calculate_table_cardinalities(const std::vector<std::string> &tables, SmManager *sm_manager_)
{
    std::unordered_map<std::string, size_t> table_cardinalities;
    for (const auto &table : tables)
    {
        try
        {
            // 确保表文件已经打开
            auto it = sm_manager_->fhs_.find(table);
            if (it == sm_manager_->fhs_.end())
            {
                if (!sm_manager_->db_.is_table(table))
                {
                    throw TableNotFoundError(table);
                }
                if (table.empty())
                {
                    throw TableNotFoundError("Empty table name");
                }
                it = sm_manager_->fhs_.emplace(table, sm_manager_->get_rm_manager()->open_file(table)).first;
            }
            const auto &file_handle = it->second;
            if (!file_handle)
            {
                table_cardinalities[table] = 1000; // 默认估计值
                continue;
            }

            table_cardinalities[table] = file_handle->get_approximate_num();

        }
        catch (const std::exception &e)
        {
            table_cardinalities[table] = 1000; // 默认估计值
        }
    }
    return table_cardinalities;
}

std::shared_ptr<Plan> Planner::make_one_rel(std::shared_ptr<Query> query, Context *context, const QueryColumnRequirement &column_requirements)
{
    // 预先计算所有表的基数
    auto table_cardinalities = calculate_table_cardinalities(query->tables, sm_manager_);

    // 为每个表创建基础扫描计划
    std::vector<std::shared_ptr<Plan>> table_plans;
    std::vector<bool> table_used(query->tables.size(), false); // 跟踪哪些表已被使用

    for (size_t i = 0; i < query->tables.size(); ++i)
    {
        IndexMeta *index_meta = nullptr;
        int max_match_col_count = -1;
        const auto &table = query->tables[i];
        for (auto &cond : query->join_conds)
        {
            if (table == cond.lhs_col.tab_name)
            {
                std::tie(index_meta, max_match_col_count) = get_index_for_join(table, cond.lhs_col);
                break;
            }
            else if (table == cond.rhs_col.tab_name)
            {
                std::tie(index_meta, max_match_col_count) = get_index_for_join(table, cond.rhs_col);
                break;
            }
        }
        if (index_meta == nullptr)
        {
            std::tie(index_meta, max_match_col_count) = get_index_cols(table, query->tab_conds[table]);
        }
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

        // 只有在非 SELECT * 查询时才添加投影节点,单表不添加
        if (!context->hasIsStarFlag() && query->tables.size() > 1)
        {
            auto post_filter_cols = column_requirements.get_post_filter_cols(table);
            if (post_filter_cols.size())
            {
                // 转换为vector并按字母顺序排序
                std::vector<TabCol> cols;
                cols.reserve(post_filter_cols.size());
                for (auto &col : post_filter_cols)
                {
                    cols.emplace_back(std::move(col));
                }

                scan_plan = std::make_shared<ProjectionPlan>(
                    PlanTag::T_Projection,
                    scan_plan,
                    cols);
            }
        }
        table_plans.emplace_back(scan_plan);
    }

    std::shared_ptr<Plan> current_plan = nullptr;

    // 先处理jointree中的SEMI JOIN
    if (!query->jointree.empty())
    {
        for (auto &join_expr : query->jointree)
        {
            if (SEMI_JOIN == join_expr.type)
            {
                // 获取左右表的索引
                auto left_it = std::find(query->tables.begin(), query->tables.end(), join_expr.left);
                auto right_it = std::find(query->tables.begin(), query->tables.end(), join_expr.right);

                if (left_it == query->tables.end() || right_it == query->tables.end())
                {
                    continue; // 跳过无效的表名
                }

                auto left_idx = std::distance(query->tables.begin(), left_it);
                auto right_idx = std::distance(query->tables.begin(), right_it);

                auto left_plan = table_plans[left_idx];
                auto right_plan = table_plans[right_idx];

                // 标记表为已使用
                table_used[left_idx] = true;
                table_used[right_idx] = true;

                // 创建SEMI JOIN计划
                auto semi_join_plan = std::make_shared<JoinPlan>(
                    T_SemiJoin,
                    std::move(left_plan),
                    std::move(right_plan),
                    join_expr.conds);

                if (!current_plan)
                {
                    current_plan = std::move(semi_join_plan);
                }
                else
                {
                    // 如果已经有其他连接，将SEMI JOIN与现有计划合并
                    current_plan = std::make_shared<JoinPlan>(
                        enable_nestedloop_join ? T_NestLoop : T_SortMerge,
                        std::move(current_plan),
                        std::move(semi_join_plan),
                        std::vector<Condition>());
                }
            }
        }
    }

    // 收集未使用的表计划
    std::vector<std::shared_ptr<Plan>> unused_table_plans;
    std::vector<size_t> unused_table_indices;

    for (size_t i = 0; i < table_plans.size(); ++i)
    {
        if (!table_used[i])
        {
            unused_table_plans.push_back(table_plans[i]);
            unused_table_indices.push_back(i);
        }
    }

    // 如果只有一个表且没有SEMI JOIN，直接返回其扫描计划
    if (unused_table_plans.size() == 1 && !current_plan)
    {
        return unused_table_plans[0];
    }

    // 如果所有表都被SEMI JOIN使用了，直接返回当前计划
    if (unused_table_plans.empty())
    {
        return current_plan ? current_plan : table_plans[0];
    }

    // 使用贪心算法选择连接顺序处理剩余的表
    if (unused_table_plans.size() >= 2)
    {
        // 1. 找到基数最小的两个表作为起点
        size_t min_i = 0, min_j = 1;
        size_t min_card = SIZE_MAX;

        for (size_t i = 0; i < unused_table_plans.size(); i++)
        {
            for (size_t j = i + 1; j < unused_table_plans.size(); j++)
            {
                auto scan_i = extract_scan_plan(unused_table_plans[i]);
                auto scan_j = extract_scan_plan(unused_table_plans[j]);
                size_t card_i = table_cardinalities[scan_i->tab_name_];
                size_t card_j = table_cardinalities[scan_j->tab_name_];
                size_t join_card = card_i * card_j; // 简单估计连接基数

                if (join_card < min_card)
                {
                    min_card = join_card;
                    min_i = i;
                    min_j = j;
                }
            }
        }

        // 2. 创建初始连接
        std::vector<Condition> join_conds;
        for (const auto &cond : query->join_conds)
        {
            if (!cond.is_rhs_val)
            {
                std::string lhs_tab = cond.lhs_col.tab_name;
                std::string rhs_tab = cond.rhs_col.tab_name;

                auto scan_i = extract_scan_plan(unused_table_plans[min_i]);
                auto scan_j = extract_scan_plan(unused_table_plans[min_j]);

                if ((lhs_tab == scan_i->tab_name_ && rhs_tab == scan_j->tab_name_) ||
                    (lhs_tab == scan_j->tab_name_ && rhs_tab == scan_i->tab_name_))
                {
                    join_conds.push_back(cond);
                }
            }
        }

        auto table_join_plan = create_ordered_join(
            enable_nestedloop_join ? T_NestLoop : T_SortMerge,
            unused_table_plans[min_i],
            unused_table_plans[min_j],
            join_conds);

        // 3. 移除已使用的表
        unused_table_plans.erase(unused_table_plans.begin() + std::max(min_i, min_j));
        unused_table_plans.erase(unused_table_plans.begin() + std::min(min_i, min_j));

        // 4. 逐个添加剩余的表
        while (!unused_table_plans.empty())
        {
            size_t best_idx = 0;
            size_t min_result_card = SIZE_MAX;
            std::vector<Condition> best_conds;

            // 找到添加后产生最小中间结果的表
            for (size_t i = 0; i < unused_table_plans.size(); i++)
            {
                auto scan_plan = extract_scan_plan(unused_table_plans[i]);
                size_t table_card = table_cardinalities[scan_plan->tab_name_];

                // 收集与当前表相关的连接条件
                std::vector<Condition> curr_conds;
                for (const auto &cond : query->join_conds)
                {
                    if (!cond.is_rhs_val)
                    {
                        std::string lhs_tab = cond.lhs_col.tab_name;
                        std::string rhs_tab = cond.rhs_col.tab_name;

                        if (lhs_tab == scan_plan->tab_name_ || rhs_tab == scan_plan->tab_name_)
                        {
                            curr_conds.push_back(cond);
                        }
                    }
                }

                // 简单估计连接结果的基数
                size_t result_card = table_card; // 可以在这里添加更复杂的基数估计

                if (result_card < min_result_card)
                {
                    min_result_card = result_card;
                    best_idx = i;
                    best_conds = curr_conds;
                }
            }

            // 添加选中的表
            table_join_plan = create_ordered_join(
                enable_nestedloop_join ? T_NestLoop : T_SortMerge,
                table_join_plan,
                unused_table_plans[best_idx],
                best_conds);

            // 移除已使用的表
            unused_table_plans.erase(unused_table_plans.begin() + best_idx);
        }

        // 5. 将表连接计划与SEMI JOIN计划合并
        if (current_plan)
        {
            current_plan = create_ordered_join(
                enable_nestedloop_join ? T_NestLoop : T_SortMerge,
                current_plan,
                table_join_plan,
                std::vector<Condition>());
        }
        else
        {
            current_plan = table_join_plan;
        }
    }
    else if (unused_table_plans.size() == 1)
    {
        // 只有一个未使用的表，直接与现有计划连接
        if (current_plan)
        {
            current_plan = create_ordered_join(
                enable_nestedloop_join ? T_NestLoop : T_SortMerge,
                current_plan,
                unused_table_plans[0],
                std::vector<Condition>());
        }
        else
        {
            current_plan = unused_table_plans[0];
        }
    }

    return current_plan;
}
std::shared_ptr<Plan> Planner::generate_agg_plan(const std::shared_ptr<Query> &query, std::shared_ptr<Plan> plan)
{
    auto x = std::static_pointer_cast<ast::SelectStmt>(query->parse);
    if (!x->has_agg && x->groupby.empty())
    {
        return plan;
    }

    // 准备 ORDER BY 列
    std::vector<TabCol> order_by_cols;
    for (size_t i = 0; i < x->order.cols.size(); ++i)
    {
        auto &order_col = x->order.cols[i];
        if (order_col.tab_name.empty())
            order_col.tab_name = query->tables[0];
        order_by_cols.emplace_back(order_col.tab_name, 
            order_col.col_name, order_col.agg_type);
    }

    // 生成聚合计划，增加 ORDER BY 列参数
    plan = std::make_shared<AggPlan>(T_Agg, std::move(plan), query->cols, query->groupby, query->having_conds, order_by_cols);
    return plan;
}
std::shared_ptr<Plan> Planner::generate_sort_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan)
{
    auto x = std::static_pointer_cast<ast::SelectStmt>(query->parse);
    std::vector<std::string> &tables = query->tables;
    if (x->order.cols.empty())
    {
        return plan;
    }

    // 准备多列排序参数
    std::vector<TabCol> sort_cols;
    std::vector<bool> is_desc_orders; // 每列对应的排序方向

    // 遍历所有排序列及其排序方向
    for (size_t i = 0; i < x->order.cols.size(); ++i)
    {
        auto &order_col = x->order.cols[i];
        auto &order_dir = x->order.dirs[i];
        is_desc_orders.emplace_back(order_dir == ast::OrderByDir::OrderBy_DESC);
        if (order_col.tab_name.empty())
            order_col.tab_name = tables[0];
        sort_cols.emplace_back(std::move(order_col.tab_name), 
            std::move(order_col.col_name), std::move(order_col.agg_type));
    }

    // 创建排序计划
    return std::make_shared<SortPlan>(T_Sort, std::move(plan), sort_cols, is_desc_orders, x->limit);
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

    if (query->parse->Nodetype() != ast::TreeNodeType::SelectStmt)
    {
        // 如果parse不是SELECT语句，抛出错误
        throw RMDBError("Not a SELECT statement");
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
    case ast::TreeNodeType::CreateTable:
    {
        auto x = std::static_pointer_cast<ast::CreateTable>(query->parse);
        std::vector<ColDef> col_defs;
        col_defs.reserve(x->fields.size());
        for (auto &field : x->fields)
        {
            if (field.Nodetype() == ast::TreeNodeType::ColDef)
            {
                auto sv_col_def = static_cast<ast::ColDef*>(&field);
                ColDef col_def = {.name = std::move(sv_col_def->col_name),
                                  .type = interp_sv_type(sv_col_def->type_len.type),
                                  .len = sv_col_def->type_len.len};
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
        auto dml_plan = std::make_shared<DMLPlan>(T_Select, projection, std::string(), std::vector<Value>(),
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

void Planner::analyze_column_requirements(std::shared_ptr<Query> query, Context *context, QueryColumnRequirement &requirements)
{
    // 清空输入的requirements（如果需要）
    requirements.clear();

    // 如果不是SELECT语句，直接返回
    if (!query || !query->parse || query->parse->Nodetype() != ast::TreeNodeType::SelectStmt)
    {
        return;
    }

    auto select_stmt = std::static_pointer_cast<ast::SelectStmt>(query->parse);

    requirements.select_cols.insert(query->cols.begin(), query->cols.end());

    // 2. 分析JOIN条件中需要的列
    for (const auto &cond : query->join_conds)
    {
        requirements.join_cols.emplace(cond.lhs_col);
        requirements.join_cols.emplace(cond.rhs_col);
    }

    // 3. 分析WHERE条件中需要的列
    for (const auto &[table_name, conds] : query->tab_conds)
    {
        for (const auto &cond : conds)
        {
            requirements.where_cols.emplace(cond.lhs_col);
        }
    }

    // 4. 分析GROUP BY子句中需要的列
    requirements.groupby_cols.insert(query->groupby.begin(), query->groupby.end());

    // 5. 分析HAVING子句中需要的列
    for (const auto &cond : query->having_conds)
    {
        requirements.having_cols.insert(cond.lhs_col);
        if (!cond.is_rhs_val)
        {
            requirements.having_cols.insert(cond.rhs_col);
        }
    }

    // 6. 分析ORDER BY子句中需要的列
    for (const auto &order_col : select_stmt->order.cols)
    {
        requirements.orderby_cols.emplace(order_col.tab_name, order_col.col_name);
    }

    // 合并所有中间分析数据到最终结果
    requirements.calculate_layered_requirements();
}

std::vector<TabCol> Planner::get_table_all_cols(const std::string &table_name, std::vector<TabCol> &table_cols, Context *context)
{
    table_cols.clear();

    // 获取隐藏列数量（通常用于跳过系统内部列）
    int hidden_column_count = 0;
    if (context && context->txn_)
    {
        hidden_column_count = context->txn_->get_txn_manager()->get_hidden_column_count();
    }

    // 从sm_manager_获取表的列信息
    const auto &sel_tab_cols = sm_manager_->db_.get_table(table_name).cols;

    // 跳过隐藏列，将ColMeta转换为TabCol
    for (auto it = sel_tab_cols.begin() + hidden_column_count; it != sel_tab_cols.end(); ++it)
    {
        table_cols.emplace_back(TabCol{table_name, it->name});
    }

    return table_cols;
}
