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

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <algorithm>
#include <iostream>
#include <sstream>
#include <iomanip>

#include "execution/execution_defs.h"
#include "execution/execution_manager.h"
#include "record/rm.h"
#include "system/sm.h"
#include "common/context.h"
#include "plan.h"
#include "parser/parser.h"
#include "common/common.h"
#include "analyze/analyze.h"
// 整个查询的列需求分析结果
struct QueryColumnRequirement
{
    // 每个表需要扫描的列（这是最终结果）
    std::map<std::string, std::set<TabCol>> table_required_cols;

    std::map<std::string, std::set<TabCol>> scan_level_cols;  // 扫描层需要的列
    std::map<std::string, std::set<TabCol>> post_filter_cols; // 过滤后需要的列
    std::map<std::string, std::set<TabCol>> post_join_cols;   // 连接后需要的列
    std::map<std::string, std::set<TabCol>> final_cols;       // 最终输出列
    // 中间分析数据（用于调试和优化）
    std::set<TabCol> select_cols;  // SELECT子句需要的列
    std::set<TabCol> join_cols;    // JOIN条件需要的列
    std::set<TabCol> where_cols;   // WHERE条件需要的列
    std::set<TabCol> groupby_cols; // GROUP BY需要的列
    std::set<TabCol> having_cols;  // HAVING条件需要的列
    std::set<TabCol> orderby_cols; // ORDER BY需要的列

    // 获取某个表需要的列
    std::set<TabCol> get_table_cols(const std::string &table_name) const
    {
        auto it = table_required_cols.find(table_name);
        return it != table_required_cols.end() ? it->second : std::set<TabCol>{};
    }
    // 获取不同层级的列需求
    std::set<TabCol> get_scan_cols(const std::string &table_name) const
    {
        auto it = scan_level_cols.find(table_name);
        return it != scan_level_cols.end() ? it->second : std::set<TabCol>{};
    }

    std::set<TabCol> get_post_filter_cols(const std::string &table_name) const
    {
        auto it = post_filter_cols.find(table_name);
        return it != post_filter_cols.end() ? it->second : std::set<TabCol>{};
    }
    // 判断某列是否只在WHERE中使用
    bool is_where_only_column(const TabCol &col) const
    {
        bool in_where = where_cols.count(col) > 0;
        bool in_select = select_cols.count(col) > 0;
        bool in_join = join_cols.count(col) > 0;
        bool in_groupby = groupby_cols.count(col) > 0;
        bool in_having = having_cols.count(col) > 0;
        bool in_orderby = orderby_cols.count(col) > 0;

        return in_where && !in_select && !in_join && !in_groupby && !in_having && !in_orderby;
    }
    void calculate_layered_requirements();
};
class Planner
{
private:
    SmManager *sm_manager_;

    bool enable_nestedloop_join = true;
    bool enable_sortmerge_join = false;

    // 投影下推相关的辅助函数
    bool is_select_star_query(const std::shared_ptr<ast::SelectStmt> &select_stmt);
    std::vector<TabCol> compute_required_columns(const std::string &table_name, const std::shared_ptr<Query> &query);
    std::shared_ptr<Plan> add_leaf_projections(std::shared_ptr<Plan> plan, const std::vector<TabCol> &required_cols);
    std::shared_ptr<Plan> apply_projection_pushdown(std::shared_ptr<Plan> plan, const std::shared_ptr<Query> &query);
    std::vector<TabCol> compute_required_columns_after_filter(const std::string &table_name, const std::shared_ptr<Query> &query);

public:
    Planner(SmManager *sm_manager) : sm_manager_(sm_manager) {}

    std::shared_ptr<Plan> do_planner(std::shared_ptr<Query> query, Context *context);

    void set_enable_nestedloop_join(bool set_val) { enable_nestedloop_join = set_val; }

    void set_enable_sortmerge_join(bool set_val) { enable_sortmerge_join = set_val; }

private:
    // 查询优化相关函数
    std::shared_ptr<Query> logical_optimization(std::shared_ptr<Query> query, Context *context);
    std::shared_ptr<Plan> physical_optimization(std::shared_ptr<Query> query, Context *context);

    // 获取表的基数（行数）
    size_t get_table_cardinality(const std::string &table_name);

    // 生成执行计划相关函数
    std::shared_ptr<Plan> make_one_rel(std::shared_ptr<Query> query, Context *context);
    std::shared_ptr<Plan> generate_agg_plan(const std::shared_ptr<Query> &query, std::shared_ptr<Plan> plan);
    std::shared_ptr<Plan> generate_sort_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan);
    std::shared_ptr<Plan> generate_select_plan(std::shared_ptr<Query> query, Context *context);

    // 列需求分析结果
    QueryColumnRequirement column_requirements_;

    // 列需求分析函数
    QueryColumnRequirement analyze_column_requirements(std::shared_ptr<Query> query);

    // 优化器辅助函数
    bool is_single_table_condition(const Condition &cond)
    {
        if (cond.is_rhs_val)
            return true;
        return cond.lhs_col.tab_name == cond.rhs_col.tab_name;
    }

    std::set<std::string> get_condition_tables(const Condition &cond)
    {
        std::set<std::string> tables;
        tables.insert(cond.lhs_col.tab_name);
        if (!cond.is_rhs_val)
        {
            tables.insert(cond.rhs_col.tab_name);
        }
        return tables;
    }
    // 新增方法：计算Filter之上需要的列（不包含Filter专用列）
    std::vector<TabCol> compute_columns_above_filter(const std::string &table_name, const std::shared_ptr<Query> &query)
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

        // 2. 添加连接条件中的列（JOIN需要的列）
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

        // 注意：不添加WHERE条件中的列，因为那些是Filter专用的

        // 转换为 TabCol 格式
        std::vector<TabCol> result;
        for (const auto &col_name : required_cols)
        {
            result.push_back({table_name, col_name});
        }

        return result;
    }
    // 判断是否为连接条件
    bool is_join_condition(const Condition &cond)
    {
        std::cout << "[Debug] Checking join condition:" << std::endl;
        std::cout << "  Left table: " << cond.lhs_col.tab_name
                  << ", Left col: " << cond.lhs_col.col_name << std::endl;
        if (!cond.is_rhs_val)
        {
            std::cout << "  Right table: " << cond.rhs_col.tab_name
                      << ", Right col: " << cond.rhs_col.col_name << std::endl;
        }
        std::cout << "  is_rhs_val: " << (cond.is_rhs_val ? "true" : "false") << std::endl;

        // 修改判断逻辑：不仅要检查表名不同，还要检查是否为值比较
        bool result = !cond.is_rhs_val &&
                      (cond.lhs_col.tab_name != cond.rhs_col.tab_name ||  // 不同表
                       (cond.lhs_col.tab_name == cond.rhs_col.tab_name && // 同表但是别名不同
                        cond.lhs_col.tab_name.find('.') != std::string::npos));

        std::cout << "  Is join condition: " << (result ? "true" : "false") << std::endl;
        return result;
    }
    bool table_matches(const std::string &col_table_name, const std::string &target_table_name,
                       const std::map<std::string, std::string> &alias_to_tab,
                       const std::map<std::string, std::string> &tab_to_alias)
    {
        // 直接匹配表名
        if (col_table_name == target_table_name)
        {
            return true;
        }

        // 检查 col_table_name 是否是 target_table_name 的别名
        auto it = tab_to_alias.find(target_table_name);
        if (it != tab_to_alias.end() && col_table_name == it->second)
        {
            return true;
        }

        // 检查 col_table_name 是否是别名，其对应的实际表名是 target_table_name
        auto it2 = alias_to_tab.find(col_table_name);
        if (it2 != alias_to_tab.end() && it2->second == target_table_name)
        {
            return true;
        }

        return false;
    }

    // 分离连接条件和选择条件
    std::pair<std::vector<Condition>, std::vector<Condition>>
    split_conditions(const std::vector<Condition> &conditions)
    {
        std::cout << "[Debug] 开始分离条件..." << std::endl;
        std::cout << "[Debug] 总条件数: " << conditions.size() << std::endl;

        std::vector<Condition> join_conds, select_conds;
        for (const auto &cond : conditions)
        {
            std::cout << "[Debug] 处理条件: " << std::endl;
            std::cout << "  左表: " << cond.lhs_col.tab_name
                      << " 左列: " << cond.lhs_col.col_name << std::endl;

            if (!cond.is_rhs_val)
            {
                std::cout << "  右表: " << cond.rhs_col.tab_name
                          << " 右列: " << cond.rhs_col.col_name << std::endl;

                // 检查是否为连接条件
                if (is_join_condition(cond))
                {
                    std::cout << "  -> 添加到连接条件" << std::endl;
                    join_conds.push_back(cond);
                    continue;
                }
            }
            else
            {
                std::cout << "  右侧为值比较" << std::endl;
            }

            // 如果不是连接条件，就是选择条件
            std::cout << "  -> 添加到选择条件" << std::endl;
            select_conds.push_back(cond);
        }

        std::cout << "[Debug] 分离结果:" << std::endl;
        std::cout << "  连接条件数: " << join_conds.size() << std::endl;
        std::cout << "  选择条件数: " << select_conds.size() << std::endl;

        return {join_conds, select_conds};
    }

    // 查找计划树中的扫描节点
    std::shared_ptr<Plan> find_scan_node(std::shared_ptr<Plan> plan, const std::string &tab_name)
    {
        std::cout << "[Debug] Finding scan node for table: " << tab_name << std::endl;
        if (!plan)
            return nullptr;

        if (plan->tag == PlanTag::T_SeqScan || plan->tag == PlanTag::T_IndexScan)
        {
            auto scan_plan = std::dynamic_pointer_cast<ScanPlan>(plan);
            if (scan_plan->tab_name_ == tab_name)
            {
                std::cout << "[Debug] Found scan node for table: " << tab_name << std::endl;
                return plan;
            }
        }

        // 递归搜索子节点
        if (plan->tag == PlanTag::T_NestLoop || plan->tag == PlanTag::T_SortMerge)
        {
            auto join_plan = std::dynamic_pointer_cast<JoinPlan>(plan);
            auto left_result = find_scan_node(join_plan->left_, tab_name);
            if (left_result)
                return left_result;
            return find_scan_node(join_plan->right_, tab_name);
        }

        std::cout << "[Debug] Scan node not found for table: " << tab_name << std::endl;
        return nullptr;
    }

    // 替换计划树中的节点
    void replace_node(std::shared_ptr<Plan> &root, std::shared_ptr<Plan> old_node,
                      std::shared_ptr<Plan> new_node);

    // 获取索引信息
    std::pair<IndexMeta *, int> get_index_cols(const std::string &tab_name,
                                               const std::vector<Condition> &curr_conds);

    // 类型转换
    ColType interp_sv_type(ast::SvType sv_type)
    {
        std::map<ast::SvType, ColType> m = {
            {ast::SV_TYPE_INT, TYPE_INT},
            {ast::SV_TYPE_FLOAT, TYPE_FLOAT},
            {ast::SV_TYPE_STRING, TYPE_STRING}};
        return m.at(sv_type);
    }
};
