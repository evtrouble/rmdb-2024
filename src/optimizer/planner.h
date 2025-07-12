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
    // std::map<std::string, std::set<TabCol>> post_join_cols;   // 连接后需要的列
    std::map<std::string, std::set<TabCol>> final_cols; // 最终输出列
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

    void calculate_layered_requirements();
    QueryColumnRequirement(const QueryColumnRequirement &) = delete;
    QueryColumnRequirement &operator=(const QueryColumnRequirement &) = delete;

    // 启用移动构造和移动赋值
    QueryColumnRequirement() = default;
    QueryColumnRequirement(QueryColumnRequirement &&) = default;
    QueryColumnRequirement &operator=(QueryColumnRequirement &&) = default;
    void clear()
    {
        table_required_cols.clear();
        scan_level_cols.clear();
        post_filter_cols.clear();
        final_cols.clear();
        select_cols.clear();
        join_cols.clear();
        where_cols.clear();
        groupby_cols.clear();
        having_cols.clear();
        orderby_cols.clear();
    }
};
class Planner
{
private:
    SmManager *sm_manager_;

    bool enable_nestedloop_join = true;
    bool enable_sortmerge_join = false;

    std::vector<TabCol> get_table_all_cols(const std::string &table_name, std::vector<TabCol> &table_cols, Context *context);

public:
    Planner(SmManager *sm_manager) : sm_manager_(sm_manager) {}

    std::unique_ptr<Plan> do_planner(std::unique_ptr<Query>& query, Context *context);

    void set_enable_nestedloop_join(bool set_val) { enable_nestedloop_join = set_val; }

    void set_enable_sortmerge_join(bool set_val) { enable_sortmerge_join = set_val; }

private:
    // 查询优化相关函数
    void logical_optimization(std::unique_ptr<Query> &query, Context *context);
    std::unique_ptr<Plan> physical_optimization(std::unique_ptr<Query> &query, Context *context);

    // 生成执行计划相关函数
    std::unique_ptr<Plan> make_one_rel(std::unique_ptr<Query> &query, Context *context, QueryColumnRequirement &column_requirements);
    std::unique_ptr<Plan> generate_agg_plan(std::unique_ptr<Query> &query, std::unique_ptr<Plan> &plan);
    std::unique_ptr<Plan> generate_sort_plan(std::unique_ptr<Query> &query, std::unique_ptr<Plan> &plan);
    std::unique_ptr<Plan> generate_select_plan(std::unique_ptr<Query> &query, Context *context);

    // 移除这一行：QueryColumnRequirement column_requirements_;

    // 列需求分析函数
    void analyze_column_requirements(std::unique_ptr<Query> &query, Context *context, QueryColumnRequirement &requirements);

    // 获取索引信息
    std::pair<IndexMeta *, int> get_index_cols(const std::string &tab_name,
                                               const std::vector<Condition> &curr_conds);

    std::pair<IndexMeta *, int> get_index_for_join(const std::string &tab_name, const TabCol &join_col);

    // 类型转换
    ColType interp_sv_type(ast::SvType sv_type)
    {
        static std::map<ast::SvType, ColType> m = {
            {ast::SV_TYPE_INT, TYPE_INT},
            {ast::SV_TYPE_FLOAT, TYPE_FLOAT},
            {ast::SV_TYPE_STRING, TYPE_STRING},
            {ast::SV_TYPE_DATETIME, TYPE_DATETIME},
        };
        return m.at(sv_type);
    }
};
