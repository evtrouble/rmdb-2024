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

#include "execution/execution_defs.h"
#include "execution/execution_manager.h"
#include "record/rm.h"
#include "system/sm.h"
#include "common/context.h"
#include "plan.h"
#include "parser/parser.h"
#include "common/common.h"
#include "analyze/analyze.h"

class Planner
{
private:
    SmManager *sm_manager_;

    bool enable_nestedloop_join = true;
    bool enable_sortmerge_join = false;

public:
    Planner(SmManager *sm_manager) : sm_manager_(sm_manager) {}

    std::shared_ptr<Plan> do_planner(std::shared_ptr<Query> query, Context *context);

    void set_enable_nestedloop_join(bool set_val) { enable_nestedloop_join = set_val; }

    void set_enable_sortmerge_join(bool set_val) { enable_sortmerge_join = set_val; }

private:
    // 查询优化相关函数
    std::shared_ptr<Query> logical_optimization(std::shared_ptr<Query> query, Context *context);
    std::shared_ptr<Plan> physical_optimization(std::shared_ptr<Query> query, Context *context);

    // 生成执行计划相关函数
    std::shared_ptr<Plan> make_one_rel(std::shared_ptr<Query> query, Context *context);
    std::shared_ptr<Plan> generate_agg_plan(const std::shared_ptr<Query> &query, std::shared_ptr<Plan> plan);
    std::shared_ptr<Plan> generate_sort_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan);
    std::shared_ptr<Plan> generate_select_plan(std::shared_ptr<Query> query, Context *context);

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

        bool result = !cond.is_rhs_val && cond.lhs_col.tab_name != cond.rhs_col.tab_name;
        std::cout << "  Is join condition: " << (result ? "true" : "false") << std::endl;
        return result;
    }

    // 分离连接条件和选择条件
    std::pair<std::vector<Condition>, std::vector<Condition>>
    split_conditions(const std::vector<Condition> &conditions)
    {
        std::cout << "[Debug] Splitting conditions, total conditions: "
                  << conditions.size() << std::endl;

        std::vector<Condition> join_conds, select_conds;
        for (const auto &cond : conditions)
        {
            std::cout << "[Debug] Processing condition:" << std::endl;
            std::cout << "  Left: " << cond.lhs_col.tab_name << "."
                      << cond.lhs_col.col_name << std::endl;
            if (!cond.is_rhs_val)
            {
                std::cout << "  Right: " << cond.rhs_col.tab_name << "."
                          << cond.rhs_col.col_name << std::endl;
            }
            else
            {
                std::cout << "  Right: value" << std::endl;
            }

            if (is_join_condition(cond))
            {
                std::cout << "  -> Adding to join conditions" << std::endl;
                join_conds.push_back(cond);
            }
            else
            {
                std::cout << "  -> Adding to select conditions" << std::endl;
                select_conds.push_back(cond);
            }
        }

        std::cout << "[Debug] Split result:" << std::endl;
        std::cout << "  Join conditions: " << join_conds.size() << std::endl;
        std::cout << "  Select conditions: " << select_conds.size() << std::endl;

        return {join_conds, select_conds};
    }

    // 获取表的基数（行数）
    size_t get_table_cardinality(const std::string &tab_name)
    {
        std::cout << "[Debug] Getting cardinality for table: " << tab_name << std::endl;
        auto fh = sm_manager_->fhs_.at(tab_name).get();
        size_t num_records = 0;
        for (int page_no = 1; page_no < fh->get_file_hdr().num_pages; page_no++)
        {
            auto page_handle = fh->fetch_page_handle(page_no);
            num_records += page_handle.page_hdr->num_records;
        }
        std::cout << "[Debug] Table " << tab_name << " has " << num_records
                  << " records" << std::endl;
        return num_records;
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
