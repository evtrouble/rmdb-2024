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
#include "parser/ast.h"

#include "parser/parser.h"

typedef enum PlanTag
{
    T_Invalid = 1,
    T_Help,
    T_ShowTable,
    T_DescTable,
    T_CreateTable,
    T_DropTable,
    T_CreateIndex,
    T_DropIndex,
    T_ShowIndex,
    T_SetKnob,
    T_Insert,
    T_Update,
    T_Delete,
    T_Select,
    T_Transaction_begin,
    T_Transaction_commit,
    T_Transaction_abort,
    T_Transaction_rollback,
    T_Create_StaticCheckPoint,
    T_SeqScan,
    T_IndexScan,
    T_NestLoop,
    T_SortMerge, // sort merge join
    T_SemiJoin,
    T_Sort,
    T_Agg,
    T_Projection,
    T_Explain,
    T_Filter,
    T_LoadData,
    T_IoEnable
    // T_Subquery // 子查询
} PlanTag;

// 查询执行计划
class Plan
{
public:
    PlanTag tag;
    Plan(PlanTag tag) : tag(tag) {}
    virtual ~Plan() = default;
};

class ScanPlan : public Plan
{
public:
    ScanPlan(PlanTag tag, SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> &conds)
        : Plan(tag), tab_name_(tab_name), fed_conds_(std::move(conds))
    {
        // TabMeta &tab = sm_manager->db_.get_table(tab_name_);
        // cols_ = tab.cols;
        // len_ = cols_.back().offset + cols_.back().len;
        // fed_conds_ = conds_;
    }

    ScanPlan(PlanTag tag, SmManager *sm_manager, const std::string &tab_name)
        : Plan(tag), tab_name_(tab_name) {}

    ScanPlan(PlanTag tag, SmManager *sm_manager, const std::string &tab_name,
            IndexMeta &index_meta, int max_match_col_count)
        : Plan(tag), tab_name_(tab_name), index_meta_(index_meta),
          max_match_col_count_(max_match_col_count) {}

     ScanPlan(PlanTag tag, SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> &conds,
            IndexMeta &index_meta, int max_match_col_count)
        : Plan(tag), tab_name_(tab_name), fed_conds_(std::move(conds)), index_meta_(index_meta),
          max_match_col_count_(max_match_col_count)
    {
        // TabMeta &tab = sm_manager->db_.get_table(tab_name_);
        // cols_ = tab.cols;
        // len_ = cols_.back().offset + cols_.back().len;
        // fed_conds_ = conds_;
    }
    ~ScanPlan() override = default;

    // 以下变量同ScanExecutor中的变量
    std::string tab_name_;
    // std::vector<ColMeta> cols_;
    // std::vector<Condition> conds_;
    // size_t len_;
    std::vector<Condition> fed_conds_;
    IndexMeta index_meta_;
    int max_match_col_count_;
};

class JoinPlan : public Plan
{
public:
    JoinPlan(PlanTag tag, std::unique_ptr<Plan>& left, std::unique_ptr<Plan>& right, std::vector<Condition> &conds)
        : Plan(tag), left_(std::move(left)), right_(std::move(right)), conds_(std::move(conds)), 
            type(JoinType::INNER_JOIN) {}
    JoinPlan(PlanTag tag, std::unique_ptr<Plan>& left, std::unique_ptr<Plan>& right)
        : Plan(tag), left_(std::move(left)), right_(std::move(right)), 
            type(JoinType::INNER_JOIN) {}
    ~JoinPlan() override = default;
    // 左节点
    std::unique_ptr<Plan> left_;
    // 右节点
    std::unique_ptr<Plan> right_;
    // 连接条件
    std::vector<Condition> conds_;
    // future TODO: 后续可以支持的连接类型
    JoinType type;
};

class ProjectionPlan : public Plan
{
public:
    ProjectionPlan(PlanTag tag, std::unique_ptr<Plan> &subplan, std::vector<TabCol> &sel_cols)
        : Plan(tag), subplan_(std::move(subplan)), sel_cols_(std::move(sel_cols)) {}
    std::unique_ptr<Plan> subplan_;
    std::vector<TabCol> sel_cols_;
    ~ProjectionPlan() override = default;
};

class SortPlan : public Plan
{
public:
    SortPlan(PlanTag tag, std::unique_ptr<Plan> &subplan, TabCol &sort_col, bool is_desc, int limit = -1)
        : Plan(tag), subplan_(std::move(subplan)), limit_(limit)
    {
        sort_cols_.emplace_back(std::move(sort_col));
        is_desc_orders_.emplace_back(is_desc);
    }
    // 多列排序构造函数（支持每列独立排序方向）
    SortPlan(PlanTag tag, std::unique_ptr<Plan> &subplan,
            std::vector<TabCol> &sort_cols,
            std::vector<bool> &is_desc_orders,
             int limit = -1)
        : Plan(tag),
          subplan_(std::move(subplan)),
          sort_cols_(std::move(sort_cols)),
          is_desc_orders_(std::move(is_desc_orders)),
          limit_(limit)
    {
        if (sort_cols.size() != is_desc_orders.size())
        {
            throw std::invalid_argument("Number of sort columns must match number of sort directions");
        }
    }
    ~SortPlan() override = default;
    std::unique_ptr<Plan> subplan_;
    std::vector<TabCol> sort_cols_;
    std::vector<bool> is_desc_orders_;
    int limit_;
};

// dml语句，包括insert; delete; update; select语句　
class DMLPlan : public Plan
{
public:
    DMLPlan(PlanTag tag, std::string &tab_name,
            std::vector<Value> &values)
        : Plan(tag), tab_name_(std::move(tab_name)),
          values_(std::move(values)) {}

    DMLPlan(PlanTag tag, std::unique_ptr<Plan> &subplan, std::string &tab_name,
            std::vector<Condition> &conds)
        : Plan(tag), subplan_(std::move(subplan)), tab_name_(std::move(tab_name)),
          conds_(std::move(conds)) {}

    DMLPlan(PlanTag tag, std::unique_ptr<Plan> &subplan, std::string &tab_name,
             std::vector<Condition> &conds,
             std::vector<SetClause> &set_clauses)
        : Plan(tag), subplan_(std::move(subplan)), tab_name_(std::move(tab_name)),
          conds_(std::move(conds)), set_clauses_(std::move(set_clauses)) {}
    DMLPlan(PlanTag tag, std::unique_ptr<Plan>& subplan)
        : Plan(tag), subplan_(std::move(subplan)) {}
    ~DMLPlan() override = default;
    std::unique_ptr<Plan> subplan_;
    std::string tab_name_;
    std::vector<Value> values_;
    std::vector<Condition> conds_;
    std::vector<SetClause> set_clauses_;
    std::unique_ptr<ast::TreeNode> parse;
};

// ddl语句, 包括create/drop table; create/drop index;
class DDLPlan : public Plan
{
public:
    DDLPlan(PlanTag tag, std::string &tab_name, std::vector<std::string> &col_names,
            std::vector<ColDef> &cols)
        : Plan(tag), tab_name_(std::move(tab_name)), tab_col_names_(std::move(col_names)),
          cols_(std::move(cols)) {}
    DDLPlan(PlanTag tag, std::string &tab_name, std::vector<std::string> &col_names)
        : Plan(tag), tab_name_(std::move(tab_name)), tab_col_names_(std::move(col_names)) {}
    DDLPlan(PlanTag tag, std::string &tab_name)
        : Plan(tag), tab_name_(std::move(tab_name)) {}
    DDLPlan(PlanTag tag, std::string &tab_name, std::vector<ColDef> &cols)
        : Plan(tag), tab_name_(std::move(tab_name)), 
          cols_(std::move(cols)) {}
    ~DDLPlan() override = default;
    std::string tab_name_;
    std::vector<std::string> tab_col_names_;
    std::vector<ColDef> cols_;
};

// help; show tables; desc tables; begin; abort; commit; rollback语句对应的plan
class OtherPlan : public Plan
{
public:
    OtherPlan(PlanTag tag, std::string &tab_name)
        : Plan(tag), tab_name_(std::move(tab_name)) {}
    OtherPlan(PlanTag tag)
        : Plan(tag) {}
    OtherPlan(PlanTag tag, std::string &tab_name, std::string &file_name)
     : Plan(tag), tab_name_(std::move(tab_name)), file_name_(std::move(file_name)) {}
    OtherPlan(PlanTag tag, bool io_enabled_) : Plan(tag), io_enable_(io_enabled_) {}

    ~OtherPlan() override = default;
    std::string tab_name_;
    std::string file_name_;
    bool io_enable_;
};

// EXPLAIN语句对应的plan
class ExplainPlan : public Plan
{
public:
    std::unique_ptr<Plan> subplan_;
    std::unique_ptr<ast::SelectStmt> select_stmt; // 存储原始的 SELECT 语句

    ExplainPlan(PlanTag tag, std::unique_ptr<Plan>& subplan)
        : Plan(tag), subplan_(std::move(subplan)), select_stmt(nullptr) {}

    ~ExplainPlan() override = default;
};

// Set Knob Plan
class SetKnobPlan : public Plan
{
public:
    SetKnobPlan(ast::SetKnobType set_knob_type, bool bool_val)
        : Plan(T_SetKnob), set_knob_type_(set_knob_type), bool_val_(bool_val) {}
    ~SetKnobPlan() override = default;
    ast::SetKnobType set_knob_type_;
    bool bool_val_;
};

class AggPlan : public Plan
{
public:
    std::unique_ptr<Plan> subplan_;
    std::vector<TabCol> sel_cols_;
    std::vector<TabCol> groupby_cols_;
    std::vector<Condition> having_conds_;
    std::vector<TabCol> order_by_cols_; // 新增 ORDER BY 列

    AggPlan(PlanTag tag, std::unique_ptr<Plan>& subplan, std::vector<TabCol> sel_cols_,
            std::vector<TabCol> &groupby_cols_, std::vector<Condition> &having_conds_,
            std::vector<TabCol> &order_by_cols_) // 增加默认参数
        : Plan(tag), subplan_(std::move(subplan)), sel_cols_(std::move(sel_cols_)),
          groupby_cols_(std::move(groupby_cols_)), having_conds_(std::move(having_conds_)),
          order_by_cols_(std::move(order_by_cols_)) {}

    ~AggPlan() override = default;
};

// 添加Filter计划节点
class FilterPlan : public Plan
{
public:
    std::unique_ptr<Plan> subplan_; // 子计划
    std::vector<Condition> conds_;  // 过滤条件

    FilterPlan(PlanTag tag, std::unique_ptr<Plan> &subplan, std::vector<Condition>& conds)
        : Plan(tag), subplan_(std::move(subplan)), conds_(std::move(conds)) {}

    ~FilterPlan() override = default;
};

// 子查询计划
// class SubqueryPlan : public Plan
// {
// public:
//     std::shared_ptr<Plan> subplan_; // 子查询的查询计划
//     bool is_scalar_;                // 是否是标量子查询(返回单行单列)
//     bool is_in_predicate_;          // 是否是IN谓词子查询

//     SubqueryPlan(PlanTag tag, std::shared_ptr<Plan> subplan, bool is_scalar, bool is_in_predicate) : Plan(tag), subplan_(std::move(subplan)), is_scalar_(is_scalar), is_in_predicate_(is_in_predicate) {}

//     ~SubqueryPlan() override = default;
// };
