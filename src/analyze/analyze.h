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

#include "parser/parser.h"
#include "system/sm.h"
#include "common/common.h"

class Query
{
public:
    std::shared_ptr<ast::TreeNode> parse;
    // where条件
    std::vector<Condition> conds;
    // 按表分组的条件
    std::map<std::string, std::vector<Condition>> tab_conds;
    // join条件
    std::vector<Condition> join_conds;

    // 投影列
    std::vector<TabCol> cols;
    // 表名
    std::vector<std::string> tables;
    // update 的set 值
    std::vector<SetClause> set_clauses;
    // insert 的values值
    std::vector<Value> values;
    // groupby字段
    std::vector<TabCol> groupby;
    // having条件
    std::vector<Condition> having_conds;
    // jointree
    std::vector<JoinExpr> jointree;
    // limit条件
    int limit = -1;
    std::unordered_map<std::string, std::string> table_alias_map; // 将表的别名映射到实际的表名

    std::shared_ptr<Query> sub_query;
    Query() {}
};

class Analyze
{
private:
    SmManager *sm_manager_;
    // 移除 table_alias_map_ 成员变量
    // std::unordered_map<std::string, std::string> table_alias_map_;

public:
    Analyze(SmManager *sm_manager) : sm_manager_(sm_manager) {}
    ~Analyze() {}

    std::shared_ptr<Query> do_analyze(std::shared_ptr<ast::TreeNode> root, Context *context);

private:
    TabCol check_column(const std::vector<ColMeta> &all_cols, TabCol target, bool is_semijoin,
                       const std::unordered_map<std::string, std::string> &table_alias_map);
    void get_all_cols(const std::vector<std::string> &tab_names,
                      std::vector<ColMeta> &all_cols, Context *context);
    void get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds, std::vector<Condition> &conds);
    void check_clause(const std::vector<std::string> &tab_names,
                      std::vector<Condition> &conds, bool is_having, Context *context,
                      const std::unordered_map<std::string, std::string> &table_alias_map);
    Value convert_sv_value(const std::shared_ptr<ast::Value> &sv_val);
    CompOp convert_sv_comp_op(ast::SvCompOp op);
    Value convert_value_type(const Value &value, ColType target_type);
    static bool can_cast_type(ColType from, ColType to);
    static void cast_value(Value &val, ColType to);
};
