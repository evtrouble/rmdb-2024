/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "analyze.h"
#include <iostream>
#include "transaction/transaction_manager.h"
#include "transaction/transaction.h"

bool is_valid_datetime_format(const std::string &datetime_str)
{
    // 简单的格式检查：YYYY-MM-DD HH:MM:SS
    if (datetime_str.length() != 19)
    {
        return false;
    }

    // 检查基本格式：YYYY-MM-DD HH:MM:SS
    if (datetime_str[4] != '-' || datetime_str[7] != '-' ||
        datetime_str[10] != ' ' || datetime_str[13] != ':' ||
        datetime_str[16] != ':')
    {
        return false;
    }

    // 检查所有应该是数字的位置
    for (int i = 0; i < 19; i++)
    {
        if (i == 4 || i == 7 || i == 10 || i == 13 || i == 16)
        {
            continue; // 跳过分隔符
        }
        if (!std::isdigit(datetime_str[i]))
        {
            return false;
        }
    }

    return true;
}
/**
 * @description: 分析器，进行语义分析和查询重写，需要检查不符合语义规定的部分
 * @param {shared_ptr<ast::TreeNode>} parse parser生成的结果集
 * @return {shared_ptr<Query>} Query
 */
std::shared_ptr<Query> Analyze::do_analyze(std::shared_ptr<ast::TreeNode> parse, Context *context)
{
    std::shared_ptr<Query> query = std::make_shared<Query>();
    std::unordered_map<std::string, TabCol> alias_to_col;

    auto& table_alias_map = query->table_alias_map;
    switch (parse->Nodetype())
    {
    case ast::TreeNodeType::SelectStmt:
    {
        auto x = std::static_pointer_cast<ast::SelectStmt>(parse);
        // 处理表名
        //!!!!这里不能move，后面会用到
        query->tables = x->tabs;

        // 建立表别名映射关系,别名->实际表名
        for (size_t i = 0; i < query->tables.size(); ++i)
        {
            std::string &table_name = query->tables[i];
            // 检查表是否存在
            if (!sm_manager_->db_.is_table(table_name))
            {
                throw TableNotFoundError(table_name);
            }
            if (x->tab_aliases.size() > i && !x->tab_aliases[i].empty())
            {
                std::string &alias = x->tab_aliases[i];
                table_alias_map.emplace(alias, table_name);
            }
            // 表名也可以作为自己的别名
            table_alias_map.emplace(table_name, table_name);
        }

        // 处理target list，在target list中添加上表名，例如 a.id
        query->cols.reserve(x->cols.size());
        for (auto &sv_sel_col : x->cols)
        {
            TabCol col(sv_sel_col.tab_name, sv_sel_col.col_name, sv_sel_col.agg_type, sv_sel_col.alias);
            query->cols.emplace_back(col);
            if (ast::AggFuncType::NO_TYPE != sv_sel_col.agg_type)
            {
                x->has_agg = true;
            }
            // 记录别名映射
            if (sv_sel_col.alias.size())
            {
                alias_to_col[sv_sel_col.alias] = col;
            }
        }

        std::vector<ColMeta> all_cols;
        get_all_cols(query->tables, all_cols, context);
        if (query->cols.empty())
        {
            // select all columns
            query->cols.reserve(all_cols.size());
            for (auto &col : all_cols)
            {
                query->cols.emplace_back(col.tab_name, col.name);
            }
            context->setIsStarFlag(true);
        }
        else
        {
            // infer table name from column name
            for (auto &sel_col : query->cols)
            {
                if (sel_col.col_name != "*")                                                 // 避免count(*)检查
                    sel_col = check_column(all_cols, sel_col, x->jointree.size(), table_alias_map); // 列元数据校验
            }

            // 检查是否为"全选"
            std::unordered_set<std::string> all_sel_col;
            for (const auto &query_col : query->cols)
            {
                all_sel_col.insert(query_col.tab_name + "." + query_col.col_name);
            }
            context->setIsStarFlag(all_sel_col.size() == all_cols.size());
        }
        // groupby检查
        // 同时包含聚合函数和非聚合列时，必须有groupby
        bool has_non_agg_col = false;
        bool has_agg_col = false;
        for (const auto &sel_col : query->cols)
        {
            if (!has_agg_col || !has_non_agg_col)
            {
                if (ast::AggFuncType::NO_TYPE != sel_col.aggFuncType)
                    has_agg_col = true;
                else
                    has_non_agg_col = true;
            }
            else
                break;
        }
        if (has_non_agg_col && has_agg_col && !x->groupby.size())
        {
            throw RMDBError("should have GROUP BY in this query");
        }
        // 非聚合列必须出现在groupby中
        if (has_non_agg_col && x->groupby.size())
        {
            for (const auto &sel_col : query->cols)
            {
                if (ast::AggFuncType::NO_TYPE == sel_col.aggFuncType)
                {
                    bool is_in_groupby = false;
                    for (const auto &group_col : x->groupby)
                    {
                        if (sel_col.col_name == group_col.col_name)
                        {
                            is_in_groupby = true;
                            break;
                        }
                    }
                    if (!is_in_groupby)
                    {
                        throw RMDBError("Non-aggregated column '" + sel_col.col_name + "' must appear in GROUP BY clause");
                    }
                }
            }
            // 检查是否同时出现非聚合列和聚合列(比如：id ,max(id))
            std::unordered_map<std::string, bool> col_agg_map;
            for (const auto &sel_col : query->cols)
            {
                if (ast::AggFuncType::NO_TYPE != sel_col.aggFuncType)
                {
                    col_agg_map[sel_col.col_name] = true;
                }
                else if (col_agg_map.try_emplace(sel_col.col_name, false).first->second)
                {
                    throw RMDBError("Column '" + sel_col.col_name + "' appears both as non-aggregated and aggregated");
                }
            }
        }
        if (x->groupby.size())
        {
            // 是否在表里
            for (auto &g : x->groupby)
            {
                TabCol group_col = {g.tab_name, g.col_name};
                group_col = check_column(all_cols, group_col, false, table_alias_map);
                query->groupby.emplace_back(group_col);
            }
        }
        // join 检查
        if (x->jointree.size())
        {
            for (auto &joinclause : x->jointree)
            {
                if (JoinType::SEMI_JOIN == joinclause.type)
                {
                    for (auto &sv_cols : query->cols)
                    {
                        if (sv_cols.tab_name == joinclause.right)
                        {
                            throw RMDBError("Only columns from the left table can be selected in the SELECT clause.");
                        }
                    }
                }
                JoinExpr join;
                join.left = joinclause.left;
                join.right = joinclause.right;
                join.type = joinclause.type;
                get_clause(joinclause.conds, join.conds);
                check_clause(query->tables, join.conds, false, context, table_alias_map);
                query->jointree.emplace_back(join);
            }
        }
        // having检查
        // having必须与groupby一起使用
        if (x->having_conds.size())
        {
            if (!x->groupby.size())
            {
                throw RMDBError("HAVING clause must be used with GROUP BY clause");
            }
            // having中的列必须出现在groupby或聚合函数中
            for (const auto &having_cond : x->having_conds)
            {
                if (having_cond.lhs.col_name != "*")
                {
                    bool is_valid = false;
                    for (const auto &group_col : query->groupby)
                    {
                        if (having_cond.lhs.col_name == group_col.col_name ||
                            ast::AggFuncType::NO_TYPE != having_cond.lhs.agg_type)
                        {
                            is_valid = true;
                            break;
                        }
                    }
                    if (!is_valid)
                    {
                        throw RMDBError("Column '" + having_cond.lhs.col_name + "' in HAVING clause must appear in GROUP BY or be used in an aggregate function");
                    }
                }
            }

            // 处理 HAVING 条件
            get_clause(x->having_conds, query->having_conds);
            check_clause(query->tables, query->having_conds, true, context, table_alias_map);
        }
        // 处理ORDER BY
        for (auto &col : x->order.cols)
        {
            // 检查是否是别名
            if (alias_to_col.find(col.col_name) != alias_to_col.end())
            {
                // 如果是别名，替换为真实列
                TabCol &real_col = alias_to_col[col.col_name];
                col.tab_name = real_col.tab_name;
                col.col_name = real_col.col_name;
                col.agg_type = real_col.aggFuncType;
            }
            // 检查ORDER BY列是否有效
            TabCol order_col = {col.tab_name, col.col_name};
            order_col = check_column(all_cols, order_col, false, table_alias_map);
            bool is_agg = false;
            if (ast::AggFuncType::NO_TYPE != col.agg_type)
            {
                is_agg = true;
            }
            // 检查ORDER BY列是否在GROUP BY中（如果有GROUP BY）
            if (x->groupby.size())
            {
                bool found_in_groupby = false;
                for (const auto &group_col : query->groupby)
                {
                    if (order_col.col_name == group_col.col_name)
                    {
                        found_in_groupby = true;
                        break;
                    }
                }
                if (!found_in_groupby && !is_agg)
                {
                    throw RMDBError("ORDER BY column '" + order_col.col_name +
                                    "'is neither in group by nor an aggregation function");
                }
            }
        }

        // 处理LIMIT
        if (x->limit >= 0)
        {
            if (!x->order.cols.empty())
            {
                throw RMDBError("LIMIT must be used together with ORDER");
            }
            query->limit = x->limit;
        }
        // 处理where条件
        get_clause(x->conds, query->conds);
        check_clause(query->tables, query->conds, false, context, table_alias_map);
    }
    break;
    case ast::TreeNodeType::UpdateStmt:
    {
        auto x = std::static_pointer_cast<ast::UpdateStmt>(parse);
        if (!sm_manager_->db_.is_table(x->tab_name))
        {
            throw TableNotFoundError(x->tab_name);
        }

        // 将表名添加到查询的表列表中
        // query->tables = {x->tab_name};
        // 获取所有列信息用于验证
        std::vector<ColMeta> all_cols;
        get_all_cols({x->tab_name}, all_cols, context);

        // 处理set子句
        for (auto &sv_set_clause : x->set_clauses)
        {
            SetClause set_clause;
            // 设置要更新的列
            TabCol col(x->tab_name, sv_set_clause.col_name);
            // 验证列是否存在
            col = check_column(all_cols, col, false, table_alias_map);
            set_clause.lhs = col;

            // 获取列的类型信息
            TabMeta &tab = sm_manager_->db_.get_table(col.tab_name);
            auto col_meta = tab.get_col(col.col_name);
            ColType target_type = col_meta->type;

            // 设置新值并进行类型转换
            Value val = convert_sv_value(&sv_set_clause.val);

            // 如果类型不匹配，尝试进行类型转换
            if (val.type != target_type)
            {
                val = convert_value_type(val, target_type);
            }

            // 初始化原始数据
            val.raw = nullptr;
            val.init_raw(col_meta->len);
            set_clause.rhs = std::move(val);
            set_clause.op = sv_set_clause.op;

            // 添加到查询的set子句列表中
            query->set_clauses.emplace_back(std::move(set_clause));
        }

        // 处理where条件
        get_clause(x->conds, query->conds);
        check_clause({x->tab_name}, query->conds, false, context, table_alias_map);
    }
    break;
    case ast::TreeNodeType::DeleteStmt:
    {
        auto x = std::static_pointer_cast<ast::DeleteStmt>(parse);
        // query->tables = {x->tab_name};
        // 处理where条件
        get_clause(x->conds, query->conds);
        check_clause({x->tab_name}, query->conds, false, context, table_alias_map);
    }
    break;
    case ast::TreeNodeType::InsertStmt:
    {
        auto x = std::static_pointer_cast<ast::InsertStmt>(parse);
        // query->tables = {x->tab_name};

        if (!sm_manager_->db_.is_table(x->tab_name))
        {
            throw TableNotFoundError(x->tab_name);
        }

        // 获取表的元数据
        TabMeta &tab = sm_manager_->db_.get_table(x->tab_name);
        size_t hidden_col_count = context->txn_->get_txn_manager()->get_hidden_column_count();

        // 检查插入的值的数量是否与表的列数匹配
        if (x->vals.size() + hidden_col_count != tab.cols.size())
        {
            throw InvalidValueCountError();
        }

        // 处理insert的values值，并进行类型转换
        query->values.reserve(x->vals.size());
        for (size_t i = 0; i < x->vals.size(); ++i)
        {
            // 获取当前列的类型信息
            auto &col = tab.cols[i + hidden_col_count];
            ColType target_type = col.type;

            // 转换值并进行类型检查
            Value val = convert_sv_value(x->vals[i].get());

            // 如果类型不匹配，尝试进行类型转换
            if (val.type != target_type)
            {
                val = convert_value_type(val, target_type);
            }

            // 初始化原始数据
            val.raw = nullptr;
            val.init_raw(col.len);

            query->values.emplace_back(std::move(val));
        }
    }
    break;
    case ast::TreeNodeType::ExplainStmt:
    {
        auto x = std::static_pointer_cast<ast::ExplainStmt>(parse);
        query->sub_query = do_analyze(x->query, context);
    }
    break;
    default:
        break;
    }
    query->parse = std::move(parse);
    return query;
}

TabCol Analyze::check_column(const std::vector<ColMeta> &all_cols, TabCol target, bool is_semijoin, std::unordered_map<std::string, std::string> &table_alias_map_)
{
    // 如果是通配符 '*'，直接返回，不需要验证
    if (target.col_name == "*")
    {
        return target;
    }
    // 如果指定了表名/别名，先解析别名
    if (!target.tab_name.empty())
    {
        // 检查是否是表别名，如果是，转换为真实表名
        auto alias_it = table_alias_map_.find(target.tab_name);
        if (alias_it != table_alias_map_.end())
        {
            target.tab_name = alias_it->second;
            // std::cout << "[Debug] Resolved table alias: " << target.tab_name << std::endl;
        }
    }
    if (target.tab_name.empty())
    {
        // Table name not specified, infer table name from column name
        std::string tab_name;
        for (auto &col : all_cols)
        {
            if (col.name == target.col_name)
            {
                if (tab_name.size())
                {
                    throw AmbiguousColumnError(target.col_name);
                }
                tab_name = col.tab_name;
                if (is_semijoin)
                {
                    break;
                }
            }
        }
        if (tab_name.empty())
        {
            throw ColumnNotFoundError(target.col_name);
        }
        target.tab_name = tab_name;
    }
    else
    {
        /** TODO: Make sure target column exists */
        auto it = std::find_if(all_cols.begin(), all_cols.end(), [&](const ColMeta &col)
                               { return col.tab_name == target.tab_name && col.name == target.col_name; });

        if (it == all_cols.end())
        {
            // 列元数据未找到，抛出列未找到错误
            throw ColumnNotFoundError(target.col_name);
        }

        // 校验聚合列类型
        if (target.aggFuncType != ast::AggFuncType::NO_TYPE && target.aggFuncType != ast::AggFuncType::COUNT)
        {
            if (it->type != ColType::TYPE_INT && it->type != ColType::TYPE_FLOAT)
            {
                throw InternalError("Unexpected sv value type 1");
            }
        }
    }

    return target;
}
void Analyze::get_all_cols(const std::vector<std::string> &tab_names,
                           std::vector<ColMeta> &all_cols, Context *context)
{
    int hidden_column_count = context->txn_->get_txn_manager()->get_hidden_column_count();
    for (auto &sel_tab_name : tab_names)
    {
        // 这里db_不能写成get_db(), 注意要传指针
        const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
        all_cols.insert(all_cols.end(), sel_tab_cols.begin() + hidden_column_count,
                        sel_tab_cols.end());
    }
}

void Analyze::get_clause(const std::vector<ast::BinaryExpr> &sv_conds, std::vector<Condition> &conds)
{
    conds.clear();
    for (auto &expr : sv_conds)
    {
        Condition cond;
        cond.lhs_col = TabCol(expr.lhs.tab_name, expr.lhs.col_name, expr.lhs.agg_type);
        cond.op = convert_sv_comp_op(expr.op);
        if (expr.rhs.Nodetype() == ast::TreeNodeType::IntLit ||
                                     expr.rhs.Nodetype() == ast::TreeNodeType::FloatLit ||
                                     expr.rhs.Nodetype() == ast::TreeNodeType::BoolLit ||
                                     expr.rhs.Nodetype() == ast::TreeNodeType::StringLit)
        {
            auto rhs_val = static_cast<const ast::Value*>(&expr.rhs);
            cond.is_rhs_val = true;
            cond.rhs_val = convert_sv_value(rhs_val);
        }
        else if (expr.rhs.Nodetype() == ast::TreeNodeType::Col)
        {
            auto rhs_col = static_cast<const ast::Col*>(&expr.rhs);
            cond.is_rhs_val = false;
            cond.rhs_col = TabCol(rhs_col->tab_name, rhs_col->col_name, expr.lhs.agg_type);
        }
        // else if (expr->rhs == nullptr)
        // {
        //     auto sub_query = std::static_pointer_cast<ast::SubQueryExpr>(expr);
        //     if (!sub_query)
        //     {
        //         throw RMDBError("Invalid expression with null right-hand side");
        //     }

        //     auto subQuery_ = std::make_shared<SubQuery>();
        //     subQuery_->stmt = sub_query->subquery;
        //     cond.is_rhs_val = false;
        //     cond.is_subquery = true;

        //     if (cond.op != OP_IN && cond.op != OP_NOT_IN)
        //     {
        //         subQuery_->is_scalar = true;
        //     }
        //     for (const auto &val : sub_query->vals)
        //     {
        //         subQuery_->result.insert(convert_sv_value(val));
        //     }

        //     cond.subQuery = std::move(subQuery_);
        // }
        conds.emplace_back(std::move(cond));
    }
}

void Analyze::check_clause(const std::vector<std::string> &tab_names,
                           std::vector<Condition> &conds, bool is_having, Context *context, std::unordered_map<std::string, std::string> &table_alias_map_)
{
    std::vector<ColMeta> all_cols;
    get_all_cols(tab_names, all_cols, context);

    for (auto &cond : conds)
    {
        // 检查 WHERE、ON 子句中是否包含聚合函数
        if (!is_having && (cond.lhs_col.aggFuncType != ast::AggFuncType::NO_TYPE ||
                           (!cond.is_rhs_val /* && !cond.is_subquery*/ && cond.rhs_col.aggFuncType != ast::AggFuncType::NO_TYPE)))
        {
            throw RMDBError("Aggregate functions are not allowed in WHERE clause");
        }

        // Infer table name from column name
        if (cond.lhs_col.col_name != "*")
        {
            cond.lhs_col = check_column(all_cols, cond.lhs_col, false, table_alias_map_);
        }
        if (!cond.is_rhs_val && /* !cond.is_subquery &&*/ cond.rhs_col.col_name != "*")
        {
            cond.rhs_col = check_column(all_cols, cond.rhs_col, false, table_alias_map_);
        }
        if ((is_having && cond.lhs_col.col_name != "*") || !is_having)
        {
            TabMeta &lhs_tab = sm_manager_->db_.get_table(cond.lhs_col.tab_name);
            auto lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
            ColType lhs_type = lhs_col->type;
            ColType rhs_type;

            if (cond.is_rhs_val /* && !cond.is_subquery*/)
            {
                rhs_type = cond.rhs_val.type;
                // 检查类型是否兼容
                if (!can_cast_type(lhs_type, rhs_type))
                {
                    throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
                }
                if (cond.rhs_val.type != lhs_type)
                    cast_value(cond.rhs_val, lhs_type);
                cond.rhs_val.raw = nullptr;
                cond.rhs_val.init_raw(lhs_col->len);
            }
            else
            {
                TabMeta &rhs_tab = sm_manager_->db_.get_table(cond.rhs_col.tab_name);
                auto rhs_col = rhs_tab.get_col(cond.rhs_col.col_name);
                rhs_type = rhs_col->type;

                // 检查类型是否兼容
                if (!can_cast_type(lhs_type, rhs_type))
                {
                    throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
                }
                // 对于列与列比较，暂不支持自动类型转换
            }
        }
    }
}
bool Analyze::can_cast_type(ColType from, ColType to)
{
    // Add logic to determine if a type can be cast to another type
    if (from == to)
        return true;
    if (from == TYPE_INT && to == TYPE_FLOAT)
        return true;
    if (from == TYPE_FLOAT && to == TYPE_INT)
        return true;
    if (from == TYPE_STRING && to == TYPE_DATETIME)
        return true;
    if (from == TYPE_DATETIME && to == TYPE_STRING)
        return true;
    return false;
}

void Analyze::cast_value(Value &val, ColType to)
{
    // Add logic to cast val to the target type
    if (val.type == TYPE_INT && to == TYPE_FLOAT)
    {
        int int_val = val.int_val;
        val.type = TYPE_FLOAT;
        val.float_val = static_cast<float>(int_val);
    }
    else if (val.type == TYPE_FLOAT && to == TYPE_INT)
    {
        float float_val = val.float_val;
        val.type = TYPE_INT;
        val.int_val = static_cast<int>(float_val);
    }
    else if (val.type == TYPE_STRING && to == TYPE_DATETIME)
    {
        if (!is_valid_datetime_format(val.str_val))
        {
            throw IncompatibleTypeError("STRING", "DATETIME - Invalid format");
        }
        val.type = TYPE_DATETIME;
    }
    else if (val.type == TYPE_DATETIME && to == TYPE_STRING)
    {
        val.type = TYPE_STRING;
    }
    else
    {
        throw IncompatibleTypeError(coltype2str(val.type), coltype2str(to));
    }
}
Value Analyze::convert_sv_value(const ast::Value *sv_val)
{
    Value val;
    switch (sv_val->Nodetype())
    {
    case ast::TreeNodeType::IntLit:
    {
        auto int_lit = static_cast<const ast::IntLit*>(sv_val);
        val.set_int(int_lit->val);
    }
    break;
    case ast::TreeNodeType::FloatLit:
    {
        auto float_lit = static_cast<const ast::FloatLit*>(sv_val);
        val.set_float(float_lit->val);
        val.str_val = float_lit->original_text; // 保存原始文本
    }
    break;
    case ast::TreeNodeType::StringLit:
    {
        auto str_lit = static_cast<const ast::StringLit*>(sv_val);
        val.set_str(str_lit->val);
    }
    break;
    default:
        throw InternalError("Unexpected sv value type 2");
    }
    return val;
}

CompOp Analyze::convert_sv_comp_op(ast::SvCompOp op)
{
    static std::map<ast::SvCompOp, CompOp> m = {
        {ast::SV_OP_EQ, OP_EQ},
        {ast::SV_OP_NE, OP_NE},
        {ast::SV_OP_LT, OP_LT},
        {ast::SV_OP_GT, OP_GT},
        {ast::SV_OP_LE, OP_LE},
        {ast::SV_OP_GE, OP_GE},
        // {ast::SV_OP_IN, OP_IN},
        // {ast::SV_OP_NOT_IN, OP_NOT_IN},
    };
    return m.at(op);
}

// 实现类型转换函数
Value Analyze::convert_value_type(const Value &value, ColType target_type)
{
    Value result = value;
    result.raw = nullptr;

    // 如果类型已经匹配，直接返回
    if (value.type == target_type)
    {
        return result;
    }

    // 处理类型转换
    switch (value.type)
    {
    case TYPE_INT:
        if (target_type == TYPE_FLOAT)
        {
            // INT 转 FLOAT
            result.set_float(static_cast<float>(value.int_val));
        }
        else if (target_type == TYPE_STRING)
        {
            // INT 转 STRING
            result.set_str(std::to_string(value.int_val));
        }
        break;

    case TYPE_FLOAT:
        if (target_type == TYPE_INT)
        {
            // FLOAT 转 INT
            result.set_int(static_cast<int>(value.float_val));
        }
        else if (target_type == TYPE_STRING)
        {
            // FLOAT 转 STRING
            result.set_str(std::to_string(value.float_val));
        }
        break;

    case TYPE_STRING:
        if (target_type == TYPE_INT)
        {
            // STRING 转 INT
            try
            {
                result.set_int(std::stoi(value.str_val));
            }
            catch (const std::exception &e)
            {
                throw IncompatibleTypeError("STRING", "INT");
            }
        }
        else if (target_type == TYPE_FLOAT)
        {
            // STRING 转 FLOAT
            try
            {
                result.set_float(std::stof(value.str_val));
            }
            catch (const std::exception &e)
            {
                throw IncompatibleTypeError("STRING", "FLOAT");
            }
        }
        else if (target_type == TYPE_DATETIME)
        {
            if (!is_valid_datetime_format(value.str_val))
            {
                throw IncompatibleTypeError("STRING", "DATETIME - Invalid format");
            }
            // 格式正确，直接将字符串赋值给 datetime
            result.set_str(value.str_val); // 先设置字符串值
        }
        // TODO: Implement datetime conversion if needed
        break;
    case TYPE_DATETIME:
        break;
    }

    return result;
}
