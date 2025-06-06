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
#include "../parser/ast.h"

/**
 * @description: 分析器，进行语义分析和查询重写，需要检查不符合语义规定的部分
 * @param {shared_ptr<ast::TreeNode>} parse parser生成的结果集
 * @return {shared_ptr<Query>} Query
 */
std::shared_ptr<Query> Analyze::do_analyze(std::shared_ptr<ast::TreeNode> parse)
{
    std::shared_ptr<Query> query = std::make_shared<Query>();
    switch (parse->Nodetype())
    {
    case ast::TreeNodeType::SelectStmt:
    {
        auto x = std::static_pointer_cast<ast::SelectStmt>(parse);
        // 处理表名
        query->tables = std::move(x->tabs);

        for (auto &tab_name : query->tables)
        {
            if (!sm_manager_->db_.is_table(tab_name))
            {
                throw TableNotFoundError(tab_name);
            }
        }

        // 处理target list，再target list中添加上表名，例如 a.id
        query->cols.reserve(x->cols.size());
        for (auto &sv_sel_col : x->cols)
        {
            query->cols.emplace_back(TabCol(sv_sel_col->tab_name, sv_sel_col->col_name, sv_sel_col->agg_type, sv_sel_col->alias));
            if (ast::AggFuncType::NO_TYPE != sv_sel_col->agg_type)
            {
                x->has_agg = true;
            }
        }

        std::vector<ColMeta> all_cols;
        get_all_cols(query->tables, all_cols);
        if (query->cols.empty())
        {
            // select all columns
            query->cols.reserve(all_cols.size());
            for (auto &col : all_cols)
            {
                query->cols.emplace_back(TabCol(col.tab_name, col.name));
            }
        }
        else
        {
            // infer table name from column name
            for (auto &sel_col : query->cols)
            {
                if (sel_col.col_name != "*")                   // 避免count(*)检查
                    sel_col = check_column(all_cols, sel_col); // 列元数据校验
            }
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
        if (has_non_agg_col && has_agg_col && !x->has_groupby)
        {
            throw RMDBError("should have GROUP BY in this query");
        }
        // 非聚合列必须出现在groupby中
        if (has_non_agg_col && x->has_groupby)
        {
            for (const auto &sel_col : query->cols)
            {
                if (ast::AggFuncType::NO_TYPE == sel_col.aggFuncType)
                {
                    bool is_in_groupby = false;
                    for (const auto &group_col : x->groupby)
                    {
                        if (sel_col.col_name == group_col->col_name)
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
                    throw RMDBError("Column '" + sel_col.col_name + "' appears both as non-aggregated and aggregated");
            }
        }
        if (x->has_groupby)
        {
            // 是否在表里
            for (auto &g : x->groupby)
            {
                TabCol group_col = {g->tab_name, g->col_name};
                group_col = check_column(all_cols, group_col);
                query->groupby.emplace_back(group_col);
            }
        }

        // having检查
        // having必须与groupby一起使用
        if (x->has_having)
        {
            if (!x->has_groupby)
            {
                throw RMDBError("HAVING clause must be used with GROUP BY clause");
            }
            // having中的列必须出现在groupby或聚合函数中
            for (const auto &having_cond : x->having_conds)
            {
                if (having_cond->lhs->col_name != "*")
                {
                    bool is_valid = false;
                    for (const auto &group_col : query->groupby)
                    {
                        if (having_cond->lhs->col_name == group_col.col_name)
                        {
                            is_valid = true;
                            break;
                        }
                    }
                    if (!is_valid)
                    {
                        for (const auto &sel_col : query->cols)
                        {
                            if (having_cond->lhs->col_name == sel_col.col_name &&
                                ast::AggFuncType::NO_TYPE != sel_col.aggFuncType)
                            {
                                is_valid = true;
                                break;
                            }
                        }
                    }
                    if (!is_valid)
                    {
                        throw RMDBError("Column '" + having_cond->lhs->col_name + "' in HAVING clause must appear in GROUP BY or be used in an aggregate function");
                    }
                }
            }

            // 处理 HAVING 条件
            get_clause(x->having_conds, query->having_conds);
            check_clause(query->tables, query->having_conds, true);
        }
        // 处理where条件
        get_clause(x->conds, query->conds);
        check_clause(query->tables, query->conds, false);
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
        query->tables = {x->tab_name};
        // 获取所有列信息用于验证
        std::vector<ColMeta> all_cols;
        get_all_cols({x->tab_name}, all_cols);

        // 处理set子句
        for (auto &sv_set_clause : x->set_clauses)
        {
            SetClause set_clause;
            // 设置要更新的列
            TabCol col(x->tab_name, sv_set_clause->col_name);
            // 验证列是否存在
            col = check_column(all_cols, col);
            set_clause.lhs = col;

            // 获取列的类型信息
            TabMeta &tab = sm_manager_->db_.get_table(col.tab_name);
            auto col_meta = tab.get_col(col.col_name);
            ColType target_type = col_meta->type;

            // 设置新值并进行类型转换
            Value val = convert_sv_value(sv_set_clause->val);

            // 如果类型不匹配，尝试进行类型转换
            if (val.type != target_type)
            {
                val = convert_value_type(val, target_type);
            }

            // 初始化原始数据
            val.raw = nullptr;
            val.init_raw(col_meta->len);
            set_clause.rhs = val;

            // 添加到查询的set子句列表中
            query->set_clauses.emplace_back(std::move(set_clause));
        }

        // 处理where条件
        get_clause(x->conds, query->conds);
        check_clause({x->tab_name}, query->conds, false);
    }
    break;
    case ast::TreeNodeType::DeleteStmt:
    {
        auto x = std::static_pointer_cast<ast::DeleteStmt>(parse);
        query->tables = {x->tab_name};
        // 处理where条件
        get_clause(x->conds, query->conds);
        check_clause({x->tab_name}, query->conds, false);
    }
    break;
    case ast::TreeNodeType::InsertStmt:
    {
        auto x = std::static_pointer_cast<ast::InsertStmt>(parse);
        query->tables = {x->tab_name};

        if (!sm_manager_->db_.is_table(x->tab_name))
        {
            throw TableNotFoundError(x->tab_name);
        }

        // 获取表的元数据
        TabMeta &tab = sm_manager_->db_.get_table(x->tab_name);

        // 检查插入的值的数量是否与表的列数匹配
        if (x->vals.size() != tab.cols.size())
        {
            throw InvalidValueCountError();
        }

        // 处理insert的values值，并进行类型转换
        query->values.reserve(x->vals.size());
        for (size_t i = 0; i < x->vals.size(); ++i)
        {
            // 获取当前列的类型信息
            auto &col = tab.cols[i];
            ColType target_type = col.type;

            // 转换值并进行类型检查
            Value val = convert_sv_value(x->vals[i]);

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
    default:
        break;
    }
    query->parse = std::move(parse);
    return query;
}

TabCol Analyze::check_column(const std::vector<ColMeta> &all_cols, TabCol target)
{
    if (target.tab_name.empty())
    {
        // Table name not specified, infer table name from column name
        std::string tab_name;
        for (auto &col : all_cols)
        {
            if (col.name == target.col_name)
            {
                if (!tab_name.empty())
                {
                    throw AmbiguousColumnError(target.col_name);
                }
                tab_name = col.tab_name;
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

void Analyze::get_all_cols(const std::vector<std::string> &tab_names, std::vector<ColMeta> &all_cols)
{
    for (auto &sel_tab_name : tab_names)
    {
        // 这里db_不能写成get_db(), 注意要传指针
        const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
        all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
    }
}

void Analyze::get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds, std::vector<Condition> &conds)
{
    conds.clear();
    for (auto &expr : sv_conds)
    {
        Condition cond;
        cond.lhs_col = TabCol(expr->lhs->tab_name, expr->lhs->col_name, expr->lhs->agg_type);
        cond.op = convert_sv_comp_op(expr->op);
        if (expr->rhs != nullptr && (expr->rhs->Nodetype() == ast::TreeNodeType::IntLit ||
                                     expr->rhs->Nodetype() == ast::TreeNodeType::FloatLit ||
                                     expr->rhs->Nodetype() == ast::TreeNodeType::BoolLit ||
                                     expr->rhs->Nodetype() == ast::TreeNodeType::StringLit))
        {
            auto rhs_val = std::static_pointer_cast<ast::Value>(expr->rhs);
            cond.is_rhs_val = true;
            cond.rhs_val = convert_sv_value(rhs_val);
        }
        else if (expr->rhs != nullptr && expr->rhs->Nodetype() == ast::TreeNodeType::Col)
        {
            auto rhs_col = std::static_pointer_cast<ast::Col>(expr->rhs);
            cond.is_rhs_val = false;
            cond.rhs_col = TabCol(rhs_col->tab_name, rhs_col->col_name, expr->lhs->agg_type);
        }
        else if (expr->rhs == nullptr)
        {
            auto sub_query = std::static_pointer_cast<ast::SubQueryExpr>(expr);
            if (!sub_query)
            {
                throw RMDBError("Invalid expression with null right-hand side");
            }

            auto subQuery_ = std::make_shared<SubQuery>();
            subQuery_->stmt = sub_query->subquery;
            cond.is_rhs_val = false;
            cond.is_subquery = true;

            if (cond.op != OP_IN && cond.op != OP_NOT_IN)
            {
                subQuery_->is_scalar = true;
            }
            for (const auto &val : sub_query->vals)
            {
                subQuery_->result.insert(convert_sv_value(val));
            }

            cond.subQuery = std::move(subQuery_);
        }
        conds.emplace_back(std::move(cond));
    }
}

void Analyze::check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds, bool is_having)
{
    std::vector<ColMeta> all_cols;
    get_all_cols(tab_names, all_cols);

    for (auto &cond : conds)
    {
        // 检查 WHERE 子句中是否包含聚合函数
        if (!is_having && (cond.lhs_col.aggFuncType != ast::AggFuncType::NO_TYPE ||
                           (!cond.is_rhs_val && !cond.is_subquery && cond.rhs_col.aggFuncType != ast::AggFuncType::NO_TYPE)))
        {
            throw RMDBError("Aggregate functions are not allowed in WHERE clause");
        }

        // Infer table name from column name
        if (cond.lhs_col.col_name != "*")
        {
            cond.lhs_col = check_column(all_cols, cond.lhs_col);
        }
        if (!cond.is_rhs_val && !cond.is_subquery && cond.rhs_col.col_name != "*")
        {
            cond.rhs_col = check_column(all_cols, cond.rhs_col);
        }
        if ((is_having && cond.lhs_col.col_name != "*") || !is_having)
        {
            TabMeta &lhs_tab = sm_manager_->db_.get_table(cond.lhs_col.tab_name);
            auto lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
            ColType lhs_type = lhs_col->type;
            ColType rhs_type;

            if (cond.is_rhs_val && !cond.is_subquery)
            {
                rhs_type = cond.rhs_val.type;

                // 处理类型转换
                if (lhs_type != rhs_type)
                {
                    // 如果左侧是FLOAT，右侧是INT，将INT转换为FLOAT
                    if (lhs_type == TYPE_FLOAT && rhs_type == TYPE_INT)
                    {
                        float float_val = static_cast<float>(cond.rhs_val.int_val);
                        cond.rhs_val.set_float(float_val);
                        rhs_type = TYPE_FLOAT;
                    }
                    // 如果左侧是INT，右侧是FLOAT，将FLOAT转换为INT
                    else if (lhs_type == TYPE_INT && rhs_type == TYPE_FLOAT)
                    {
                        int int_val = static_cast<int>(cond.rhs_val.float_val);
                        cond.rhs_val.set_int(int_val);
                        rhs_type = TYPE_INT;
                    }
                }
                cond.rhs_val.raw = nullptr;
                cond.rhs_val.init_raw(lhs_col->len);
                // 检查类型是否兼容
                if (lhs_type != rhs_type)
                {
                    // 允许 INT 和 FLOAT 之间的比较，但此处不执行转换
                    if (!((lhs_type == TYPE_INT && rhs_type == TYPE_FLOAT) ||
                          (lhs_type == TYPE_FLOAT && rhs_type == TYPE_INT)))
                    {
                        throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
                    }
                }
            }
            else if (!cond.is_subquery)
            {
                // Get rhs column metadata
                TabMeta &rhs_tab = sm_manager_->db_.get_table(cond.rhs_col.tab_name);
                auto rhs_col = rhs_tab.get_col(cond.rhs_col.col_name);
                rhs_type = rhs_col->type;

                // 检查类型是否兼容
                if (lhs_type != rhs_type)
                {
                    // 允许 INT 和 FLOAT 之间的比较，但此处不执行转换
                    if (!((lhs_type == TYPE_INT && rhs_type == TYPE_FLOAT) ||
                          (lhs_type == TYPE_FLOAT && rhs_type == TYPE_INT)))
                    {
                        throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
                    }
                }
                // 对于列与列比较，暂不支持自动类型转换
            }
            else if (cond.is_subquery && cond.subQuery->stmt != nullptr)
            {
                // 1、子查询的列数只能为1
                if (cond.subQuery->stmt->cols.size() != 1)
                {
                    throw RMDBError("Subquery must return only one column");
                }

                // 2、获取子查询的列类型
                TabMeta &sub_tab = sm_manager_->db_.get_table(cond.subQuery->stmt->tabs[0]);
                auto sub_col = sub_tab.get_col(cond.subQuery->stmt->cols[0]->col_name);

                // 3、检查子查询的列类型是否与左边的列类型相同
                cond.subQuery->subquery_type = sub_col->type;
                if (!can_cast_type(sub_col->type, lhs_type))
                {
                    throw RMDBError("Subquery Type Error");
                }

                // 4、分析子查询计划
                cond.subQuery->query = do_analyze(cond.subQuery->stmt);
            }
            else if (cond.is_subquery)
            {
                // 子查询是数组（IN/NOT IN 操作）
                // 创建一个临时集合来存储转换后的值
                std::unordered_set<Value> tempResult;

                // 遍历子查询结果
                for (const Value &val : cond.subQuery->result)
                {
                    Value newVal = val;

                    // 检查类型是否一致或可转换
                    if (lhs_type != val.type)
                    {
                        if (!can_cast_type(val.type, lhs_type))
                        {
                            throw RMDBError("Subquery Type Error");
                        }
                        else
                        {
                            cast_value(newVal, lhs_type);
                        }
                    }
                    tempResult.insert(newVal); // 将转换后的值插入临时集合
                }

                // 用临时集合替换原始集合
                cond.subQuery->result = std::move(tempResult);
            }
            else
            {
                TabMeta &rhs_tab = sm_manager_->db_.get_table(cond.rhs_col.tab_name);
                auto rhs_col = rhs_tab.get_col(cond.rhs_col.col_name);
                rhs_type = rhs_col->type;

                // 检查类型是否兼容
                if (lhs_type != rhs_type)
                {
                    // 允许 INT 和 FLOAT 之间的比较，但此处不执行转换
                    if (!((lhs_type == TYPE_INT && rhs_type == TYPE_FLOAT) ||
                          (lhs_type == TYPE_FLOAT && rhs_type == TYPE_INT)))
                    {
                        throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
                    }
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
        // do not things
    }
    else
    {
        throw IncompatibleTypeError(coltype2str(val.type), coltype2str(to));
    }
}
Value Analyze::convert_sv_value(const std::shared_ptr<ast::Value> &sv_val)
{
    Value val;
    switch (sv_val->Nodetype())
    {
    case ast::TreeNodeType::IntLit:
    {
        auto int_lit = std::static_pointer_cast<ast::IntLit>(sv_val);
        val.set_int(int_lit->val);
    }
    break;
    case ast::TreeNodeType::FloatLit:
    {
        auto float_lit = std::static_pointer_cast<ast::FloatLit>(sv_val);
        val.set_float(float_lit->val);
    }
    break;
    case ast::TreeNodeType::StringLit:
    {
        auto str_lit = std::static_pointer_cast<ast::StringLit>(sv_val);
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
        {ast::SV_OP_IN, OP_IN},
        {ast::SV_OP_NOT_IN, OP_NOT_IN},
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
        break;
    }

    return result;
}
