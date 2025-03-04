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

/**
 * @description: 分析器，进行语义分析和查询重写，需要检查不符合语义规定的部分
 * @param {shared_ptr<ast::TreeNode>} parse parser生成的结果集
 * @return {shared_ptr<Query>} Query
 */
std::shared_ptr<Query> Analyze::do_analyze(std::shared_ptr<ast::TreeNode> parse)
{
    std::shared_ptr<Query> query = std::make_shared<Query>();
    if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(parse))
    {
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
            query->cols.emplace_back(TabCol(sv_sel_col->tab_name, sv_sel_col->col_name));
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
                sel_col = check_column(all_cols, sel_col); // 列元数据校验
            }
        }
        // 处理where条件
        get_clause(x->conds, query->conds);
        check_clause(query->tables, query->conds);
    }
    else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(parse))
    {

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
        check_clause({x->tab_name}, query->conds);
    }
    else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(parse))
    {
        query->tables = {x->tab_name};
        // 处理where条件
        get_clause(x->conds, query->conds);
        check_clause({x->tab_name}, query->conds);
    }
    else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(parse))
    {
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
        for (size_t i = 0; i < x->vals.size(); i++)
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
    else
    {
        // do nothing
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
        bool found = false;
        for (auto &col : all_cols)
        {
            if (col.tab_name == target.tab_name && col.name == target.col_name)
            {
                found = true;
                break;
            }
        }
        if (!found)
        {
            throw ColumnNotFoundError(target.col_name);
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
        cond.lhs_col = TabCol(expr->lhs->tab_name, expr->lhs->col_name);
        cond.op = convert_sv_comp_op(expr->op);
        if (auto rhs_val = std::dynamic_pointer_cast<ast::Value>(expr->rhs))
        {
            cond.is_rhs_val = true;
            cond.rhs_val = convert_sv_value(rhs_val);
        }
        else if (auto rhs_col = std::dynamic_pointer_cast<ast::Col>(expr->rhs))
        {
            cond.is_rhs_val = false;
            cond.rhs_col = TabCol(rhs_col->tab_name, rhs_col->col_name);
        }
        conds.emplace_back(std::move(cond));
    }
}

void Analyze::check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds)
{
    // auto all_cols = get_all_cols(tab_names);
    std::vector<ColMeta> all_cols;
    get_all_cols(tab_names, all_cols);
    // Get raw values in where clause
    for (auto &cond : conds)
    {
        // Infer table name from column name
        cond.lhs_col = check_column(all_cols, cond.lhs_col);
        if (!cond.is_rhs_val)
        {
            cond.rhs_col = check_column(all_cols, cond.rhs_col);
        }
        TabMeta &lhs_tab = sm_manager_->db_.get_table(cond.lhs_col.tab_name);
        auto lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
        ColType lhs_type = lhs_col->type;
        ColType rhs_type;
        if (cond.is_rhs_val)
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
        }
        else
        {
            TabMeta &rhs_tab = sm_manager_->db_.get_table(cond.rhs_col.tab_name);
            auto rhs_col = rhs_tab.get_col(cond.rhs_col.col_name);
            rhs_type = rhs_col->type;

            // 对于列与列比较，暂不支持自动类型转换
        }

        // 检查类型是否兼容
        if (lhs_type != rhs_type)
        {
            // 如果是INT和FLOAT之间的比较，我们已经处理了转换
            if (!((lhs_type == TYPE_INT && rhs_type == TYPE_FLOAT) ||
                  (lhs_type == TYPE_FLOAT && rhs_type == TYPE_INT)))
            {
                throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
            }
        }
    }
}

Value Analyze::convert_sv_value(const std::shared_ptr<ast::Value> &sv_val)
{
    Value val;
    if (auto int_lit = std::dynamic_pointer_cast<ast::IntLit>(sv_val))
    {
        val.set_int(int_lit->val);
    }
    else if (auto float_lit = std::dynamic_pointer_cast<ast::FloatLit>(sv_val))
    {
        val.set_float(float_lit->val);
    }
    else if (auto str_lit = std::dynamic_pointer_cast<ast::StringLit>(sv_val))
    {
        val.set_str(str_lit->val);
    }
    else
    {
        throw InternalError("Unexpected sv value type");
    }
    return val;
}

CompOp Analyze::convert_sv_comp_op(ast::SvCompOp op)
{
    std::map<ast::SvCompOp, CompOp> m = {
        {ast::SV_OP_EQ, OP_EQ},
        {ast::SV_OP_NE, OP_NE},
        {ast::SV_OP_LT, OP_LT},
        {ast::SV_OP_GT, OP_GT},
        {ast::SV_OP_LE, OP_LE},
        {ast::SV_OP_GE, OP_GE},
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
