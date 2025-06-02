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

#include <vector>
#include <string>
#include <memory>

enum JoinType
{
    INNER_JOIN,
    LEFT_JOIN,
    RIGHT_JOIN,
    FULL_JOIN
};
namespace ast
{

    enum SvType
    {
        SV_TYPE_INT,
        SV_TYPE_FLOAT,
        SV_TYPE_STRING,
        SV_TYPE_BOOL,
        SV_TYPE_DATETIME
    };

    enum SvCompOp
    {
        SV_OP_EQ,
        SV_OP_NE,
        SV_OP_LT,
        SV_OP_GT,
        SV_OP_LE,
        SV_OP_GE,
        SV_OP_IN,
        SV_OP_NOT_IN
    };
    enum AggFuncType
    {
        NO_TYPE,
        SUM,
        COUNT,
        MAX,
        MIN
    };
    enum OrderByDir
    {
        OrderBy_DEFAULT,
        OrderBy_ASC,
        OrderBy_DESC
    };

    enum SetKnobType
    {
        EnableNestLoop,
        EnableSortMerge
    };
    enum class TreeNodeType
    {
        Help,
        ShowTables,
        TxnBegin,
        TxnCommit,
        TxnAbort,
        TxnRollback,
        TypeLen,
        Field,
        ColDef,
        CreateTable,
        DropTable,
        DescTable,
        CreateIndex,
        DropIndex,
        ShowIndex,
        Expr,
        Value,
        IntLit,
        FloatLit,
        StringLit,
        BoolLit,
        Col,
        SetClause,
        BinaryExpr,
        OrderBy,
        InsertStmt,
        DeleteStmt,
        UpdateStmt,
        JoinExpr,
        SelectStmt,
        SetStmt,
        InExpr,
        ExplainStmt
    };
    // Base class for tree nodes
    struct TreeNode
    {
        virtual ~TreeNode() = default; // enable polymorphism
        virtual TreeNodeType Nodetype() const = 0;
    };

    struct Help : public TreeNode
    {
        TreeNodeType Nodetype() const override { return TreeNodeType::Help; }
    };
    struct ExplainStmt : public TreeNode
    {
        std::shared_ptr<TreeNode> query;

        ExplainStmt(std::shared_ptr<TreeNode> query_) : query(std::move(query_)) {}
    };
    struct ShowTables : public TreeNode
    {
        TreeNodeType Nodetype() const override { return TreeNodeType::ShowTables; }
    };

    struct TxnBegin : public TreeNode
    {
        TreeNodeType Nodetype() const override { return TreeNodeType::TxnBegin; }
    };

    struct TxnCommit : public TreeNode
    {
        TreeNodeType Nodetype() const override { return TreeNodeType::TxnCommit; }
    };

    struct TxnAbort : public TreeNode
    {
        TreeNodeType Nodetype() const override { return TreeNodeType::TxnAbort; }
    };

    struct TxnRollback : public TreeNode
    {
        TreeNodeType Nodetype() const override { return TreeNodeType::TxnRollback; }
    };

    struct TypeLen : public TreeNode
    {
        SvType type;
        int len;

        TypeLen(SvType type_, int len_) : type(type_), len(len_) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::TypeLen; }
    };

    struct Field : public TreeNode
    {
        virtual TreeNodeType Nodetype() const override { return TreeNodeType::Field; }
    };

    struct ColDef : public Field
    {
        std::string col_name;
        std::shared_ptr<TypeLen> type_len;

        ColDef(const std::string &col_name_, std::shared_ptr<TypeLen> type_len_) : col_name(std::move(col_name_)), type_len(std::move(type_len_)) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::ColDef; }
    };

    struct CreateTable : public TreeNode
    {
        std::string tab_name;
        std::vector<std::shared_ptr<Field>> fields;

        CreateTable(const std::string &tab_name_, const std::vector<std::shared_ptr<Field>> &fields_) : tab_name(std::move(tab_name_)), fields(std::move(fields_)) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::CreateTable; }
    };

    struct DropTable : public TreeNode
    {
        std::string tab_name;

        DropTable(const std::string &tab_name_) : tab_name(std::move(tab_name_)) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::DropTable; }
    };

    struct DescTable : public TreeNode
    {
        std::string tab_name;

        DescTable(const std::string &tab_name_) : tab_name(std::move(tab_name_)) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::DescTable; }
    };

    struct CreateIndex : public TreeNode
    {
        std::string tab_name;
        std::vector<std::string> col_names;

        CreateIndex(const std::string &tab_name_, const std::vector<std::string> &col_names_) : tab_name(std::move(tab_name_)), col_names(std::move(col_names_)) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::CreateIndex; }
    };

    struct DropIndex : public TreeNode
    {
        std::string tab_name;
        std::vector<std::string> col_names;

        DropIndex(const std::string &tab_name_, const std::vector<std::string> &col_names_) : tab_name(std::move(tab_name_)), col_names(std::move(col_names_)) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::DropIndex; }
    };

    struct ShowIndex : public TreeNode
    {
        std::string tab_name;

        ShowIndex(const std::string &tab_name_) : tab_name(std::move(tab_name_)) {};
        TreeNodeType Nodetype() const override { return TreeNodeType::ShowIndex; }
    };

    struct Expr : public TreeNode
    {
        TreeNodeType Nodetype() const override { return TreeNodeType::Expr; }
    };

    struct Value : public Expr
    {
        Value() {};
        TreeNodeType Nodetype() const override { return TreeNodeType::Value; }
    };

    struct IntLit : public Value
    {
        int val;

        IntLit(int val_) : val(val_) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::IntLit; }
    };

    struct FloatLit : public Value
    {
        float val;

        FloatLit(float val_) : val(val_) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::FloatLit; }
    };

    struct StringLit : public Value
    {
        std::string val;

        StringLit(const std::string &val_) : val(std::move(val_)) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::StringLit; }
    };

    struct BoolLit : public Value
    {
        bool val;

        BoolLit(bool val_) : val(val_) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::BoolLit; }
    };

    struct Col : public Expr
    {
        std::string tab_name;
        std::string col_name;
        AggFuncType agg_type = NO_TYPE;
        std::string alias = "";

        Col(const std::string &tab_name_, const std::string &col_name_) : tab_name(std::move(tab_name_)), col_name(std::move(col_name_)) {}

        Col(std::string tab_name_, std::string col_name_, AggFuncType agg_type, std::string alias_ = "") : tab_name(std::move(tab_name_)), col_name(std::move(col_name_)), agg_type(agg_type), alias(std::move(alias_)) {}

        TreeNodeType Nodetype() const override { return TreeNodeType::Col; }
    };

    struct SetClause : public TreeNode
    {
        std::string col_name;
        std::shared_ptr<Value> val;

        SetClause(const std::string &col_name_, std::shared_ptr<Value> val_) : col_name(std::move(col_name_)), val(std::move(val_)) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::SetClause; }
    };

    struct BinaryExpr : public TreeNode
    {
        std::shared_ptr<Col> lhs;
        SvCompOp op;
        std::shared_ptr<Expr> rhs;

        BinaryExpr(std::shared_ptr<Col> lhs_, SvCompOp op_, std::shared_ptr<Expr> rhs_) : lhs(std::move(lhs_)), op(op_), rhs(std::move(rhs_)) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::BinaryExpr; }
    };

    struct OrderBy : public TreeNode
    {
        std::shared_ptr<Col> cols;
        OrderByDir orderby_dir;
        OrderBy(std::shared_ptr<Col> cols_, OrderByDir orderby_dir_) : cols(std::move(cols_)), orderby_dir(std::move(orderby_dir_)) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::OrderBy; }
    };

    struct InsertStmt : public TreeNode
    {
        std::string tab_name;
        std::vector<std::shared_ptr<Value>> vals;

        InsertStmt(const std::string &tab_name_, const std::vector<std::shared_ptr<Value>> &vals_) : tab_name(std::move(tab_name_)), vals(std::move(vals_)) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::InsertStmt; }
    };

    struct DeleteStmt : public TreeNode
    {
        std::string tab_name;
        std::vector<std::shared_ptr<BinaryExpr>> conds;

        DeleteStmt(const std::string &tab_name_, const std::vector<std::shared_ptr<BinaryExpr>> &conds_) : tab_name(std::move(tab_name_)), conds(std::move(conds_)) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::DeleteStmt; }
    };

    struct UpdateStmt : public TreeNode
    {
        std::string tab_name;
        std::vector<std::shared_ptr<SetClause>> set_clauses;
        std::vector<std::shared_ptr<BinaryExpr>> conds;

        UpdateStmt(const std::string &tab_name_,
                   const std::vector<std::shared_ptr<SetClause>> &set_clauses_,
                   const std::vector<std::shared_ptr<BinaryExpr>> &conds_) : tab_name(std::move(tab_name_)), set_clauses(std::move(set_clauses_)), conds(std::move(conds_)) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::UpdateStmt; }
    };

    struct JoinExpr : public TreeNode
    {
        std::string left;
        std::string right;
        std::vector<std::shared_ptr<BinaryExpr>> conds;
        JoinType type;

        JoinExpr(const std::string &left_, const std::string &right_,
                 const std::vector<std::shared_ptr<BinaryExpr>> &conds_, JoinType type_) : left(std::move(left_)), right(std::move(right_)), conds(std::move(conds_)), type(type_) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::JoinExpr; }
    };

    struct SelectStmt : public TreeNode
    {
        std::vector<std::shared_ptr<Col>> cols;
        std::vector<std::string> tabs;
        std::vector<std::shared_ptr<BinaryExpr>> conds;
        std::vector<std::shared_ptr<JoinExpr>> jointree;
        std::vector<std::shared_ptr<Col>> groupby;
        std::vector<std::shared_ptr<BinaryExpr>> having_conds;

        bool has_agg = false;
        bool has_groupby;
        bool has_having;
        bool has_sort;
        std::shared_ptr<OrderBy> order;

        SelectStmt(const std::vector<std::shared_ptr<Col>> &cols_,
                   const std::vector<std::string> &tabs_,
                   const std::vector<std::shared_ptr<BinaryExpr>> &conds_,
                   std::vector<std::shared_ptr<Col>> groupby_,
                   std::vector<std::shared_ptr<BinaryExpr>> having_conds_,
                   std::shared_ptr<OrderBy> order_) : cols(std::move(cols_)), tabs(std::move(tabs_)), conds(std::move(conds_)),
                                                      groupby(std::move(groupby_)), having_conds(std::move(having_conds_)), order(std::move(order_))
        {
            has_sort = (bool)order;
            has_groupby = (!groupby.empty());
            has_having = (!having_conds.empty());
        }
        TreeNodeType Nodetype() const override { return TreeNodeType::SelectStmt; }
    };

    // set enable_nestloop
    struct SetStmt : public TreeNode
    {
        SetKnobType set_knob_type_;
        bool bool_val_;

        SetStmt(SetKnobType &type, bool bool_value) : set_knob_type_(type), bool_val_(bool_value) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::SetStmt; }
    };

    // Semantic value
    struct SemValue
    {
        int sv_int;
        float sv_float;
        std::string sv_str;
        bool sv_bool;
        OrderByDir sv_orderby_dir;
        std::vector<std::string> sv_strs;

        std::shared_ptr<TreeNode> sv_node;

        SvCompOp sv_comp_op;

        std::shared_ptr<TypeLen> sv_type_len;

        std::shared_ptr<Field> sv_field;
        std::vector<std::shared_ptr<Field>> sv_fields;

        std::shared_ptr<Expr> sv_expr;

        std::shared_ptr<Value> sv_val;
        std::vector<std::shared_ptr<Value>> sv_vals;

        std::shared_ptr<Col> sv_col;
        std::vector<std::shared_ptr<Col>> sv_cols;

        std::shared_ptr<SetClause> sv_set_clause;
        std::vector<std::shared_ptr<SetClause>> sv_set_clauses;

        std::shared_ptr<BinaryExpr> sv_cond;
        std::vector<std::shared_ptr<BinaryExpr>> sv_conds;

        std::shared_ptr<OrderBy> sv_orderby;

        SetKnobType sv_setKnobType;
    };

    extern std::shared_ptr<ast::TreeNode> parse_tree;

}

#define YYSTYPE ast::SemValue
