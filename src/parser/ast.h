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
#include <unordered_map>
enum JoinType
{
    INNER_JOIN,
    LEFT_JOIN,
    RIGHT_JOIN,
    FULL_JOIN,
    SEMI_JOIN
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
        MIN,
        AVG
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

    enum UpdateOp
    {
        SELF_ADD = 0,
        SELF_SUB,
        SELF_MUT,
        SELF_DIV,
        ASSINGMENT,
        UNKNOWN
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
        CreateStaticCheckpoint,
        ExplainStmt,
        LoadStmt,
        IoEnable
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
        std::unique_ptr<TreeNode> query;

        ExplainStmt(TreeNode* query_) : query(query_) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::ExplainStmt; }
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
        TypeLen type_len;

        ColDef(std::string &col_name_, TypeLen& type_len_) : col_name(std::move(col_name_)), type_len(type_len_) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::ColDef; }
    };

    struct CreateTable : public TreeNode
    {
        std::string tab_name;
        std::vector<std::unique_ptr<Field>> fields;

        CreateTable(std::string &tab_name_, std::vector<std::unique_ptr<Field>>& fields_) 
            : tab_name(std::move(tab_name_)), fields(std::move(fields_)) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::CreateTable; }
    };

    struct DropTable : public TreeNode
    {
        std::string tab_name;

        DropTable(std::string &tab_name_) : tab_name(std::move(tab_name_)) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::DropTable; }
    };

    struct DescTable : public TreeNode
    {
        std::string tab_name;

        DescTable(std::string &tab_name_) : tab_name(std::move(tab_name_)) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::DescTable; }
    };

    struct CreateIndex : public TreeNode
    {
        std::string tab_name;
        std::vector<std::string> col_names;

        CreateIndex(std::string &tab_name_, std::vector<std::string> &col_names_) : tab_name(std::move(tab_name_)), col_names(std::move(col_names_)) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::CreateIndex; }
    };

    struct DropIndex : public TreeNode
    {
        std::string tab_name;
        std::vector<std::string> col_names;

        DropIndex(std::string &tab_name_, std::vector<std::string> &col_names_) : tab_name(std::move(tab_name_)), col_names(std::move(col_names_)) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::DropIndex; }
    };

    struct ShowIndex : public TreeNode
    {
        std::string tab_name;

        ShowIndex(std::string &tab_name_) : tab_name(std::move(tab_name_)) {};
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

        StringLit(std::string &val_) : val(std::move(val_)) {}
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

        Col(std::string &tab_name_, std::string &col_name_) : tab_name(std::move(tab_name_)), col_name(std::move(col_name_)) {}
        Col(std::string &col_name_) : col_name(std::move(col_name_)) {}

        Col(std::string tab_name_, std::string col_name_, AggFuncType agg_type) 
            : tab_name(std::move(tab_name_)), col_name(std::move(col_name_)), agg_type(agg_type) {}

        TreeNodeType Nodetype() const override { return TreeNodeType::Col; }
    };

    struct SetClause : public TreeNode
    {
        std::string col_name;
        std::unique_ptr<Value> val;
        UpdateOp op;
        // 构造函数
        SetClause(std::string &col_name_, Value* val_, UpdateOp op) 
            : col_name(std::move(col_name_)), val(val_), op(op) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::SetClause; }
    };

    struct BinaryExpr : public TreeNode
    {
        Col lhs;
        SvCompOp op;
        std::unique_ptr<Expr> rhs;

        BinaryExpr(Col& lhs_, SvCompOp op_, Expr* rhs_) : lhs(std::move(lhs_)), op(op_), rhs(rhs_) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::BinaryExpr; }
    };

    struct OrderBy : public TreeNode
    {
        std::vector<Col> cols;
        std::vector<OrderByDir> dirs;
        OrderBy() = default;

        // 添加单个列和方向的构造函数
        OrderBy(Col& col, OrderByDir dir)
        {
            cols.emplace_back(std::move(col));
            dirs.emplace_back(dir);
        }

        // 添加多个列和方向的构造函数
        OrderBy(std::vector<Col>& cols_, std::vector<OrderByDir>& dirs_)
            : cols(std::move(cols_)), dirs(std::move(dirs_)) {}

        TreeNodeType Nodetype() const override { return TreeNodeType::OrderBy; }

        // 添加一个列和方向的方法
        void addItem(Col& col, OrderByDir dir)
        {
            cols.emplace_back(std::move(col));
            dirs.emplace_back(dir);
        }
    };

    struct InsertStmt : public TreeNode
    {
        std::string tab_name;
        std::vector<std::unique_ptr<Value>> vals;

        InsertStmt(std::string &tab_name_, std::vector<std::unique_ptr<Value>> &vals_) 
        : tab_name(std::move(tab_name_)), vals(std::move(vals_)) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::InsertStmt; }
    };
    struct LoadStmt : public TreeNode
    {
        std::string file_name;
        std::string tab_name;

        LoadStmt(std::string& file_name_, std::string& table_name_) : file_name(std::move(file_name_)), tab_name(std::move(table_name_)) {}
        TreeNodeType Nodetype() const override
        {
            return TreeNodeType::LoadStmt; // 或者对应的枚举值
        }
    };
    struct DeleteStmt : public TreeNode
    {
        std::string tab_name;
        std::vector<BinaryExpr> conds;

        DeleteStmt(std::string &tab_name_) : tab_name(std::move(tab_name_)) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::DeleteStmt; }
    };
    struct IoEnable : public TreeNode
    {
        bool set_io_enable;
        explicit IoEnable(bool set_io_enable_) : set_io_enable(set_io_enable_) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::IoEnable; }
    };

    struct UpdateStmt : public TreeNode
    {
        std::string tab_name;
        std::vector<SetClause> set_clauses;
        std::vector<BinaryExpr> conds;

        UpdateStmt(std::string &tab_name_,
                std::vector<SetClause> &set_clauses_) : tab_name(std::move(tab_name_)), set_clauses(std::move(set_clauses_)) {}
        TreeNodeType Nodetype() const override { return TreeNodeType::UpdateStmt; }
    };

    struct JoinExpr : public TreeNode
    {
        std::string left;
        std::string right;
        std::vector<BinaryExpr> conds;
        JoinType type;

        // 添加默认构造函数
        JoinExpr() : type(INNER_JOIN) {}

        JoinExpr(std::string left_, std::string right_, JoinType type_)
            : left(std::move(left_)), right(std::move(right_)),
              type(type_) {}

        TreeNodeType Nodetype() const override { return TreeNodeType::JoinExpr; }
    };

    struct SelectStmt : public TreeNode
    {
        std::vector<Col> cols;
        std::vector<std::string> tabs;
        std::vector<BinaryExpr> conds;
        std::vector<JoinExpr> jointree;
        std::vector<Col> groupby;
        std::vector<BinaryExpr> having_conds;
        OrderBy order;
        int limit = -1;
        std::vector<std::string> tab_aliases;

        bool has_agg = false;

        SelectStmt(std::vector<Col> &cols_,
                std::vector<std::string> &tabs_,
                std::vector<JoinExpr> &jointree_,
                int limit_ = -1)
            : cols(std::move(cols_)), tabs(std::move(tabs_)),
              jointree(std::move(jointree_)), limit(limit_)
        {
            // 确保 tab_aliases 的大小与 tabs 相同
            if (tab_aliases.size() < tabs.size())
            {
                tab_aliases.resize(tabs.size());
            }

            // 检查列中是否包含聚合函数
            for (const auto &col : cols)
            {
                if (col.agg_type != NO_TYPE)
                {
                    has_agg = true;
                    break;
                }
            }
        }

        // 检查是否是 SELECT * 查询
        bool is_select_star_query() const
        {
            // 情况1：cols 向量为空（隐式的 SELECT *）
            if (cols.empty())
                return true;

            // 情况2：只有一个列且列名为 "*"，且没有指定表名
            if (cols.size() == 1 &&
                cols[0].col_name == "*" &&
                cols[0].tab_name.empty())
            {
                return true;
            }

            return false;
        }

        // 获取表的实际使用名称（如果有别名则返回别名，否则返回原表名）
        std::string get_table_name(size_t index) const
        {
            if (index >= tabs.size())
                return "";
            return tab_aliases[index].empty() ? tabs[index] : tab_aliases[index];
        }

        // 根据原表名或别名查找其对应的原表名
        std::string find_original_table(const std::string &name) const
        {
            // 先检查是否是原表名
            for (size_t i = 0; i < tabs.size(); ++i)
            {
                if (tabs[i] == name)
                {
                    return name;
                }
            }
            // 再检查是否是别名
            for (size_t i = 0; i < tab_aliases.size(); ++i)
            {
                if (tab_aliases[i] == name)
                {
                    return tabs[i];
                }
            }
            return name; // 如果都找不到，返回原名
        }

        // 根据原表名查找其别名（如果有的话）
        std::string find_alias(const std::string &table_name) const
        {
            for (size_t i = 0; i < tabs.size(); ++i)
            {
                if (tabs[i] == table_name && i < tab_aliases.size() && !tab_aliases[i].empty())
                {
                    return tab_aliases[i];
                }
            }
            return table_name; // 如果没有别名，返回原表名
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

    struct TableList {
        std::vector<std::string> tables;
        std::vector<std::string> aliases;
        std::vector<JoinExpr> jointree;
    };

    union SemValue {
        int sv_int;
        float sv_float;
        // char* sv_raw_str;
        std::string* sv_str;
        bool sv_bool;
        OrderByDir sv_orderby_dir;
        std::vector<std::string>* sv_strs;

        TreeNode* sv_node;

        SvCompOp sv_comp_op;

        TypeLen* sv_type_len;

        Field* sv_field;
        std::vector<std::unique_ptr<Field>>* sv_fields;

        Expr* sv_expr;

        Value* sv_val;
        std::vector<std::unique_ptr<Value>>* sv_vals;

        Col* sv_col;
        std::vector<Col>* sv_cols;

        SetClause* sv_set_clause;
        std::vector<SetClause>* sv_set_clauses;

        BinaryExpr* sv_cond;
        std::vector<BinaryExpr>* sv_conds;

        OrderBy* sv_orderby;
        std::pair<Col, OrderByDir>* sv_order_item;
        SetKnobType sv_setKnobType;
        TableList* sv_table_list;
    };
    // Semantic value
    // struct SemValue
    // {
    //     int sv_int;
    //     float sv_float;
    //     std::string sv_str;
    //     bool sv_bool;
    //     OrderByDir sv_orderby_dir;
    //     std::vector<std::string> sv_strs;
    //     std::vector<std::string> sv_aliases;

    //     std::shared_ptr<TreeNode> sv_node;

    //     SvCompOp sv_comp_op;

    //     std::shared_ptr<TypeLen> sv_type_len;

    //     std::shared_ptr<ColDef> sv_ColDef;
    //     std::vector<std::shared_ptr<ColDef>> sv_ColDefs;

    //     std::shared_ptr<Expr> sv_expr;

    //     std::shared_ptr<Value> sv_val;
    //     std::vector<std::shared_ptr<Value>> sv_vals;

    //     std::shared_ptr<Col> sv_col;
    //     std::vector<std::shared_ptr<Col>> sv_cols;

    //     std::shared_ptr<SetClause> sv_set_clause;
    //     std::vector<std::shared_ptr<SetClause>> sv_set_clauses;

    //     std::shared_ptr<BinaryExpr> sv_cond;
    //     std::vector<std::shared_ptr<BinaryExpr>> sv_conds;

    //     std::shared_ptr<OrderBy> sv_orderby;
    //     std::pair<std::shared_ptr<Col>, OrderByDir> sv_order_item;
    //     SetKnobType sv_setKnobType;
    //     std::shared_ptr<TableList> sv_table_list;
    // };

    struct CreateStaticCheckpoint : public TreeNode
    {
    public:
        virtual TreeNodeType Nodetype() const override { return TreeNodeType::CreateStaticCheckpoint; }
    };

    extern std::unique_ptr<ast::TreeNode> parse_tree;

}
#define YYSTYPE ast::SemValue
