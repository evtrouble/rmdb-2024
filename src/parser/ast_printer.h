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

#include "ast.h"
#include <cassert>
#include <iostream>
#include <map>

namespace ast
{

    class TreePrinter
    {
    public:
        static void print(const std::shared_ptr<TreeNode> &node)
        {
            print_node(node, 0);
        }

    private:
        static std::string offset2string(int offset)
        {
            return std::string(offset, ' ');
        }

        template <typename T>
        static void print_val(const T &val, int offset)
        {
            std::cout << offset2string(offset) << val << '\n';
        }

        template <typename T>
        static void print_val_list(const std::vector<T> &vals, int offset)
        {
            std::cout << offset2string(offset) << "LIST\n";
            offset += 2;
            for (auto &val : vals)
            {
                print_val(val, offset);
            }
        }

        static std::string type2str(SvType type)
        {
            static std::map<SvType, std::string> m{
                {SV_TYPE_INT, "INT"},
                {SV_TYPE_FLOAT, "FLOAT"},
                {SV_TYPE_STRING, "STRING"}, 
                {SV_TYPE_DATETIME, "DATETIME"}
            };
            return m.at(type);
        }

        static std::string op2str(SvCompOp op)
        {
            static std::map<SvCompOp, std::string> m{
                {SV_OP_EQ, "=="},
                {SV_OP_NE, "!="},
                {SV_OP_LT, "<"},
                {SV_OP_GT, ">"},
                {SV_OP_LE, "<="},
                {SV_OP_GE, ">="},
            };
            return m.at(op);
        }

        template <typename T>
        static void print_node_list(std::vector<T> &nodes, int offset)
        {
            std::cout << offset2string(offset);
            offset += 2;
            std::cout << "LIST\n";
            for (auto &node : nodes)
            {
                print_node(node, offset);
            }
        }

        static void print_node(const std::shared_ptr<TreeNode> &node, int offset)
        {
            std::cout << offset2string(offset);
            offset += 2;
            switch (node->Nodetype())
            {
            case ast::TreeNodeType::Help:
                std::cout << "HELP\n";
                break;
            case ast::TreeNodeType::ShowTables:
                std::cout << "SHOW_TABLES\n";
                break;
            case ast::TreeNodeType::CreateTable:
            {
                auto x = std::static_pointer_cast<CreateTable>(node);
                std::cout << "CREATE_TABLE\n";
                print_val(x->tab_name, offset);
                print_node_list(x->fields, offset);
            }
            break;
            case ast::TreeNodeType::DropTable:
            {
                auto x = std::static_pointer_cast<DropTable>(node);
                std::cout << "DROP_TABLE\n";
                print_val(x->tab_name, offset);
            }
            break;
            case ast::TreeNodeType::DescTable:
            {
                auto x = std::static_pointer_cast<DescTable>(node);
                std::cout << "DESC_TABLE\n";
                print_val(x->tab_name, offset);
            }
            break;
            case ast::TreeNodeType::CreateIndex:
            {
                auto x = std::static_pointer_cast<CreateIndex>(node);
                std::cout << "CREATE_INDEX\n";
                print_val(x->tab_name, offset);
                // print_val(x->col_name, offset);
                for (auto &col_name : x->col_names)
                    print_val(col_name, offset);
            }
            break;
            case ast::TreeNodeType::DropIndex:
            {
                auto x = std::static_pointer_cast<DropIndex>(node);
                std::cout << "DROP_INDEX\n";
                print_val(x->tab_name, offset);
                // print_val(x->col_name, offset);
                for (auto &col_name : x->col_names)
                    print_val(col_name, offset);
            }
            break;
            case ast::TreeNodeType::ColDef:
            {
                auto x = std::static_pointer_cast<ColDef>(node);
                std::cout << "COL_DEF\n";
                print_val(x->col_name, offset);
                print_node(x->type_len, offset);
            }
            break;
            case ast::TreeNodeType::Col:
            {
                auto x = std::static_pointer_cast<Col>(node);
                std::cout << "COL\n";
                print_val(x->tab_name, offset);
                print_val(x->col_name, offset);
            }
            break;
            case ast::TreeNodeType::TypeLen:
            {
                auto x = std::static_pointer_cast<TypeLen>(node);
                std::cout << "TYPE_LEN\n";
                print_val(type2str(x->type), offset);
                print_val(x->len, offset);
            }
            break;
            case ast::TreeNodeType::IntLit:
            {
                auto x = std::static_pointer_cast<IntLit>(node);
                std::cout << "INT_LIT\n";
                print_val(x->val, offset);
            }
            break;
            case ast::TreeNodeType::FloatLit:
            {
                auto x = std::static_pointer_cast<FloatLit>(node);
                std::cout << "FLOAT_LIT\n";
                print_val(x->val, offset);
            }
            break;
            case ast::TreeNodeType::StringLit:
            {
                auto x = std::static_pointer_cast<StringLit>(node);
                std::cout << "STRING_LIT\n";
                print_val(x->val, offset);
            }
            break;
            case ast::TreeNodeType::SetClause:
            {
                auto x = std::static_pointer_cast<SetClause>(node);
                std::cout << "SET_CLAUSE\n";
                print_val(x->col_name, offset);
                print_node(x->val, offset);
            }
            break;
            case ast::TreeNodeType::BinaryExpr:
            {
                auto x = std::static_pointer_cast<BinaryExpr>(node);
                std::cout << "BINARY_EXPR\n";
                print_node(x->lhs, offset);
                print_val(op2str(x->op), offset);
                print_node(x->rhs, offset);
            }
            break;
            case ast::TreeNodeType::InsertStmt:
            {
                auto x = std::static_pointer_cast<InsertStmt>(node);
                std::cout << "INSERT\n";
                print_val(x->tab_name, offset);
                print_node_list(x->vals, offset);
            }
            break;
            case ast::TreeNodeType::DeleteStmt:
            {
                auto x = std::static_pointer_cast<DeleteStmt>(node);
                std::cout << "DELETE\n";
                print_val(x->tab_name, offset);
                print_node_list(x->conds, offset);
            }
            break;
            case ast::TreeNodeType::UpdateStmt:
            {
                auto x = std::static_pointer_cast<UpdateStmt>(node);
                std::cout << "UPDATE\n";
                print_val(x->tab_name, offset);
                print_node_list(x->set_clauses, offset);
                print_node_list(x->conds, offset);
            }
            break;
            case ast::TreeNodeType::SelectStmt:
            {
                auto x = std::static_pointer_cast<SelectStmt>(node);
                std::cout << "SELECT\n";
                print_node_list(x->cols, offset);
                print_val_list(x->tabs, offset);
                print_node_list(x->conds, offset);
            }
            break;
            case ast::TreeNodeType::SubqueryExpr:
            {
                auto x = std::static_pointer_cast<SubQueryExpr>(node);
                std::cout << "SUBQUERY_EXPR\n";
                print_node(x->lhs, offset);
                print_val(op2str(x->op), offset);
                print_node(x->subquery, offset);
            }
            break;
            case ast::TreeNodeType::TxnBegin:
                std::cout << "BEGIN\n";
                break;
            case ast::TreeNodeType::TxnCommit:
                std::cout << "COMMIT\n";
                break;
            case ast::TreeNodeType::TxnAbort:
                std::cout << "ABORT\n";
                break;
            case ast::TreeNodeType::TxnRollback:
                std::cout << "ROLLBACK\n";
                break;
            default:
                assert(0);
            }
        }
    };

}
