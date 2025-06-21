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

#include <map>
#include <iostream>

#include "errors.h"
#include "execution/execution.h"
#include "parser/parser.h"
#include "system/sm.h"
#include "common/context.h"
#include "transaction/transaction_manager.h"
#include "planner.h"
#include "plan.h"
#include "execution/execution_manager.h"
#include "record/rm_file_handle.h"
#include <memory>
#include <vector>

// 添加TabCol的比较运算符
bool operator==(const TabCol &lhs, const TabCol &rhs)
{
    return lhs.tab_name == rhs.tab_name && lhs.col_name == rhs.col_name;
}

class Optimizer
{
private:
    SmManager *sm_manager_;
    Planner *planner_;
    std::map<std::string, std::unique_ptr<RmFileHandle>> fhs_;

public:
    Optimizer(SmManager *sm_manager, Planner *planner)
        : sm_manager_(sm_manager), planner_(planner)
    {
        // 从SmManager获取文件句柄
        for (const auto &[tab_name, fh] : sm_manager->fhs_)
        {
            fhs_[tab_name] = std::unique_ptr<RmFileHandle>(fh.get());
        }
    }

    std::shared_ptr<Plan> plan_query(std::shared_ptr<Query> query, Context *context)
    {
        switch (query->parse->Nodetype())
        {
        case ast::TreeNodeType::Help:
            return std::make_shared<OtherPlan>(T_Help);
        case ast::TreeNodeType::ShowTables:
            return std::make_shared<OtherPlan>(T_ShowTable);
        case ast::TreeNodeType::DescTable:
        {
            auto x = std::static_pointer_cast<ast::DescTable>(query->parse);
            return std::make_shared<OtherPlan>(T_DescTable, x->tab_name);
        }
        case ast::TreeNodeType::TxnBegin:
            return std::make_shared<OtherPlan>(T_Transaction_begin);
        case ast::TreeNodeType::TxnAbort:
            return std::make_shared<OtherPlan>(T_Transaction_abort);
        case ast::TreeNodeType::TxnCommit:
            return std::make_shared<OtherPlan>(T_Transaction_commit);
        case ast::TreeNodeType::TxnRollback:
            return std::make_shared<OtherPlan>(T_Transaction_rollback);
        case ast::TreeNodeType::CreateStaticCheckpoint:
            return std::make_shared<OtherPlan>(T_Create_StaticCheckPoint);
        case ast::TreeNodeType::SetStmt:
        {
            auto x = std::static_pointer_cast<ast::SetStmt>(query->parse);
            return std::make_shared<SetKnobPlan>(x->set_knob_type_, x->bool_val_);
        }
        case ast::TreeNodeType::ExplainStmt:
        {
            auto x = std::static_pointer_cast<ast::ExplainStmt>(query->parse);
            query->parse = x->query;
            auto subplan = plan_query(query, context);
            return std::make_shared<ExplainPlan>(T_Explain, subplan);
        }
        default:
            return planner_->do_planner(query, context);
        }
    }
};
