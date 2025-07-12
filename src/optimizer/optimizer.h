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

    std::unique_ptr<Plan> plan_query(std::unique_ptr<Query>& query, Context *context)
    {
        switch (query->parse->Nodetype())
        {
        case ast::TreeNodeType::Help:
            return std::make_unique<OtherPlan>(T_Help);
        case ast::TreeNodeType::ShowTables:
            return std::make_unique<OtherPlan>(T_ShowTable);
        case ast::TreeNodeType::DescTable:
        {
            auto x = static_cast<ast::DescTable*>(query->parse.get());
            return std::make_unique<OtherPlan>(T_DescTable, x->tab_name);
        }
        case ast::TreeNodeType::TxnBegin:
            return std::make_unique<OtherPlan>(T_Transaction_begin);
        case ast::TreeNodeType::TxnAbort:
            return std::make_unique<OtherPlan>(T_Transaction_abort);
        case ast::TreeNodeType::TxnCommit:
            return std::make_unique<OtherPlan>(T_Transaction_commit);
        case ast::TreeNodeType::TxnRollback:
            return std::make_unique<OtherPlan>(T_Transaction_rollback);
        case ast::TreeNodeType::CreateStaticCheckpoint:
            return std::make_unique<OtherPlan>(T_Create_StaticCheckPoint);
        case ast::TreeNodeType::SetStmt:
        {
            auto x = static_cast<ast::SetStmt*>(query->parse.get());
            return std::make_unique<SetKnobPlan>(x->set_knob_type_, x->bool_val_);
        }
        case ast::TreeNodeType::ExplainStmt:
        {
            auto subplan = plan_query(query->sub_query, context);
            return std::make_unique<ExplainPlan>(T_Explain, subplan);
        }
        case ast::TreeNodeType::LoadStmt:
        {
            auto x = static_cast<ast::LoadStmt*>(query->parse.get());
            return std::make_unique<OtherPlan>(T_LoadData, x->tab_name, x->file_name);
        }
        case ast::TreeNodeType::IoEnable:
        {
            auto x = static_cast<ast::IoEnable*>(query->parse.get());
            return std::make_unique<OtherPlan>(T_IoEnable, x->set_io_enable);
        }
        default:
            return planner_->do_planner(query, context);
        }
    }
};
