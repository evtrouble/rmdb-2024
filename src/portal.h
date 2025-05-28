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

#include <cerrno>
#include <cstring>
#include <string>
#include "optimizer/plan.h"
#include "execution/executor_abstract.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_update.h"
#include "execution/executor_insert.h"
#include "execution/executor_delete.h"
#include "execution/execution_sort.h"
#include "execution/executor_mergejoin.h"
#include "execution/execution_agg.h"
#include "common/common.h"

typedef enum portalTag
{
    PORTAL_Invalid_Query = 0,
    PORTAL_ONE_SELECT,
    PORTAL_DML_WITHOUT_SELECT,
    PORTAL_MULTI_QUERY,
    PORTAL_CMD_UTILITY
} portalTag;

struct PortalStmt
{
    portalTag tag;

    std::vector<TabCol> sel_cols;
    std::unique_ptr<AbstractExecutor> root;
    std::shared_ptr<Plan> plan;

    PortalStmt(portalTag tag_, std::vector<TabCol> sel_cols_, std::unique_ptr<AbstractExecutor> root_, std::shared_ptr<Plan> plan_) : tag(tag_), sel_cols(std::move(sel_cols_)), root(std::move(root_)), plan(std::move(plan_)) {}
};

class Portal
{
private:
    SmManager *sm_manager_;

public:
    Portal(SmManager *sm_manager) : sm_manager_(sm_manager) {}
    ~Portal() {}

    // 将查询执行计划转换成对应的算子树
    std::shared_ptr<PortalStmt> start(std::shared_ptr<Plan> plan, Context *context)
    {
        // 这里可以将select进行拆分，例如：一个select，带有return的select等
        switch (plan->tag)
        {
        case PlanTag::T_Help:
        case PlanTag::T_ShowTable:
        case PlanTag::T_DescTable:
        case PlanTag::T_Transaction_begin:
        case PlanTag::T_Transaction_abort:
        case PlanTag::T_Transaction_commit:
        case PlanTag::T_Transaction_rollback:
        case PlanTag::T_SetKnob:
            return std::make_shared<PortalStmt>(PORTAL_CMD_UTILITY, std::vector<TabCol>(), std::unique_ptr<AbstractExecutor>(), plan);
        case PlanTag::T_CreateTable:
        case PlanTag::T_DropTable:
        case PlanTag::T_CreateIndex:
        case PlanTag::T_DropIndex:
        case PlanTag::T_ShowIndex:
            return std::make_shared<PortalStmt>(PORTAL_MULTI_QUERY, std::vector<TabCol>(), std::unique_ptr<AbstractExecutor>(), plan);
        case T_select:
        {
            auto x = std::static_pointer_cast<DMLPlan>(plan);
            std::shared_ptr<ProjectionPlan> p = std::static_pointer_cast<ProjectionPlan>(x->subplan_);
            std::unique_ptr<AbstractExecutor> root = convert_plan_executor(p, context);
            return std::make_shared<PortalStmt>(PORTAL_ONE_SELECT, std::move(p->sel_cols_), std::move(root), plan);
        }
        case T_Update:
        {
            auto x = std::static_pointer_cast<DMLPlan>(plan);
            std::unique_ptr<AbstractExecutor> scan = convert_plan_executor(x->subplan_, context);
            std::vector<Rid> rids;
            for (scan->beginTuple(); !scan->is_end(); scan->nextTuple())
            {
                rids.emplace_back(scan->rid());
            }
            std::unique_ptr<AbstractExecutor> root = std::make_unique<UpdateExecutor>(sm_manager_,
                                                                                      x->tab_name_, x->set_clauses_, rids, context);
            return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(), std::move(root), plan);
        }
        case T_Delete:
        {
            auto x = std::static_pointer_cast<DMLPlan>(plan);
            std::unique_ptr<AbstractExecutor> scan = convert_plan_executor(x->subplan_, context);
            std::vector<Rid> rids;
            for (scan->beginTuple(); !scan->is_end(); scan->nextTuple())
            {
                rids.emplace_back(scan->rid());
            }

            std::unique_ptr<AbstractExecutor> root =
                std::make_unique<DeleteExecutor>(sm_manager_, x->tab_name_, rids, context);

            return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(), std::move(root), plan);
        }
        case T_Insert:
        {
            auto x = std::static_pointer_cast<DMLPlan>(plan);
            std::unique_ptr<AbstractExecutor> root =
                std::make_unique<InsertExecutor>(sm_manager_, x->tab_name_, x->values_, context);

            return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(), std::move(root), plan);
        }
        default:
            throw InternalError("Unexpected field type");
            break;
        }

        return nullptr;
    }

    // 遍历算子树并执行算子生成执行结果
    void run(std::shared_ptr<PortalStmt> portal, QlManager *ql, txn_id_t *txn_id, Context *context)
    {
        switch (portal->tag)
        {
        case PORTAL_ONE_SELECT:
        {
            ql->select_from(std::move(portal->root), std::move(portal->sel_cols), context);
            break;
        }

        case PORTAL_DML_WITHOUT_SELECT:
        {
            ql->run_dml(std::move(portal->root));
            break;
        }
        case PORTAL_MULTI_QUERY:
        {
            ql->run_mutli_query(portal->plan, context);
            break;
        }
        case PORTAL_CMD_UTILITY:
        {
            ql->run_cmd_utility(portal->plan, txn_id, context);
            break;
        }
        default:
        {
            throw InternalError("Unexpected field type");
        }
        }
    }

    // 清空资源
    void drop() {}

    std::unique_ptr<AbstractExecutor> convert_plan_executor(std::shared_ptr<Plan> plan, Context *context)
    {
        switch (plan->tag)
        {
        case PlanTag::T_Projection:
        {
            auto x = std::static_pointer_cast<ProjectionPlan>(plan);
            return std::make_unique<ProjectionExecutor>(convert_plan_executor(x->subplan_, context),
                                                        x->sel_cols_);
        }
        case PlanTag::T_SeqScan:
        {
            auto x = std::static_pointer_cast<ScanPlan>(plan);
            // 处理条件里面的子查询
            // for (auto &cond : x->conds_)
            // {
            //     if (!cond.is_subquery || cond.subQuery->stmt == nullptr)
            //         continue;
            //     // 如果条件左边是浮点数，右边是整数，需要转换
            //     bool convert = false;
            //     TabMeta &tab = sm_manager_->db_.get_table(x->tab_name_);
            //     auto lhs_col = tab.get_col(cond.lhs_col.col_name);
            //     if (lhs_col->type == TYPE_FLOAT && cond.subQuery->subquery_type == TYPE_INT)
            //         convert = true;
            //     cond.subQuery->result = QlManager::sub_select_from(std::move(start(cond.subQuery->plan, context)->root), convert);
            //     // 如果是标量子查询，结果集大小不为1，报错
            //     if (cond.subQuery->is_scalar && cond.subQuery->result.size() != 1)
            //     {
            //         throw RMDBError("Scalar subquery result size is not 1");
            //     }
            // }

            return std::make_unique<SeqScanExecutor>(sm_manager_, x->tab_name_, x->fed_conds_, context);
        }
        case PlanTag::T_IndexScan:
        {
            auto x = std::static_pointer_cast<ScanPlan>(plan);
            // 处理条件里面的子查询
            // for (auto &cond : x->conds_)
            // {
            //     if (!cond.is_subquery || cond.subQuery->stmt == nullptr)
            //         continue;
            //     // 如果条件左边是浮点数，右边是整数，需要转换
            //     bool convert = false;
            //     TabMeta &tab = sm_manager_->db_.get_table(x->tab_name_);
            //     auto lhs_col = tab.get_col(cond.lhs_col.col_name);
            //     if (lhs_col->type == TYPE_FLOAT && cond.subQuery->subquery_type == TYPE_INT)
            //         convert = true;
            //     cond.subQuery->result = QlManager::sub_select_from(std::move(start(cond.subQuery->plan, context)->root), convert);
            //     // 如果是标量子查询，结果集大小不为1，报错
            //     if (cond.subQuery->is_scalar && cond.subQuery->result.size() != 1)
            //     {
            //         throw RMDBError("Scalar subquery result size is not 1");
            //     }
            // }
            return std::make_unique<IndexScanExecutor>(sm_manager_, x->tab_name_, x->fed_conds_, x->index_meta_, 
                x->max_match_col_count_, context);
        }
        case PlanTag::T_NestLoop:
        {
            auto x = std::static_pointer_cast<JoinPlan>(plan);
            std::unique_ptr<AbstractExecutor> left = convert_plan_executor(x->left_, context);
            std::unique_ptr<AbstractExecutor> right = convert_plan_executor(x->right_, context);
            std::unique_ptr<AbstractExecutor> join;
            context->setJoinFlag(true); // 设置 join 标志位
            return std::make_unique<NestedLoopJoinExecutor>(std::move(left), std::move(right), std::move(x->conds_));
        }
        case PlanTag::T_SortMerge:
        {
            auto x = std::static_pointer_cast<JoinPlan>(plan);
            std::unique_ptr<AbstractExecutor> left = convert_plan_executor(x->left_, context);
            std::unique_ptr<AbstractExecutor> right = convert_plan_executor(x->right_, context);
            std::unique_ptr<AbstractExecutor> join;
            context->setJoinFlag(true); // 设置 join 标志位
            return std::make_unique<MergeJoinExecutor>(std::move(left), std::move(right), std::move(x->conds_));
        }
        case PlanTag::T_Sort:
        {
            auto x = std::static_pointer_cast<SortPlan>(plan);
            return std::make_unique<SortExecutor>(convert_plan_executor(x->subplan_, context),
                                                  x->sel_cols_, x->is_desc_, x->limit_, context);
        }
        case PlanTag::T_Agg:
        {
            auto x = std::static_pointer_cast<AggPlan>(plan);
            return std::make_unique<AggExecutor>(convert_plan_executor(x->subplan_, context),
                                                 x->sel_cols_, x->groupby_cols_, x->having_conds_, context);
        }
        default:
            break;
        }
        return nullptr;
    }
};