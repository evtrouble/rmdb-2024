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
#include "execution/executor_seq_cache_scan.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_index_cache_scan.h"
#include "execution/executor_update.h"
#include "execution/executor_insert.h"
#include "execution/executor_delete.h"
#include "execution/execution_sort.h"
#include "execution/executor_mergejoin.h"
#include "execution/executor_semijoin.h"
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
    PortalStmt(portalTag tag_, std::unique_ptr<AbstractExecutor> root_, std::shared_ptr<Plan> plan_) : tag(tag_), root(std::move(root_)), plan(std::move(plan_)) {}
    PortalStmt(portalTag tag_, std::shared_ptr<Plan> plan_) : tag(tag_), plan(std::move(plan_)) {}
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
        case PlanTag::T_Create_StaticCheckPoint:
            return std::make_shared<PortalStmt>(PORTAL_CMD_UTILITY, plan);
        case PlanTag::T_CreateTable:
        case PlanTag::T_DropTable:
        case PlanTag::T_CreateIndex:
        case PlanTag::T_DropIndex:
        case PlanTag::T_ShowIndex:
        case PlanTag::T_Explain:
            return std::make_shared<PortalStmt>(PORTAL_MULTI_QUERY, plan);
        case T_Select:
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
            auto batch = scan->rid_batch();

            while(batch.size()) {
                rids.insert(rids.end(), batch.begin(), batch.end());
                batch = scan->rid_batch();
            }
            std::unique_ptr<AbstractExecutor> root = std::make_unique<UpdateExecutor>(sm_manager_,
                                            x->tab_name_, x->set_clauses_, rids, context);
            return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::move(root), plan);
        }
        case T_Delete:
        {
            auto x = std::static_pointer_cast<DMLPlan>(plan);
            std::unique_ptr<AbstractExecutor> scan = convert_plan_executor(x->subplan_, context);
            std::vector<Rid> rids;
            auto batch = scan->rid_batch();

            while(batch.size()) {
                rids.insert(rids.end(), batch.begin(), batch.end());
                batch = scan->rid_batch();
            }
            std::unique_ptr<AbstractExecutor> root =
                std::make_unique<DeleteExecutor>(sm_manager_, x->tab_name_, rids, context);

            return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::move(root), plan);
        }
        case T_Insert:
        {
            auto x = std::static_pointer_cast<DMLPlan>(plan);
            std::unique_ptr<AbstractExecutor> root =
                std::make_unique<InsertExecutor>(sm_manager_, x->tab_name_, x->values_, context);

            return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::move(root), plan);
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
            // 检查子计划是否是 ScanPlan，如果是，将条件下推到 ScanPlan
            auto child_physical_plan = convert_plan_executor(x->subplan_, context);
            if(child_physical_plan->type() == ExecutionType::IndexScan || 
                child_physical_plan->type() == ExecutionType::SeqScan)
            {
                child_physical_plan->set_cols(x->sel_cols_);
                return child_physical_plan;
            }
            return std::make_unique<ProjectionExecutor>(std::move(child_physical_plan), x->sel_cols_);
        }
        case PlanTag::T_SeqScan:
        {
            auto x = std::static_pointer_cast<ScanPlan>(plan);
            if(context->hasJoinFlag()) {
                return std::make_unique<SeqCacheScanExecutor>(sm_manager_, x->tab_name_, x->fed_conds_, context);
            }
            return std::make_unique<SeqScanExecutor>(sm_manager_, x->tab_name_, x->fed_conds_, context);
        }
        case PlanTag::T_IndexScan:
        {
            auto x = std::static_pointer_cast<ScanPlan>(plan);
            if(context->hasJoinFlag()) {
                std::make_unique<IndexCacheScanExecutor>(sm_manager_, x->tab_name_, x->fed_conds_, x->index_meta_,
                                                       x->max_match_col_count_, context);
            }
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
        case PlanTag::T_SemiJoin:
        {
            auto x = std::static_pointer_cast<JoinPlan>(plan);
            std::unique_ptr<AbstractExecutor> left = convert_plan_executor(x->left_, context);
            std::unique_ptr<AbstractExecutor> right = convert_plan_executor(x->right_, context);
            std::unique_ptr<AbstractExecutor> join;
            context->setJoinFlag(true); // 设置 join 标志位
            return std::make_unique<SemiJoinExecutor>(std::move(left), std::move(right), std::move(x->conds_));
        }
        case PlanTag::T_Sort:
        {
            auto x = std::static_pointer_cast<SortPlan>(plan);
            return std::make_unique<SortExecutor>(convert_plan_executor(x->subplan_, context),
                                                  x->sel_cols_, x->is_desc_orders_, x->limit_, context);
        }
        case PlanTag::T_Agg:
        {
            auto x = std::static_pointer_cast<AggPlan>(plan);
            context->setAggFlag(true);
            return std::make_unique<AggExecutor>(convert_plan_executor(x->subplan_, context),
                                                 x->sel_cols_, x->groupby_cols_, x->having_conds_, context);
        }
        case PlanTag::T_Filter:
        {
            auto x = std::static_pointer_cast<FilterPlan>(plan);

            // 检查子计划是否是 ScanPlan，如果是，将条件下推到 ScanPlan
            auto child_plan = x->subplan_;
            if (child_plan->tag == PlanTag::T_SeqScan || child_plan->tag == PlanTag::T_IndexScan)
            {
                auto scan_plan = std::static_pointer_cast<ScanPlan>(child_plan);

                // 将 FilterPlan 的条件合并到 ScanPlan 的条件中
                std::vector<Condition> merged_conditions = scan_plan->fed_conds_;
                merged_conditions.insert(merged_conditions.end(),
                                         x->conds_.begin(), x->conds_.end());

                // 创建新的 ScanPlan，包含合并后的条件
                auto new_scan_plan = std::make_shared<ScanPlan>(
                    scan_plan->tag,
                    this->sm_manager_,
                    scan_plan->tab_name_,
                    merged_conditions,
                    scan_plan->index_meta_,
                    scan_plan->max_match_col_count_);
                // 递归处理新的 ScanPlan
                return convert_plan_executor(new_scan_plan, context);
            }
            else
            {

                // auto child_executor = convert_plan_executor(child_plan, context);
                // 临时解决方案
                throw InternalError("FilterExecutor not implemented for non-scan children");
            }
        }
        default:
            break;
        }
        return nullptr;
    }
};