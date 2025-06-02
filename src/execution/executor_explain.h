#pragma once

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class ExplainExecutor : public AbstractExecutor
{
private:
    std::shared_ptr<Plan> plan_;
    bool is_first_;

public:
    ExplainExecutor(std::shared_ptr<Plan> plan) : plan_(plan), is_first_(true) {}

    std::string getType() override { return "ExplainExecutor"; }

    ExecutionType type() const override { return ExecutionType::EXPLAIN; }

    bool is_end() const override { return !is_first_; }

    Rid &rid() override { return _abstract_rid; }

    std::string plan_to_string(std::shared_ptr<Plan> plan, int indent = 0)
    {
        std::string result;
        std::string indent_str(indent * 2, ' ');

        switch (plan->tag)
        {
        case T_select:
            result = indent_str + "SELECT\n";
            break;
        case T_SeqScan:
            result = indent_str + "SEQ SCAN\n";
            break;
        case T_IndexScan:
            result = indent_str + "INDEX SCAN\n";
            break;
        case T_NestLoop:
            result = indent_str + "NESTED LOOP JOIN\n";
            if (auto join_plan = std::dynamic_pointer_cast<JoinPlan>(plan))
            {
                result += plan_to_string(join_plan->left_, indent + 1);
                result += plan_to_string(join_plan->right_, indent + 1);
            }
            break;
        case T_SortMerge:
            result = indent_str + "SORT MERGE JOIN\n";
            if (auto join_plan = std::dynamic_pointer_cast<JoinPlan>(plan))
            {
                result += plan_to_string(join_plan->left_, indent + 1);
                result += plan_to_string(join_plan->right_, indent + 1);
            }
            break;
        case T_Sort:
            result = indent_str + "SORT\n";
            if (auto sort_plan = std::dynamic_pointer_cast<SortPlan>(plan))
            {
                result += plan_to_string(sort_plan->subplan_, indent + 1);
            }
            break;
        case T_Projection:
            result = indent_str + "PROJECTION\n";
            if (auto proj_plan = std::dynamic_pointer_cast<ProjectionPlan>(plan))
            {
                result += plan_to_string(proj_plan->subplan_, indent + 1);
            }
            break;
        default:
            result = indent_str + "UNKNOWN\n";
            break;
        }
        return result;
    }

    std::unique_ptr<RmRecord> Next() override
    {
        if (!is_first_)
        {
            return nullptr;
        }
        is_first_ = false;

        std::string plan_str = "EXPLAIN\n" + plan_to_string(plan_, 1);
        int len = plan_str.length();
        auto record = std::make_unique<RmRecord>(len);
        memcpy(record->data, plan_str.c_str(), len);
        return record;
    }
};