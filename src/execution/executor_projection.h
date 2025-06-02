#pragma once

#include <typeinfo>

#include "execution_defs.h"
#include "execution_manager.h"
#include "execution_sort.h"
#include "executor_abstract.h"
#include "executor_seq_scan.h"
#include "index/ix.h"
#include "system/sm.h"

class ProjectionExecutor : public AbstractExecutor
{
private:
    std::unique_ptr<AbstractExecutor> prev_; // 投影节点的儿子节点
    std::vector<ColMeta> cols_;              // 需要投影的字段

   public:
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols) 
        : prev_(std::move(prev))
    {
        auto &prev_cols = prev_->cols();
        cols_.reserve(sel_cols.size());
        for (auto &sel_col : sel_cols)
        {
            auto pos = get_col(prev_cols, sel_col);
            cols_.emplace_back(*pos);
        }
    }

    const std::vector<ColMeta> &cols() const override
    {
        switch (prev_->type())
        {
            case ExecutionType::SeqScan:
            case ExecutionType::NestedLoopJoin:
            case ExecutionType::Sort:
            case ExecutionType::SemiJoin:
                return cols_; 
            default:
                return prev_->cols();
        }
    };

    void beginTuple() override { prev_->beginTuple(); }

    void nextTuple() override { prev_->nextTuple(); }

    std::unique_ptr<RmRecord> Next() override { return prev_->Next(); }

    bool is_end() const override { return prev_->is_end(); };

    Rid &rid() override { return _abstract_rid; }

    ExecutionType type() const override { return ExecutionType::Projection; }
};