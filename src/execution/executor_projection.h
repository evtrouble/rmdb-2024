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
    size_t len_;                             // 字段总长度
                                             //    std::vector<size_t> sel_idxs_;

   public:
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols) 
        : prev_(std::move(prev))
    {
        size_t curr_offset = 0;
        auto &prev_cols = prev_->cols();
        cols_.reserve(sel_cols.size());
        for (auto &sel_col : sel_cols)
        {
            auto pos = get_col(prev_cols, sel_col);
            cols_.emplace_back(*pos);
            cols_.back().offset = curr_offset;
            curr_offset += cols_.back().len;
        }
        len_ = curr_offset;
    }

    const std::vector<ColMeta> &cols() const override
    {
        if (dynamic_cast<SeqScanExecutor *>(prev_.get()) != nullptr)
            return cols_;
        else if (dynamic_cast<NestedLoopJoinExecutor *>(prev_.get()) != nullptr)
            return cols_;
        else if (dynamic_cast<SortExecutor *>(prev_.get()) != nullptr)
            return cols_;
        else
            return prev_->cols();
    };

    void beginTuple() override { prev_->beginTuple(); }

    void nextTuple() override { prev_->nextTuple(); }

    std::unique_ptr<RmRecord> Next() override { return prev_->Next(); }

    bool is_end() const override { return prev_->is_end(); };

    Rid &rid() override { return _abstract_rid; }
};