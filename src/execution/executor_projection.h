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
    std::vector<size_t> col_indices_;        // 在原始记录中的列索引
    size_t tuple_len_;                       // 投影后记录的长度

    // 计算投影后的记录长度和列偏移
    void calculate_projection_info()
    {
        tuple_len_ = 0;
        for (auto &col : cols_)
        {
            // 重新计算投影后的偏移
            col.offset = tuple_len_;
            tuple_len_ += col.len;
        }
    }

public:
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols)
        : prev_(std::move(prev))
    {
        auto &prev_cols = prev_->cols();
        cols_.reserve(sel_cols.size());
        col_indices_.reserve(sel_cols.size());

        for (auto &sel_col : sel_cols)
        {
            auto pos = get_col(prev_cols, sel_col, true);

            cols_.emplace_back(*pos);
            col_indices_.emplace_back(pos - prev_cols.begin());
        }
        calculate_projection_info();
    }
    const std::vector<ColMeta> &cols() const override
    {
        return cols_;
    }

    void beginTuple() override { prev_->beginTuple(); }

    void nextTuple() override { prev_->nextTuple(); }
    size_t tupleLen() const override { return tuple_len_; };

    std::unique_ptr<RmRecord> Next() override
    {
        auto prev_record = prev_->Next();
        if (!prev_record)
        {
            return nullptr;
        }

        // 创建新的投影记录
        auto projected_record = std::make_unique<RmRecord>(tuple_len_);

        // 获取原始记录的列信息
        const auto &prev_cols = prev_->cols();

        // 复制选定的列到新记录中
        for (size_t i = 0; i < cols_.size(); ++i)
        {
            const auto &src_col = prev_cols[col_indices_[i]];
            const auto &dst_col = cols_[i];

            // 从原记录复制数据到新记录
            memcpy(projected_record->data + dst_col.offset,
                   prev_record->data + src_col.offset,
                   src_col.len);
        }

        return projected_record;
    }
    bool is_end() const override { return prev_->is_end(); };

    Rid &rid() override { return _abstract_rid; }

    ExecutionType type() const override { return ExecutionType::Projection; }
};