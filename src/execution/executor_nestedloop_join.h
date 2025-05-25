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
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class NestedLoopJoinExecutor : public AbstractExecutor
{
private:
    std::unique_ptr<AbstractExecutor> left_;  // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_; // 右儿子节点（需要join的表）
    size_t len_;                              // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;               // join后获得的记录的字段

    std::vector<Condition> fed_conds_; // join条件
    std::unique_ptr<RmRecord> left_current_;
    std::unique_ptr<RmRecord> right_current_;
    bool isend;
    void find_valid_tuples()
    {
        while (!left_->is_end())
        {
            while (!right_->is_end())
            {
                bool is_valid = std::all_of(fed_conds_.begin(), fed_conds_.end(),
                                            [&](const Condition &cond)
                                            { return check_cond(cond); });
                if (is_valid)
                {
                    return;
                }
                else
                {
                    right_->nextTuple();
                    right_current_ = right_->Next();
                }
            }
            left_->nextTuple();
            right_->beginTuple();
            left_current_ = left_->Next();
            right_current_ = right_->Next();
        }
        isend = true;
    }
    bool check_cond(const Condition &cond)
    {
        auto left_col = left_->get_col(left_->cols(), cond.lhs_col);
        char *left_value = left_current_->data + left_col->offset;
        char *right_value;
        ColType right_type;
        if (cond.is_rhs_val)
        {
            right_type = cond.rhs_val.type;
            right_value = cond.rhs_val.raw->data;
        }
        else
        {
            auto rhs_col = right_->get_col(right_->cols(), cond.rhs_col);
            right_type = rhs_col->type;
            right_value = right_current_->data + rhs_col->offset;
        }
        return check_condition(left_value, left_col->type, right_value, right_type, cond.op, left_col->len);
    }

public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                           const std::vector<Condition> &conds)
        : left_(std::move(left)), right_(std::move(right)),
          fed_conds_(std::move(conds)), isend(false)
    {
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols)
        {
            col.offset += left_->tupleLen();
        }
        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
    }

    void beginTuple() override
    {
        left_->beginTuple();
        right_->beginTuple();
        isend = false;
        left_current_ = left_->Next();
        right_current_ = right_->Next();
        find_valid_tuples();
    }

    void nextTuple() override
    {
        if (right_->is_end())
        {
            left_->nextTuple();
            right_->beginTuple();
            left_current_ = left_->Next();
            right_current_ = right_->Next();
        }
        else
        {
            right_->nextTuple();
            right_current_ = right_->Next();
        }
        find_valid_tuples();
    }

    std::unique_ptr<RmRecord> Next() override
    {
        auto record = std::make_unique<RmRecord>(len_);
        std::memcpy(record->data, left_current_->data, left_->tupleLen());
        std::memcpy(record->data + left_->tupleLen(), right_current_->data, right_->tupleLen());
        return record;
    }

    Rid &rid() override { return _abstract_rid; }

    bool is_end() const override { return isend; }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const { return cols_; }

    ExecutionType type() const override { return ExecutionType::NestedLoopJoin; }
};