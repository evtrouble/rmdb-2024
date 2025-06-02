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

class SemiJoinExecutor : public AbstractExecutor
{
private:
    std::unique_ptr<AbstractExecutor> left_;  // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_; // 右儿子节点（需要join的表）
    size_t len_;                              // 结果记录的长度（只包含左表的列）
    std::vector<ColMeta> cols_;               // 结果记录的字段（只包含左表的列）

    std::vector<Condition> fed_conds_; // join条件
    std::unique_ptr<RmRecord> left_current_;
    std::unique_ptr<RmRecord> right_current_;
    bool isend;
    
    // 标记左表当前行是否已经匹配过
    bool current_left_matched_;
    
    void find_valid_tuples()
    {
        current_left_matched_ = false;
        while (!left_->is_end())
        { 
            // 重置右表迭代器
            right_->beginTuple();
            right_current_ = right_->Next();
            
            // 检查当前左表行是否在右表中有匹配
            while (!right_->is_end())
            {
                bool is_valid = std::all_of(fed_conds_.begin(), fed_conds_.end(),
                                            [&](const Condition &cond)
                                            { return check_cond(cond); });
                
                if (is_valid)
                {
                    current_left_matched_ = true;
                    break; // 只要找到一个匹配即可
                }
                else
                {
                    right_->nextTuple();
                    right_current_ = right_->Next();
                }
            }
            
            // 如果当前左表行有匹配，则返回
            if (current_left_matched_)
            {
                return;
            }
            
            // 否则继续检查下一行左表
            left_->nextTuple();
            left_current_ = left_->Next();
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
    SemiJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                     const std::vector<Condition> &conds)
        : left_(std::move(left)), right_(std::move(right)),
          fed_conds_(std::move(conds)), isend(false), current_left_matched_(false)
    {
        // 只保留左表的列
        len_ = left_->tupleLen();
        cols_ = left_->cols();
    }

    void beginTuple() override
    {
        left_->beginTuple();
        right_->beginTuple();
        isend = false;
        current_left_matched_ = false;
        left_current_ = left_->Next();
        right_current_ = right_->Next();
        find_valid_tuples();
    }

    void nextTuple() override
    {
        // 移动到左表下一行
        left_->nextTuple();
        left_current_ = left_->Next();
        find_valid_tuples();
    }

    std::unique_ptr<RmRecord> Next() override
    {
        // 只返回左表的记录
        auto record = std::make_unique<RmRecord>(len_);
        std::memcpy(record->data, left_current_->data, len_);
        return record;
    }

    Rid &rid() override { return _abstract_rid; }

    bool is_end() const override { return isend; }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const { return cols_; }

    ExecutionType type() const override { return ExecutionType::SemiJoin; }
};