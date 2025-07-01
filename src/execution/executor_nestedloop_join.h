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

class NestedLoopJoinExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件

    const size_t BLOCK_SIZE = 100;             // 缓冲区大小(记录数)
    std::vector<std::unique_ptr<RmRecord>> left_buffer_;  // 左表缓冲区
    std::vector<std::unique_ptr<RmRecord>> right_buffer_; // 右表缓冲区
    
    size_t left_block_idx_;      // 当前左表缓冲区的索引
    size_t right_block_idx_;     // 当前右表缓冲区的索引

    bool isend;

    // 填充左表缓冲区
    void fill_left_buffer() {
        left_buffer_.clear();
        while (!left_->is_end() && left_buffer_.size() < BLOCK_SIZE) {
            left_buffer_.emplace_back(left_->Next());
            left_->nextTuple();
        }
    }

    // 填充右表缓冲区
    void fill_right_buffer() {
        right_buffer_.clear();
        while (!right_->is_end() && right_buffer_.size() < BLOCK_SIZE) {
            right_buffer_.emplace_back(right_->Next());
            right_->nextTuple();
        }
    }

    void find_valid_tuples() {
        while (true) {
            // 如果左表缓冲区已遍历完，则填充新块（右表）
            if (left_block_idx_ >= left_buffer_.size()) {
                fill_right_buffer();
                left_block_idx_ = 0;
                // 如果右表全部遍历完了，则填充新块（左表），并重置右表
                if (right_buffer_.empty()) {
                    fill_left_buffer();
                    // 如果左表全部遍历完了，标志结束
                    if(left_buffer_.empty()){
                        isend = true;
                        return;
                    } else {
                        right_->beginTuple();
                        fill_right_buffer();
                    }
                }
            }
            
            // 如果右表缓冲区为空或已遍历完，则填充新块
            if (right_buffer_.empty() || right_block_idx_ >= right_buffer_.size()) {
                fill_right_buffer();
                right_block_idx_ = 0;
                if (right_buffer_.empty()) {
                    isend = true;
                    return;
                }
            }
            
            // 检查当前记录对是否满足条件
            bool is_valid = std::all_of(fed_conds_.begin(), fed_conds_.end(),
                [&](const Condition& cond) {
                    return check_cond(cond, left_buffer_[left_block_idx_], right_buffer_[right_block_idx_]);
                });
            
            if (is_valid) {
                return;
            }
            
            // 移动到下一对记录
            right_block_idx_++;
            if (right_block_idx_ >= right_buffer_.size()) {
                right_block_idx_ = 0;
                left_block_idx_++;
            }
        }
    }

    bool check_cond(const Condition& cond, 
                   const std::unique_ptr<RmRecord>& left_rec,
                   const std::unique_ptr<RmRecord>& right_rec) {
        auto left_col = left_->get_col(left_->cols(), cond.lhs_col);
        char* left_value = left_rec->data + left_col->offset;
        char* right_value;
        ColType right_type;
        
        if (cond.is_rhs_val) {
            right_type = cond.rhs_val.type;
            right_value = cond.rhs_val.raw->data;
        } else {
            auto rhs_col = right_->get_col(right_->cols(), cond.rhs_col);
            right_type = rhs_col->type;
            right_value = right_rec->data + rhs_col->offset;
        }
        return check_condition(left_value, left_col->type, right_value, right_type, cond.op,left_col->len);
    }

public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, 
                            const std::vector<Condition> &conds)
        : left_(std::move(left)), right_(std::move(right)), 
        fed_conds_(std::move(conds)), left_block_idx_(0), right_block_idx_(0), isend(false)
    {
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }
        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
    }

    void beginTuple() override {
        left_->beginTuple();
        right_->beginTuple();
        isend = false;
        isend = false;
        left_block_idx_ = 0;
        right_block_idx_ = 0;
        // 初始填充缓冲区
        fill_left_buffer();
        fill_right_buffer();
        find_valid_tuples();
    }

    void nextTuple() override {
        right_block_idx_++;
        if (static_cast<size_t>(right_block_idx_) >= right_buffer_.size()) {
            right_block_idx_ = 0;
            left_block_idx_++;
            if (static_cast<size_t>(left_block_idx_) >= left_buffer_.size()) {
                left_block_idx_ = 0;
                fill_right_buffer();
                if (right_buffer_.empty()) {
                    right_->beginTuple();
                    fill_left_buffer();
                    if(left_buffer_.empty()){
                        isend = true;
                        return;
                    }else {
                        fill_right_buffer();
                    }
                }
            }
        }
        find_valid_tuples();
    }

    std::unique_ptr<RmRecord> Next() override {
        auto record = std::make_unique<RmRecord>(len_);
        auto& left_rec = left_buffer_[left_block_idx_];
        auto& right_rec = right_buffer_[right_block_idx_];
        std::memcpy(record->data, left_rec->data, left_->tupleLen());
        std::memcpy(record->data + left_->tupleLen(), right_rec->data, right_->tupleLen());
        return record;
    }

    Rid &rid() override { return _abstract_rid; }

    bool is_end() const override { return isend; }
    
    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const { return cols_; }

    ExecutionType type() const override { return ExecutionType::NestedLoopJoin;  }
};
