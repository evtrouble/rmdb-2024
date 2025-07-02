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
    // 迭代状态
    std::vector<std::unique_ptr<RmRecord>> current_left_batch_;
    size_t left_batch_index_ = 0;
    std::vector<std::unique_ptr<RmRecord>> current_right_batch_;
    size_t right_batch_index_ = 0;
    bool is_end_ = false;
    bool is_initialized_ = false;

    // 检查连接条件
    bool check_cond(const std::unique_ptr<RmRecord>& left_rec, 
                   const std::unique_ptr<RmRecord>& right_rec,
                   const Condition &cond) {
        char *lhs_value;
        ColType lhs_type;
        int lhs_len;
        auto left_col = left_->get_col(left_->cols(), cond.lhs_col);
        lhs_value = left_rec->data + left_col->offset;
        lhs_type = left_col->type;
        lhs_len = left_col->len;
        char *rhs_value;
        ColType rhs_type;
        int rhs_len = lhs_len;

        if (cond.is_rhs_val) {
            rhs_type = cond.rhs_val.type;
            rhs_value = cond.rhs_val.raw->data;
        } else {
            auto rhs_col = right_->get_col(right_->cols(), cond.rhs_col);
            rhs_value = right_rec->data + rhs_col->offset;
            rhs_type = rhs_col->type;
            rhs_len = rhs_col->len;
        }
        int compare_len = (lhs_type == TYPE_STRING && rhs_type == TYPE_STRING) 
                        ? std::min(lhs_len, rhs_len) : std::max(lhs_len, rhs_len);
        return check_condition(lhs_value, lhs_type, rhs_value, rhs_type, cond.op, compare_len);
    }

    // 获取下一批左表数据
    bool fetch_next_left_batch() {
        current_left_batch_ = left_->next_batch();
        left_batch_index_ = 0;
        return !current_left_batch_.empty();
    }

    // 获取下一批右表数据
    bool fetch_next_right_batch() {
        current_right_batch_ = right_->next_batch();
        right_batch_index_ = 0;
        return !current_right_batch_.empty();
    }

public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, 
                         std::unique_ptr<AbstractExecutor> right,
                         const std::vector<Condition> &conds)
        : left_(std::move(left)), right_(std::move(right)),
          fed_conds_(std::move(conds)) {
        int left_tupleLen = left_->tupleLen();
        len_ = left_tupleLen + right_->tupleLen();
        cols_ = left_->cols();
        auto &right_cols = right_->cols();
        int right_start = cols_.size();
        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        for (size_t i = right_start; i < cols_.size(); ++i) {
            cols_[i].offset += left_tupleLen;
        }
    }

    // 初始化执行器状态
    void initialize() {
        if (!is_initialized_) {
            left_->beginTuple();
            right_->beginTuple();
            current_left_batch_ = left_->next_batch();
            current_right_batch_ = right_->next_batch();
            is_end_ = current_left_batch_.empty() || current_right_batch_.empty();
            is_initialized_ = true;
        }
    }

    std::vector<std::unique_ptr<RmRecord>> next_batch(size_t batch_size = BATCH_SIZE) override {
        std::vector<std::unique_ptr<RmRecord>> result;
        // 首次调用时初始化
        initialize();
        if (is_end_) {
            return result;
        }

        while (result.size() < batch_size && !is_end_) {
            // 获取当前左右记录
            auto& left_rec = current_left_batch_[left_batch_index_];
            auto& right_rec = current_right_batch_[right_batch_index_];
            
            // 检查所有连接条件
            bool valid = std::all_of(fed_conds_.begin(), fed_conds_.end(),
                [&](const Condition& cond) {
                    return check_cond(left_rec, right_rec, cond);
                });
            
            if (valid) {
                // 创建连接后的记录
                auto record = std::make_unique<RmRecord>(len_);
                std::memcpy(record->data, left_rec->data, left_->tupleLen());
                std::memcpy(record->data + left_->tupleLen(), right_rec->data, right_->tupleLen());
                result.emplace_back(std::move(record));
            }
            
            // 移动到下一对记录
            right_batch_index_++;
            if (right_batch_index_ >= current_right_batch_.size()) {
                if (!fetch_next_right_batch()) {
                    left_batch_index_++;
                    if (left_batch_index_ >= current_left_batch_.size()) {
                        if (!fetch_next_left_batch()) {
                            is_end_ = true;
                            break;
                        }
                    }
                    right_->beginTuple();
                    if (!fetch_next_right_batch()) {
                        is_end_ = true;
                        break;
                    }
                }
            }
        }
        
        return result;
    }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    ExecutionType type() const override { return ExecutionType::NestedLoopJoin; }
};