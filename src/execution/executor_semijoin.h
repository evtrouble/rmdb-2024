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
#include <vector>

class SemiJoinExecutor : public AbstractExecutor
{
private:
    std::unique_ptr<AbstractExecutor> left_;   // 左儿子节点
    std::unique_ptr<AbstractExecutor> right_;  // 右儿子节点
    size_t len_;                               // 结果记录长度(只包含左表列)
    std::vector<ColMeta> cols_;                // 结果记录的字段(只包含左表列)
    std::vector<Condition> fed_conds_;          // join条件
    
    std::vector<RmRecord> left_batch_;         // 当前左表批次
    std::vector<RmRecord> right_batch_;        // 当前右表批次
    size_t left_batch_pos_;                    // 左表批次当前位置
    size_t right_batch_pos_;                   // 右表批次当前位置
    
    std::vector<RmRecord> result_batch_;       // 结果批次
    size_t result_pos_;                        // 结果批次当前位置
    
    bool is_end_ = false;                              // 是否结束
    bool is_initialized_ = false;
    
    // 检查条件是否满足
    bool check_cond(const Condition &cond, const RmRecord &left_rec, const RmRecord &right_rec)
    {
        auto left_col = left_->get_col(left_->cols(), cond.lhs_col);
        char *left_value = const_cast<char*>(left_rec.data) + left_col->offset;
        char *right_value;
        ColType right_type;
        
        if (cond.is_rhs_val) {
            right_type = cond.rhs_val.type;
            right_value = cond.rhs_val.raw->data;
        } else {
            auto rhs_col = right_->get_col(right_->cols(), cond.rhs_col);
            right_type = rhs_col->type;
            right_value = const_cast<char*>(right_rec.data) + rhs_col->offset;
        }
        
        return check_condition(left_value, left_col->type, right_value, right_type, 
                             cond.op, left_col->len);
    }

    // 加载左表下一批次
    void load_left_batch(size_t batch_size = BATCH_SIZE) {
        left_batch_.clear();
        auto batch = left_->next_batch(batch_size);
        for (auto &rec : batch) {
            left_batch_.push_back(*rec);
        }
        left_batch_pos_ = 0;
    }

    // 加载右表下一批次
    void load_right_batch(size_t batch_size = BATCH_SIZE) {
        right_batch_.clear();
        auto batch = right_->next_batch(batch_size);
        for (auto &rec : batch) {
            right_batch_.push_back(*rec);
        }
        right_batch_pos_ = 0;
    }

    // 处理当前批次寻找匹配
    void process_batch(size_t batch_size = BATCH_SIZE) {
        result_batch_.clear();
        result_pos_ = 0;
        
        while (result_batch_.size() < batch_size && !is_end_) {
            // 如果左表批次已处理完，加载下一批
            if (left_batch_pos_ >= left_batch_.size()) {
                load_left_batch(batch_size);
                if (left_batch_.empty()) {
                    is_end_ = true;
                    break;
                }
            }
            
            // 如果右表批次已处理完，加载下一批
            if (right_batch_pos_ >= right_batch_.size()) {
                load_right_batch(batch_size);
                if (right_batch_.empty()) {
                    // 右表为空，左表当前行无匹配
                    left_batch_pos_++;
                    continue;
                }
            }
            
            const auto &left_rec = left_batch_[left_batch_pos_];
            bool matched = false;
            
            // 检查当前左表行是否在右表中有匹配
            for (size_t i = 0; i < right_batch_.size(); ++i) {
                bool is_valid = std::all_of(fed_conds_.begin(), fed_conds_.end(),
                    [&](const Condition &cond) {
                        return check_cond(cond, left_rec, right_batch_[i]);
                    });
                
                if (is_valid) {
                    matched = true;
                    break;
                }
            }
            
            // 如果匹配，加入结果集
            if (matched) {
                RmRecord result_rec(len_);
                memcpy(result_rec.data, left_rec.data, len_);
                result_batch_.push_back(std::move(result_rec));
            }
            
            // 移动到左表下一行
            left_batch_pos_++;
            
            // 如果右表批次处理完，重置右表位置
            if (right_batch_pos_ >= right_batch_.size()) {
                right_batch_pos_ = 0;
            }
        }
    }

public:
    SemiJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                    std::vector<Condition> &conds)
        : left_(std::move(left)), right_(std::move(right)),
        len_(left_->tupleLen()), cols_(left_->cols()),
        fed_conds_(std::move(conds)), 
        left_batch_pos_(0), right_batch_pos_(0), result_pos_(0),
        is_end_(false) 
    {
        // 只保留左表的列
        len_ = left_->tupleLen();
        cols_ = left_->cols();
    }

    void initialize() {
        if (!is_initialized_)
        {
            left_->beginTuple();
            right_->beginTuple();
            is_end_ = false;
            left_batch_pos_ = 0;
            right_batch_pos_ = 0;
            result_pos_ = 0;
            result_batch_.clear();
            load_left_batch();
            load_right_batch();
            is_initialized_ = true;
        }
    }

    std::vector<std::unique_ptr<RmRecord>> next_batch(size_t batch_size = BATCH_SIZE) override {
        std::vector<std::unique_ptr<RmRecord>> batch;
        initialize();
        // 如果结果批次已用完，处理下一批
        if (result_pos_ >= result_batch_.size()) {
            process_batch(batch_size);
        }
        
        // 收集结果
        while (batch.size() < batch_size && result_pos_ < result_batch_.size()) {
            batch.push_back(std::make_unique<RmRecord>(result_batch_[result_pos_]));
            result_pos_++;
        }
        
        return batch;
    }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    ExecutionType type() const override { return ExecutionType::SemiJoin; }
};