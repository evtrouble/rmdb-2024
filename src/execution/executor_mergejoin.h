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
#include <algorithm>

class MergeJoinExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> left_;    // 左子执行器
    std::unique_ptr<AbstractExecutor> right_;   // 右子执行器
    size_t len_;                                // 连接后记录长度
    std::vector<ColMeta> cols_;                // 连接后记录的字段
    
    std::vector<Condition> fed_conds_;          // 连接条件
    std::vector<std::unique_ptr<RmRecord>> left_cache_; // 左表全部数据缓存
    std::vector<std::unique_ptr<RmRecord>> right_cache_; // 右表全部数据缓存
    size_t left_idx_ = 0;                      // 左表当前索引
    size_t right_idx_ = 0;                     // 右表当前索引
    bool is_end_ = false;                      // 是否结束
    
    // 获取左表键值
    std::unique_ptr<RmRecord> get_left_key(size_t idx) {
        auto cond = fed_conds_[0];
        auto left_col = left_->get_col(left_->cols(), cond.lhs_col);
        auto key_rec = std::make_unique<RmRecord>(left_col->len);
        memcpy(key_rec->data, left_cache_[idx]->data + left_col->offset, left_col->len);
        return key_rec;
    }

    // 获取右表键值
    std::unique_ptr<RmRecord> get_right_key(size_t idx) {
        auto cond = fed_conds_[0];
        auto right_col = right_->get_col(right_->cols(), cond.rhs_col);
        auto key_rec = std::make_unique<RmRecord>(right_col->len);
        memcpy(key_rec->data, right_cache_[idx]->data + right_col->offset, right_col->len);
        return key_rec;
    }

    // 比较键值
    inline int compare_keys(std::unique_ptr<RmRecord>& key1, std::unique_ptr<RmRecord>& key2) {
        return memcmp(key1->data, key2->data, key1->size);
    }

    // 缓存表的所有数据
    void cache_all_data() {
        // 缓存左表所有数据
        left_->beginTuple();
        while (true) {
            auto batch = left_->next_batch();
            if (batch.empty()) break;
            for (auto& rec : batch) {
                left_cache_.push_back(std::move(rec));
            }
        }
        
        // 缓存右表所有数据
        right_->beginTuple();
        while (true) {
            auto batch = right_->next_batch();
            if (batch.empty()) break;
            for (auto& rec : batch) {
                right_cache_.push_back(std::move(rec));
            }
        }
        
        // 重置索引
        left_idx_ = 0;
        right_idx_ = 0;
        is_end_ = left_cache_.empty() || right_cache_.empty();
    }

public:
    MergeJoinExecutor(std::unique_ptr<AbstractExecutor> left, 
                     std::unique_ptr<AbstractExecutor> right,
                     const std::vector<Condition> &conds)
        : left_(std::move(left)), right_(std::move(right)), 
          fed_conds_(conds) {
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }
        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        
        // 初始化时缓存所有数据
        cache_all_data();
    }

    void beginTuple() override {
        left_idx_ = 0;
        right_idx_ = 0;
        is_end_ = left_cache_.empty() || right_cache_.empty();
    }

    std::vector<std::unique_ptr<RmRecord>> next_batch(size_t batch_size = BATCH_SIZE) override {
        std::vector<std::unique_ptr<RmRecord>> batch;
        
        if (is_end_) {
            return batch;
        }

        while (batch.size() < batch_size && !is_end_) {
            auto left_key = get_left_key(left_idx_);
            auto right_key = get_right_key(right_idx_);
            int cmp = compare_keys(left_key, right_key);
            
            if (cmp == 0) {
                // 找到匹配键值，处理所有重复键
                size_t left_start = left_idx_;
                size_t right_start = right_idx_;
                
                // 找出左表所有相同键值记录
                while (left_idx_ < left_cache_.size()) {
                    auto next_key = get_left_key(left_idx_);
                    if (compare_keys(left_key, next_key) != 0) break;
                    left_idx_++;
                }
                
                // 找出右表所有相同键值记录
                while (right_idx_ < right_cache_.size()) {
                    auto next_key = get_right_key(right_idx_);
                    if (compare_keys(right_key, next_key) != 0) break;
                    right_idx_++;
                }
                
                // 生成笛卡尔积
                for (size_t l = left_start; l < left_idx_ && batch.size() < batch_size; l++) {
                    for (size_t r = right_start; r < right_idx_ && batch.size() < batch_size; r++) {
                        auto record = std::make_unique<RmRecord>(len_);
                        std::memcpy(record->data, left_cache_[l]->data, left_->tupleLen());
                        std::memcpy(record->data + left_->tupleLen(), right_cache_[r]->data, right_->tupleLen());
                        batch.push_back(std::move(record));
                    }
                }
            } 
            else if (cmp < 0) {
                // 左表键值小，移动左表
                left_idx_++;
            }
            else {
                // 右表键值小，移动右表
                right_idx_++;
            }
            
            // 检查是否结束
            if (left_idx_ >= left_cache_.size() || right_idx_ >= right_cache_.size()) {
                is_end_ = true;
            }
        }
        
        return batch;
    }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    ExecutionType type() const override { return ExecutionType::MergeJoin; }
};