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
    // 缓存全部数据
    std::vector<std::unique_ptr<RmRecord>> left_cache_;
    std::vector<std::unique_ptr<RmRecord>> right_cache_;
    // 迭代状态
    size_t left_idx_ = 0;
    size_t right_idx_ = 0;
    // 维护两个unordered_set分别存储左右子树的列信息
    std::unordered_set<std::string> left_cols_set_;
    std::unordered_set<std::string> right_cols_set_;
    bool isend = false;
    void init_cols_sets()
    {
        // 初始化左表列集合
        for (const auto &col : left_->cols())
        {
            left_cols_set_.insert(col.tab_name + "." + col.name);
        }
        // 初始化右表列集合
        for (const auto &col : right_->cols())
        {
            right_cols_set_.insert(col.tab_name + "." + col.name);
        }
    }

    bool check_cond(const std::unique_ptr<RmRecord>& left_rec, 
                   const std::unique_ptr<RmRecord>& right_rec,
                   const Condition &cond) {
        char *lhs_value;
        ColType lhs_type;
        int lhs_len;

        // 确定左操作数来自哪个子树
        std::string lhs_col_full_name = cond.lhs_col.tab_name + "." + cond.lhs_col.col_name;
        if (left_cols_set_.find(lhs_col_full_name) != left_cols_set_.end()) {
            auto left_col = left_->get_col(left_->cols(), cond.lhs_col);
            lhs_value = left_rec->data + left_col->offset;
            lhs_type = left_col->type;
            lhs_len = left_col->len;
        } else if (right_cols_set_.find(lhs_col_full_name) != right_cols_set_.end()) {
            auto left_col = right_->get_col(right_->cols(), cond.lhs_col);
            lhs_value = right_rec->data + left_col->offset;
            lhs_type = left_col->type;
            lhs_len = left_col->len;
        } else {
            throw ColumnNotFoundError(lhs_col_full_name);
        }

        char *rhs_value;
        ColType rhs_type;
        int rhs_len = lhs_len;

        if (cond.is_rhs_val) {
            rhs_type = cond.rhs_val.type;
            rhs_value = cond.rhs_val.raw->data;
        } else {
            std::string rhs_col_full_name = cond.rhs_col.tab_name + "." + cond.rhs_col.col_name;
            if (left_cols_set_.find(rhs_col_full_name) != left_cols_set_.end()) {
                auto rhs_col = left_->get_col(left_->cols(), cond.rhs_col);
                rhs_value = left_rec->data + rhs_col->offset;
                rhs_type = rhs_col->type;
                rhs_len = rhs_col->len;
            } else if (right_cols_set_.find(rhs_col_full_name) != right_cols_set_.end()) {
                auto rhs_col = right_->get_col(right_->cols(), cond.rhs_col);
                rhs_value = right_rec->data + rhs_col->offset;
                rhs_type = rhs_col->type;
                rhs_len = rhs_col->len;
            } else {
                throw ColumnNotFoundError(rhs_col_full_name);
            }
        }

        int compare_len = (lhs_type == TYPE_STRING && rhs_type == TYPE_STRING) 
                        ? std::min(lhs_len, rhs_len) : std::max(lhs_len, rhs_len);
        return check_condition(lhs_value, lhs_type, rhs_value, rhs_type, cond.op, compare_len);
    }

    // 缓存所有数据
    void cache_all_data() {
        left_->beginTuple();
        right_->beginTuple();
        
        // 缓存左表所有数据
        while (true) {
            auto batch = left_->next_batch();
            if (batch.empty()) break;
            for (auto& rec : batch) {
                left_cache_.emplace_back(std::move(rec));
            }
        }
        
        // 缓存右表所有数据
        while (true) {
            auto batch = right_->next_batch();
            if (batch.empty()) break;
            for (auto& rec : batch) {
                right_cache_.emplace_back(std::move(rec));
            }
        }
        
        // 重置迭代状态
        left_idx_ = 0;
        right_idx_ = 0;
        isend = left_cache_.empty() || right_cache_.empty();
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
        init_cols_sets();
        cache_all_data();  // 初始化时缓存所有数据
    }

    void beginTuple() override {
        left_idx_ = 0;
        right_idx_ = 0;
        isend = left_cache_.empty() || right_cache_.empty();
    }

    std::vector<std::unique_ptr<RmRecord>> next_batch(size_t batch_size = BATCH_SIZE) override {
        std::vector<std::unique_ptr<RmRecord>> result;
        
        if (isend) {
            return result;
        }

        while (result.size() < batch_size && !isend) {
            // 遍历当前左右记录
            auto& left_rec = left_cache_[left_idx_];
            auto& right_rec = right_cache_[right_idx_];
            
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
            right_idx_++;
            if (right_idx_ >= right_cache_.size()) {
                right_idx_ = 0;
                left_idx_++;
                if (left_idx_ >= left_cache_.size()) {
                    isend = true;
                    break;
                }
            }
        }
        
        return result;
    }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    ExecutionType type() const override { return ExecutionType::NestedLoopJoin; }
};