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

class MergeJoinExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    std::unique_ptr<RmRecord> left_current_;
    std::unique_ptr<RmRecord> right_current_;
    bool isend;
    std::vector<std::unique_ptr<RmRecord>> left_cache_; // 缓存左表重复键的记录
    size_t left_cache_idx_;                             // 当前缓存的左表记录索引
    std::vector<std::unique_ptr<RmRecord>> right_cache_;// 缓存右表重复键的记录
    size_t right_cache_idx_;                            // 当前缓存的右表记录索引
    

    // 因为只考虑只有一个where条件，所以这个cond一定是连接条件
    void find_valid_tuples() {
        while (!left_->is_end() && !right_->is_end()) {
            int cmp = check_cond(fed_conds_[0]);
            if (cmp == 0) {
                // 缓存左表所有相同键值的记录
                cache_left_duplicates();
                cache_right_duplicates();
                return;
            } 
            else if (cmp > 0) {
                right_->nextTuple();
                right_current_ = right_->Next();
            }
            else{
                left_->nextTuple();
                left_current_ = left_->Next();
            }
        }
        isend = true;
    }
    
    // 缓存左表中所有与当前记录键值相同的记录
    void cache_left_duplicates() {
        left_cache_.clear();
        left_cache_idx_ = 0;
        
        // 保存当前左表记录
        auto current_key = get_left_key(left_current_.get());
        left_cache_.emplace_back(std::move(left_current_));
        
        // 遍历左表后续记录，缓存所有相同键值的记录
        while (true) {
            left_->nextTuple();
            if (left_->is_end()) break;
            
            auto next_rec = left_->Next();
            auto next_key = get_left_key(next_rec.get());
            
            if (compare_keys(current_key, next_key, next_key->size) != 0) {
                // 遇到不同的键值，回退并停止缓存
                left_current_ = std::move(next_rec);
                break;
            }
            
            left_cache_.emplace_back(std::move(next_rec));
        }
    }

    std::unique_ptr<RmRecord> get_left_key(RmRecord* rec) {
        auto cond = fed_conds_[0];
        auto left_col = left_->get_col(left_->cols(), cond.lhs_col);
        auto key_rec = std::make_unique<RmRecord>(left_col->len);
        memcpy(key_rec->data, rec->data + left_col->offset, left_col->len);
        return key_rec;
    }

    // 缓存右表中所有与当前记录键值相同的记录
    void cache_right_duplicates() {
        right_cache_.clear();
        right_cache_idx_ = 0;
        
        // 保存当前右表记录
        auto current_key = get_right_key(right_current_.get());
        right_cache_.emplace_back(std::move(right_current_));
        
        // 遍历右表后续记录，缓存所有相同键值的记录
        while (true) {
            right_->nextTuple();
            if (right_->is_end()) break;
            
            auto next_rec = right_->Next();
            auto next_key = get_right_key(next_rec.get());
            
            if (compare_keys(current_key, next_key, next_key->size) != 0) {
                // 遇到不同的键值，回退并停止缓存
                right_current_ = std::move(next_rec);
                break;
            }
            
            right_cache_.emplace_back(std::move(next_rec));
        }
    }

    std::unique_ptr<RmRecord> get_right_key(RmRecord* rec) {
        auto cond = fed_conds_[0];
        auto right_col = right_->get_col(right_->cols(), cond.rhs_col);
        auto key_rec = std::make_unique<RmRecord>(right_col->len);
        memcpy(key_rec->data, rec->data + right_col->offset, right_col->len);
        return key_rec;
    }

    // 比较两个键值
    inline int compare_keys(std::unique_ptr<RmRecord>& key1, std::unique_ptr<RmRecord>& key2, size_t len) {
        return memcmp(key1->data, key2->data, len);
    }

    int check_cond(const Condition& cond) {
        auto left_col = left_->get_col(left_->cols(), cond.lhs_col);
        char* left_value = left_current_->data + left_col->offset;
        char* right_value;
        ColType right_type;
        if (cond.is_rhs_val) {
            right_type = cond.rhs_val.type;
            right_value = cond.rhs_val.raw->data;
        } else {
            auto rhs_col = right_->get_col(right_->cols(), cond.rhs_col);
            right_type = rhs_col->type;
            right_value = right_current_->data + rhs_col->offset;
        }
        return compare_values(left_value, left_col->type, right_value, right_type, cond.op);
    }

    int compare_values(char *lhs_value, ColType lhs_type, char *rhs_value, ColType rhs_type, CompOp op){
        int cmp;

        // 处理INT和FLOAT类型之间的比较
        if ((lhs_type == TYPE_INT && rhs_type == TYPE_FLOAT) ||
            (lhs_type == TYPE_FLOAT && rhs_type == TYPE_INT))
        {
            float lhs_float, rhs_float;

            // 将INT转换为FLOAT进行比较
            if (lhs_type == TYPE_INT) {
                lhs_float = static_cast<float>(*(int *)lhs_value);
                rhs_float = *(float *)rhs_value;
            } else {
                lhs_float = *(float *)lhs_value;
                rhs_float = static_cast<float>(*(int *)rhs_value);
            }

            cmp = (lhs_float < rhs_float) ? -1 : ((lhs_float > rhs_float) ? 1 : 0);
        }
        else if (lhs_type != rhs_type)
        {
            throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
        }
        else
        {
            // 相同类型的比较
            switch (lhs_type)
            {
            case TYPE_INT:
            {
                int lhs = *(int *)lhs_value;
                int rhs = *(int *)rhs_value;
                cmp = lhs - rhs;
                break;
            }
            case TYPE_FLOAT:
            {
                float lhs = *(float *)lhs_value;
                float rhs = *(float *)rhs_value;
                cmp = (lhs < rhs) ? -1 : ((lhs > rhs) ? 1 : 0);
                break;
            }
            case TYPE_STRING:
            case TYPE_DATETIME:
            {
                cmp = memcmp(lhs_value, rhs_value, strlen(rhs_value));
                break;
            }
            default:
                throw InternalError("Unsupported column type");
            }
        }
        return cmp;
    }


public:
    MergeJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, 
                            const std::vector<Condition> &conds)
        : left_(std::move(left)), right_(std::move(right)), 
        fed_conds_(std::move(conds)), isend(false),left_cache_idx_(0)
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
        left_current_ = left_->Next();
        right_current_ = right_->Next();
        left_cache_.clear();
        left_cache_idx_ = 0;
        right_cache_.clear();
        right_cache_idx_ = 0;
        find_valid_tuples();
    }

    void nextTuple() override {
        if (left_cache_.size() && right_cache_.size()) {
            ++right_cache_idx_;
            if (right_cache_idx_ >= right_cache_.size()) {
                // 已经遍历完所有缓存的右表记录，移动左表缓存
                ++left_cache_idx_;
                right_cache_idx_ = 0;
                if(left_cache_idx_ >= left_cache_.size()){
                    // 重复键全部遍历完了
                    left_cache_idx_ = 0;
                    find_valid_tuples();
                }
            }
        } else {
            right_->nextTuple();
            right_current_ = right_->Next();
            find_valid_tuples();
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        auto record = std::make_unique<RmRecord>(len_);
        
        if (left_cache_.size() && right_cache_.size()) {
            std::memcpy(record->data, left_cache_[left_cache_idx_]->data, left_->tupleLen());
            std::memcpy(record->data + left_->tupleLen(), right_cache_[right_cache_idx_]->data, right_->tupleLen());
        } else {
            std::memcpy(record->data, left_current_->data, left_->tupleLen());
            std::memcpy(record->data + left_->tupleLen(), right_current_->data, right_->tupleLen());
        }
        return record;
    }

    Rid &rid() override { return _abstract_rid; }

    bool is_end() const override { return isend; }
    
    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const { return cols_; }

    ExecutionType type() const override { return ExecutionType::MergeJoin;  }
};