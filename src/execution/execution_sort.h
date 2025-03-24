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

class SortExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    ColMeta cols_;                              // 框架中只支持一个键排序，需要自行修改数据结构支持多个键排序
    size_t tuple_num;
    bool is_desc_;
    std::vector<size_t> used_tuple;
    std::unique_ptr<RmRecord> current_tuple;
    std::vector<std::unique_ptr<RmRecord>> sorted_tuples_; // 排序后的记录
    size_t current_index_;                      // 当前记录的索引

    Value get_col_value(const RmRecord &record, const ColMeta &col_meta) {
        Value value;
        const char *data = record.data + col_meta.offset;

        switch (col_meta.type) {
            case TYPE_INT:
                value.set_int(*reinterpret_cast<const int*>(data));
                break;
            case TYPE_FLOAT:
                value.set_float(*reinterpret_cast<const float*>(data));
                break;
            case TYPE_STRING:
                value.set_str(std::string(data, col_meta.len));
                break;
            default:
                throw RMDBError("Unsupported column type");
        }

        return value;
    }

    void sortInMemory() {
        // 将所有记录读入内存
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple())
        {
            tuple_num++;
            sorted_tuples_.emplace_back(prev_->Next());
        }
        if (sorted_tuples_.empty())
            return;

        // 对记录进行排序
        std::sort(sorted_tuples_.begin(), sorted_tuples_.end(), 
            [this](const std::unique_ptr<RmRecord> &a, const std::unique_ptr<RmRecord> &b) {
                Value val_a = get_col_value(*a, cols_);
                Value val_b = get_col_value(*b, cols_);
                if (is_desc_) {
                    return val_a > val_b;
                } else {
                    return val_a < val_b;
                }
            });
    }
   public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, const TabCol &sel_cols, bool is_desc) 
        : prev_(std::move(prev)), tuple_num(0), is_desc_(is_desc)
    {
        cols_ = *get_col(prev_->cols(), sel_cols);
    }

    void beginTuple() override { 
        current_index_ = 0;
        if (!sorted_tuples_.empty())
            return;
        sortInMemory();
    }

    void nextTuple() override {
        if (current_index_ < sorted_tuples_.size())
        {
            ++current_index_;
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end())
        {
            return nullptr;
        }
        return std::make_unique<RmRecord>(*sorted_tuples_[current_index_]);
    }

    Rid &rid() override { return _abstract_rid; }

    bool is_end() const override { return sorted_tuples_.empty() || current_index_ >= sorted_tuples_.size(); }
    
    const std::vector<ColMeta> &cols() const override { return prev_->cols(); }
};