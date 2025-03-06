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
#include <cfloat>
#include <climits>
#include <unordered_map>
#include "executor_abstract.h"
#include "../parser/ast.h"

class AggExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> child_executor_; // 子执行器
    std::vector<TabCol> sel_cols_;                    // 聚合的目标列
    std::vector<ColMeta> output_cols_;                // 输出列的元数据
    size_t TupleLen;                                  // 输出元组的长度
    std::vector<Value> agg_values_;                   // 聚合值
    std::vector<std::vector<ColMeta>::const_iterator> sel_col_metas_;// 目标列元数据
    bool is_aggregated_;                              // 是否已完成聚合

    void aggregate(const RmRecord &record);           // 聚合计算
    Value get_agg_value(size_t index) const;          // 获取聚合值

public:
    AggExecutor(std::unique_ptr<AbstractExecutor> child_executor, std::vector<TabCol> sel_cols, Context *context)
        : AbstractExecutor(context), child_executor_(std::move(child_executor)), sel_cols_(std::move(sel_cols)), is_aggregated_(false) {
        // 初始化输出列
        TupleLen = 0;
        int offset = 0;
        for (const auto &col : sel_cols_) {
            ColMeta col_meta;
            if (ast::AggFuncType::COUNT == col.aggFuncType) {
                col_meta = {col.tab_name, col.col_name, TYPE_INT, sizeof(int), offset, false};
                TupleLen += col_meta.len;
                offset += col_meta.len;
                output_cols_.emplace_back(col_meta);
                sel_col_metas_.emplace_back(output_cols_.begin());
            } else {
                if (col.col_name == "*") {
                    throw InvalidAggTypeError("*", std::to_string(col.aggFuncType));
                }
                auto temp = get_col(child_executor_->cols(), col);
                sel_col_metas_.push_back(temp);
                col_meta = *temp;
                col_meta.offset = offset;
                TupleLen += col_meta.len;
                offset += col_meta.len;
                output_cols_.emplace_back(col_meta);
            }
        }
        // 初始化聚合值
        agg_values_.resize(sel_cols_.size());
        for (size_t i = 0; i < sel_cols_.size(); ++i) {
            auto agg_type = sel_cols_[i].aggFuncType;
            if (ast::AggFuncType::COUNT == agg_type)
            {
                agg_values_[i].set_int(0);
            }
            else if (ast::AggFuncType::SUM == agg_type || ast::AggFuncType::MAX == agg_type || ast::AggFuncType::MIN == agg_type)
            {
                auto col = get_col(child_executor_->cols(), {sel_cols_[i].tab_name, sel_cols_[i].col_name});
                switch (col->type)
                {
                case TYPE_INT:
                    if (agg_type == ast::AggFuncType::MIN)
                        agg_values_[i].set_int(std::numeric_limits<int>::max());
                    else if (agg_type == ast::AggFuncType::MAX)
                        agg_values_[i].set_int(std::numeric_limits<int>::min());
                    else
                        agg_values_[i].set_int(0);
                    break;
                case TYPE_FLOAT:
                    if (agg_type == ast::AggFuncType::MIN)
                        agg_values_[i].set_float(std::numeric_limits<float>::max());
                    else if (agg_type == ast::AggFuncType::MAX)
                        agg_values_[i].set_float(std::numeric_limits<float>::lowest());
                    else
                        agg_values_[i].set_float(0.0f);
                    break;
                case TYPE_STRING:
                    if (agg_type == ast::AggFuncType::MIN)
                    {
                        auto str_len = col->len;
                        std::string max_str(str_len, '~');
                        agg_values_[i].set_str(max_str);
                    }
                    else if (agg_type == ast::AggFuncType::MAX)
                    {
                        agg_values_[i].set_str("");
                    }
                    else
                    {
                        throw RMDBError();
                    }
                    break;
                }
            }
        }
    }
    size_t tupleLen() const {
        return TupleLen;
    }

    const std::vector<ColMeta> &cols() const {
        return output_cols_;
    }

    void beginTuple() {
        child_executor_->beginTuple();
        is_aggregated_ = false;
    }

    void nextTuple() {
        if (!is_aggregated_) {
            while (!child_executor_->is_end()) {
                auto record = child_executor_->Next();
                if (record) {
                    aggregate(*record);
                }
                child_executor_->nextTuple();
            }
            is_aggregated_ = true;
        }
    }

    bool is_end() const {
        return is_aggregated_;
    }

    Rid &rid() {
        return _abstract_rid;
    }

    std::unique_ptr<RmRecord> Next() {
        if (!is_aggregated_) {
            nextTuple();
        }
        RmRecord record(TupleLen);
        int offset = 0;
        for (size_t i = 0; i < sel_cols_.size(); ++i) {
            auto value = get_agg_value(i);
            switch (value.type) {
                case TYPE_INT:
                    *(int*)(record.data + offset) = value.int_val;
                    break;
                case TYPE_FLOAT:
                    *(float*)(record.data + offset) = value.float_val;
                    break;
                case TYPE_STRING:
                    memcpy(record.data + offset, value.str_val.c_str(), value.str_val.size());
                    break;
                default:
                    throw InternalError("Unexpected sv value type");
            }
            offset += output_cols_[i].len;
        }
        return std::make_unique<RmRecord>(record);
    }
};

void AggExecutor::aggregate(const RmRecord &record) {
    for (size_t i = 0; i < sel_cols_.size(); ++i) {
        auto agg_type = sel_cols_[i].aggFuncType;
        auto col_meta = *sel_col_metas_[i];
        Value value;
        value.type = col_meta.type;

        switch (col_meta.type) {
            case TYPE_INT:
                value.set_int(*reinterpret_cast<const int*>(record.data + col_meta.offset));
                break;
            case TYPE_FLOAT:
                value.set_float(*reinterpret_cast<const float*>(record.data + col_meta.offset));
                break;
            case TYPE_STRING:
                value.set_str(std::string(record.data + col_meta.offset, col_meta.len));
                break;
            default:
                throw InternalError("Unexpected sv value type");
        }

        switch (agg_type) {
            case ast::AggFuncType::SUM:
                if (value.type == TYPE_INT)
                {
                    agg_values_[i].int_val += value.int_val;
                }
                else if (value.type == TYPE_FLOAT)
                {
                    agg_values_[i].float_val += value.float_val;
                }
                break;
            case ast::AggFuncType::COUNT:
                agg_values_[i].set_int(agg_values_[i].int_val + 1);
                break;
            case ast::AggFuncType::MAX:
                if (value.type == TYPE_INT && agg_values_[i].int_val < value.int_val)
                {
                    agg_values_[i].int_val = value.int_val;
                }
                else if (value.type == TYPE_FLOAT && agg_values_[i].float_val < value.float_val)
                {
                    agg_values_[i].float_val = value.float_val;
                }
                break;
            case ast::AggFuncType::MIN:
                if (value.type == TYPE_INT && agg_values_[i].int_val > value.int_val)
                {
                    agg_values_[i].int_val = value.int_val;
                }
                else if (value.type == TYPE_FLOAT && agg_values_[i].float_val > value.float_val)
                {
                    agg_values_[i].float_val = value.float_val;
                }
                break;
            case ast::AggFuncType::NO_TYPE:
                break;
            default:
                throw InvalidAggTypeError(sel_cols_[i].col_name, std::to_string(agg_type));
        }
    }
}

Value AggExecutor::get_agg_value(size_t index) const {
    return agg_values_[index];
}