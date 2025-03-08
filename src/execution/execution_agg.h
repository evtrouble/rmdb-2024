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
    std::vector<TabCol> group_by_cols_;               // GROUP BY 列
    Context *context_;                                // 执行器上下文
    std::vector<ColMeta> output_cols_;                // 输出列的元数据
    size_t TupleLen;                                  // 输出元组的长度
    std::unordered_map<std::string, std::vector<Value>> agg_groups_; // 分组聚合值
    std::vector<std::vector<ColMeta>::const_iterator> sel_col_metas_;// 目标列元数据
    std::vector<std::vector<ColMeta>::const_iterator> group_by_col_metas_;// GROUP BY 列元数据
    bool is_aggregated_;                              // 是否已完成聚合
    std::unordered_map<std::string, std::vector<Value>>::iterator group_iter_; // 分组迭代器
    std::vector<std::string> insert_order_; // 记录分组键的插入顺序
    size_t current_group_index_; // 当前遍历的分组索引

    void aggregate(const RmRecord &record);           // 聚合计算
    std::string get_group_key(const RmRecord &record); // 获取分组键
    Value get_agg_value(size_t index) const;          // 获取聚合值

public:
    AggExecutor(std::unique_ptr<AbstractExecutor> child_executor, std::vector<TabCol> sel_cols, std::vector<TabCol> group_by_cols, Context *context)
        : child_executor_(std::move(child_executor)), sel_cols_(std::move(sel_cols)), group_by_cols_(std::move(group_by_cols)), context_(context), is_aggregated_(false),  current_group_index_(0) {
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
                sel_col_metas_.emplace_back(temp);
                col_meta = *temp;
                col_meta.offset = offset;
                TupleLen += col_meta.len;
                offset += col_meta.len;
                output_cols_.emplace_back(col_meta);
            }
        }
        // 初始化 GROUP BY 列元数据
        for (const auto &col : group_by_cols_) {
            auto temp = get_col(child_executor_->cols(), col);
            group_by_col_metas_.emplace_back(temp);
        }
        // 初始化聚合值
        agg_groups_.clear();
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
        agg_groups_.clear();
        group_iter_ = agg_groups_.begin();
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
            group_iter_ = agg_groups_.begin();
        } else {
            if (group_iter_ != agg_groups_.end()) {
                ++group_iter_;
            }
        }
    }

    bool is_end() const {
        return is_aggregated_ && group_iter_ == agg_groups_.end();
    }

    Rid &rid() {
        return _abstract_rid;
    }

    std::unique_ptr<RmRecord> Next() {
        if (!is_aggregated_) {
            nextTuple();
        }
        if (current_group_index_ >= insert_order_.size()) {
            return nullptr;
        }

        // 获取当前分组键
        const std::string &group_key = insert_order_[current_group_index_];
        auto &agg_values = agg_groups_[group_key];

        RmRecord record(TupleLen);
        int offset = 0;

        // 遍历 sel_cols_，动态写入值
        for (size_t i = 0; i < sel_cols_.size(); ++i) {
            const auto &value = agg_values[i];
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

        // 移动到下一个分组
        ++current_group_index_;

        return std::make_unique<RmRecord>(record);
    }
};

void AggExecutor::aggregate(const RmRecord &record) {
    std::string group_key = get_group_key(record);
    auto &agg_values = agg_groups_[group_key];
    if (agg_values.empty()) {
        insert_order_.emplace_back(group_key); // 记录分组键的插入顺序
        agg_values.resize(sel_cols_.size());
        for (size_t i = 0; i < sel_cols_.size(); ++i) {
            auto agg_type = sel_cols_[i].aggFuncType;
            if (ast::AggFuncType::COUNT == agg_type) {
                agg_values[i].set_int(0);
            } else if (ast::AggFuncType::SUM == agg_type || ast::AggFuncType::MAX == agg_type || ast::AggFuncType::MIN == agg_type) {
                auto col = get_col(child_executor_->cols(), {sel_cols_[i].tab_name, sel_cols_[i].col_name});
                switch (col->type) {
                    case TYPE_INT:
                        if (ast::AggFuncType::MIN == agg_type)
                            agg_values[i].set_int(std::numeric_limits<int>::max());
                        else if (ast::AggFuncType::MAX == agg_type)
                            agg_values[i].set_int(std::numeric_limits<int>::min());
                        else
                            agg_values[i].set_int(0);
                        break;
                    case TYPE_FLOAT:
                        if (ast::AggFuncType::MIN == agg_type)
                            agg_values[i].set_float(std::numeric_limits<float>::max());
                        else if (ast::AggFuncType::MAX == agg_type)
                            agg_values[i].set_float(std::numeric_limits<float>::lowest());
                        else
                            agg_values[i].set_float(0.0f);
                        break;
                    case TYPE_STRING:
                        if (ast::AggFuncType::MIN == agg_type) {
                            auto str_len = col->len;
                            std::string max_str(str_len, '~');
                            agg_values[i].set_str(max_str);
                        } else if (ast::AggFuncType::MAX == agg_type) {
                            agg_values[i].set_str("");
                        } else {
                            throw RMDBError();
                        }
                        break;
                }
            }
            // 如果是 GROUP BY 列，初始化其值
            else if (ast::AggFuncType::NO_TYPE == agg_type) {
                auto col_meta = *get_col(child_executor_->cols(), sel_cols_[i]);
                switch (col_meta.type) {
                    case TYPE_INT:
                        agg_values[i].set_int(*reinterpret_cast<const int*>(record.data + col_meta.offset));
                        break;
                    case TYPE_FLOAT:
                        agg_values[i].set_float(*reinterpret_cast<const float*>(record.data + col_meta.offset));
                        break;
                    case TYPE_STRING:
                        agg_values[i].set_str(std::string(record.data + col_meta.offset, col_meta.len));
                        break;
                    default:
                        throw InternalError("Unexpected sv value type");
                }
            }
        }
    }

    // 更新聚合值
    for (size_t i = 0; i < sel_cols_.size(); ++i) {
        auto agg_type = sel_cols_[i].aggFuncType;
        if (ast::AggFuncType::NO_TYPE == agg_type) {
            continue; // GROUP BY 列不需要更新
        }
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
                if (TYPE_INT == value.type) {
                    agg_values[i].int_val += value.int_val;
                } else if (TYPE_FLOAT == value.type) {
                    agg_values[i].float_val += value.float_val;
                }
                break;
            case ast::AggFuncType::COUNT:
                agg_values[i].set_int(agg_values[i].int_val + 1);
                break;
            case ast::AggFuncType::MAX:
                if (TYPE_INT == value.type && agg_values[i].int_val < value.int_val) {
                    agg_values[i].int_val = value.int_val;
                } else if (TYPE_FLOAT == value.type && agg_values[i].float_val < value.float_val) {
                    agg_values[i].float_val = value.float_val;
                }
                break;
            case ast::AggFuncType::MIN:
                if (TYPE_INT == value.type && agg_values[i].int_val > value.int_val) {
                    agg_values[i].int_val = value.int_val;
                } else if (TYPE_FLOAT == value.type && agg_values[i].float_val > value.float_val) {
                    agg_values[i].float_val = value.float_val;
                }
                break;
            default:
                break;
        }
    }
}

std::string AggExecutor::get_group_key(const RmRecord &record) {
    if (group_by_cols_.empty())
    {
        return "no_groupby";
    }
    std::string key;
    for (size_t i = 0; i < group_by_cols_.size(); ++i) {
        auto col_meta = *group_by_col_metas_[i];
        switch (col_meta.type) {
            case TYPE_INT:
                key += std::to_string(*reinterpret_cast<const int*>(record.data + col_meta.offset));
                break;
            case TYPE_FLOAT:
                key += std::to_string(*reinterpret_cast<const float*>(record.data + col_meta.offset));
                break;
            case TYPE_STRING:
                key += std::string(record.data + col_meta.offset, col_meta.len);
                break;
            default:
                throw InternalError("Unexpected sv value type");
        }
        key += "|"; // 分隔符
    }
    return key;
}