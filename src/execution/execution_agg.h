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
    std::vector<Condition> having_conds_;              // HAVING 条件
    std::vector<ColMeta> output_cols_;                // 输出列的元数据
    size_t TupleLen;                                  // 输出元组的长度
    std::unordered_map<std::string, std::vector<Value>> agg_groups_; // 分组聚合值
    std::unordered_map<std::string, std::vector<Value>> having_lhs_agg_groups_;// HAVING 左分组聚合值
    std::unordered_map<std::string, std::vector<Value>> having_rhs_agg_groups_;// HAVING 右分组聚合值
    std::vector<std::vector<ColMeta>::const_iterator> sel_col_metas_;// 目标列元数据
    std::vector<std::vector<ColMeta>::const_iterator> group_by_col_metas_;// GROUP BY 列元数据
    std::vector<TabCol>  having_lhs_cols_;                    //  HAVING 左聚合的目标列
    std::vector<TabCol>  having_rhs_cols_;                    //  HAVING 右聚合的目标列
    std::vector<std::vector<ColMeta>::const_iterator> having_lhs_col_metas_;// HAVING 左列元数据
    std::vector<std::vector<ColMeta>::const_iterator> having_rhs_col_metas_;// HAVING 右列元数据
    std::vector<std::string> insert_order_; // 记录分组键的插入顺序
    size_t current_group_index_; // 当前遍历的分组索引
    std::vector<RmRecord> results_; // 储存结果
    std::vector<RmRecord>::iterator result_it_; // 结果迭代器

    void init(std::vector<Value> &agg_values, std::vector<TabCol> sel_cols_, const RmRecord &record);
    void aggregate_values(std::vector<Value> &agg_values, const std::vector<TabCol> sel_cols_, const std::vector<std::vector<ColMeta>::const_iterator> sel_col_metas_, const RmRecord &record);
    void aggregate(const RmRecord &record);                             // 聚合计算
    std::string get_group_key(const RmRecord &record);                  // 获取分组键
    bool check_having_conditions(const std::vector<Value> &having_lhs_agg_values, const std::vector<Value> &having_rhs_agg_values);// 判断having
    bool compare_values(const Value &lhs_value, const Value &rhs_value, CompOp op);// 比较两个value对象的值是否满足指定的比较操作符

public:
    AggExecutor(std::unique_ptr<AbstractExecutor> child_executor, const std::vector<TabCol> &sel_cols, 
        const std::vector<TabCol> &group_by_cols, const std::vector<Condition> &having_conds, 
        Context *context)
        : AbstractExecutor(context),child_executor_(std::move(child_executor)), 
        sel_cols_(std::move(sel_cols)), group_by_cols_(std::move(group_by_cols)), having_conds_(std::move(having_conds)), 
        TupleLen(0), current_group_index_(0) {
        // 初始化输出列
        int offset = 0;
        for (const auto &col : sel_cols_) {
            ColMeta col_meta;
            if (ast::AggFuncType::COUNT == col.aggFuncType) {
                col_meta = {col.tab_name, col.col_name, TYPE_INT, sizeof(int), offset, false};
                TupleLen += col_meta.len;
                offset += col_meta.len;
                output_cols_.emplace_back(std::move(col_meta));
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
                output_cols_.emplace_back(std::move(col_meta));
            }
        }
        // 初始化 GROUP BY 列元数据
        for (const auto &col : group_by_cols_) {
            auto temp = get_col(child_executor_->cols(), col);
            group_by_col_metas_.emplace_back(std::move(temp));
        }
        // 初始化 HAVING 列元数据
        for (const auto &cond : having_conds_) {
            std::vector<ColMeta> col_metas;
            if (ast::AggFuncType::COUNT == cond.lhs_col.aggFuncType) {
                ColMeta col_meta = {cond.lhs_col.tab_name, cond.lhs_col.col_name, TYPE_INT, sizeof(int), 0, false};
                having_lhs_cols_.emplace_back(cond.lhs_col);
                col_metas.emplace_back(std::move(col_meta));
                having_lhs_col_metas_.emplace_back(col_metas.begin());
            } else {
                if (cond.lhs_col.col_name == "*") {
                    throw InvalidAggTypeError("*", std::to_string(cond.lhs_col.aggFuncType));
                }
                auto temp = get_col(child_executor_->cols(), cond.lhs_col);
                having_lhs_cols_.emplace_back(cond.lhs_col);
                having_lhs_col_metas_.emplace_back(std::move(temp));
            }

            if (!cond.is_rhs_val) {
                if (ast::AggFuncType::COUNT == cond.rhs_col.aggFuncType) {
                    ColMeta col_meta = {cond.rhs_col.tab_name, cond.rhs_col.col_name, TYPE_INT, sizeof(int), 0, false};
                    having_lhs_cols_.emplace_back(cond.rhs_col);
                    col_metas.emplace_back(std::move(col_meta));
                    having_lhs_col_metas_.emplace_back(col_metas.begin());
                } else {
                    if (cond.rhs_col.col_name == "*") {
                        throw InvalidAggTypeError("*", std::to_string(cond.rhs_col.aggFuncType));
                    }
                    auto temp = get_col(child_executor_->cols(), cond.rhs_col);
                    having_lhs_cols_.emplace_back(cond.rhs_col);
                    having_lhs_col_metas_.emplace_back(std::move(temp));
                }
            }
        }
        // 初始化聚合值
        agg_groups_.clear();
        having_lhs_agg_groups_.clear();
        having_rhs_agg_groups_.clear();
    }

    size_t tupleLen() const {
        return TupleLen;
    }

    const std::vector<ColMeta> &cols() const {
        return output_cols_;
    }

    void beginTuple() {
        child_executor_->beginTuple();
        agg_groups_.clear();
        having_lhs_agg_groups_.clear();
        having_rhs_agg_groups_.clear();
        while (!child_executor_->is_end()) {
            auto record = child_executor_->Next();
            if (record) {
                aggregate(*record);
            }
            child_executor_->nextTuple();
        }
        addResult();
        result_it_ = results_.begin();
    }

    void nextTuple() {
        if (result_it_ != results_.end()) {
            ++result_it_;
        }
    }
    
    bool is_end() const {
        return result_it_ == results_.end();
    }

    Rid &rid() {
        return _abstract_rid;
    }

    void addResult() {
        // 如果 agg_groups_ 为空，返回初始值
        if (agg_groups_.empty()) {
            RmRecord record(TupleLen);
            int offset = 0;
            std::vector<Value> agg_values; 
            agg_values.resize(sel_cols_.size());
            for (size_t i = 0; i < sel_cols_.size(); ++i) {
                auto agg_type = sel_cols_[i].aggFuncType;
                if (ast::AggFuncType::COUNT == agg_type) {
                    agg_values[i].set_int(0);
                } else if (ast::AggFuncType::SUM == agg_type || ast::AggFuncType::MAX == agg_type || ast::AggFuncType::MIN == agg_type) {
                    auto col = get_col(child_executor_->cols(), {sel_cols_[i].tab_name, sel_cols_[i].col_name});
                    if (ast::AggFuncType::MIN == agg_type)
                        agg_values[i].set_max(col->type, col->len);
                    else if (ast::AggFuncType::MAX == agg_type)
                        agg_values[i].set_min(col->type, col->len);
                    else {
                        switch (col->type)
                        {
                            case TYPE_INT:
                                agg_values[i].set_int(0);
                                break;
                            case TYPE_FLOAT:
                                agg_values[i].set_float(0.0f);
                                break;
                            default:
                                throw RMDBError();
                        }
                    }
                }
            }
            for (size_t i = 0; i < sel_cols_.size(); ++i) {
                memcpy(record.data + offset, agg_values[i].raw->data, output_cols_[i].len);
                offset += output_cols_[i].len;
            }
            results_.emplace_back(std::move(record));   
            return;
        }
        if (current_group_index_ >= insert_order_.size()) {
            return;
        }

        // 获取当前分组键
        const std::string &group_key = insert_order_[current_group_index_];
        auto &agg_values = agg_groups_[group_key];
        auto &having_lhs_agg_values = having_lhs_agg_groups_[group_key];
        auto &having_rhs_agg_values = having_rhs_agg_groups_[group_key];

        // 检查是否满足having条件
        if (!check_having_conditions(having_lhs_agg_values, having_rhs_agg_values))
        {
            ++current_group_index_;
            return addResult(); // 跳过不满足条件的分组
        }

        RmRecord record(TupleLen);
        int offset = 0;

        // 遍历 sel_cols_，动态写入值
        for (size_t i = 0; i < sel_cols_.size(); ++i) {
            memcpy(record.data + offset, agg_values[i].raw->data, output_cols_[i].len);
            offset += output_cols_[i].len;
        }

        // 移动到下一个分组
        ++current_group_index_;
        results_.emplace_back(std::move(record));
        return addResult();
    }

    std::unique_ptr<RmRecord> Next() {
        if (result_it_ == results_.end())
        {
            return nullptr;
        }
        return std::make_unique<RmRecord>(*result_it_);
    }

    ExecutionType type() const override { return ExecutionType::Agg;  }
};

void AggExecutor::init(std::vector<Value> &agg_values, const std::vector<TabCol> sel_cols_, const RmRecord &record){
    for (size_t i = 0; i < sel_cols_.size(); ++i) {
        auto agg_type = sel_cols_[i].aggFuncType;
        if (ast::AggFuncType::COUNT == agg_type) {
            agg_values[i].set_int(0);
        } else if (ast::AggFuncType::SUM == agg_type || ast::AggFuncType::MAX == agg_type || ast::AggFuncType::MIN == agg_type) {
            auto col = get_col(child_executor_->cols(), {sel_cols_[i].tab_name, sel_cols_[i].col_name});
            if (ast::AggFuncType::MIN == agg_type)
                agg_values[i].set_max(col->type, col->len);
            else if (ast::AggFuncType::MAX == agg_type)
                agg_values[i].set_min(col->type, col->len);
            else {
                switch (col->type)
                {
                    case TYPE_INT:
                        agg_values[i].set_int(0);
                        break;
                    case TYPE_FLOAT:
                        agg_values[i].set_float(0.0f);
                        break;
                    default:
                        throw RMDBError();
                }
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
                    throw InternalError("Unexpected sv value type 4");
            }
        }
    }
}

void AggExecutor::aggregate_values(std::vector<Value> &agg_values, const std::vector<TabCol> sel_cols_, const std::vector<std::vector<ColMeta>::const_iterator> sel_col_metas_,const RmRecord &record){
    for (size_t i = 0; i < sel_cols_.size(); ++i) {
        auto agg_type = sel_cols_[i].aggFuncType;
        // GROUP BY 列不需要更新
        if (ast::AggFuncType::NO_TYPE == agg_type) {
            continue;
        }
        // COUNT 列直接+1
        if (ast::AggFuncType::COUNT == agg_type)
        {
            ++agg_values[i].int_val;
            continue;
        }
        auto &col_meta = *sel_col_metas_[i];
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
                throw InternalError("Unexpected sv value type 5");
        }

        switch (agg_type) {
            case ast::AggFuncType::SUM:
                if (TYPE_INT == value.type) {
                    agg_values[i].int_val += value.int_val;
                } else if (TYPE_FLOAT == value.type) {
                    agg_values[i].float_val += value.float_val;
                }
                break;
            case ast::AggFuncType::MAX:
                agg_values[i] = std::max(agg_values[i], value);
                break;
            case ast::AggFuncType::MIN:
                agg_values[i] = std::min(agg_values[i], value);
                break;
            default:
                break;
        }
    }
}

void AggExecutor::aggregate(const RmRecord &record) {
    std::string group_key = get_group_key(record);
    auto &agg_values = agg_groups_[group_key];
    auto &having_lhs_agg_values = having_lhs_agg_groups_[group_key];
    auto &having_rhs_agg_values = having_rhs_agg_groups_[group_key];
    if (agg_values.empty()) {
        insert_order_.emplace_back(group_key); // 记录分组键的插入顺序
        agg_values.resize(sel_cols_.size());
        having_lhs_agg_values.resize(having_lhs_cols_.size());
        having_rhs_agg_values.resize(having_rhs_cols_.size());
        init(agg_values, sel_cols_, record);
        init(having_lhs_agg_values, having_lhs_cols_, record);
        init(having_rhs_agg_values, having_rhs_cols_, record);
    }
    aggregate_values(agg_values, sel_cols_, sel_col_metas_, record);
    aggregate_values(having_lhs_agg_values, having_lhs_cols_, having_lhs_col_metas_, record);
    aggregate_values(having_rhs_agg_values, having_rhs_cols_, having_rhs_col_metas_, record);
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
                throw InternalError("Unexpected sv value type 6");
        }
        key += "|"; // 分隔符
    }
    return key;
}

bool AggExecutor::check_having_conditions(const std::vector<Value> &having_lhs_agg_values, const std::vector<Value> &having_rhs_agg_values)
{
    auto lhs_it = having_lhs_agg_values.begin();
    auto rhs_it = having_rhs_agg_values.begin();
    for (const auto &cond : having_conds_) {
        // 获取左操作数的值
        Value lhs_value = *lhs_it;
        ++lhs_it;

        // 获取右操作数的值
        Value rhs_value;
        if (cond.is_rhs_val) {
            rhs_value = cond.rhs_val;
        } else {
            rhs_value = *rhs_it;
            ++rhs_it;
        }

        // 检查条件是否满足
        if (!compare_values(lhs_value, rhs_value, cond.op)) {
            return false;
        }
    }
    return true;
}

bool AggExecutor::compare_values(const Value &lhs_value, const Value &rhs_value, CompOp op)
{
    switch (op)
    {
        case OP_EQ:
            return lhs_value == rhs_value;
        case OP_NE:
            return lhs_value != rhs_value;
        case OP_LT:
            return lhs_value < rhs_value;
        case OP_LE:
            return lhs_value <= rhs_value;
        case OP_GT:
            return lhs_value > rhs_value;
        case OP_GE:
            return lhs_value >= rhs_value;
        default:
            throw InternalError("Unexpected comparison operator");
    }
}