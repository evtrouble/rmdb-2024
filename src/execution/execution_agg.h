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
#include <iomanip>

class AggExecutor : public AbstractExecutor
{
private:
    std::unique_ptr<AbstractExecutor> child_executor_;                          // 子执行器
    std::vector<TabCol> sel_cols_;                                              // 聚合的目标列
    std::vector<TabCol> group_by_cols_;                                         // GROUP BY 列
    std::vector<Condition> having_conds_;                                       // HAVING 条件
    std::vector<ColMeta> output_cols_;                                          // 输出列的元数据
    size_t TupleLen;                                                            // 输出元组的长度
    std::unordered_map<std::string, std::vector<Value>> agg_groups_;            // 分组聚合值
    std::unordered_map<std::string, std::vector<Value>> having_lhs_agg_groups_; // HAVING 左分组聚合值
    std::unordered_map<std::string, std::vector<Value>> having_rhs_agg_groups_; // HAVING 右分组聚合值
    std::vector<std::vector<ColMeta>::const_iterator> sel_col_metas_;           // 目标列元数据
    std::vector<std::vector<ColMeta>::const_iterator> group_by_col_metas_;      // GROUP BY 列元数据
    std::vector<TabCol> having_lhs_cols_;                                       // HAVING 左聚合的目标列
    std::vector<TabCol> having_rhs_cols_;                                       // HAVING 右聚合的目标列
    std::vector<std::vector<ColMeta>::const_iterator> having_lhs_col_metas_;    // HAVING 左列元数据
    std::vector<std::vector<ColMeta>::const_iterator> having_rhs_col_metas_;    // HAVING 右列元数据
    std::vector<std::string> insert_order_;                                     // 记录分组键的插入顺序
    size_t current_group_index_;                                                // 当前遍历的分组索引
    std::vector<RmRecord> results_;                                             // 储存结果
    std::vector<RmRecord>::iterator result_it_;                                 // 结果迭代器
    bool initialized_ = false;                                                 // 是否已初始化
    
    // 用于跟踪 AVG 聚合的中间状态
    struct AvgState
    {
        double sum = 0.0; // 可能丢失精度
        int count = 0;
    };
    std::unordered_map<std::string, std::vector<AvgState>> avg_states_;            // 分组键 -> AVG状态数组
    std::unordered_map<std::string, std::vector<AvgState>> having_lhs_avg_states_; // HAVING左AVG状态
    std::unordered_map<std::string, std::vector<AvgState>> having_rhs_avg_states_; // HAVING右AVG状态

    void avg_calculate(const std::vector<TabCol> &sel_cols, std::vector<AvgState> avg_states, std::vector<Value> &agg_values);
    void init(std::vector<Value> &agg_values, const std::vector<TabCol> &sel_cols_, const RmRecord &record);
    void aggregate_values(std::vector<Value> &agg_values, std::vector<AvgState> &avg_states, 
                         const std::vector<TabCol> &sel_cols_, 
                         const std::vector<std::vector<ColMeta>::const_iterator> &sel_col_metas_, 
                         const RmRecord &record);
    void aggregate_batch(const std::vector<RmRecord> &records);
    std::string get_group_key(const RmRecord &record);                                                                              
    bool check_having_conditions(const std::vector<Value> &having_lhs_agg_values, const std::vector<Value> &having_rhs_agg_values);
    bool compare_values(const Value &lhs_value, const Value &rhs_value, CompOp op);
    void generate_results_batch(size_t batch_size);

public:
    AggExecutor(std::unique_ptr<AbstractExecutor> child_executor, const std::vector<TabCol> &sel_cols,
                const std::vector<TabCol> &group_by_cols, const std::vector<Condition> &having_conds,
                Context *context)
        : AbstractExecutor(context), child_executor_(std::move(child_executor)),
          sel_cols_(std::move(sel_cols)), group_by_cols_(std::move(group_by_cols)), having_conds_(std::move(having_conds)),
          TupleLen(0), current_group_index_(0)
    {
        // 初始化输出列
        int offset = 0;
        for (const auto &col : sel_cols_)
        {
            ColMeta col_meta;
            if (ast::AggFuncType::COUNT == col.aggFuncType)
            {
                col_meta = {col.tab_name, col.col_name, TYPE_INT, sizeof(int), offset};
                sel_col_metas_.emplace_back(output_cols_.begin());
            }
            else if (ast::AggFuncType::AVG == col.aggFuncType)
            {
                col_meta = {col.tab_name, col.col_name, TYPE_STRING, 20, offset};
                auto temp = get_col(child_executor_->cols(), col);
                sel_col_metas_.emplace_back(temp);
            }
            else
            {
                if (col.col_name == "*")
                {
                    throw InvalidAggTypeError("*", std::to_string(col.aggFuncType));
                }
                auto temp = get_col(child_executor_->cols(), col);
                sel_col_metas_.emplace_back(temp);
                col_meta = *temp;
                col_meta.offset = offset;
            }
            col_meta.aggFuncType = col.aggFuncType;
            TupleLen += col_meta.len;
            offset += col_meta.len;
            output_cols_.emplace_back(std::move(col_meta));
        }
        // 初始化 GROUP BY 列元数据
        for (const auto &col : group_by_cols_)
        {
            auto temp = get_col(child_executor_->cols(), col);
            group_by_col_metas_.emplace_back(std::move(temp));
        }        
        // 初始化 HAVING 列元数据
        for (const auto &cond : having_conds_)
        {
            std::vector<ColMeta> col_metas;
            if (ast::AggFuncType::COUNT == cond.lhs_col.aggFuncType)
            {
                ColMeta col_meta = {cond.lhs_col.tab_name, cond.lhs_col.col_name, TYPE_INT, sizeof(int), 0};
                having_lhs_cols_.emplace_back(cond.lhs_col);
                col_metas.emplace_back(std::move(col_meta));
                having_lhs_col_metas_.emplace_back(col_metas.begin());
            }
            else
            {
                if (cond.lhs_col.col_name == "*")
                {
                    throw InvalidAggTypeError("*", std::to_string(cond.lhs_col.aggFuncType));
                }
                auto temp = get_col(child_executor_->cols(), cond.lhs_col);
                having_lhs_cols_.emplace_back(cond.lhs_col);
                having_lhs_col_metas_.emplace_back(std::move(temp));
            }

            if (!cond.is_rhs_val)
            {
                if (ast::AggFuncType::COUNT == cond.rhs_col.aggFuncType)
                {
                    ColMeta col_meta = {cond.rhs_col.tab_name, cond.rhs_col.col_name, TYPE_INT, sizeof(int), 0};
                    having_lhs_cols_.emplace_back(cond.rhs_col);
                    col_metas.emplace_back(std::move(col_meta));
                    having_lhs_col_metas_.emplace_back(col_metas.begin());
                }
                else
                {
                    if (cond.rhs_col.col_name == "*")
                    {
                        throw InvalidAggTypeError("*", std::to_string(cond.rhs_col.aggFuncType));
                    }
                    auto temp = get_col(child_executor_->cols(), cond.rhs_col);
                    having_lhs_cols_.emplace_back(cond.rhs_col);
                    having_lhs_col_metas_.emplace_back(std::move(temp));
                }
            }
        }
    }

    size_t tupleLen() const override { return TupleLen; }

    const std::vector<ColMeta> &cols() const override { return output_cols_; }

    void beginTuple() override
    {
        if (!initialized_)
        {
            initialize();
        }
        result_it_ = results_.begin();
    }

    std::vector<std::unique_ptr<RmRecord>> next_batch(size_t batch_size = BATCH_SIZE) override {
        std::vector<std::unique_ptr<RmRecord>> batch_result;
        
        if (!initialized_)
        {
            initialize();
        }
        
        // 如果还有未处理的记录，先处理它们
        if (result_it_ == results_.end())
        {
            // 获取子执行器的一批记录
            auto input_batch = child_executor_->next_batch(batch_size);
            
            if (!input_batch.empty())
            {
                // 处理输入批次
                std::vector<RmRecord> records;
                for (auto &rec_ptr : input_batch)
                {
                    records.push_back(*rec_ptr);
                }
                aggregate_batch(records);
            }
            
            // 生成结果批次
            if (current_group_index_ < insert_order_.size())
            {
                generate_results_batch(batch_size);
            }
        }
        
        // 收集结果批次
        while (batch_result.size() < batch_size && result_it_ != results_.end())
        {
            batch_result.push_back(std::make_unique<RmRecord>(*result_it_));
            ++result_it_;
        }
        
        return batch_result;
    }

    ExecutionType type() const override { return ExecutionType::Agg; }

private:
    void initialize()
    {
        agg_groups_.clear();
        having_lhs_agg_groups_.clear();
        having_rhs_agg_groups_.clear();
        avg_states_.clear();
        having_lhs_avg_states_.clear();
        having_rhs_avg_states_.clear();
        insert_order_.clear();
        results_.clear();
        current_group_index_ = 0;
        
        // 处理初始批次
        auto input_batch = child_executor_->next_batch(BATCH_SIZE);
        while (!input_batch.empty())
        {
            std::vector<RmRecord> records;
            for (auto &rec_ptr : input_batch)
            {
                records.push_back(*rec_ptr);
            }
            aggregate_batch(records);
            input_batch = child_executor_->next_batch(BATCH_SIZE);
        }
        
        // 生成初始结果
        if (!insert_order_.empty())
        {
            generate_results_batch(BATCH_SIZE);
        }
        else
        {
            // 如果没有分组，添加默认结果
            RmRecord record(TupleLen);
            int offset = 0;
            std::vector<Value> agg_values;
            agg_values.resize(sel_cols_.size());
            for (size_t i = 0; i < sel_cols_.size(); ++i)
            {
                auto agg_type = sel_cols_[i].aggFuncType;
                if (ast::AggFuncType::COUNT == agg_type)
                {
                    agg_values[i].set_int(0);
                }
                else if (ast::AggFuncType::AVG == agg_type)
                {
                    agg_values[i].type = TYPE_STRING;
                    agg_values[i].set_str("0.000000");
                }
                else if (ast::AggFuncType::SUM == agg_type 
                || ast::AggFuncType::MAX == agg_type 
                || ast::AggFuncType::MIN == agg_type)
                {
                    auto col = get_col(child_executor_->cols(), {sel_cols_[i].tab_name, sel_cols_[i].col_name});
                    if (ast::AggFuncType::MIN == agg_type)
                        agg_values[i].set_max(col->type, col->len);
                    else if (ast::AggFuncType::MAX == agg_type)
                        agg_values[i].set_min(col->type, col->len);
                    else
                    {
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
            for (size_t i = 0; i < sel_cols_.size(); ++i)
            {
                agg_values[i].export_val(record.data + offset, output_cols_[i].len);
                offset += output_cols_[i].len;
            }
            results_.emplace_back(std::move(record));
        }
        result_it_ = results_.begin();
        initialized_ = true;
    }
};

// AVG计算实现
void AggExecutor::avg_calculate(const std::vector<TabCol> &sel_cols, 
                                std::vector<AvgState> avg_states, std::vector<Value> &agg_values)
{
    for (size_t i = 0; i < sel_cols.size(); ++i)
    {
        if (sel_cols[i].aggFuncType == ast::AggFuncType::AVG)
        {
            auto &state = avg_states[i];
            if (state.count > 0)
            {
                double b = state.sum / state.count;
                double rounded = std::round(b * 1e6) / 1e6;
                std::string str = std::to_string(rounded);
                agg_values[i].type = TYPE_STRING;
                agg_values[i].set_str(str);
            }
        }
    }
}

// 初始化聚合值
void AggExecutor::init(std::vector<Value> &agg_values, 
                        const std::vector<TabCol> &sel_cols_, 
                        const RmRecord &record)
{
    for (size_t i = 0; i < sel_cols_.size(); ++i)
    {
        auto agg_type = sel_cols_[i].aggFuncType;
        if (ast::AggFuncType::COUNT == agg_type)
        {
            agg_values[i].set_int(0);
        }
        else if (ast::AggFuncType::AVG == agg_type)
        {
            agg_values[i].type = TYPE_STRING;
            agg_values[i].set_str("0.0");
        }
        else if (ast::AggFuncType::SUM == agg_type 
        || ast::AggFuncType::MAX == agg_type 
        || ast::AggFuncType::MIN == agg_type)
        {
            auto col = get_col(child_executor_->cols(), {sel_cols_[i].tab_name, sel_cols_[i].col_name});
            if (ast::AggFuncType::MIN == agg_type)
                agg_values[i].set_max(col->type, col->len);
            else if (ast::AggFuncType::MAX == agg_type)
                agg_values[i].set_min(col->type, col->len);
            else
            {
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
        else if (ast::AggFuncType::NO_TYPE == agg_type)
        {
            auto col_meta = *get_col(child_executor_->cols(), sel_cols_[i]);
            switch (col_meta.type)
            {
            case TYPE_INT:
                agg_values[i].set_int(*reinterpret_cast<const int *>(record.data + col_meta.offset));
                break;
            case TYPE_FLOAT:
                agg_values[i].set_float(*reinterpret_cast<const float *>(record.data + col_meta.offset));
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

// 聚合值计算
void AggExecutor::aggregate_values(std::vector<Value> &agg_values, std::vector<AvgState> &avg_states, 
                                const std::vector<TabCol> &sel_cols_, 
                                const std::vector<std::vector<ColMeta>::const_iterator> &sel_col_metas_, 
                                const RmRecord &record)
{
    for (size_t i = 0; i < sel_cols_.size(); ++i)
    {
        auto agg_type = sel_cols_[i].aggFuncType;
        if (ast::AggFuncType::NO_TYPE == agg_type)
        {
            continue;
        }
        if (ast::AggFuncType::COUNT == agg_type)
        {
            ++agg_values[i].int_val;
            continue;
        }
        auto &col_meta = *sel_col_metas_[i];
        Value value;
        value.type = col_meta.type;

        switch (col_meta.type)
        {
        case TYPE_INT:
            value.set_int(*reinterpret_cast<const int *>(record.data + col_meta.offset));
            break;
        case TYPE_FLOAT:
            value.set_float(*reinterpret_cast<const float *>(record.data + col_meta.offset));
            break;
        case TYPE_STRING:
            value.set_str(std::string(record.data + col_meta.offset, col_meta.len));
            break;
        default:
            throw InternalError("Unexpected sv value type 5");
        }

        switch (agg_type)
        {
        case ast::AggFuncType::SUM:
            if (TYPE_INT == value.type)
            {
                agg_values[i].int_val += value.int_val;
            }
            else if (TYPE_FLOAT == value.type)
            {
                agg_values[i].float_val += value.float_val;
            }
            break;
        case ast::AggFuncType::MAX:
            agg_values[i] = std::max(agg_values[i], value);
            break;
        case ast::AggFuncType::MIN:
            agg_values[i] = std::min(agg_values[i], value);
            break;
        case ast::AggFuncType::AVG:
            if (TYPE_INT == value.type)
            {
                avg_states[i].sum += static_cast<double>(value.int_val);
            }
            else if (TYPE_FLOAT == value.type)
            {
                avg_states[i].sum += static_cast<double>(value.float_val);
            }
            ++avg_states[i].count;
            break;
        default:
            break;
        }
    }
}

// 批量聚合处理
void AggExecutor::aggregate_batch(const std::vector<RmRecord> &records)
{
    for (const auto &record : records)
    {
        std::string group_key = get_group_key(record);
        auto &agg_values = agg_groups_[group_key];
        auto &having_lhs_agg_values = having_lhs_agg_groups_[group_key];
        auto &having_rhs_agg_values = having_rhs_agg_groups_[group_key];

        auto &avg_states = avg_states_[group_key];
        auto &having_lhs_avg_states = having_lhs_avg_states_[group_key];
        auto &having_rhs_avg_states = having_rhs_avg_states_[group_key];
        
        if (agg_values.empty())
        {
            insert_order_.emplace_back(group_key);
            agg_values.resize(sel_cols_.size());
            having_lhs_agg_values.resize(having_lhs_cols_.size());
            having_rhs_agg_values.resize(having_rhs_cols_.size());

            avg_states.resize(sel_cols_.size());
            having_lhs_avg_states.resize(having_lhs_cols_.size());
            having_rhs_avg_states.resize(having_rhs_cols_.size());

            init(agg_values, sel_cols_, record);
            init(having_lhs_agg_values, having_lhs_cols_, record);
            init(having_rhs_agg_values, having_rhs_cols_, record);
        }
        
        aggregate_values(agg_values, avg_states, sel_cols_, sel_col_metas_, record);
        aggregate_values(having_lhs_agg_values, having_lhs_avg_states, having_lhs_cols_, having_lhs_col_metas_, record);
        aggregate_values(having_rhs_agg_values, having_rhs_avg_states, having_rhs_cols_, having_rhs_col_metas_, record);
    }
}

std::string AggExecutor::get_group_key(const RmRecord &record)
{
    if (group_by_cols_.empty())
    {
        return "no_groupby";
    }
    std::string key;
    for (size_t i = 0; i < group_by_cols_.size(); ++i)
    {
        auto col_meta = *group_by_col_metas_[i];
        switch (col_meta.type)
        {
        case TYPE_INT:
            key += std::to_string(*reinterpret_cast<const int *>(record.data + col_meta.offset));
            break;
        case TYPE_FLOAT:
            key += std::to_string(*reinterpret_cast<const float *>(record.data + col_meta.offset));
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

bool AggExecutor::check_having_conditions(const std::vector<Value> &having_lhs_agg_values, 
                                            const std::vector<Value> &having_rhs_agg_values)
{
    auto lhs_it = having_lhs_agg_values.begin();
    auto rhs_it = having_rhs_agg_values.begin();
    for (const auto &cond : having_conds_)
    {
        // 获取左操作数的值
        Value lhs_value = *lhs_it;
        ++lhs_it;

        // 获取右操作数的值
        Value rhs_value;
        if (cond.is_rhs_val)
        {
            rhs_value = cond.rhs_val;
        }
        else
        {
            rhs_value = *rhs_it;
            ++rhs_it;
        }

        // 检查条件是否满足
        if (!compare_values(lhs_value, rhs_value, cond.op))
        {
            return false;
        }
    }
    return true;
}

bool AggExecutor::compare_values(const Value &lhs_value, const Value &rhs_value, CompOp op)
{
    // 如果其中一个是字符串数值（AVG 结果），转换为 double 进行比较
    auto convert_to_double = [](const Value &val) -> double
    {
        if (val.type == TYPE_STRING)
        {
            try
            {
                return std::stod(val.str_val);
            }
            catch (...)
            {
                throw InternalError("Invalid numeric string in AVG comparison");
            }
        }
        else if (val.type == TYPE_INT)
        {
            return static_cast<double>(val.int_val);
        }
        else if (val.type == TYPE_FLOAT)
        {
            return static_cast<double>(val.float_val);
        }
        else
        {
            throw InternalError("Unsupported type in AVG comparison");
        }
    };

    double lhs_num = convert_to_double(lhs_value);
    double rhs_num = convert_to_double(rhs_value);

    switch (op)
    {
    case OP_EQ:
        return std::abs(lhs_num - rhs_num) < 1e-9;
    case OP_NE:
        return std::abs(lhs_num - rhs_num) >= 1e-9;
    case OP_LT:
        return lhs_num < rhs_num;
    case OP_LE:
        return lhs_num <= rhs_num;
    case OP_GT:
        return lhs_num > rhs_num;
    case OP_GE:
        return lhs_num >= rhs_num;
    default:
        throw InternalError("Unexpected comparison operator");
    }
}

// 批量生成结果
void AggExecutor::generate_results_batch(size_t batch_size)
{
    size_t count = 0;
    while (current_group_index_ < insert_order_.size() && count < batch_size)
    {
        const std::string &group_key = insert_order_[current_group_index_];
        auto &agg_values = agg_groups_[group_key];
        auto &having_lhs_agg_values = having_lhs_agg_groups_[group_key];
        auto &having_rhs_agg_values = having_rhs_agg_groups_[group_key];

        auto &avg_states = avg_states_[group_key];
        auto &having_lhs_avg_states = having_lhs_avg_states_[group_key];
        auto &having_rhs_avg_states = having_rhs_avg_states_[group_key];

        avg_calculate(sel_cols_, avg_states, agg_values);
        avg_calculate(having_lhs_cols_, having_lhs_avg_states, having_lhs_agg_values);
        avg_calculate(having_rhs_cols_, having_rhs_avg_states, having_rhs_agg_values);

        if (check_having_conditions(having_lhs_agg_values, having_rhs_agg_values))
        {
            RmRecord record(TupleLen);
            int offset = 0;

            for (size_t i = 0; i < sel_cols_.size(); ++i)
            {
                agg_values[i].export_val(record.data + offset, output_cols_[i].len);
                offset += output_cols_[i].len;
            }
            results_.push_back(std::move(record));
            count++;
        }
        current_group_index_++;
    }
    result_it_ = results_.begin();
}