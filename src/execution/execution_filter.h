#pragma once

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "system/sm.h"

class FilterExecutor : public AbstractExecutor
{
private:
    std::unique_ptr<AbstractExecutor> prev_; // 子执行器
    std::vector<Condition> conds_;           // 过滤条件
    SmManager *sm_manager_;                  // 用于获取表元数据

    // 获取指定列的值
    inline char *get_col_value(const RmRecord *rec, const ColMeta &col)
    {
        return rec->data + col.offset;
    }

    // 查找列的元数据
    ColMeta *get_col_meta(const std::string &tab_name, const std::string &col_name)
    {
        const auto &cols = prev_->cols();
        for (const auto &col : cols)
        {
            if ((tab_name.empty() || col.tab_name == tab_name) && col.name == col_name)
            {
                return const_cast<ColMeta *>(&col);
            }
        }
        return nullptr;
    }

    // 检查记录是否满足所有条件
    bool satisfy_conditions(const RmRecord *rec)
    {
        return std::all_of(conds_.begin(), conds_.end(), [&](const auto &cond)
                           {
            // 获取左侧列的元数据和值
            ColMeta *left_col = get_col_meta(cond.lhs_col.tab_name, cond.lhs_col.col_name);
            if (!left_col) {
                throw ColumnNotFoundError(cond.lhs_col.tab_name + "." + cond.lhs_col.col_name);
            }

            char *lhs_value = get_col_value(rec, *left_col);
            char *rhs_value;
            ColType rhs_type;

            if (cond.is_rhs_val) {
                // 如果右侧是值
                rhs_value = const_cast<char *>(cond.rhs_val.raw->data);
                rhs_type = cond.rhs_val.type;
            } else {
                // 如果右侧是列
                ColMeta *right_col = get_col_meta(cond.rhs_col.tab_name, cond.rhs_col.col_name);
                if (!right_col) {
                    throw ColumnNotFoundError(cond.rhs_col.tab_name + "." + cond.rhs_col.col_name);
                }
                rhs_value = get_col_value(rec, *right_col);
                rhs_type = right_col->type;
            }

            return check_condition(lhs_value, left_col->type, rhs_value, rhs_type, cond.op, left_col->len); });
    }

public:
    FilterExecutor(std::unique_ptr<AbstractExecutor> prev, std::vector<Condition> conds, SmManager *sm_manager = nullptr)
        : prev_(std::move(prev)), conds_(std::move(conds)), sm_manager_(sm_manager)
    {
        // 对条件进行排序，以便后续处理
        std::sort(conds_.begin(), conds_.end());
    }

    const std::vector<ColMeta> &cols() const override
    {
        return prev_->cols();
    }

    size_t tupleLen() const override
    {
        return prev_->tupleLen();
    }

    void beginTuple() override
    {
        prev_->beginTuple();
        // 找到第一个满足条件的元组
        while (!prev_->is_end())
        {
            auto record = prev_->Next();
            if (record && satisfy_conditions(record.get()))
            {
                break;
            }
            prev_->nextTuple();
        }
    }

    void nextTuple() override
    {
        if (prev_->is_end())
        {
            return;
        }

        prev_->nextTuple();
        // 找到下一个满足条件的元组
        while (!prev_->is_end())
        {
            auto record = prev_->Next();
            if (record && satisfy_conditions(record.get()))
            {
                break;
            }
            prev_->nextTuple();
        }
    }

    std::unique_ptr<RmRecord> Next() override
    {
        if (prev_->is_end())
        {
            return nullptr;
        }

        auto record = prev_->Next();
        if (record && satisfy_conditions(record.get()))
        {
            return record;
        }

        return nullptr;
    }

    bool is_end() const override
    {
        return prev_->is_end();
    }

    Rid &rid() override
    {
        return prev_->rid();
    }

    ExecutionType type() const override
    {
        return ExecutionType::Filter;
    }
};