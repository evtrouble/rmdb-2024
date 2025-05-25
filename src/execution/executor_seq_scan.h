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

class SeqScanExecutor : public AbstractExecutor
{
private:
    std::string tab_name_;             // 表的名称
    std::vector<Condition> conds_;     // scan的条件
    RmFileHandle *fh_;                 // 表的数据文件句柄
    std::vector<ColMeta> cols_;        // scan后生成的记录的字段
    size_t len_;                       // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_; // 同conds_，两个字段相同
    TabMeta tab_;                      // 表的元数据

    Rid rid_;
    std::unique_ptr<RecScan> scan_; // table_iterator

    SmManager *sm_manager_;
    bool first = true;
    // 获取指定列的值
    inline char *get_col_value(const RmRecord *rec, const ColMeta &col)
    {
        return rec->data + col.offset;
    }

    // 查找列的元数据
    ColMeta &get_col_meta(const std::string &col_name)
    {
        auto iter = tab_.cols_map.find(col_name);
        if (iter != tab_.cols_map.end())
            return tab_.cols.at(iter->second);
        throw ColumnNotFoundError(col_name);
    }

    // 检查记录是否满足所有条件
    bool satisfy_conditions(const RmRecord *rec)
    {
        return std::all_of(conds_.begin(), conds_.end(), [&](auto &cond)
                           {
            if (!cond.is_subquery) {
                // 处理普通条件
                ColMeta &left_col = get_col_meta(cond.lhs_col.col_name);
                char *lhs_value = get_col_value(rec, left_col);
                char *rhs_value;
                ColType rhs_type;

                if (cond.is_rhs_val)
                {
                    // 如果右侧是值
                    rhs_value = const_cast<char *>(cond.rhs_val.raw->data);
                    rhs_type = cond.rhs_val.type;
                }
                else
                {
                    // 如果右侧是列
                    ColMeta &right_col = get_col_meta(cond.rhs_col.col_name);
                    rhs_value = get_col_value(rec, right_col);
                    rhs_type = right_col.type;
                }
                return check_condition(lhs_value, left_col.type, rhs_value, rhs_type, cond.op);
            } else {
                // 处理子查询条件
                ColMeta &left_col = get_col_meta(cond.lhs_col.col_name);
                char *lhs_buf = get_col_value(rec, left_col);
                Value lhs;

                switch (left_col.type)
                {
                case TYPE_INT:
                {
                    int lhs_value = *reinterpret_cast<int *>(lhs_buf);
                    lhs.set_int(lhs_value);
                    break;
                }
                case TYPE_FLOAT:
                {
                    float lhs_value = *reinterpret_cast<float *>(lhs_buf);
                    lhs.set_float(lhs_value);
                    break;
                }
                case TYPE_STRING:
                {
                    std::string lhs_value(lhs_buf, strnlen(lhs_buf, left_col.len));
                    lhs.set_str(lhs_value);
                    break;
                }
                }

                if (cond.subQuery->is_scalar)
                {
                    if (cond.subQuery->result.size() != 1)
                    {
                        throw RMDBError("Scalar subquery result size is not 1");
                    }

                    const auto &subQueryResult = *cond.subQuery->result.begin();
                    // 比较左值和子查询结果
                    return compare_values(lhs, subQueryResult, cond.op);
                }

                // 对于 IN/NOT IN 类型的子查询
                if (lhs.type == TYPE_INT && cond.subQuery->subquery_type == TYPE_FLOAT)
                {
                    lhs.set_float((float)lhs.int_val);
                }

                bool found = (cond.subQuery->result.find(lhs) != cond.subQuery->result.end());
                return (cond.op == OP_IN) == found;
            } });
    }

    // 比较两个 Value 值
    bool compare_values(const Value &lhs, const Value &rhs, CompOp op)
    {
        int cmp;
        if (lhs.type == TYPE_INT && rhs.type == TYPE_INT)
        {
            cmp = lhs.int_val - rhs.int_val;
        }
        else if (lhs.type == TYPE_FLOAT && rhs.type == TYPE_FLOAT)
        {
            cmp = lhs.float_val < rhs.float_val ? -1 : (lhs.float_val > rhs.float_val ? 1 : 0);
        }
        else if (lhs.type == TYPE_INT && rhs.type == TYPE_FLOAT)
        {
            cmp = lhs.int_val < rhs.float_val ? -1 : (lhs.int_val > rhs.float_val ? 1 : 0);
        }
        else if (lhs.type == TYPE_FLOAT && rhs.type == TYPE_INT)
        {
            cmp = lhs.float_val < rhs.int_val ? -1 : (lhs.float_val > rhs.int_val ? 1 : 0);
        }
        else if (lhs.type == TYPE_STRING && rhs.type == TYPE_STRING)
        {
            cmp = lhs.str_val.compare(rhs.str_val);
        }
        else
        {
            throw RMDBError("Cannot compare values of different types");
        }

        switch (op)
        {
        case OP_EQ:
            return cmp == 0;
        case OP_NE:
            return cmp != 0;
        case OP_LT:
            return cmp < 0;
        case OP_GT:
            return cmp > 0;
        case OP_LE:
            return cmp <= 0;
        case OP_GE:
            return cmp >= 0;
        default:
            throw RMDBError("Unsupported comparison operator for scalar subquery");
        }
    }
    void find_next_valid_tuple()
    {
        while (!scan_->is_end())
        {
            rid_ = scan_->rid();
            std::unique_ptr<RmRecord> rid_record = fh_->get_record(rid_, context_);
            if (satisfy_conditions(rid_record.get()))
            {
                sm_manager_->get_bpm()->unpin_page({fh_->GetFd(), rid_.page_no}, false);
                return;
            }
            sm_manager_->get_bpm()->unpin_page({fh_->GetFd(), rid_.page_no}, false);
            scan_->next();
        }
        rid_.page_no = RM_NO_PAGE; // 设置 rid_ 以指示结束
    }

public:
    SeqScanExecutor(SmManager *sm_manager, const std::string &tab_name, const std::vector<Condition> &conds,
                    Context *context) : AbstractExecutor(context), tab_name_(tab_name),
                                        conds_(conds), sm_manager_(sm_manager), first(true)
    {
        // 检查文件句柄是否存在
        auto fh_iter = sm_manager_->fhs_.find(tab_name_);
        if (fh_iter == sm_manager_->fhs_.end())
        {
            throw std::runtime_error("File handle for table " + tab_name_ + " not found");
        }

        tab_ = sm_manager_->db_.get_table(tab_name_);
        fh_ = fh_iter->second.get();
        cols_ = tab_.cols;
        len_ = tab_.cols.back().offset + tab_.cols.back().len;

        // 对条件进行排序，以便后续处理
        std::sort(conds_.begin(), conds_.end());
    }
    void beginTuple() override
    {
        scan_ = std::make_unique<RmScan>(fh_);
        find_next_valid_tuple();
    }

    void nextTuple() override
    {
        scan_->next();
        find_next_valid_tuple();
    }

    bool is_end() const override { return rid_.page_no == RM_NO_PAGE; }

    std::unique_ptr<RmRecord> Next() override
    {
        if (!is_end())
        {
            std::unique_ptr<RmRecord> rid_record = fh_->get_record(rid_, context_);
            std::unique_ptr<RmRecord> ret = std::make_unique<RmRecord>(rid_record->size, rid_record->data);
            sm_manager_->get_bpm()->unpin_page({fh_->GetFd(), rid_.page_no}, false);
            return ret;
        }
        return nullptr;
    }

    const std::vector<ColMeta> &cols() const override { return tab_.cols; }
    size_t tupleLen() const override { return len_; }

    Rid &rid() override { return rid_; }

    ExecutionType type() const override { return ExecutionType::SeqScan; }
};