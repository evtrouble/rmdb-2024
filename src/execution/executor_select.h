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

class SelectExecutor : public AbstractExecutor
{
private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

    // 添加用于迭代的成员变量
    std::unique_ptr<RmScan> scan_;
    bool is_end_;
    size_t rid_idx_;

public:
    SelectExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context)
    {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
        is_end_ = true; // 初始化为true，在beginTuple时会重置
    }

    void beginTuple() override
    {
        if (rids_.empty())
        {
            scan_ = std::make_unique<RmScan>(fh_);
            is_end_ = scan_->is_end();
        }
        else
        {
            rid_idx_ = 0;
            is_end_ = (rid_idx_ >= rids_.size());
        }
    }

    void nextTuple() override
    {
        if (rids_.empty())
        {
            if (!scan_->is_end())
            {
                scan_->next();
                is_end_ = scan_->is_end();
            }
        }
        else
        {
            rid_idx_++;
            is_end_ = (rid_idx_ >= rids_.size());
        }
    }

    bool is_end() const override
    {
        return is_end_;
    }

    std::unique_ptr<RmRecord> Next() override
    {

        // 循环直到找到满足条件的记录或到达结束
        while (!is_end_)
        {
            std::unique_ptr<RmRecord> record;
            if (rids_.empty())
            {
                // 使用扫描器获取记录
                Rid rid = scan_->rid();
                record = fh_->get_record(rid);
            }
            else
            {
                // 使用指定的RID获取记录
                Rid &rid = rids_[rid_idx_];
                record = fh_->get_record(rid);
            }

            // 检查该记录是否满足所有条件
            bool satisfy = true;
            for (auto &cond : conds_)
            {
                // 获取条件左值
                auto lhs_col = tab_.get_col(cond.lhs_col.col_name);
                char *lhs_value = record->data + (*lhs_col).offset;

                // 获取条件右值
                char *rhs_value;
                ColType rhs_type;
                if (cond.is_rhs_val)
                {
                    // 右值为常量
                    cond.rhs_val.init_raw((*lhs_col).len);
                    rhs_value = cond.rhs_val.raw->data;
                    rhs_type = cond.rhs_val.type;
                }
                else
                {
                    // 右值为列
                    auto rhs_col = tab_.get_col(cond.rhs_col.col_name);
                    rhs_value = record->data + (*rhs_col).offset;
                    rhs_type = (*rhs_col).type;
                }

                // 比较值
                if (!check_condition(lhs_value, (*lhs_col).type, rhs_value, rhs_type, cond.op))
                {
                    satisfy = false;
                    break;
                }
            }

            if (satisfy)
            {
                // 找到满足条件的记录，移动到下一条记录并返回当前记录
                nextTuple();
                return record;
            }

            // 记录不满足条件，移动到下一条记录并继续查找
            nextTuple();
        }

        // 如果到达结束位置，返回nullptr
        return nullptr;
    }
    Rid &rid() override
    {
        return _abstract_rid;
    }
};