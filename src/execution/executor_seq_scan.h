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
#include "record/rm_scan.h"

class SeqScanExecutor : public AbstractExecutor
{
private:
    std::string tab_name_;             // 表的名称
    std::vector<Condition> conds_;     // scan的条件
    RmFileHandle *fh_;                 // 表的数据文件句柄
    size_t len_;                       // scan后生成的每条记录的长度
    TabMeta tab_;                      // 表的元数据
    std::vector<Condition> gap_conds_; // 用于加间隙锁的条件

    Rid rid_;
    std::unique_ptr<RmScan> scan_; // table_iterator

    SmManager *sm_manager_;

private:
    // 获取指定列的值
    char *get_col_value(const RmRecord *rec, const ColMeta &col)
    {
        return rec->data + col.offset;
    }

    // 查找列的元数据
    ColMeta &get_col_meta(const std::string &col_name)
    {
        auto iter = tab_.cols_map.find(col_name);
        if(iter != tab_.cols_map.end())
            return tab_.cols.at(iter->second);
        throw ColumnNotFoundError(col_name);
    }

    // 检查记录是否满足所有条件
    bool satisfy_conditions(const RmRecord *rec)
    {
        for (const auto &cond : conds_)
        {
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

            if (!check_condition(lhs_value, left_col.type, rhs_value, rhs_type, cond.op))
            {
                return false;
            }
        }
        return true;
    }

public:
    SeqScanExecutor(SmManager *sm_manager, const std::string &tab_name, const std::vector<Condition> &conds, 
        Context *context) : AbstractExecutor(context), tab_name_(std::move(tab_name)), 
        conds_(std::move(conds)), sm_manager_(sm_manager)
    {
        // 检查文件句柄是否存在
        auto fh_iter = sm_manager_->fhs_.find(tab_name_);
        if (fh_iter == sm_manager_->fhs_.end())
        {
            throw std::runtime_error("File handle for table " + tab_name_ + " not found");
        }

        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = fh_iter->second.get();
        len_ = tab.cols.back().offset + tab.cols.back().len;
        tab_ = tab;
        for(auto &cond : conds_){
            if(cond.op != CompOp::OP_NE && cond.is_rhs_val){
                gap_conds_.emplace_back(cond);
            }
        }
    }

    void beginTuple() override
    {
        // 初始化扫描表
        if(gap_conds_.empty())
            context_->lock_mgr_->lock_shared_on_table(context_->txn_, fh_->GetFd());
        else {
            GapLock gaplock;
            if(!gaplock.init(gap_conds_, tab_)) {
                rid_.page_no = RM_NO_PAGE; // 设置 rid_ 以指示结束
                return;
            }
            context_->lock_mgr_->lock_shared_on_gap(context_->txn_, fh_->GetFd(), gaplock);                
        }
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
            std::unique_ptr<RmRecord> rid_record = fh_->get_record(rid_);
            std::unique_ptr<RmRecord> ret = std::make_unique<RmRecord>(rid_record->size, rid_record->data);
            sm_manager_->get_bpm()->unpin_page({fh_->GetFd(), rid_.page_no}, false);
            return ret;
        }
        return nullptr;
    }

    const std::vector<ColMeta> &cols() const override { return tab_.cols; }

    Rid &rid() override { return rid_; }

private:
    void find_next_valid_tuple()
    {
        while (!scan_->is_end())
        {
            rid_ = scan_->rid();
            std::unique_ptr<RmRecord> rid_record = fh_->get_record(rid_);
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
};