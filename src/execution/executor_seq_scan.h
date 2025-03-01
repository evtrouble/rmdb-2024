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
    std::vector<ColMeta> cols_;        // scan后生成的记录的字段
    size_t len_;                       // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_; // 同conds_，两个字段相同
    TabMeta tab_;                      // 表的元数据

    Rid rid_;
    std::unique_ptr<RmScan> scan_; // table_iterator

    SmManager *sm_manager_;

private:
    // 辅助函数：检查条件是否满足
    bool check_condition(char *lhs_value, ColType lhs_type, char *rhs_value, ColType rhs_type, CompOp op)
    {
        return AbstractExecutor::check_condition(lhs_value, lhs_type, rhs_value, rhs_type, op);
    }

    // 获取指定列的值
    char *get_col_value(const RmRecord *rec, const ColMeta &col)
    {
        return rec->data + col.offset;
    }

    // 查找列的元数据
    ColMeta get_col_meta(const std::string &col_name)
    {
        for (const auto &col : cols_)
        {
            if (col.name == col_name)
            {
                return col;
            }
        }
        throw ColumnNotFoundError(col_name);
    }

    // 检查记录是否满足所有条件
    bool satisfy_conditions(const RmRecord *rec)
    {

        if (fed_conds_.empty())
        {
            return true;
        }

        for (const auto &cond : fed_conds_)
        {
            ColMeta left_col = get_col_meta(cond.lhs_col.col_name);
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
                ColMeta right_col = get_col_meta(cond.rhs_col.col_name);
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
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;
        fed_conds_ = conds_;
        tab_ = tab;
    }

    void beginTuple() override
    {

        // 初始化扫描表
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

    const std::vector<ColMeta> &cols() const override { return cols_; }

    Rid &rid() override { return rid_; }

private:
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
};