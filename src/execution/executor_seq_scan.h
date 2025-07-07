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
    std::string tab_name_;                   // 表的名称
    std::shared_ptr<RmFileHandle_Final> fh_; // 表的数据文件句柄
    size_t len_;                             // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;       // scan后生成的记录的字段
    TabMeta tab_;                            // 表的元数据
    std::vector<ColMeta> cols_;              // scan后生成的记录的字段

    std::unique_ptr<RmScan_Final> scan_; // table_iterator
    SmManager *sm_manager_;
    std::vector<size_t> col_indices_; // 在原始记录中的列索引

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
        return std::all_of(fed_conds_.begin(), fed_conds_.end(), [&](auto &cond)
                           {
                ColMeta &left_col = get_col_meta(cond.lhs_col.col_name);
                char *lhs_value = get_col_value(rec, left_col);
                char *rhs_value;
                ColType rhs_type;

                if (cond.is_rhs_val)
                {
                    rhs_value = const_cast<char *>(cond.rhs_val.raw->data);
                    rhs_type = cond.rhs_val.type;
                }
                else
                {
                    ColMeta &right_col = get_col_meta(cond.rhs_col.col_name);
                    rhs_value = get_col_value(rec, right_col);
                    rhs_type = right_col.type;
                }
                return check_condition(lhs_value, left_col.type, rhs_value, rhs_type, cond.op, left_col.len); });
    }

public:
    SeqScanExecutor(SmManager *sm_manager, const std::string &tab_name, const std::vector<Condition> &conds,
                    Context *context) : AbstractExecutor(context), tab_name_(std::move(tab_name)),
                                        fed_conds_(conds), sm_manager_(sm_manager)
    {
        tab_ = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->get_table_handle(tab_name_);
        len_ = tab_.cols.back().offset + tab_.cols.back().len;
        std::sort(fed_conds_.begin(), fed_conds_.end());
        scan_ = std::make_unique<RmScan_Final>(fh_, context_);
    }

    // 批量获取下一个batch_size个满足条件的元组，最少一页，最多batch_size且为页的整数倍
    std::vector<std::unique_ptr<RmRecord>> next_batch(size_t batch_size = BATCH_SIZE) override
    {
        std::vector<std::unique_ptr<RmRecord>> batch;
        batch.reserve(batch_size);
        while (batch.size() < batch_size && !scan_->is_end())
        {
            auto scan_batch = scan_->record_batch();
            for (auto &rec : scan_batch)
            {
                if (satisfy_conditions(rec.get()))
                {
                    batch.emplace_back(project(rec));
                }
            }
            scan_->next_batch();
        }
        return batch;
    }

    // 获取当前批次所有可见记录的RID
    std::vector<Rid> rid_batch(size_t batch_size = BATCH_SIZE) override
    {
        std::vector<Rid> batch;
        batch.reserve(batch_size);
        while (batch.size() < batch_size && !scan_->is_end())
        {
            auto scan_batch = scan_->record_batch();
            auto rids = scan_->rid_batch();
            assert(rids.size() == scan_batch.size());
            for (size_t id = 0; id < scan_batch.size(); id++)
            {
                if (satisfy_conditions(scan_batch[id].get()))
                {
                    batch.emplace_back(rids[id]);
                }
            }
            scan_->next_batch();
        }
        return batch;
    }

    void set_cols(const std::vector<TabCol> &sel_cols) override
    {
        auto &prev_cols = tab_.cols;
        cols_.reserve(sel_cols.size());
        col_indices_.reserve(sel_cols.size());

        for (auto &sel_col : sel_cols)
        {
            auto pos = get_col(prev_cols, sel_col);

            cols_.emplace_back(*pos);
            col_indices_.emplace_back(pos - prev_cols.begin());
        }
        len_ = 0;
        for (auto &col : cols_)
        {
            // 重新计算投影后的偏移
            col.offset = len_;
            len_ += col.len;
        }
    }

    std::unique_ptr<RmRecord> project(std::unique_ptr<RmRecord> &prev_record)
    {
        if (cols_.empty())
            return std::move(prev_record);

        // 创建新的投影记录
        auto projected_record = std::make_unique<RmRecord>(len_);

        // 获取原始记录的列信息
        const auto &prev_cols = tab_.cols;

        // 复制选定的列到新记录中
        for (size_t i = 0; i < cols_.size(); ++i)
        {
            const auto &src_col = prev_cols[col_indices_[i]];
            const auto &dst_col = cols_[i];

            // 从原记录复制数据到新记录
            memcpy(projected_record->data + dst_col.offset,
                   prev_record->data + src_col.offset,
                   src_col.len);
        }

        return projected_record;
    }

    const std::vector<ColMeta> &cols() const override
    {
        return (cols_.size() ? cols_ : tab_.cols);
    }
    size_t tupleLen() const override { return len_; }
    ExecutionType type() const override { return ExecutionType::SeqScan; }
};