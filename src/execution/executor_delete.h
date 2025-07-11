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
#include <memory>
#include <vector>
#include <stdexcept>
#include <iostream>

class DeleteExecutor : public AbstractExecutor
{
private:
    TabMeta tab_;                  // 表的元数据
    std::vector<Condition> conds_; // delete的条件
    std::shared_ptr<RmFileHandle> fh_;             // 表的数据文件句柄
    std::vector<Rid> rids_;        // 需要删除的记录的位置
    std::string tab_name_;         // 表名称
    SmManager *sm_manager_;
    std::vector<std::shared_ptr<IxIndexHandle>> ihs_; // 缓存索引句柄

    // 构建索引键的辅助函数
    std::unique_ptr<char[]> build_index_key(const IndexMeta &index, const RmRecord &rec)
    {
        std::unique_ptr<char[]> key(new char[index.col_tot_len]);
        int offset = 0;
        for (int i = 0; i < index.col_num; ++i)
        {
            memcpy(key.get() + offset, rec.data + index.cols[i].offset, index.cols[i].len);
            offset += index.cols[i].len;
        }
        return key;
    }

public:
    DeleteExecutor(SmManager *sm_manager, std::string &tab_name,
                std::vector<Rid> &rids, Context *context)
        : AbstractExecutor(context), rids_(std::move(rids)),
          tab_name_(std::move(tab_name)), sm_manager_(sm_manager)
    {
        tab_ = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->get_table_handle(tab_name_);

        ihs_.reserve(tab_.indexes.size());
        for (auto &index : tab_.indexes)
        {
            ihs_.emplace_back(sm_manager_->get_index_handle(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)));
        }
    }

    std::vector<std::unique_ptr<RmRecord>> next_batch(size_t batch_size = BATCH_SIZE) override
    {
        TransactionManager *txn_mgr = context_->txn_->get_txn_manager();
        // 遍历所有需要删除的记录
        for (auto &rid : rids_)
        {
            // 获取要删除的记录
            RmRecord rec = *fh_->get_record(rid, context_);

            // 删除相关的索引
            for (size_t i = 0; i < tab_.indexes.size(); ++i)
            {
                auto &index = tab_.indexes[i];
                auto ih = ihs_[i];

                // 构建索引键
                auto key = build_index_key(index, rec);

                // 删除索引项
                ih->delete_entry(key.get(), rid, context_->txn_);
            }

            if(!context_->lock_mgr_->lock_exclusive_on_key(context_->txn_, fh_->GetFd(), 
                rec.data + txn_mgr->get_start_offset())) {
                throw TransactionAbortException(context_->txn_->get_transaction_id(), 
                    AbortReason::UPGRADE_CONFLICT);
            }
            // 删除记录
            fh_->delete_record(rid, context_);

            //记录日志
            DeleteLogRecord log_record(context_->txn_->get_transaction_id(), 
                rec, rid, tab_name_);
            context_->log_mgr_->add_log_to_buffer(&log_record);
            if(context_->txn_->get_txn_manager()->get_concurrency_mode() != 
                ConcurrencyMode::MVCC)
            {
                auto write_record = new WriteRecord(WType::DELETE_TUPLE,
                            tab_name_, rid, rec);
                context_->txn_->append_write_record(write_record);
            }
        }

        return {};
    }

    ExecutionType type() const override { return ExecutionType::Delete; }
};