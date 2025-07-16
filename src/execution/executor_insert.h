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

class InsertExecutor : public AbstractExecutor {
private:
    std::vector<Value> values_; // 需要插入的数据
    std::shared_ptr<RmFileHandle> fh_;          // 表的数据文件句柄
    std::string tab_name_;      // 表名称
    Rid rid_;                   // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
    SmManager *sm_manager_;
    TabMeta &tab_;               // 表的元数据

public:
    InsertExecutor(SmManager *sm_manager, std::string &tab_name, std::vector<Value> &values, Context *context)
        : AbstractExecutor(context), values_(std::move(values)),
          tab_name_(std::move(tab_name)), sm_manager_(sm_manager), tab_(sm_manager_->db_.get_table(tab_name_))
    {
        fh_ = sm_manager_->get_table_handle(tab_name_);
    };

    std::vector<std::unique_ptr<RmRecord>> next_batch(size_t batch_size = BATCH_SIZE) override {
        // Make record buffer
        RmRecord rec(fh_->get_file_hdr().record_size);
        
        // 复制用户提供的数据
        TransactionManager *txn_mgr = context_->txn_->get_txn_manager();
        for (size_t i = 0; i < values_.size(); ++i) {
            auto &col = tab_.cols[i + txn_mgr->get_hidden_column_count()];
            auto &val = values_[i];
            if (col.type != val.type && (col.type != TYPE_DATETIME || val.type != TYPE_STRING)) {
                throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
            }
            memcpy(rec.data + col.offset, val.raw->data, col.len);
        }

        // Insert into record file
        if(!context_->lock_mgr_->lock_exclusive_on_key(context_->txn_, fh_->GetFd(), 
            rec.data + txn_mgr->get_start_offset())) {
            throw TransactionAbortException(context_->txn_->get_transaction_id(), 
                AbortReason::UPGRADE_CONFLICT);
        }
        txn_mgr->set_record_txn_id(rec.data, context_->txn_, false);
        rid_ = fh_->insert_record(rec.data, context_);
        context_->txn_->append_write_record(new WriteRecord(WType::INSERT_TUPLE,
                                                            tab_name_, rid_));

        // 更新索引
        // Insert into index
        for (size_t id = 0; id < tab_.indexes.size(); id++) {
            auto &index = tab_.indexes[id];
            std::unique_ptr<char[]> key(new char[index.col_tot_len]);
            int offset = 0;
            for (int i = 0; i < index.col_num; ++i) {
                memcpy(key.get() + offset, rec.data + index.cols[i].offset, index.cols[i].len);
                offset += index.cols[i].len;
            }
            sm_manager_->get_index_handle(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols))
                ->insert_entry(key.get(), rid_, context_->txn_);
        }

        //记录日志
        InsertLogRecord log_record(context_->txn_->get_transaction_id(), rec, rid_, tab_name_);
        context_->log_mgr_->add_log_to_buffer(&log_record);
        return {};
    }

    ExecutionType type() const override { return ExecutionType::Insert; }
};