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

class InsertExecutor : public AbstractExecutor
{
private:
    TabMeta tab_;               // 表的元数据
    std::vector<Value> values_; // 需要插入的数据
    RmFileHandle *fh_;          // 表的数据文件句柄
    std::string tab_name_;      // 表名称
    Rid rid_;                   // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
    SmManager *sm_manager_;

public:
    InsertExecutor(SmManager *sm_manager, const std::string &tab_name, const std::vector<Value> &values, Context *context)
        : AbstractExecutor(context), values_(std::move(values)),
          tab_name_(std::move(tab_name)), sm_manager_(sm_manager)
    {
        tab_ = sm_manager_->db_.get_table(tab_name_);
        if (values.size() != tab_.cols.size())
        {
            throw InvalidValueCountError();
        }
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
    };

    std::unique_ptr<RmRecord> Next() override
    {
        GapLock gaplock;
        gaplock.init(values_);

        std::vector<IxIndexHandle *> ihs;
        ihs.reserve(tab_.indexes.size());
        for (auto &index : tab_.indexes)
        {
            GapLock ix_gaplock(gaplock);
            ihs.emplace_back(sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get());
            auto ih = ihs.back();
            context_->lock_mgr_->lock_exclusive_on_gap(context_->txn_, ih->get_fd(), ix_gaplock);
        }
        context_->lock_mgr_->lock_exclusive_on_gap(context_->txn_, fh_->GetFd(), gaplock);

        // Make record buffer
        RmRecord rec(fh_->get_file_hdr().record_size);
        for (size_t i = 0; i < values_.size(); ++i)
        {
            auto &col = tab_.cols[i];
            auto &val = values_[i];
            if (col.type != val.type)
            {
                throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
            }
            val.export_val(rec.data + col.offset, col.len);
        }

        // Insert into record file
        rid_ = fh_->insert_record(rec.data);
        context_->txn_->append_write_record(WriteRecord(WType::INSERT_TUPLE, 
            tab_name_, rid_, rec));

        // Insert into index
        for (size_t id = 0; id < tab_.indexes.size(); id++)
        {
            auto &index = tab_.indexes[id];
            char *key = new char[index.col_tot_len];
            int offset = 0;
            for (int i = 0; i < index.col_num; ++i)
            {
                memcpy(key + offset, rec.data + index.cols[i].offset, index.cols[i].len);
                offset += index.cols[i].len;
            }
            ihs[id]->insert_entry(key, rid_, context_->txn_);
            delete[] key;
        }

        return nullptr;
    }
    Rid &rid() override { return rid_; }
    ExecutionType type() const override { return ExecutionType::Insert;  }
};