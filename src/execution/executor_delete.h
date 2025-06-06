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

class DeleteExecutor : public AbstractExecutor
{
private:
    TabMeta tab_;                  // 表的元数据
    RmFileHandle *fh_;             // 表的数据文件句柄
    std::vector<Rid> rids_;        // 需要删除的记录的位置
    std::string tab_name_;         // 表名称
    SmManager *sm_manager_;
    GapLock gaplock_;

   public:
    DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, 
                   const std::vector<Rid> &rids, Context *context, const GapLock& gaplock) 
        : AbstractExecutor(context), rids_(std::move(rids)), 
        tab_name_(std::move(tab_name)), sm_manager_(sm_manager), gaplock_(std::move(gaplock))
    {
        tab_ = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
    }

    std::unique_ptr<RmRecord> Next() override
    {
        if(rids_.empty())
            return nullptr;

        std::vector<IxIndexHandle*> ihs;
        ihs.reserve(tab_.indexes.size());
        for (auto &index : tab_.indexes)
        {
            GapLock ix_gaplock(gaplock_);
            ihs.emplace_back(sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get());
            auto ih = ihs.back();
            context_->lock_mgr_->lock_exclusive_on_gap(context_->txn_, ih->get_fd(), ix_gaplock);
        }
        context_->lock_mgr_->lock_exclusive_on_gap(context_->txn_, fh_->GetFd(), gaplock_);

        // 遍历所有需要删除的记录
        for (auto &rid : rids_)
        {
            // 获取要删除的记录
            RmRecord rec = *fh_->get_record(rid);

            // 删除相关的索引
            for (auto &index : tab_.indexes)
            {
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();

                // 构建索引键
                char *key = new char[index.col_tot_len];
                int offset = 0;
                for (int i = 0; i < index.col_num; ++i)
                {
                    memcpy(key + offset, rec.data + index.cols[i].offset, index.cols[i].len);
                    offset += index.cols[i].len;
                }

                // 删除索引项
                ih->delete_entry(key, rid, context_->txn_);
                delete[] key;
            }

            // 删除记录
            fh_->delete_record(rid);
            context_->txn_->append_write_record(WriteRecord(WType::DELETE_TUPLE, 
                tab_name_, rid, rec));
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }

    ExecutionType type() const override { return ExecutionType::Delete;  }
};