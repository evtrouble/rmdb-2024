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

class UpdateExecutor : public AbstractExecutor
{
private:
    TabMeta tab_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;
    std::unordered_set<int> changes;

public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, const std::vector<SetClause> &set_clauses,
        const std::vector<Rid> &rids, Context *context) 
        : AbstractExecutor(context), rids_(std::move(rids)), 
        tab_name_(std::move(tab_name)), set_clauses_(std::move(set_clauses)),
        sm_manager_(sm_manager)
    {
        tab_ = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        for (auto &set_clause : set_clauses_)
        {
            // 找到要更新的列的元数据
            auto col = tab_.get_col(set_clause.lhs.col_name);
            // 检查类型是否匹配
            if (col->type != set_clause.rhs.type)
            {
                throw IncompatibleTypeError(coltype2str(col->type), coltype2str(set_clause.rhs.type));
            }
            changes.insert(col->offset);
        }
    }

    std::unique_ptr<RmRecord> Next() override
    {
        // 遍历所有需要更新的记录
        for (auto &rid : rids_)
        {
            // 获取原记录
            RmRecord rec = *fh_->get_record(rid);
            RmRecord old_rec = rec;
            // 根据set_clauses_更新记录值
            for (auto &set_clause : set_clauses_)
            {
                // 找到要更新的列的元数据
                auto col = tab_.get_col(set_clause.lhs.col_name);
                set_clause.rhs.export_val(rec.data + col->offset, col->len);
            }

            // 更新索引
            for (auto &index : tab_.indexes)
            {
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();

                // 删除旧索引
                char *key = new char[index.col_tot_len];
                int offset = 0;
                bool exist = false;
                for (int i = 0; i < index.col_num; ++i)
                {
                    if(changes.count(index.cols[i].offset)) 
                        exist = true;
                    memcpy(key + offset, rec.data + index.cols[i].offset, index.cols[i].len);
                    offset += index.cols[i].len;
                }
                if(exist) {
                    ih->delete_entry(key, rid, context_->txn_);
                    // 插入新索引
                    offset = 0;
                    for (int i = 0; i < index.col_num; ++i)
                    {
                        if(changes.count(index.cols[i].offset)) 
                            memcpy(key + offset, rec.data + index.cols[i].offset, index.cols[i].len);
                        offset += index.cols[i].len;
                    }
                    ih->insert_entry(key, rid, context_->txn_);
                }
                delete[] key;
            }

            // 更新记录
            fh_->update_record(rid, rec.data);
            context_->txn_->append_write_record(WriteRecord(WType::UPDATE_TUPLE, 
                tab_name_, rid, old_rec));
        }

        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }

    ExecutionType type() const override { return ExecutionType::Update;  }
};