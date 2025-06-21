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
    std::shared_ptr<RmFileHandle> fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;
    std::unordered_set<int> changes;
    std::vector<ColMeta*> set_col_metas; // 缓存set_clauses对应的列元数据
    std::vector<std::pair<std::shared_ptr<IxIndexHandle>, IndexMeta &>> ihs; // 缓存需要更新的索引信息
    std::vector<std::unique_ptr<char[]>> datas; // 缓存索引键数据

    // 初始化需要更新的索引信息和列元数据
    void init() {
        // 缓存set_clauses对应的列元数据
        set_col_metas.reserve(set_clauses_.size());
        for (auto &set_clause : set_clauses_) {
            set_col_metas.emplace_back(&*tab_.get_col(set_clause.lhs.col_name));
        }

        // 找出需要更新索引的情况
        ihs.reserve(tab_.indexes.size());
        datas.reserve(tab_.indexes.size());
        for (auto &index : tab_.indexes) {
            for (int i = 0; i < index.col_num; ++i) {
                if (changes.count(index.cols[i].offset)) {
                    ihs.emplace_back(sm_manager_->get_index_handle(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)), index);
                    datas.emplace_back(new char[index.col_tot_len]);
                    break;
                }
            }
        }
    }

    // 更新索引
    void update_indexes(const RmRecord& old_rec, const RmRecord& rec, const Rid& rid) {
        for (size_t id = 0; id < ihs.size(); id++) {
            auto &[ih, index] = ihs[id];
            char *key = datas[id].get();
            // 删除旧索引
            int offset = 0;
            for (int i = 0; i < index.col_num; ++i) {
                memcpy(key + offset, old_rec.data + index.cols[i].offset, index.cols[i].len);
                offset += index.cols[i].len;
            }
            ih->delete_entry(key, rid, context_->txn_);
            // 插入新索引
            offset = 0;
            for (int i = 0; i < index.col_num; ++i) {
                if (changes.count(index.cols[i].offset))
                    memcpy(key + offset, rec.data + index.cols[i].offset, index.cols[i].len);
                offset += index.cols[i].len;
            }
            ih->insert_entry(key, rid, context_->txn_);
        }
    }

public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, const std::vector<SetClause> &set_clauses,
                   const std::vector<Rid> &rids, Context *context)
            : AbstractExecutor(context), rids_(std::move(rids)),
              tab_name_(std::move(tab_name)), set_clauses_(std::move(set_clauses)),
              sm_manager_(sm_manager) {
        tab_ = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->get_table_handle(tab_name_);
        for (auto &set_clause : set_clauses_) {
            // 找到要更新的列的元数据
            auto col = tab_.get_col(set_clause.lhs.col_name);
            // 检查类型是否匹配
            if (col->type != set_clause.rhs.type) {
                throw IncompatibleTypeError(coltype2str(col->type), coltype2str(set_clause.rhs.type));
            }
            changes.insert(col->offset);
        }
        init(); // 初始化需要更新的索引信息和列元数据
    }

    std::unique_ptr<RmRecord> Next() override {
        if (rids_.empty())
            return nullptr;

        // 遍历所有需要更新的记录
        TransactionManager *txn_mgr = context_->txn_->get_txn_manager();
        RmRecord old_rec;
        for (auto &rid : rids_) {
            // 获取原记录
            RmRecord rec = *fh_->get_record(rid, context_);
            old_rec = rec;
            // 根据set_clauses_更新记录值
            for (size_t i = 0; i < set_clauses_.size(); ++i) {
                auto& set_clause = set_clauses_[i];
                auto& set_col_meta = set_col_metas[i];
                switch (set_clause.op) {
                case ast::UpdateOp::SELF_ADD:
                    switch (set_col_meta->type)
                    {
                        case TYPE_INT:*reinterpret_cast<int *>(rec.data + set_col_meta->offset) = 
                            *reinterpret_cast<int *>(old_rec.data + set_col_meta->offset) + set_clause.rhs.int_val;
                            break;
                        case TYPE_FLOAT:*reinterpret_cast<float *>(rec.data + set_col_meta->offset) =
                            *reinterpret_cast<float *>(old_rec.data + set_col_meta->offset) + set_clause.rhs.float_val;
                            break;
                        case TYPE_STRING:
                            std::strncat(rec.data + set_col_meta->offset, set_clause.rhs.str_val.c_str(), 
                            set_col_meta->len - std::strlen(rec.data + set_col_meta->offset));
                            break;
                        default:
                            break; // TODO: Handle datetime addition if needed
                    }
                    break;
                case ast::UpdateOp::SELF_SUB:
                    switch (set_col_meta->type)
                    {
                        case TYPE_INT:*reinterpret_cast<int *>(rec.data + set_col_meta->offset) = 
                            *reinterpret_cast<int *>(old_rec.data + set_col_meta->offset) - set_clause.rhs.int_val;
                            break;
                        case TYPE_FLOAT:*reinterpret_cast<float *>(rec.data + set_col_meta->offset) =
                            *reinterpret_cast<float *>(old_rec.data + set_col_meta->offset) - set_clause.rhs.float_val;
                            break;
                        default:
                            break; // TODO: Handle datetime addition if needed
                    }
                    break;
                case ast::UpdateOp::SELF_MUT:
                    switch (set_col_meta->type)
                    {
                        case TYPE_INT:*reinterpret_cast<int *>(rec.data + set_col_meta->offset) = 
                            *reinterpret_cast<int *>(old_rec.data + set_col_meta->offset) * set_clause.rhs.int_val;
                            break;
                        case TYPE_FLOAT:*reinterpret_cast<float *>(rec.data + set_col_meta->offset) =
                            *reinterpret_cast<float *>(old_rec.data + set_col_meta->offset) * set_clause.rhs.float_val;
                            break;
                        default:
                            break; // TODO: Handle datetime addition if needed
                    }
                    break;
                case ast::UpdateOp::SELF_DIV:
                    switch (set_col_meta->type)
                    {
                        case TYPE_INT:
                            if(set_clause.rhs.int_val == 0) {
                                throw DivisionByZeroError("Division by zero in update operation");
                            }
                            *reinterpret_cast<int *>(rec.data + set_col_meta->offset) = 
                                *reinterpret_cast<int *>(old_rec.data + set_col_meta->offset) / set_clause.rhs.int_val;
                            break;
                        case TYPE_FLOAT:
                            if(set_clause.rhs.float_val == 0) {
                                throw DivisionByZeroError("Division by zero in update operation");
                            }
                            *reinterpret_cast<float *>(rec.data + set_col_meta->offset) =
                                *reinterpret_cast<float *>(old_rec.data + set_col_meta->offset) / set_clause.rhs.float_val;
                            break;
                        default:
                            break; // TODO: Handle datetime addition if needed
                    }
                    break;
                case ast::UpdateOp::ASSINGMENT:
                    memcpy(rec.data + set_col_metas[i]->offset, set_clauses_[i].rhs.raw->data, set_col_metas[i]->len);
                    break;
                case ast::UpdateOp::UNKNOWN:
                    std::cerr << "Unknown operation in update_record" << std::endl;
                    break;
                }
            }

            // 更新索引
            update_indexes(old_rec, rec, rid);

            // 更新记录
            // if(!context_->lock_mgr_->lock_exclusive_on_key(context_->txn_, fh_->GetFd(),
            //      old_rec.data + txn_mgr->get_start_offset())) {
            //     throw TransactionAbortException(context_->txn_->get_transaction_id(), 
            //         AbortReason::UPGRADE_CONFLICT);
            // }
            if(!context_->lock_mgr_->lock_exclusive_on_key(context_->txn_, fh_->GetFd(),
                 rec.data + txn_mgr->get_start_offset())) {
                throw TransactionAbortException(context_->txn_->get_transaction_id(),
                    AbortReason::UPGRADE_CONFLICT);
            }
            fh_->update_record(rid, rec.data, context_);

            // 记录日志
            UpdateLogRecord log_record(context_->txn_->get_transaction_id(), rid, old_rec, rec, tab_name_);
            context_->log_mgr_->add_log_to_buffer(&log_record);

            if(txn_mgr->get_concurrency_mode() != ConcurrencyMode::MVCC)
            {
                auto write_record = new WriteRecord(WType::UPDATE_TUPLE,
                    tab_name_, rid, old_rec);
                context_->txn_->append_write_record(write_record);
            }
        }

        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }

    ExecutionType type() const override { return ExecutionType::Update; }
};