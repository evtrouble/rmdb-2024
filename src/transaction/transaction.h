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

#include <atomic>
#include <deque>
#include <string>
#include <thread>
#include <memory>
#include <unordered_set>

#include "txn_defs.h"
#include "system/sm_meta.h"

struct Version
{
    struct
    {
        PageId page_id;
        int slot_no;
    } next_version;
    timestamp_t version;
};

class Transaction
{
public:
    explicit Transaction(txn_id_t txn_id, IsolationLevel isolation_level = IsolationLevel::SERIALIZABLE)
        : state_(TransactionState::DEFAULT), isolation_level_(isolation_level), txn_id_(txn_id)
    {
        write_set_ = std::make_shared<std::deque<WriteRecord>>();
        lock_set_ = std::make_shared<std::unordered_set<int>>();
        index_latch_page_set_ = std::make_shared<std::deque<Page *>>();
        index_deleted_page_set_ = std::make_shared<std::deque<Page *>>();
        prev_lsn_ = INVALID_LSN;
        thread_id_ = std::this_thread::get_id();
    }

    ~Transaction() = default;

    inline txn_id_t get_transaction_id() { return txn_id_; }

    inline std::thread::id get_thread_id() { return thread_id_; }

    inline void set_txn_mode(bool txn_mode) { txn_mode_ = txn_mode; }
    inline bool get_txn_mode() { return txn_mode_; }

    inline void set_start_ts(timestamp_t start_ts) { start_ts_ = start_ts; }
    inline timestamp_t get_start_ts() { return start_ts_; }
    inline void set_commit_ts(timestamp_t commit_ts) { commit_ts_ = commit_ts; }
    inline timestamp_t get_commit_ts() { return commit_ts_; }

    inline IsolationLevel get_isolation_level() { return isolation_level_; }

    inline TransactionState get_state() { return state_; }
    inline void set_state(TransactionState state) { state_ = state; }

    inline lsn_t get_prev_lsn() { return prev_lsn_; }
    inline void set_prev_lsn(lsn_t prev_lsn) { prev_lsn_ = prev_lsn; }

    inline std::shared_ptr<std::deque<WriteRecord>> get_write_set() { return write_set_; }
    inline void append_write_record(const WriteRecord &write_record) { write_set_->emplace_back(std::move(write_record)); }

    inline std::shared_ptr<std::deque<Page *>> get_index_deleted_page_set() { return index_deleted_page_set_; }
    inline void append_index_deleted_page(Page *page) { index_deleted_page_set_->emplace_back(page); }

    inline std::shared_ptr<std::deque<Page *>> get_index_latch_page_set() { return index_latch_page_set_; }
    inline void append_index_latch_page_set(Page *page) { index_latch_page_set_->emplace_back(page); }

    inline std::shared_ptr<std::unordered_set<int>> get_lock_set() { return lock_set_; }
    inline void append_lock_set(int fd) { lock_set_->emplace(fd); }

    inline std::shared_ptr<std::deque<Version *>> get_write_version_set() { return write_version_set_; }
    inline void append_write_version_set(Version *version) { write_version_set_->emplace_back(version); }

private:
    bool txn_mode_;                  // 用于标识当前事务为显式事务还是单条SQL语句的隐式事务
    TransactionState state_;         // 事务状态
    IsolationLevel isolation_level_; // 事务的隔离级别，默认隔离级别为可串行化
    std::thread::id thread_id_;      // 当前事务对应的线程id
    lsn_t prev_lsn_;                 // 当前事务执行的最后一条操作对应的lsn，用于系统故障恢复
    txn_id_t txn_id_;                // 事务的ID，唯一标识符
    timestamp_t start_ts_;           // 事务的开始时间戳
    timestamp_t commit_ts_;          // 事务的提交时间戳

    std::shared_ptr<std::deque<WriteRecord>> write_set_;         // 事务包含的所有写操作
    std::shared_ptr<std::deque<Version *>> write_version_set_;   // 事务包含的所有写操作，MVCC需要修改对应的版本号
    std::shared_ptr<std::unordered_set<int>> lock_set_;          // 事务申请的所有锁
    std::shared_ptr<std::deque<Page *>> index_latch_page_set_;   // 维护事务执行过程中加锁的索引页面
    std::shared_ptr<std::deque<Page *>> index_deleted_page_set_; // 维护事务执行过程中删除的索引页面
};
