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
#include <memory>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "common/common.h"
#include "transaction/txn_defs.h"
#include "record/rm_defs.h"
#include "concurrency/lock_manager.h"

class TransactionManager;

class Transaction
{
public:
  explicit Transaction(txn_id_t txn_id, TransactionManager *txn_manager, IsolationLevel isolation_level = IsolationLevel::SERIALIZABLE)
      : state_(TransactionState::DEFAULT), isolation_level_(isolation_level), txn_id_(txn_id), txn_manager_(txn_manager)
  {
    write_set_ = std::make_shared<std::deque<WriteRecord *>>();
    write_index_set_ = std::make_shared<std::deque<WriteRecord *>>();
    lock_set_ = std::make_shared<std::unordered_set<LockDataId>>();
    index_latch_page_set_ = std::make_shared<std::deque<Page_Final *>>();
    index_deleted_page_set_ = std::make_shared<std::deque<Page_Final *>>();
    prev_lsn_ = INVALID_LSN;
    thread_id_ = std::this_thread::get_id();
  }

  explicit Transaction(TransactionManager *txn_manager)
      : commit_ts_{0}, ref_count_{0}, txn_manager_(txn_manager) {
        index_latch_page_set_ = std::make_shared<std::deque<Page_Final *>>();
      }

  ~Transaction() = default;

  inline txn_id_t get_transaction_id() { return txn_id_; }

  inline std::thread::id get_thread_id() { return thread_id_; }

  inline void set_txn_mode(bool txn_mode) { txn_mode_ = txn_mode; }
  inline bool get_txn_mode() { return txn_mode_; }

  inline void set_start_ts(timestamp_t start_ts) { start_ts_ = start_ts; }
  inline timestamp_t get_start_ts() { return start_ts_; }

  inline IsolationLevel get_isolation_level() { return isolation_level_; }

  inline TransactionState get_state() { return state_; }
  inline void set_state(TransactionState state) { state_ = state; }

  inline lsn_t get_prev_lsn() { return prev_lsn_; }
  inline void set_prev_lsn(lsn_t prev_lsn) { prev_lsn_ = prev_lsn; }

  inline std::shared_ptr<std::deque<WriteRecord *>> get_write_set() { return write_set_; }
  inline void append_write_record(WriteRecord *write_record) { write_set_->push_back(write_record); }
  inline std::shared_ptr<std::deque<WriteRecord *>> get_write_index_set() { return write_index_set_; }
  inline void append_write_index_record(WriteRecord *write_record) { write_index_set_->push_back(write_record); }

  inline std::shared_ptr<std::deque<Page_Final *>> get_index_deleted_page_set() { return index_deleted_page_set_; }
  inline void append_index_deleted_page(Page_Final *page) { index_deleted_page_set_->push_back(page); }

  inline std::shared_ptr<std::deque<Page_Final *>> get_index_latch_page_set() { return index_latch_page_set_; }
  inline void append_index_latch_page_set(Page_Final *page) { index_latch_page_set_->push_back(page); }

  // inline TimestampRef *get_timestame_ref() { return ref; }
  inline void set_commit_ts(timestamp_t commit_ts) { commit_ts_ = commit_ts; }
  inline std::shared_ptr<std::unordered_set<LockDataId>> get_lock_set() { return lock_set_; }
  inline void append_lock_set(LockDataId& lock_id) { lock_set_->emplace(std::move(lock_id)); }

  // inline timestamp_t get_read_ts() const { return read_ts_; }
  inline timestamp_t get_commit_ts() const { return commit_ts_; }
  inline TransactionManager *get_txn_manager() const { return txn_manager_; }
  inline void reset() {
    lock_set_.reset();
    write_set_.reset();
    index_latch_page_set_.reset();
    index_deleted_page_set_.reset();
    write_index_set_.reset();
  }

  inline void dup() {
    ref_count_.fetch_add(1, std::memory_order_relaxed);
  }

  void release();

private:
  bool txn_mode_;                  // 用于标识当前事务为显式事务还是单条SQL语句的隐式事务
  TransactionState state_;         // 事务状态
  IsolationLevel isolation_level_; // 事务的隔离级别，默认隔离级别为可串行化
  std::thread::id thread_id_;      // 当前事务对应的线程id
  lsn_t prev_lsn_;                 // 当前事务执行的最后一条操作对应的lsn，用于系统故障恢复
  txn_id_t txn_id_;                // 事务的ID，唯一标识符
  timestamp_t start_ts_;           // 事务的开始时间戳

  std::shared_ptr<std::deque<WriteRecord *>> write_set_;       // 事务包含的所有写操作
  std::shared_ptr<std::deque<WriteRecord *>> write_index_set_;       // 事务包含的所有索引写操作
  std::shared_ptr<std::unordered_set<LockDataId>> lock_set_;   // 事务申请的所有锁
  std::shared_ptr<std::deque<Page_Final *>> index_latch_page_set_;   // 维护事务执行过程中加锁的索引页面
  std::shared_ptr<std::deque<Page_Final *>> index_deleted_page_set_; // 维护事务执行过程中删除的索引页面

  // std::atomic<timestamp_t> read_ts_{0};
  /** 提交时间戳 */
  std::atomic<timestamp_t> commit_ts_{INVALID_TS};
  std::atomic<int> ref_count_{1};

  /** 用于访问事务级撤销日志的锁。 */
  std::shared_mutex latch_;

  TransactionManager *txn_manager_{nullptr}; // 事务管理器指针，用于访问全局事务表等资源
};

struct UndoLog
{
  /* 此日志是否为删除标记 */
  // bool is_deleted_;
  /* 此撤销日志修改的字段 */
  // std::vector<bool> modified_fields_;
  /* 修改后的字段 */
  // std::vector<Value> tuple_;
  RmRecord tuple_;
  /* 此撤销日志的时间戳 */
  // timestamp_t ts_{INVALID_TS};
  Transaction *txn_;
  /* 撤销日志的前一个版本 */
  UndoLog* prev_version_{nullptr};

  // 不需要增加引用计数，在写入表中时已经增加过
  UndoLog(const RmRecord& tuple, Transaction* txn)
   : tuple_(std::move(tuple)), txn_(txn), prev_version_{nullptr} {}

  ~UndoLog() { txn_->release(); }
};