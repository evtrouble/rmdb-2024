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
#include <unordered_map>
#include <optional>
#include <functional>
#include <shared_mutex>

#include "transaction.h"
#include "watermark.h"
#include "recovery/log_manager.h"
#include "concurrency/lock_manager.h"
#include "system/sm_manager.h"
#include "common/exception.h"

/* 系统采用的并发控制算法，当前题目中要求两阶段封锁并发控制算法 */
enum class ConcurrencyMode
{
    TWO_PHASE_LOCKING = 0,
    BASIC_TO,
    MVCC
};

struct PageVersionInfo
{
    std::shared_mutex mutex_;
    /** 存储所有槽的先前版本信息。注意：不要使用 `[x]` 来访问它，因为
     * 即使不存在也会创建新元素。请使用 `find` 来代替。
     */
    std::unordered_map<slot_offset_t, UndoLog*> prev_version_;
};

class TransactionManager
{
public:
    explicit TransactionManager(LockManager *lock_manager, SmManager *sm_manager,
                                ConcurrencyMode concurrency_mode = ConcurrencyMode::TWO_PHASE_LOCKING)
    {
        sm_manager_ = sm_manager;
        lock_manager_ = lock_manager;
        concurrency_mode_ = concurrency_mode;
    }

    ~TransactionManager() {
        if(!terminate_purge_cleaner_) {
            terminate_purge_cleaner_ = true;
            if (purgeCleaner.joinable()) {
                purgeCleaner.join();
            }
        }
    }

    Transaction *begin(Transaction *txn, LogManager *log_manager);

    void commit(Context *context, LogManager *log_manager);

    void abort(Context *context, LogManager *log_manager);

    ConcurrencyMode get_concurrency_mode() { return concurrency_mode_; }

    void set_concurrency_mode(ConcurrencyMode concurrency_mode) { concurrency_mode_ = concurrency_mode; }

    LockManager *get_lock_manager() { return lock_manager_; }

    /**
     * @description: 获取事务ID为txn_id的事务对象
     * @return {Transaction*} 事务对象的指针
     * @param {txn_id_t} txn_id 事务ID
     */
    Transaction *get_transaction(txn_id_t txn_id)
    {
        if (txn_id == INVALID_TXN_ID)
            return nullptr;

        std::unique_lock<std::mutex> lock(latch_);
        assert(TransactionManager::txn_map.find(txn_id) != TransactionManager::txn_map.end());
        auto *res = TransactionManager::txn_map[txn_id];
        lock.unlock();
        assert(res != nullptr);
        assert(res->get_thread_id() == std::this_thread::get_id());

        return res;
    }

    static std::unordered_map<txn_id_t, Transaction *> txn_map; // 全局事务表，存放事务ID与事务对象的映射关系
    std::shared_mutex txn_map_mutex_;
    /** ------------------------以下函数仅可能在MVCC当中使用------------------------------------------*/

    std::shared_ptr<PageVersionInfo> GetPageVersionInfo(const PageId &page_id);

    void TruncateVersionChain(std::shared_ptr<PageVersionInfo> page_info,
                                                  const Rid &rid, timestamp_t watermark);
    void TruncateVersionChain(int fd, const Rid &rid, timestamp_t watermark);

    void DeleteVersionChain(int fd, const Rid &rid);
    void DeleteVersionChain(std::shared_ptr<PageVersionInfo> page_info, const PageId &page_id, const Rid &rid);

    void StartPurgeCleaner();
    void StopPurgeCleaner();

    std::optional<RmRecord> GetVisibleRecord(int fd, const Rid &rid, Transaction *current_txn);

    bool UpdateUndoLink(int fd, const Rid &rid, UndoLog* prev_link,
                                            std::function<bool(UndoLog*)> &&check = nullptr);
    /** 保护版本信息 */
    std::shared_mutex version_info_mutex_;
    /** 存储表堆中每个元组的先前版本。 */
    std::unordered_map<PageId, std::shared_ptr<PageVersionInfo>, PageIdHash> version_info_;

    // 定义MVCC隐藏字段
    static constexpr const char* COMMIT_TS_FIELD = "__commit_ts";
    static constexpr const char* TXN_ID_FIELD = "__txn_id";

    // 获取MVCC所需的隐藏字段定义
    std::vector<ColDef> get_hidden_columns() const {
        if (concurrency_mode_ != ConcurrencyMode::MVCC) [[unlikely]] {
            return {};
        }
        return {
            {
                .name = COMMIT_TS_FIELD,
                .type = TYPE_INT,
                .len = sizeof(timestamp_t)
                // .visible = false
            },
            {
                .name = TXN_ID_FIELD,
                .type = TYPE_INT,
                .len = sizeof(txn_id_t)
                // .visible = false
            }
        };
    }

    // 获取隐藏字段数量
    size_t get_hidden_column_count() const {
        return (concurrency_mode_ == ConcurrencyMode::MVCC) ? 2 : 0;
    }

    txn_id_t get_record_txn_id(const char* data) const {
        if (concurrency_mode_ != ConcurrencyMode::MVCC) [[unlikely]] {
            return INVALID_TXN_ID;
        }
        txn_id_t txn_id;
        memcpy(&txn_id, data + sizeof(timestamp_t), sizeof(txn_id_t));
        return txn_id;
    }

    void set_record_txn_id(char* data, txn_id_t txn_id, bool is_delete = false) const {
        if (concurrency_mode_ != ConcurrencyMode::MVCC) [[unlikely]] {
            return;
        }
        if(is_delete)
            txn_id |= TXN_DELETE_TAG;
        memcpy(data + sizeof(timestamp_t), &txn_id, sizeof(txn_id_t));
    }

    void set_record_commit_ts(char* data, timestamp_t commit_ts) const {
        if (concurrency_mode_ != ConcurrencyMode::MVCC) [[unlikely]] {
            return;
        }
        memcpy(data, &commit_ts, sizeof(timestamp_t));
    }

    bool is_write_conflict(const char* data, Transaction* txn) const {
        if (concurrency_mode_ != ConcurrencyMode::MVCC) [[unlikely]] {
            return false; // 在非MVCC模式下，不存在写冲突
        }
        txn_id_t record_txn_id = get_record_txn_id(data);

        if(record_txn_id == txn->get_transaction_id()){
            return false;
        }
        timestamp_t record_commit_ts = get_record_commit_ts(data);

        if(record_commit_ts == INVALID_TIMESTAMP) {
            return true; // 记录未提交或不存在
        }
        return false;
    }

    /**
     * @brief 判断记录的可见性和是否需要查找版本链
     * @return {bool} 如果返回true，表示记录已被删除或当前版本不可见(需要查找版本链)
     */
    bool need_find_version_chain(timestamp_t commit_ts, txn_id_t txn_id, Transaction* txn) const {
        if (concurrency_mode_ != ConcurrencyMode::MVCC) [[unlikely]] {
            return false; // 在非MVCC模式下，记录不会被删除或不可见
        }
        txn_id &= (TXN_DELETE_TAG - 1);
        if(txn_id == txn->get_transaction_id())
            return false;
        if (commit_ts == INVALID_TIMESTAMP || commit_ts > txn->get_start_ts())
        {
            return true;
        }
        return false; // 记录可见且已提交
    }

    inline bool is_deleted(timestamp_t commit_ts) const
    {
        if (concurrency_mode_ != ConcurrencyMode::MVCC) [[unlikely]] {
            return false; // 在非MVCC模式下，记录不会被删除或不可见
        }
        return (commit_ts & TXN_DELETE_TAG);
    }

    timestamp_t get_record_commit_ts(const char* data) const {
        if (concurrency_mode_ != ConcurrencyMode::MVCC) [[unlikely]] {
            return INVALID_TIMESTAMP;
        }
        timestamp_t commit_ts;
        memcpy(&commit_ts, data, sizeof(timestamp_t));
        return commit_ts;
    }

    void mark_deleted(char* data) {
        if (concurrency_mode_ != ConcurrencyMode::MVCC) [[unlikely]] {
            return;
        }
        txn_id_t record_txn_id = get_record_txn_id(data);
        set_record_txn_id(data, record_txn_id, true);
    }

private:
    ConcurrencyMode concurrency_mode_;           // 事务使用的并发控制算法，目前只需要考虑2PL
    std::atomic<txn_id_t> next_txn_id_{0};       // 用于分发事务ID
    std::atomic<timestamp_t> next_timestamp_{0}; // 用于分发事务时间戳
    std::mutex latch_;                           // 用于txn_map的并发
    SmManager *sm_manager_;
    LockManager *lock_manager_;
    std::thread purgeCleaner;
    bool terminate_purge_cleaner_{false}; // 用于终止清理线程

    std::atomic<timestamp_t> last_commit_ts_{0}; // 最后提交的时间戳,仅用于MVCC
    Watermark running_txns_{0};                  // 存储所有正在运行事务的读取时间戳，以便于垃圾回收，仅用于MVCC
};