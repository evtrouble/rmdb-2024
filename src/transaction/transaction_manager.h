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
    std::unordered_map<slot_offset_t, UndoLink> prev_version_;
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

    ~TransactionManager() = default;

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

    void DeleteVersionChain(std::shared_ptr<PageVersionInfo> page_info, const PageId &page_id, const Rid &rid);

    void DeleteVersionChain(int fd, const Rid &rid);

    std::optional<UndoLog> GetVisibleRecord(int fd, const Rid &rid, Transaction *current_txn);

    bool UpdateUndoLink(int fd, const Rid &rid, std::optional<UndoLink> prev_link,
                                            std::function<bool(std::optional<UndoLink>)> &&check);
    /** 保护版本信息 */
    std::shared_mutex version_info_mutex_;
    /** 存储表堆中每个元组的先前版本。 */
    std::unordered_map<PageId, std::shared_ptr<PageVersionInfo>, PageIdHash> version_info_;

private:
    ConcurrencyMode concurrency_mode_;           // 事务使用的并发控制算法，目前只需要考虑2PL
    std::atomic<txn_id_t> next_txn_id_{0};       // 用于分发事务ID
    std::atomic<timestamp_t> next_timestamp_{0}; // 用于分发事务时间戳
    std::mutex latch_;                           // 用于txn_map的并发
    SmManager *sm_manager_;
    LockManager *lock_manager_;

    std::atomic<timestamp_t> last_commit_ts_{0}; // 最后提交的时间戳,仅用于MVCC
    Watermark running_txns_{0};                  // 存储所有正在运行事务的读取时间戳，以便于垃圾回收，仅用于MVCC
};