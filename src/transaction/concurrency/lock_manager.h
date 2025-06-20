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

#include <mutex>
#include <condition_variable>
#include <string>
#include <functional>
#include <unordered_map>
#include <list>

#include "defs.h"
#include "common/config.h"
class Transaction;

static const std::string GroupLockModeStr[10] = {"NON_LOCK", "IS", "IX", "S", "X", "SIX"};

struct LockDataId {
    int tab_fd; // 表标识
    std::string key_bytes; // 唯一key序列化后的字节串（可为空，表示表锁或行锁）
    Rid rid; // 可选：用于行锁
    enum class Type { TABLE, ROW, UNIQUE_KEY } type = Type::TABLE;
    LockDataId() = default;
    LockDataId(int tab_fd) : tab_fd(tab_fd), type(Type::TABLE) {}
    // 物理行锁
    LockDataId(int tab_fd, const Rid& rid) : tab_fd(tab_fd), rid(rid), type(Type::ROW) {}
    // 逻辑行锁（如联合唯一约束）
    LockDataId(int tab_fd, const std::string& key_bytes, const Rid& rid)
        : tab_fd(tab_fd), key_bytes(key_bytes), rid(rid), type(Type::ROW) {}
    // 唯一key锁
    LockDataId(int tab_fd, const std::string& key_bytes) : tab_fd(tab_fd), key_bytes(key_bytes), type(Type::UNIQUE_KEY) {}

    bool operator==(const LockDataId& other) const {
        if (tab_fd != other.tab_fd || type != other.type) return false;
        if (type == Type::TABLE) return true;
        if (type == Type::ROW) return rid == other.rid && key_bytes == other.key_bytes;
        return key_bytes == other.key_bytes;
    }
    // 便于后续扩展：判断是否为唯一key锁
    bool is_unique_key() const { return type == Type::UNIQUE_KEY; }
    // 判断是否为行锁
    bool is_row() const { return type == Type::ROW; }
    // 判断是否为表锁
    bool is_table() const { return type == Type::TABLE; }
    ~LockDataId() = default;
};

namespace std {
    template<>
    struct hash<LockDataId> {
        size_t operator()(const LockDataId& k) const {
            size_t h = std::hash<int>()(k.tab_fd);
        
            // 使用更安全的哈希组合方式
            auto combine = [](size_t seed, size_t hash) {
                return seed ^ (hash + 0x9e3779b9 + (seed << 6) + (seed >> 2));
            };
            
            h = combine(h, std::hash<int>()(static_cast<int>(k.type)));
            
            if (k.type == LockDataId::Type::ROW) {
                h = combine(h, std::hash<int>()(k.rid.page_no));
                h = combine(h, std::hash<int>()(k.rid.slot_no));
                h = combine(h, std::hash<std::string>()(k.key_bytes));
            } else if (k.type == LockDataId::Type::UNIQUE_KEY) {
                h = combine(h, std::hash<std::string>()(k.key_bytes));
            }
            
            return h;
        }
    };
}

class LockManager {
public:
    enum class LockMode { SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, S_IX };
    enum class GroupLockMode { NON_LOCK, IS, IX, S, X, SIX};

    class LockRequest {
    public:
        LockRequest(txn_id_t txn_id, LockMode lock_mode)
            : txn_id_(txn_id), lock_mode_(lock_mode), granted_(false) {}
        txn_id_t txn_id_;
        LockMode lock_mode_;
        bool granted_;
    };

    class LockRequestQueue {
    public:
        std::list<LockRequest> request_queue_;
        std::condition_variable cv_;
        GroupLockMode group_lock_mode_ = GroupLockMode::NON_LOCK;
    };

    LockManager() {}
    ~LockManager() {}

    // 只实现唯一key锁和物理行锁的排他加锁/解锁（MVCC下只对写加锁）
    bool lock_exclusive_on_key(Transaction *txn, int tab_fd, const std::string &key_bytes);
    bool unlock_key(Transaction* txn, const LockDataId &lock_id);

    // 物理行锁接口
    // bool lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    //     std::unique_lock<std::mutex> lk(latch_);
    //     LockDataId lock_id(tab_fd, rid);
    //     auto& queue = lock_table_[lock_id];
    //     txn_id_t txn_id = txn->get_transaction_id();
    //     for (auto& req : queue.request_queue_) {
    //         if (req.txn_id_ == txn_id && req.lock_mode_ == LockMode::EXLUCSIVE && req.granted_) {
    //             return true;
    //         }
    //     }
    //     bool can_grant = true;
    //     for (auto& req : queue.request_queue_) {
    //         if (req.granted_ && req.txn_id_ != txn_id) {
    //             can_grant = false;
    //             break;
    //         }
    //     }
    //     queue.request_queue_.emplace_back(txn_id, LockMode::EXLUCSIVE);
    //     auto it = std::prev(queue.request_queue_.end());
    //     if (can_grant) {
    //         it->granted_ = true;
    //         queue.group_lock_mode_ = GroupLockMode::X;
    //         txn->add_row_lock(lock_id);
    //         return true;
    //     } else {
    //         return false;
    //     }
    // }
    bool unlock(Transaction* txn, LockDataId lock_data_id);
    // 只保留接口声明，表锁/意向锁/共享锁可后续实现
    bool lock_shared_on_record(Transaction *txn, const Rid &rid, int tab_fd);
    bool lock_shared_on_table(Transaction *txn, int tab_fd);
    bool lock_exclusive_on_table(Transaction *txn, int tab_fd);
    bool lock_exclusive_on_record(Transaction *txn, const Rid &rid, int tab_fd);
    bool lock_IS_on_table(Transaction *txn, int tab_fd);
    bool lock_IX_on_table(Transaction *txn, int tab_fd);

private:
    std::mutex latch_;
    std::unordered_map<LockDataId, LockRequestQueue> lock_table_;
};
