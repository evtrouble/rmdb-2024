/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lock_manager.h"
#include "transaction/transaction.h"

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    
    return true;
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {

    return true;
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    
    return true;
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    
    return true;
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    
    return true;
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    
    return true;
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
   
    return true;
}

// 只实现唯一key锁和物理行锁的排他加锁/解锁（MVCC下只对写加锁）
bool LockManager::lock_exclusive_on_key(Transaction* txn, int tab_fd, const std::string& key_bytes) {
    std::unique_lock lk(latch_);
    LockDataId lock_id(tab_fd, key_bytes);
    auto& queue = lock_table_[lock_id];
    txn_id_t txn_id = txn->get_transaction_id();
    for (auto& req : queue.request_queue_) {
        if (req.txn_id_ == txn_id && req.lock_mode_ == LockMode::EXCLUSIVE && req.granted_) {
            return true;
        }
    }
    bool can_grant = true;
    for (auto& req : queue.request_queue_) {
        if (req.granted_ && req.txn_id_ != txn_id) {
            can_grant = false;
            break;
        }
    }
    queue.request_queue_.emplace_back(txn_id, LockMode::EXCLUSIVE);
    auto it = std::prev(queue.request_queue_.end());
    if (can_grant) {
        it->granted_ = true;
        queue.group_lock_mode_ = GroupLockMode::X;
        txn->append_lock_set(lock_id);
        return true;
    } 
    return false;
}

bool LockManager::unlock_key(Transaction* txn, const LockDataId& lock_id)
{
    std::unique_lock lk(latch_);
    auto it = lock_table_.find(lock_id);
    if (it == lock_table_.end()) return false;
    auto& queue = it->second.request_queue_;
    txn_id_t txn_id = txn->get_transaction_id();
    for (auto req_it = queue.begin(); req_it != queue.end(); ++req_it) {
        if (req_it->txn_id_ == txn_id && req_it->lock_mode_ == LockMode::EXCLUSIVE) {
            queue.erase(req_it);
            break;
        }
    }
    if (queue.empty()) {
        lock_table_.erase(it);
    }
    return true;
}