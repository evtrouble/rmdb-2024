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

void LockManager::lock_shared_on_gap(Transaction* txn, int tab_fd, const GapLock &gaplock)
{
    if(!gaplock.valid())
        return lock_shared_on_table(txn, tab_fd);

    assert(tab_fd < MAX_TABLE_NUMBER);
    if(txn->get_state() == TransactionState::SHRINKING){
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }
    auto &lock_queue = lock_table_[tab_fd];

    std::unique_lock lock(lock_queue.latch_);
    lock_queue.cv_.wait(lock, [&](){
        for (auto &[txn_, lock_request] : lock_queue.request_queue_)
        {
            if(txn_->get_transaction_id() == txn->get_transaction_id())continue;
            if(!lock_request.shared_gap_compatible(gaplock)) {
                if(txn->get_start_ts() < txn_->get_start_ts())
                    throw TransactionAbortException(txn->get_transaction_id(), 
                        AbortReason::DEADLOCK_PREVENTION);
                return false;
            }
        }
        return true;
    });
    auto [iter, success] = lock_queue.request_queue_.try_emplace(txn, LockRequest());
    if(success)
        txn->append_lock_set(tab_fd);
    iter->second.shared_gaps.emplace_back(std::move(gaplock));
}

void LockManager::lock_exclusive_on_gap(Transaction* txn, int tab_fd, const GapLock &gaplock)
{
    if(!gaplock.valid())
        return lock_exclusive_on_table(txn, tab_fd);

    assert(tab_fd < MAX_TABLE_NUMBER);
    if(txn->get_state() == TransactionState::SHRINKING){
        throw TransactionAbortException(txn->get_transaction_id(),AbortReason::LOCK_ON_SHIRINKING);
    }
    auto &lock_queue = lock_table_[tab_fd];

    std::unique_lock lock(lock_queue.latch_);
    lock_queue.cv_.wait(lock, [&](){
        for (auto &[txn_, lock_request] : lock_queue.request_queue_)
        {
            if(txn_->get_transaction_id() == txn->get_transaction_id())continue;
            if(!lock_request.exclusive_gap_compatible(gaplock)) {
                if(txn->get_start_ts() < txn_->get_start_ts())
                    throw TransactionAbortException(txn->get_transaction_id(), 
                        AbortReason::DEADLOCK_PREVENTION);
                return false;
            }
        }
        return true;
    });
    auto [iter, success] = lock_queue.request_queue_.try_emplace(txn, LockRequest());
    if(success)
        txn->append_lock_set(tab_fd);
    iter->second.exclusive_gaps.emplace_back(std::move(gaplock));
}

void LockManager::unlock(Transaction* txn, int tab_fd)
{
    assert(tab_fd < MAX_TABLE_NUMBER);
    txn->set_state(TransactionState::SHRINKING);
    auto &lock_queue = lock_table_[tab_fd];
    std::unique_lock lock(lock_queue.latch_);
    lock_queue.request_queue_.erase(txn);
    lock_queue.cv_.notify_all();
}

void LockManager::lock_shared_on_table(Transaction* txn, int tab_fd)
{
    assert(tab_fd < MAX_TABLE_NUMBER);
    if(txn->get_state() == TransactionState::SHRINKING){
        throw TransactionAbortException(txn->get_transaction_id(),AbortReason::LOCK_ON_SHIRINKING);
    }

    auto &lock_queue = lock_table_[tab_fd];

    std::unique_lock lock(lock_queue.latch_);
    lock_queue.cv_.wait(lock, [&](){
        for (auto &[txn_, lock_request] : lock_queue.request_queue_)
        {
            if(txn_->get_transaction_id() == txn->get_transaction_id())continue;
            if(!lock_request.shared_table_compatible()) {
                if(txn->get_start_ts() < txn_->get_start_ts())
                    throw TransactionAbortException(txn->get_transaction_id(), 
                        AbortReason::DEADLOCK_PREVENTION);
                return false;
            }
        }
        return true;
    });
    auto [iter, success] = lock_queue.request_queue_.try_emplace(txn, LockRequest());
    if(success)
        txn->append_lock_set(tab_fd);
    iter->second.mode = LockMode::SHARED;
}

void LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd)
{
    assert(tab_fd < MAX_TABLE_NUMBER);
    if(txn->get_state() == TransactionState::SHRINKING){
        throw TransactionAbortException(txn->get_transaction_id(),AbortReason::LOCK_ON_SHIRINKING);
    }

    auto &lock_queue = lock_table_[tab_fd];

    std::unique_lock lock(lock_queue.latch_);
    lock_queue.cv_.wait(lock, [&](){
        for (auto &[txn_, lock_request] : lock_queue.request_queue_)
        {
            if(txn_->get_transaction_id() == txn->get_transaction_id())continue;
            if(!lock_request.exclusive_table_compatible()) {
                if(txn->get_start_ts() < txn_->get_start_ts())
                    throw TransactionAbortException(txn->get_transaction_id(), 
                        AbortReason::DEADLOCK_PREVENTION);
                return false;
            }
        }
        return true;
    });
    auto [iter, success] = lock_queue.request_queue_.try_emplace(txn, LockRequest());
    if(success)
        txn->append_lock_set(tab_fd);
    iter->second.mode = LockMode::EXLUCSIVE;
}