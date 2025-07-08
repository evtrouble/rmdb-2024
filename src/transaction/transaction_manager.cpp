/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

#include <unordered_set>

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction *TransactionManager::begin(Transaction *txn, LogManager *log_manager)
{
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    // 3. 把开始事务加入到全局事务表中
    // 4. 返回当前事务指针
    // 如果需要支持MVCC请在上述过程中添加代码
    if (txn == nullptr)
    {
        // 创建新事务，使用默认的可串行化隔离级别
        txn_id_t new_txn_id = next_txn_id_++;
        txn = new Transaction(new_txn_id, this);
        // 设置事务开始时间戳
        txn->set_start_ts(next_timestamp_);
    }

    BeginLogRecord log_record(txn->get_transaction_id());
    log_manager->add_log_to_buffer(&log_record);

    // 设置事务状态为GROWING（增长阶段）
    txn->set_state(TransactionState::GROWING);

    // 将事务加入全局事务表
    {
        std::lock_guard lock(txn_map_mutex_);
        txn_map.emplace(txn->get_transaction_id(), txn);
    }

    running_txns_.AddTxn(txn->get_start_ts());
    return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Context *context, LogManager *log_manager)
{
    Transaction *txn = context->txn_;
    // 1. 如果存在未提交的写操作，提交所有的写操作
    // 2. 释放所有锁
    // 3. 释放事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    // 如果需要支持MVCC请在上述过程中添加代码
    auto write_set = txn->get_write_set();
    for(auto write : *write_set)
    {
        delete write;
    }
    write_set->clear(); // 清空写集

    auto write_index_set = txn->get_write_index_set();
    for(auto write : *write_index_set)
    {
        delete write;
    }
    write_index_set->clear(); // 清空写集

    txn->set_commit_ts(++next_timestamp_); // 设置提交时间戳

    // 2. 释放所有锁
    auto lock_set = txn->get_lock_set();
    for (auto &lock_data_id : *lock_set)
    {
        lock_manager_->unlock_key(txn, lock_data_id);
    }

    // 3. 清空事务相关资源
    lock_set->clear(); // 清空锁集

    // 4. 把事务日志刷入磁盘
    if (log_manager != nullptr)
    {
        CommitLogRecord log_record(context->txn_->get_transaction_id());
        log_manager->add_log_to_buffer(&log_record);
        // log_manager->flush_log_to_disk();
    }

    // 5. 更新事务状态为已提交
    txn->set_state(TransactionState::COMMITTED);

    running_txns_.UpdateCommitTs(txn->get_commit_ts());
    running_txns_.RemoveTxn(txn->get_start_ts());
    txn->reset(); 
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Context *context, LogManager *log_manager)
{
    Transaction *txn = context->txn_;
    // 1. 回滚所有写操作
    // 2. 释放所有锁
    // 3. 清空事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    // 如果需要支持MVCC请在上述过程中添加代码

    // 1. 回滚所有写操作
    auto write_set = txn->get_write_set();
    while (write_set->size())
    {
        auto write_record = write_set->front(); // 获取最后一个写记录
        write_set->pop_front();      // 移除最后一个写记录
        Rid rid = write_record->GetRid();
        // 根据写操作类型进行回滚
        switch (write_record->GetWriteType())
        {
            case WType::INSERT_TUPLE:
                txn->release();
                sm_manager_->get_table_handle(write_record->GetTableName())
                    ->abort_insert_record(rid);
                break;
            case WType::DELETE_TUPLE: {
                auto undolog = write_record->GetUndoLog();
                auto fh = sm_manager_->get_table_handle(write_record->GetTableName());
                if (undolog == nullptr)
                {
                    fh->abort_delete_record(rid, write_record->GetRecord().data);
                }
                else
                {
                    undolog->txn_->dup(); // 增加引用计数
                    fh->abort_delete_record(rid, undolog->tuple_.data);
                    txn->release();
                }
                break;
            }
            case WType::UPDATE_TUPLE:{
                auto undolog = write_record->GetUndoLog();
                auto fh = sm_manager_->get_table_handle(write_record->GetTableName());
                if (undolog == nullptr)
                {
                    fh->abort_update_record(rid, write_record->GetRecord().data);
                }
                else
                {
                    undolog->txn_->dup(); // 增加引用计数
                    fh->abort_update_record(rid, undolog->tuple_.data);
                    txn->release();
                }
                break;
            }
            default:
                throw InternalError("Unknown write type in abort");
        }
        delete write_record;
    }

    auto write_index_set = txn->get_write_index_set();
    while (write_index_set->size())
    {
        auto write_record = write_index_set->back(); // 获取最后一个写记录
        write_index_set->pop_back();      // 移除最后一个写记录
        Rid rid = write_record->GetRid();
        // 根据写操作类型进行回滚
        switch (write_record->GetWriteType())
        {
            case WType::IX_INSERT_TUPLE:
                sm_manager_->get_index_handle(write_record->GetTableName())
                    ->delete_entry(write_record->GetRecord().data, rid, txn, true);
                break;
            case WType::IX_DELETE_TUPLE:
                sm_manager_->get_index_handle(write_record->GetTableName())
                    ->insert_entry(write_record->GetRecord().data, rid, txn, true);
                break;
            default:
                throw InternalError("Unknown write type in abort");
        }
        delete write_record;
    }
    
    // 2. 释放所有锁
    auto lock_set = txn->get_lock_set();
    for (auto &lock_data_id : *lock_set)
    {
        lock_manager_->unlock_key(txn, lock_data_id);
    }
    // 3. 清空事务相关资源，eg.锁集
    lock_set->clear(); // 清空锁集
    // 4. 把事务日志刷入磁盘中
    if (log_manager != nullptr)
    {
        AbortLogRecord log_record(context->txn_->get_transaction_id());
        log_manager->add_log_to_buffer(&log_record);
        // log_manager->flush_log_to_disk();
    }
    // 5. 更新事务状态
    txn->set_state(TransactionState::ABORTED);
    running_txns_.RemoveTxn(txn->get_start_ts());
    txn->reset();
}

void TransactionManager::PurgeCleaning()
{
    std::vector<std::shared_ptr<RmFileHandle>> tables;
    bool all_tables_done = true;
    
    // 自适应休眠时间参数
    int base_sleep_ms = 100;   // 基础休眠时间(ms)
    int max_sleep_ms = 5000;   // 最大休眠时间(ms)
    int min_sleep_ms = 50;     // 最小休眠时间(ms)
    int current_sleep_ms = base_sleep_ms;

    while (!terminate_purge_cleaner_)
    {
        timestamp_t watermark = running_txns_.GetWatermark();
        tables = sm_manager_->get_all_table_handle();
        
        size_t tables_with_work = 0;  // 有工作要做的表数量
        all_tables_done = true;
        
        for(auto& fh : tables) {
            if(terminate_purge_cleaner_) 
                break;
                
            // 清理当前表的一批页面
            bool table_done = fh->clean_pages(this, watermark);
            if (!table_done) {
                all_tables_done = false;
                tables_with_work++;
            }
        }

        // 动态调整休眠时间
        if (all_tables_done) {
            // 如果所有表都清理完成,增加休眠时间
            current_sleep_ms = std::min(current_sleep_ms * 2, max_sleep_ms);
        } else if (tables_with_work > tables.size() / 2) {
            // 如果超过一半的表都有工作要做,减少休眠时间
            current_sleep_ms = std::max(current_sleep_ms / 2, min_sleep_ms);
        }

        if(terminate_purge_cleaner_) 
            break;
        std::this_thread::sleep_for(std::chrono::milliseconds(current_sleep_ms));
    }
}

void TransactionManager::StartPurgeCleaner()
{
    if(concurrency_mode_ == ConcurrencyMode::MVCC)
    {
        purgeCleaner = std::thread(&TransactionManager::PurgeCleaning, this);
    }
}
