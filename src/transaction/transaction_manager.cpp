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

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction *TransactionManager::begin(Transaction *txn, LogManager *log_manager)
{
    // Todo:
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    // 3. 把开始事务加入到全局事务表中
    // 4. 返回当前事务指针

    if (txn == nullptr)
    {
        // 生成新的事务ID
        txn_id_t new_txn_id = next_txn_id_++;
        // 创建新事务，使用默认的可串行化隔离级别
        txn = new Transaction(new_txn_id);
        // 设置事务开始时间戳
        txn->set_start_ts(next_timestamp_++);
    }

    // 设置事务状态为GROWING（增长阶段）
    txn->set_state(TransactionState::GROWING);

    // 将事务加入全局事务表
    {
        std::unique_lock<std::mutex> lock(latch_);
        txn_map[txn->get_transaction_id()] = txn;
    }

    return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction *txn, LogManager *log_manager)
{
    // 1. 提交所有未提交的写操作
    // 写操作已经在执行时完成，这里不需要额外操作
    txn->get_write_set()->clear();

    // 2. 释放所有锁
    auto lock_set = txn->get_lock_set();
    for (auto &lock_data_id : *lock_set)
    {
        lock_manager_->unlock(txn, lock_data_id);
    }

    // 3. 清空事务相关资源
    lock_set->clear();

    // 4. 把事务日志刷入磁盘
    if (log_manager != nullptr)
    {
        log_manager->flush_log_to_disk();
    }

    // 5. 更新事务状态为已提交
    txn->set_state(TransactionState::COMMITTED);
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction *txn, LogManager *log_manager)
{
    // 1. 回滚所有写操作
    auto write_set = txn->get_write_set();
    while (!write_set->empty())
    {
        auto write_record = std::move(write_set->back()); // 获取最后一个写记录
        write_set->pop_back();                 // 移除最后一个写记录
        //auto &indexes = sm_manager_->db_.get_table(NameManager::get_name(write_record.fd_))->indexes;
        // 根据写操作类型进行回滚
        switch (write_record.GetWriteType())
        {
            case WType::INSERT_TUPLE:
                sm_manager_->fhs_.at(write_record.GetTableName())
                    ->delete_record(write_record.GetRid());
                break;
            case WType::DELETE_TUPLE:
                sm_manager_->fhs_.at(write_record.GetTableName())
                    ->insert_record(write_record.GetRid(), write_record.GetRecord().data);
                break;
            case WType::UPDATE_TUPLE:
                sm_manager_->fhs_.at(write_record.GetTableName())
                    ->update_record(write_record.GetRid(), write_record.GetRecord().data);
                break;
            case WType::IX_INSERT_TUPLE:
                sm_manager_->ihs_.at(write_record.GetTableName())            
                    ->delete_entry(write_record.GetRecord().data, write_record.GetRid(), txn);
                break;
            case WType::IX_DELETE_TUPLE:
                sm_manager_->ihs_.at(write_record.GetTableName())            
                    ->insert_entry(write_record.GetRecord().data, write_record.GetRid(), txn);
                break;
        }
    }
    // 2. 释放所有锁
    auto lock_set = txn->get_lock_set();
    for (auto &lock_data_id : *lock_set)
    {
        lock_manager_->unlock(txn, lock_data_id);
    }
    // 3. 清空事务相关资源，eg.锁集
    lock_set->clear();
    // 4. 把事务日志刷入磁盘中
    if (log_manager != nullptr)
    {
        log_manager->flush_log_to_disk();
    }
    // 5. 更新事务状态
    txn->set_state(TransactionState::ABORTED);
}