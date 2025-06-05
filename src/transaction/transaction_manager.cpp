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
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    // 3. 把开始事务加入到全局事务表中
    // 4. 返回当前事务指针
    // 如果需要支持MVCC请在上述过程中添加代码
    if (txn == nullptr)
    {
        // 生成新的事务ID
        txn_id_t new_txn_id = next_txn_id_++;
        // 创建新事务，使用默认的可串行化隔离级别
        txn = new Transaction(new_txn_id, this);
        // 设置事务开始时间戳
        txn->set_start_ts(next_timestamp_++);
    }

    // 设置事务状态为GROWING（增长阶段）
    txn->set_state(TransactionState::GROWING);

    // 将事务加入全局事务表
    {
        std::unique_lock lock(latch_);
        txn_map.emplace(txn->get_transaction_id(), txn);
    }

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
    // Todo:
    // 1. 如果存在未提交的写操作，提交所有的写操作
    // 2. 释放所有锁
    // 3. 释放事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    // 如果需要支持MVCC请在上述过程中添加代码
    for(auto write : *txn->get_write_set())
    {
        delete write;
    }
    txn->get_write_set()->clear();

    // txn->set_commit_ts(next_timestamp_++); // 设置提交时间戳

    // 阶段2：更新内存中的版本链（关键！）
    // auto write_version_set = txn->get_write_version_set();
    // for (auto& modified_row : *write_version_set) {
    //     modified_row->version = txn->get_commit_ts(); // 内存中标记新版本生效
    //     modified_row->next_version = tx.undo_log.back(); // 挂接Undo记录
    // }

    // 2. 释放所有锁
    // auto lock_set = txn->get_lock_set();
    // for (auto &lock_data_id : *lock_set)
    // {
    //     lock_manager_->unlock(txn, lock_data_id);
    // }

    // 3. 清空事务相关资源
    // lock_set->clear();

    // 4. 把事务日志刷入磁盘
    if (log_manager != nullptr)
    {
        log_manager->flush_log_to_disk();
    }

    // 5. 更新事务状态为已提交
    txn->set_state(TransactionState::COMMITTED);
    {
        std::unique_lock lock(latch_);
        txn_map.erase(txn->get_transaction_id());
    }
    delete txn;
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
    while (!write_set->empty())
    {
        auto write_record = std::move(write_set->back()); // 获取最后一个写记录
        write_set->pop_back();                 // 移除最后一个写记录
        //auto &indexes = sm_manager_->db_.get_table(NameManager::get_name(write_record.fd_))->indexes;
        // 根据写操作类型进行回滚
        switch (write_record->GetWriteType())
        {
            case WType::INSERT_TUPLE:
                sm_manager_->fhs_.at(write_record->GetTableName())
                    ->delete_record(write_record->GetRid(), context);
                break;
            case WType::DELETE_TUPLE:
                sm_manager_->fhs_.at(write_record->GetTableName())
                    ->insert_record(write_record->GetRecord().data, context);
                break;
            case WType::UPDATE_TUPLE:
                sm_manager_->fhs_.at(write_record->GetTableName())
                    ->update_record(write_record->GetRid(), write_record->GetRecord().data, context);
                break;
            case WType::IX_INSERT_TUPLE:
                sm_manager_->ihs_.at(write_record->GetTableName())
                    ->delete_entry(write_record->GetRecord().data, write_record->GetRid(), txn, true);
                break;
            case WType::IX_DELETE_TUPLE:
                sm_manager_->ihs_.at(write_record->GetTableName())
                    ->insert_entry(write_record->GetRecord().data, write_record->GetRid(), txn, true);
                break;
        }
        delete write_record;
    }
    // 2. 释放所有锁
    // auto lock_set = txn->get_lock_set();
    // for (auto &lock_data_id : *lock_set)
    // {
    //     lock_manager_->unlock(txn, lock_data_id);
    // }
    // 3. 清空事务相关资源，eg.锁集
    // lock_set->clear();
    // 4. 把事务日志刷入磁盘中
    if (log_manager != nullptr)
    {
        log_manager->flush_log_to_disk();
    }
    // 5. 更新事务状态
    // txn->set_commit_ts(next_timestamp_++);  // 设置中止时间戳
    txn->set_state(TransactionState::ABORTED);
    {
        std::unique_lock lock(latch_);
        txn_map.erase(txn->get_transaction_id());
    }
    delete txn;
}

bool TransactionManager::UpdateUndoLink(int fd, const Rid& rid, std::optional<UndoLink> prev_link,
                    std::function<bool(std::optional<UndoLink>)>&& check) {
        // 1. 快速路径：如果不需要更新版本且没有check函数，直接返回
    if (!prev_link && !check) {
        return true;
    }
    
    PageId page_id{fd, rid.page_no};
    std::shared_ptr<PageVersionInfo> page_info;
    {
        // 2. 获取页面版本信息 - 使用局部作用域减少全局锁持有时间
        std::shared_lock<std::shared_mutex> global_lock(version_info_mutex_);
        auto it = version_info_.find(page_id);
        if (it != version_info_.end()) {
            page_info = it->second;
        }
    }
    
    // 3. 如果需要创建新页面，使用写锁重新检查和创建
    if (!page_info) {
        std::unique_lock global_lock(version_info_mutex_);
        page_info = version_info_.try_emplace(page_id, std::make_shared<PageVersionInfo>()).first->second;
    }
    
    // 4. 更新版本链 - 使用页面级锁
    std::unique_lock page_lock(page_info->mutex_);
    
    // 5. 获取当前版本用于check
    std::optional<UndoLink> current_version;
    auto version_it = page_info->prev_version_.find(rid.slot_no);
    if (version_it != page_info->prev_version_.end()) {
        current_version = version_it->second;
    }
    
    // 6. 执行check函数
    if (check && !check(current_version)) {
        return false;
    }
    
    // 7. 更新版本信息
    if (current_version) {
        prev_link->txn_->ModifyUndoLog(prev_link->prev_log_idx_, *current_version);
    }
    // 更新或插入新版本
    page_info->prev_version_.insert_or_assign(rid.slot_no, *prev_link);
    return true;
}

std::optional<UndoLog> TransactionManager::GetVisibleRecord(int fd, const Rid& rid, Transaction* current_txn) {
    if (!current_txn) {
        return std::nullopt;
    }

    PageId page_id{fd, rid.page_no};
    // 获取版本链头
    std::shared_lock global_lock(version_info_mutex_);
    auto page_info = version_info_.find(page_id);
    if (page_info == version_info_.end()) {
        return std::nullopt;
    }
    auto page_info_ptr = page_info->second;
    global_lock.unlock(); // 释放全局锁以避免长时间持有
    
    // 获取记录版本链
    std::shared_lock lock(page_info_ptr->mutex_);
    auto it = page_info_ptr->prev_version_.find(rid.slot_no);
    if (it == page_info_ptr->prev_version_.end()) {
        return std::nullopt;
    }
    auto current = it->second;
    lock.unlock(); // 释放锁以避免长时间持有

    // 遍历版本链找到可见版本
    while (current.IsValid()) {
        auto* version_txn = current.txn_;

        // 同一事务的版本可见
        if (version_txn->get_transaction_id() == current_txn->get_transaction_id()) {
            return version_txn->GetUndoLog(current.prev_log_idx_);
        }
        
        // 已提交事务的版本根据时间戳判断可见性
        if (version_txn->get_commit_ts() <= current_txn->get_start_ts()) {
            return version_txn->GetUndoLog(current.prev_log_idx_);
        }

        // 获取前一个版本
        auto undo_log = version_txn->GetUndoLog(current.prev_log_idx_);
        current = undo_log.prev_version_;
    }
    
    return std::nullopt;
}

void TransactionManager::TruncateVersionChain(int fd, const Rid& rid, timestamp_t watermark) {
    // 先用读锁获取page_info
    PageId page_id{fd, rid.page_no};
    std::shared_lock global_read_lock(version_info_mutex_);
    auto page_it = version_info_.find(page_id);
    if (page_it == version_info_.end()) {
        return;
    }
    auto page_info = page_it->second;
    global_read_lock.unlock();  // 获取到版本链头后释放全局读锁
    
    TruncateVersionChain(page_info, rid, watermark);
}

void TransactionManager::DeleteVersionChain(int fd, const Rid& rid) {
    // 先用读锁获取page_info
    PageId page_id{fd, rid.page_no};
    std::shared_lock global_read_lock(version_info_mutex_);
    auto page_it = version_info_.find(page_id);
    if (page_it == version_info_.end()) {
        return;
    }
    auto page_info = page_it->second;
    global_read_lock.unlock();
    
    DeleteVersionChain(page_info, page_id, rid);
}

void TransactionManager::DeleteVersionChain(std::shared_ptr<PageVersionInfo> page_info, 
                                                   const PageId& page_id, const Rid& rid) {
    // 获取page级别的写锁来保护空检查和删除操作
    std::unique_lock page_lock(page_info->mutex_);
    
    auto version_it = page_info->prev_version_.find(rid.slot_no);
    if (version_it != page_info->prev_version_.end()) {
        auto current = version_it->second;
        page_info->prev_version_.erase(version_it);
        
        // 检查是否删除后变为空
        bool is_empty = page_info->prev_version_.empty();
        page_lock.unlock();  // 释放页面锁以避免长时间持有
        if (is_empty) {
            std::unique_lock global_write_lock(version_info_mutex_);
            // 二次检查,因为可能在释放页面锁后其他线程添加了记录
            if (page_info->prev_version_.empty()) {
                version_info_.erase(page_id);
            }
        }
        
        // 清理链上所有版本的引用计数
        while (current.IsValid()) {
            current.txn_->ReleaseVersionRef();
            auto undo_log = current.txn_->GetUndoLog(current.prev_log_idx_);
            current = undo_log.prev_version_;
        }
    }
}

void TransactionManager::DeleteVersionChainHead(int fd, const Rid& rid) {
    // 先用读锁获取page_info
    PageId page_id{fd, rid.page_no};
    std::shared_lock global_read_lock(version_info_mutex_);
    auto page_it = version_info_.find(page_id);
    if (page_it == version_info_.end()) {
        return;
    }
    auto page_info = page_it->second;
    global_read_lock.unlock();
    
    DeleteVersionChainHead(page_info, page_id, rid);
}

void TransactionManager::DeleteVersionChainHead(std::shared_ptr<PageVersionInfo> page_info,
                                              const PageId& page_id, const Rid& rid) {
    // 获取page级别的写锁来保护头节点删除操作
    std::unique_lock page_lock(page_info->mutex_);
    
    auto version_it = page_info->prev_version_.find(rid.slot_no);
    if (version_it == page_info->prev_version_.end()) {
        return;
    }

    // 获取当前版本链头和它的下一个版本
    auto current = version_it->second;
    auto next_version = current.txn_->GetUndoLog(current.prev_log_idx_).prev_version_;
    
    if (next_version.IsValid()) {
        // 如果存在后续版本,将版本链头更新为下一个版本
        page_info->prev_version_.insert_or_assign(rid.slot_no, next_version);
    } else {
        // 如果不存在后续版本,直接删除版本链头
        page_info->prev_version_.erase(version_it);
        
        // 检查是否删除后变为空
        bool is_empty = page_info->prev_version_.empty();
        page_lock.unlock();  // 释放页面锁以避免长时间持有
        
        if (is_empty) {
            std::unique_lock global_write_lock(version_info_mutex_);
            // 二次检查,因为可能在释放页面锁后其他线程添加了记录
            if (page_info->prev_version_.empty()) {
                version_info_.erase(page_id);
            }
        }
    }

    // 减少被删除版本的引用计数
    current.txn_->ReleaseVersionRef();
}

void TransactionManager::TruncateVersionChain(std::shared_ptr<PageVersionInfo> page_info,
                                                        const Rid &rid, timestamp_t watermark) {
    // 获取页面级读锁保护版本链遍历
    std::shared_lock page_lock(page_info->mutex_);
    auto version_it = page_info->prev_version_.find(rid.slot_no);
    if (version_it == page_info->prev_version_.end()) {
        return;
    }
    auto current = version_it->second;
    page_lock.unlock();  // 释放读锁以避免长时间持有，后续操作需要写锁
   
    
    // 遍历版本链找到截断点 - 保持页面级读锁
    UndoLog undo_log;
    while (current.IsValid()) {
        if (current.txn_->get_commit_ts() < watermark) {
            // 设置截断点的下一个版本为空
            auto next = current.txn_->GetUndoLog(current.prev_log_idx_).prev_version_;

            current.txn_->ModifyUndoLog(current.prev_log_idx_, UndoLink());
            
            // 清理被截断部分的引用计数
            while (next.IsValid()) {
                next.txn_->ReleaseVersionRef();
                undo_log = next.txn_->GetUndoLog(next.prev_log_idx_);
                next = undo_log.prev_version_;
            }
            return;
        }
        
        undo_log = current.txn_->GetUndoLog(current.prev_log_idx_);
        current = undo_log.prev_version_;
    }
}

std::shared_ptr<PageVersionInfo> TransactionManager::GetPageVersionInfo(const PageId& page_id) {
    // 获取页面版本信息
    std::shared_lock global_read_lock(version_info_mutex_);
    auto page_it = version_info_.find(page_id);
    if (page_it == version_info_.end()) {
        return nullptr;
    }
    return page_it->second;
}

void TransactionManager::StartPurgeCleaner()
{
    if(concurrency_mode_ == ConcurrencyMode::MVCC)
    {
        
    }
    // sm_manager_->
}

void TransactionManager::StopPurgeCleaner()
{
    // sm_manager_->
}