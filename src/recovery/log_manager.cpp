/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <cstring>
#include <unordered_set>
#include "log_manager.h"
#include "transaction/transaction_manager.h"

/**
 * @description: 添加日志记录到日志缓冲区中，并返回日志记录号
 * @param {LogRecord*} log_record 要写入缓冲区的日志记录
 * @return {lsn_t} 返回该日志的日志记录号
 */
lsn_t LogManager::add_log_to_buffer(LogRecord *log_record)
{
    std::lock_guard lock(latch_);
    return add_log_to_buffer_without_lock(log_record);
}

lsn_t LogManager::add_log_to_buffer_without_lock(LogRecord *log_record)
{
    LogBuffer* active_buffer = &log_buffers_[active_buffer_index_];
    
    // 如果当前缓冲区空间不足，触发缓冲区交换
    if(active_buffer->is_full(log_record->log_tot_len_)){
        swap_buffers();
        active_buffer = &log_buffers_[active_buffer_index_];
    }
    
    // 分配一个日志记录号
    log_record->lsn_ = global_lsn_++;
    // 将日志记录添加到当前活跃缓冲区中
    active_buffer->append(log_record);
    // 设置脏标志
    is_dirty_ = true;
    return log_record->lsn_;
}

/**
 * @description: 交换双缓冲区
 */
void LogManager::swap_buffers() {
    // 获取当前非活跃缓冲区的索引
    int flush_buffer_index = 1 - active_buffer_index_;
    LogBuffer* flush_buffer = &log_buffers_[flush_buffer_index];
    
    // 如果非活跃缓冲区有数据，先刷盘
    if (flush_buffer->size() > 0) {
        disk_manager_->write_log(flush_buffer->buffer_, flush_buffer->size());
        persist_lsn_ = global_lsn_ - 1;
        flush_buffer->reset();
    }
    
    // 交换活跃缓冲区
    active_buffer_index_ = flush_buffer_index;
    
    // 通知刷盘线程有新数据需要处理
    flush_cv_.notify_one();
}

/**
 * @description: 把日志缓冲区的内容刷到磁盘中
 */
void LogManager::flush_log_to_disk()
{
    std::lock_guard lock(latch_);
    flush_log_to_disk_without_lock();
}

void LogManager::flush_log_to_disk_without_lock()
{
    if(is_dirty_) {
        // 刷新当前活跃缓冲区
        LogBuffer* active_buffer = &log_buffers_[active_buffer_index_];
        if (active_buffer->size() > 0) {
            disk_manager_->write_log(active_buffer->buffer_, active_buffer->size());
            active_buffer->reset();
        }
        
        // 刷新非活跃缓冲区
        LogBuffer* inactive_buffer = &log_buffers_[1 - active_buffer_index_];
        if (inactive_buffer->size() > 0) {
            disk_manager_->write_log(inactive_buffer->buffer_, inactive_buffer->size());
            inactive_buffer->reset();
        }
        
        persist_lsn_ = global_lsn_ - 1;
        is_dirty_ = false;
    }
}

/**
 * @description: 周期性的把日志缓冲区的内容刷到磁盘中
 */
void LogManager::flush_log_to_disk_periodically() {
    while (!shutdown_) {
        std::unique_lock<std::mutex> lock(flush_mutex_);
        
        // 等待一定时间或被唤醒
        flush_cv_.wait_for(lock, std::chrono::milliseconds(10), [this] {
            return shutdown_.load() || is_dirty_;
        });
        
        if (shutdown_) {
            break;
        }
        
        // 立即交换缓冲区并刷盘
        {
            std::lock_guard<std::mutex> log_lock(latch_);
            if (is_dirty_) {
                // 获取非活跃缓冲区进行刷盘
                int flush_buffer_index = 1 - active_buffer_index_;
                LogBuffer* flush_buffer = &log_buffers_[flush_buffer_index];
                
                if (flush_buffer->size() > 0) {
                    disk_manager_->write_log(flush_buffer->buffer_, flush_buffer->size());
                    persist_lsn_ = global_lsn_ - 1;
                    flush_buffer->reset();
                }
                
                // 如果当前活跃缓冲区也有数据，交换后刷盘
                LogBuffer* active_buffer = &log_buffers_[active_buffer_index_];
                if (active_buffer->size() > 0) {
                    // 交换缓冲区
                    active_buffer_index_ = flush_buffer_index;
                    
                    // 刷新原来的活跃缓冲区（现在变成非活跃）
                    disk_manager_->write_log(active_buffer->buffer_, active_buffer->size());
                    persist_lsn_ = global_lsn_ - 1;
                    active_buffer->reset();
                }
                
                is_dirty_ = false;
            }
        }
    }
}

void LogManager::create_static_check_point(TransactionManager *txn_mgr)
{
    std::lock_guard lock(latch_);
    flush_log_to_disk_without_lock();

    int offset = 0;
    buffer_pool_manager_->force_flush_all_pages();

    std::unordered_set<int> finish_txns_;
    LogRecord *log_record = nullptr;
    while (true)
    {
        log_record = read_log(offset);
        if (log_record == nullptr)
        {
            break;
        }
        offset += log_record->log_tot_len_;
        if (log_record->log_type_ == LogType::COMMIT || log_record->log_type_ == LogType::ABORT)
        {
            finish_txns_.insert(log_record->log_tid_);
        }
        delete log_record;
    }

    offset = 0;
    disk_manager_->create_new_log_file();
    while (true)
    {
        log_record = read_log(offset);
        if (log_record == nullptr)
        {
            break;
        }
        offset += log_record->log_tot_len_;
        if (finish_txns_.find(log_record->log_tid_) == finish_txns_.end())
        {
            add_log_to_buffer_without_lock(log_record);
        }
        delete log_record;
    }
    disk_manager_->change_log_file();
    flush_log_to_disk_without_lock();
    txn_mgr->flush_txn_id();
}

LogRecord *LogManager::read_log(long long offset)
{
    // 读取日志记录头
    char log_header[LOG_HEADER_SIZE];
    if (disk_manager_->read_log(log_header, LOG_HEADER_SIZE, offset) == 0)
    {
        return nullptr;
    }
    // 解析日志记录头
    LogType log_type = *reinterpret_cast<const LogType *>(log_header);
    uint32_t log_size = *reinterpret_cast<const uint32_t *>(log_header + OFFSET_LOG_TOT_LEN);
    // 读取日志记录
    char log_data[log_size];
    disk_manager_->read_log(log_data, log_size, offset);
    // 解析日志记录
    LogRecord *log_record = nullptr;
    switch (log_type)
    {
    case BEGIN:
        log_record = new BeginLogRecord();
        break;
    case COMMIT:
        log_record = new CommitLogRecord();
        break;
    case ABORT:
        log_record = new AbortLogRecord();
        break;
    case UPDATE:
        log_record = new UpdateLogRecord();
        break;
    case INSERT:
        log_record = new InsertLogRecord();
        break;
    case DELETE:
        log_record = new DeleteLogRecord();
        break;
    }

    if (log_record != nullptr)
    {
        log_record->deserialize(log_data);
    }
    return log_record;
}