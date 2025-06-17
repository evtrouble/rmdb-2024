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

/**
 * @description: 添加日志记录到日志缓冲区中，并返回日志记录号
 * @param {LogRecord*} log_record 要写入缓冲区的日志记录
 * @return {lsn_t} 返回该日志的日志记录号
 */
lsn_t LogManager::add_log_to_buffer(LogRecord *log_record)
{
  // 争用互斥锁
    std::unique_lock lock(latch_);
    if(log_buffer_.is_full(log_record->log_tot_len_)){//如果缓冲区已满
        latch_.unlock();
        flush_log_to_disk();
        latch_.lock();
    }
    // 分配一个日志记录号
    log_record->lsn_ = global_lsn_++;
    // 将日志记录添加到缓冲区中
    log_buffer_.append(log_record);
    // 设置脏标志
    is_dirty_ = true;
    return log_record->lsn_;
}

lsn_t LogManager::add_log_to_buffer_without_lock(LogRecord *log_record)
{
    if(log_buffer_.is_full(log_record->log_tot_len_)){//如果缓冲区已满
      flush_log_to_disk();
    }
    // 分配一个日志记录号
    log_record->lsn_ = global_lsn_++;
    // 将日志记录添加到缓冲区中
    log_buffer_.append(log_record);
    // 设置脏标志
    is_dirty_ = true;
    return log_record->lsn_;
}

/**
 * @description: 把日志缓冲区的内容刷到磁盘中，由于目前只设置了一个缓冲区，因此需要阻塞其他日志操作
 */
void LogManager::flush_log_to_disk()
{
    // 争用互斥锁
    std::lock_guard lock(latch_);
    flush_log_to_disk_without_lock();
}

void LogManager::flush_log_to_disk_without_lock()
{
  if(is_dirty_) {
    // 写入日志到磁盘
    disk_manager_->write_log(log_buffer_.buffer_, log_buffer_.offset_);
    // 重置缓冲区，更新lsn
    // memset(log_buffer_.buffer_, 0, sizeof(log_buffer_.offset_));
    log_buffer_.offset_ = 0; 
    persist_lsn_ = global_lsn_ - 1;
    is_dirty_ = false;
  }
}

/**
 * @description: 周期性的把日志缓冲区的内容刷到磁盘中
 */
void LogManager::flush_log_to_disk_periodically(){
    while(!shutdown_){
        if(is_dirty_){
            flush_log_to_disk();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

void LogManager::create_static_check_point()
{
    // std::lock_guard lock(latch_);
    // flush_log_to_disk_without_lock();

    // int offset = 0;
    // buffer_pool_manager_->force_flush_all_pages();

    // std::unordered_set<int> finish_txns_;
    // LogRecord* log_record = nullptr;
    // while(true){
    //     log_record = read_log(offset);
    //     if(log_record == nullptr){
    //         break;
    //     }
    //     offset += log_record->log_tot_len_;
    //     if(log_record->log_type_ == LogType::COMMIT || log_record->log_type_ == LogType::ABORT){
    //        finish_txns_.insert(log_record->log_tid_);
    //     }
    //     delete log_record;
    // }

    // offset = 0;
    // while(true){
    //     log_record = read_log(offset);
    //     if(log_record == nullptr){
    //         break;
    //     }
    //     offset += log_record->log_tot_len_;
    //     if (finish_txns_.find(log_record->log_tid_) == finish_txns_.end())
    //     {
    //         add_log_to_buffer_without_lock(log_record);
    //     }
    //     delete log_record;
    // }
    // disk_manager_->clear_log();
    // flush_log_to_disk_without_lock();
}

LogRecord *LogManager::read_log(long long offset) {
    // 读取日志记录头
    char log_header[LOG_HEADER_SIZE];
    if(disk_manager_->read_log(log_header, LOG_HEADER_SIZE, offset) == 0){
        return nullptr;
    }
    // 解析日志记录头
    LogType log_type = *reinterpret_cast<const LogType*>(log_header); 
    uint32_t log_size = *reinterpret_cast<const uint32_t*>(log_header + OFFSET_LOG_TOT_LEN);
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

    if(log_record != nullptr){
        log_record->deserialize(log_data);
    }
    return log_record;
}