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
    std::lock_guard lock(latch_);
    flush_log_to_disk_without_lock();
    auto log_records_ = read_logs_from_disk_without_lock();
    disk_manager_->clear_log();
    buffer_pool_manager_->force_flush_all_pages();

    std::unordered_set<int> finish_txns_;
    for (const auto &log_record : log_records_)
    {
        if (log_record->log_type_ == LogType::COMMIT)
        {
            finish_txns_.insert(log_record->log_tid_);
        }
    }

    for (const auto &log_record : log_records_)
    {
        if (finish_txns_.find(log_record->log_tid_) == finish_txns_.end())
        {
            add_log_to_buffer_without_lock(log_record);
        }
        delete log_record;
    }
    flush_log_to_disk_without_lock();
}

std::vector<LogRecord *> LogManager::read_logs_from_disk_without_lock()
{
  std::vector<LogRecord *> log_records_;
  const auto file_size_ = disk_manager_->get_file_size(LOG_FILE_NAME);
  auto buffer = new char[std::max(file_size_, 1)];
  disk_manager_->read_log(buffer, file_size_, 0);
  auto *tmp_log_ = new LogRecord();
  int offset = 0;

  while (offset < file_size_)
  {
      tmp_log_->deserialize(buffer + offset);
      switch (tmp_log_->log_type_)
      {
        case LogType::BEGIN:
        {
            auto log_record_ = new BeginLogRecord();
            offset += log_record_->deserialize(buffer + offset);
            log_records_.emplace_back(log_record_);
        }
        break;
        case LogType::COMMIT:
        {
            auto log_record_ = new CommitLogRecord();
            offset += log_record_->deserialize(buffer + offset);
            log_records_.emplace_back(log_record_);
        }
        break;
        case LogType::DELETE:
        {
            auto log_record_ = new DeleteLogRecord();
            offset += log_record_->deserialize(buffer + offset);
            log_records_.emplace_back(log_record_);
        }
        break;
        case LogType::INSERT:
        {
            auto log_record_ = new InsertLogRecord();
            offset += log_record_->deserialize(buffer + offset);
            log_records_.emplace_back(log_record_);
        }
        break;
        case LogType::UPDATE:
        {
            auto log_record_ = new UpdateLogRecord();
            offset += log_record_->deserialize(buffer + offset);
            log_records_.emplace_back(log_record_);
        }
        break;
      default:
        break;
      }
  }

  delete tmp_log_;
  return log_records_;
}