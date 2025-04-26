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
#include "log_manager.h"

/**
 * @description: 添加日志记录到日志缓冲区中，并返回日志记录号
 * @param {LogRecord*} log_record 要写入缓冲区的日志记录
 * @return {lsn_t} 返回该日志的日志记录号
 */
lsn_t LogManager::add_log_to_buffer(LogRecord* log_record) {
  std::lock_guard<std::mutex> lock(latch_);  // 加锁保证线程安全
  // 为日志记录分配LSN（日志序列号）
  log_record->lsn_ = ++global_lsn_;
  // 序列化日志记录
  char serialized_log[log_record->log_tot_len_];
  log_record->serialize(serialized_log);
  // 检查缓冲区是否有足够空间
  if (log_buffer_.is_full(log_record->log_tot_len_)) {
    flush_log_to_disk();  // 缓冲区满时先刷盘
  }
  // 将日志记录添加到缓冲区
  memcpy(log_buffer_.buffer_ + log_buffer_.offset_, serialized_log, log_record->log_tot_len_);
  log_buffer_.offset_ += log_record->log_tot_len_;
  return log_record->lsn_;
}

/**
 * @description: 把日志缓冲区的内容刷到磁盘中，由于目前只设置了一个缓冲区，因此需要阻塞其他日志操作
 */
void LogManager::flush_log_to_disk() {
  std::lock_guard<std::mutex> lock(latch_);  // 加锁保证线程安全
  if (log_buffer_.offset_ == 0) {
    return;  // 没有数据需要刷盘
  }
  // 将缓冲区内容写入磁盘
  disk_manager_->write_log(log_buffer_.buffer_, log_buffer_.offset_);
  // 更新持久化LSN
  persist_lsn_ = global_lsn_;
  // 清空缓冲区
  log_buffer_.offset_ = 0;
  memset(log_buffer_.buffer_, 0, LOG_BUFFER_SIZE);
}
