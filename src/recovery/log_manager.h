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
#include <vector>
#include <iostream>
#include <thread>
#include "log_defs.h"
#include "common/config.h"
#include "record/rm_defs.h"

class TransactionManager;

/* 日志记录对应操作的类型 */
enum LogType : int
{
    UPDATE = 0,
    INSERT,
    DELETE,
    BEGIN,
    COMMIT,
    ABORT
};
static std::string LogTypeStr[] = {
    "UPDATE",
    "INSERT",
    "DELETE",
    "BEGIN",
    "COMMIT",
    "ABORT"};

class LogRecord
{
public:
    LogType log_type_;     /* 日志对应操作的类型 */
    lsn_t lsn_;            /* 当前日志的lsn */
    uint32_t log_tot_len_; /* 整个日志记录的长度 */
    txn_id_t log_tid_;     /* 创建当前日志的事务ID */
    lsn_t prev_lsn_;       /* 事务创建的前一条日志记录的lsn，用于undo */

    // 把日志记录序列化到dest中
    virtual void serialize(char *dest) const
    {
        memcpy(dest + OFFSET_LOG_TYPE, &log_type_, sizeof(LogType));
        memcpy(dest + OFFSET_LSN, &lsn_, sizeof(lsn_t));
        memcpy(dest + OFFSET_LOG_TOT_LEN, &log_tot_len_, sizeof(uint32_t));
        memcpy(dest + OFFSET_LOG_TID, &log_tid_, sizeof(txn_id_t));
        memcpy(dest + OFFSET_PREV_LSN, &prev_lsn_, sizeof(lsn_t));
    }
    // 从src中反序列化出一条日志记录
    virtual size_t deserialize(const char *src)
    {
        log_type_ = *reinterpret_cast<const LogType *>(src);
        lsn_ = *reinterpret_cast<const lsn_t *>(src + OFFSET_LSN);
        log_tot_len_ = *reinterpret_cast<const uint32_t *>(src + OFFSET_LOG_TOT_LEN);
        log_tid_ = *reinterpret_cast<const txn_id_t *>(src + OFFSET_LOG_TID);
        prev_lsn_ = *reinterpret_cast<const lsn_t *>(src + OFFSET_PREV_LSN);
        return LOG_HEADER_SIZE;
    }
    // used for debug
    virtual void format_print()
    {
        std::cout << "log type in father_function: " << LogTypeStr[log_type_] << "\n";
        printf("Print Log Record:\n");
        printf("log_type_: %s\n", LogTypeStr[log_type_].c_str());
        printf("lsn: %d\n", lsn_);
        printf("log_tot_len: %d\n", log_tot_len_);
        printf("log_tid: %d\n", log_tid_);
        printf("prev_lsn: %d\n", prev_lsn_);
    }
    virtual ~LogRecord() = default;
};

class BeginLogRecord : public LogRecord
{
public:
    BeginLogRecord()
    {
        log_type_ = LogType::BEGIN;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    BeginLogRecord(txn_id_t txn_id) : BeginLogRecord()
    {
        log_tid_ = txn_id;
    }

    // 序列化Begin日志记录到dest中
    void serialize(char *dest) const override
    {
        LogRecord::serialize(dest);
    }
    // 从src中反序列化出一条Begin日志记录
    size_t deserialize(const char *src) override
    {
        return LogRecord::deserialize(src);
    }
    virtual void format_print() override
    {
        std::cout << "log type in son_function: " << LogTypeStr[log_type_] << "\n";
        LogRecord::format_print();
    }
};

/**
 * commit操作的日志记录
 */
class CommitLogRecord : public LogRecord
{
public:
    CommitLogRecord()
    {
        log_type_ = LogType::COMMIT;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    CommitLogRecord(txn_id_t txn_id) : CommitLogRecord()
    {
        log_tid_ = txn_id;
    }
    // 序列化Commit日志记录到dest中
    void serialize(char *dest) const override
    {
        LogRecord::serialize(dest);
    }
    // 从src中反序列化出一条Commit日志记录
    size_t deserialize(const char *src) override
    {
        return LogRecord::deserialize(src);
    }
    virtual void format_print() override
    {
        std::cout << "log type in son_function: " << LogTypeStr[log_type_] << "\n";
        LogRecord::format_print();
    }
};

/**
 * abort操作的日志记录
 */
class AbortLogRecord : public LogRecord
{
public:
    AbortLogRecord()
    {
        log_type_ = LogType::ABORT;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
    }
    AbortLogRecord(txn_id_t txn_id) : AbortLogRecord()
    {
        log_tid_ = txn_id;
    }
    // 序列化Abort日志记录到dest中
    void serialize(char *dest) const override
    {
        LogRecord::serialize(dest);
    }
    // 从src中反序列化出一条Abort日志记录
    size_t deserialize(const char *src) override
    {
        return LogRecord::deserialize(src);
    }
    virtual void format_print() override
    {
        std::cout << "log type in son_function: " << LogTypeStr[log_type_] << "\n";
        LogRecord::format_print();
    }
};

class InsertLogRecord : public LogRecord
{
public:
    InsertLogRecord()
    {
        log_type_ = LogType::INSERT;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
        table_name_ = nullptr;
    }
    InsertLogRecord(txn_id_t txn_id, RmRecord &insert_value, Rid &rid, std::string &table_name)
        : InsertLogRecord()
    {
        log_tid_ = txn_id;
        insert_value_ = insert_value;
        rid_ = rid;
        log_tot_len_ += sizeof(int);
        log_tot_len_ += insert_value_.size;
        log_tot_len_ += sizeof(Rid);
        table_name_size_ = table_name.length();
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, table_name.c_str(), table_name_size_);
        log_tot_len_ += sizeof(size_t) + table_name_size_;
    }
    ~InsertLogRecord()
    {
        if (table_name_ != nullptr)
            delete[] table_name_;
    }

    // 把insert日志记录序列化到dest中
    void serialize(char *dest) const override
    {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        memcpy(dest + offset, &insert_value_.size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, insert_value_.data, insert_value_.size);
        offset += insert_value_.size;
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_, table_name_size_);
    }
    // 从src中反序列化出一条Insert日志记录
    size_t deserialize(const char *src) override
    {
        // 先反序列化基类部分
        int offset = LogRecord::deserialize(src);

        // 反序列化记录内容
        insert_value_.Deserialize(src + offset);
        offset += insert_value_.size + sizeof(int);

        // 反序列化RID
        rid_ = *reinterpret_cast<const Rid *>(src + offset);
        offset += sizeof(Rid);

        // 反序列化表名
        table_name_size_ = *reinterpret_cast<const size_t *>(src + offset);
        offset += sizeof(size_t);

        // 释放旧的表名内存（如果存在）
        if (table_name_ != nullptr)
        {
            delete[] table_name_;
        }

        // 分配新的表名内存并复制
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, src + offset, table_name_size_);

        return offset + table_name_size_;
    }
    void format_print() override
    {
        printf("insert record\n");
        LogRecord::format_print();
        printf("insert_value: %s\n", insert_value_.data);
        printf("insert rid: %d, %d\n", rid_.page_no, rid_.slot_no);
        printf("table name: %s\n", table_name_);
    }

    RmRecord insert_value_;  // 插入的记录
    Rid rid_;                // 记录插入的位置
    char *table_name_;       // 插入记录的表名称
    size_t table_name_size_; // 表名称的大小
};

/**
 * delete操作的日志记录
 */
class DeleteLogRecord : public LogRecord
{
public:
    DeleteLogRecord()
    {
        log_type_ = LogType::DELETE;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
        table_name_ = nullptr;
    }
    DeleteLogRecord(txn_id_t txn_id, RmRecord &delete_value, Rid &rid, const std::string &table_name)
        : DeleteLogRecord()
    {
        log_tid_ = txn_id;
        delete_value_ = delete_value;
        rid_ = rid;
        log_tot_len_ += sizeof(int);
        log_tot_len_ += delete_value_.size;
        log_tot_len_ += sizeof(Rid);
        table_name_size_ = table_name.length();
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, table_name.c_str(), table_name_size_);
        log_tot_len_ += sizeof(size_t) + table_name_size_;
    }

    // 把delete日志记录序列化到dest中
    void serialize(char *dest) const override
    {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        memcpy(dest + offset, &delete_value_.size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, delete_value_.data, delete_value_.size);
        offset += delete_value_.size;
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_, table_name_size_);
    }
    // 从src中反序列化出一条Delete日志记录
    size_t deserialize(const char *src) override
    {
        // 先反序列化基类部分
        int offset = LogRecord::deserialize(src);

        delete_value_.Deserialize(src + offset);
        offset += delete_value_.size + sizeof(int);

        // 反序列化RID
        rid_ = *reinterpret_cast<const Rid *>(src + offset);
        offset += sizeof(Rid);

        // 反序列化表名
        table_name_size_ = *reinterpret_cast<const size_t *>(src + offset);
        offset += sizeof(size_t);

        // 释放旧的表名内存（如果存在）
        if (table_name_ != nullptr)
        {
            delete[] table_name_;
        }

        // 分配新的表名内存并复制
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, src + offset, table_name_size_);

        return offset + table_name_size_;
    }
    void format_print() override
    {
        printf("delete record\n");
        LogRecord::format_print();
        printf("delete_value: %s\n", delete_value_.data);
        printf("delete rid: %d, %d\n", rid_.page_no, rid_.slot_no);
        printf("table name: %s\n", table_name_);
    }
    ~DeleteLogRecord()
    {
        if (table_name_ != nullptr)
        {
            delete[] table_name_;
        }
    }

    RmRecord delete_value_;  // 删除的记录
    Rid rid_;                // 记录删除的位置
    char *table_name_;       // 删除记录的表名称
    size_t table_name_size_; // 表名称的大小
};

/**
 * update操作的日志记录
 */
class UpdateLogRecord : public LogRecord
{
public:
    UpdateLogRecord()
    {
        log_type_ = LogType::UPDATE;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
        table_name_ = nullptr;
    }
    UpdateLogRecord(txn_id_t txn_id, Rid &rid, RmRecord &before_value, RmRecord &after_value, std::string &table_name)
        : UpdateLogRecord()
    {
        log_tid_ = txn_id;
        before_value_ = before_value;
        after_value_ = after_value;
        rid_ = rid;
        log_tot_len_ += sizeof(int);
        log_tot_len_ += before_value_.size;
        log_tot_len_ += sizeof(int);
        log_tot_len_ += after_value_.size;
        log_tot_len_ += sizeof(Rid);
        table_name_size_ = table_name.length();
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, table_name.c_str(), table_name_size_);
        log_tot_len_ += sizeof(size_t) + table_name_size_;
    }

    // 把update日志记录序列化到dest中
    void serialize(char *dest) const override
    {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        memcpy(dest + offset, &before_value_.size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, before_value_.data, before_value_.size);
        offset += before_value_.size;
        memcpy(dest + offset, &after_value_.size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, after_value_.data, after_value_.size);
        offset += after_value_.size;
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_, table_name_size_);
    }
    // 从src中反序列化出一条Update日志记录
    size_t deserialize(const char *src) override
    {
        // 先反序列化基类部分
        int offset = LogRecord::deserialize(src);

        // 反序列化更新前的记录
        before_value_.Deserialize(src + offset);
        offset += before_value_.size + sizeof(int);
        after_value_.Deserialize(src + offset);
        offset += after_value_.size + sizeof(int);

        // 反序列化RID
        rid_ = *reinterpret_cast<const Rid *>(src + offset);
        offset += sizeof(Rid);

        // 反序列化表名
        table_name_size_ = *reinterpret_cast<const size_t *>(src + offset);
        offset += sizeof(size_t);

        // 释放旧的表名内存（如果存在）
        if (table_name_ != nullptr)
        {
            delete[] table_name_;
        }

        // 分配新的表名内存并复制
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, src + offset, table_name_size_);

        return offset + table_name_size_;
    }
    void format_print() override
    {
        printf("update record\n");
        LogRecord::format_print();
        printf("before_value: %s\n", before_value_.data);
        printf("after_value: %s\n", after_value_.data);
        printf("update rid: %d, %d\n", rid_.page_no, rid_.slot_no);
        printf("table name: %s\n", table_name_);
    }
    ~UpdateLogRecord()
    {
        if (table_name_ != nullptr)
        {
            delete[] table_name_;
        }
    }

    RmRecord before_value_;  // 更新前的记录
    RmRecord after_value_;   // 更新后的记录
    Rid rid_;                // 记录更新的位置
    char *table_name_;       // 更新记录的表名称
    size_t table_name_size_; // 表名称的大小
};

/* 日志缓冲区，只有一个buffer，因此需要阻塞地去把日志写入缓冲区中 */

class LogBuffer
{
public:
    LogBuffer() : offset_(0) {}

    inline bool is_full(int append_size)
    {
        return (offset_ + append_size > LOG_BUFFER_SIZE);
    }

    inline void append(LogRecord *log_record)
    {
        log_record->serialize(buffer_ + offset_);
        offset_ += log_record->log_tot_len_;
    }

    // 新增：重置缓冲区
    inline void reset() {
        offset_ = 0;
    }

    // 新增：获取当前数据大小
    inline int size() const {
        return offset_;
    }

    char buffer_[LOG_BUFFER_SIZE + 1];
    int offset_; // 写入log的offset
};

/* 日志管理器，负责把日志写入日志缓冲区，以及把日志缓冲区中的内容写入磁盘中 */
class LogManager
{
public:
    LogManager(DiskManager_Final *disk_manager, BufferPoolManager_Final *buffer_pool_manager)
        : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager)
    {
        flush_thread_ = std::thread(&LogManager::flush_log_to_disk_periodically, this);
    }

    ~LogManager()
    {
        shutdown_.store(true);
        flush_cv_.notify_all();  // 唤醒刷盘线程
        if (flush_thread_.joinable())
        {
            flush_thread_.join();
        }
        flush_log_to_disk();
    }

    lsn_t add_log_to_buffer(LogRecord *log_record);
    void flush_log_to_disk();

    LogBuffer *get_log_buffer() { return &log_buffers_[active_buffer_index_]; }

    void create_static_check_point(TransactionManager *txn_mgr);

private:
    void flush_log_to_disk_periodically();
    LogRecord *read_log(long long offset);
    void flush_log_to_disk_without_lock();
    lsn_t add_log_to_buffer_without_lock(LogRecord *log_record);
    void swap_buffers();  // 新增：交换缓冲区

private:
    std::atomic<lsn_t> global_lsn_{0}; // 全局lsn，递增，用于为每条记录分发lsn
    std::mutex latch_;                 // 用于对log_buffer_的互斥访问
    
    // 双缓冲区实现
    LogBuffer log_buffers_[2];         // 双缓冲区
    std::atomic<int> active_buffer_index_{0};  // 当前活跃缓冲区索引
    std::mutex flush_mutex_;           // 刷盘操作的互斥锁
    std::condition_variable flush_cv_; // 用于唤醒刷盘线程
    
    lsn_t persist_lsn_;                // 记录已经持久化到磁盘中的最后一条日志的日志号
    DiskManager_Final *disk_manager_;
    BufferPoolManager_Final *buffer_pool_manager_;
    std::thread flush_thread_;          // 异步刷盘线程
    bool is_dirty_ = false;             // 标记log_buffer_是否被修改
    std::atomic<bool> shutdown_{false}; // 标记是否停止异步刷盘线程
};