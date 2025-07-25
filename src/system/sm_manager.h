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
#include <thread>
#include <queue>
#include <mutex>
#include <shared_mutex>
#include <fcntl.h>  // for open()
#include <unistd.h> // for read(), close()
#include "index/ix.h"
#include "record/rm_file_handle_final.h"
#include "sm_defs.h"
#include "sm_meta.h"
#include "common/context.h"

class Context;

struct ColDef
{
    std::string name; // Column name
    ColType type;     // Type of column
    int len;          // Length of column
};

/* 系统管理器，负责元数据管理和DDL语句的执行 */
class SmManager
{
public:
    DbMeta db_;                                                                // 当前打开的数据库的元数据
    std::unordered_map<std::string, std::shared_ptr<RmFileHandle_Final>> fhs_; // file name -> record file handle, 当前数据库中每张表的数据文件
    std::unordered_map<std::string, std::shared_ptr<IxIndexHandle>> ihs_;      // file name -> index file handle, 当前数据库中每个索引的文件
    bool io_enabled_ = true;
    // 添加新的批量数据结构
    struct BatchDataChunk
    {
        std::vector<std::unique_ptr<char[]>> records;
        std::vector<std::string> raw_lines; // 用于错误处理
        bool is_final;

        BatchDataChunk() : is_final(false)
        {
            records.reserve(1000); // 预分配空间
            raw_lines.reserve(1000);
        }
    };

    struct ThreadSafeBatchQueue
    {
        std::queue<std::shared_ptr<BatchDataChunk>> queue;
        std::mutex mtx;
        std::condition_variable cv;
        bool finished = false;
        std::exception_ptr exception_ptr = nullptr;

        void push(std::shared_ptr<BatchDataChunk> chunk)
        {
            std::lock_guard<std::mutex> lock(mtx);
            queue.push(chunk);
            cv.notify_one();
        }

        std::shared_ptr<BatchDataChunk> pop()
        {
            std::unique_lock<std::mutex> lock(mtx);
            cv.wait(lock, [this]
                    { return !queue.empty() || finished || exception_ptr; });

            if (exception_ptr)
            {
                std::rethrow_exception(exception_ptr);
            }

            if (queue.empty())
            {
                return nullptr;
            }

            auto chunk = queue.front();
            queue.pop();
            return chunk;
        }

        void set_finished()
        {
            std::lock_guard<std::mutex> lock(mtx);
            finished = true;
            cv.notify_all();
        }

        void set_exception(std::exception_ptr ptr)
        {
            std::lock_guard<std::mutex> lock(mtx);
            exception_ptr = ptr;
            finished = true;
            cv.notify_all();
        }
    };

    // 新的方法声明
    void load_csv_data_threaded_batch(std::string &file_name, std::string &tab_name, Context *context);
    void batch_reader_thread_func(const std::string &file_name, ThreadSafeBatchQueue &queue,
                                  const TabMeta &tab, int hidden_column_count, Context *context);
    void batch_processor_thread_func(ThreadSafeBatchQueue &queue, const std::string &tab_name, Context *context);

private:
    DiskManager_Final *disk_manager_;
    BufferPoolManager_Final *buffer_pool_manager_;
    RmManager_Final *rm_manager_;
    IxManager *ix_manager_;
    std::shared_mutex fhs_latch_; // 保护fhs_的读写锁，保证对文件句柄的并发访问安全
    std::shared_mutex ihs_latch_; // 保护ihs_的读写锁，保证对索引句柄的并发访问安全
    struct DataChunk
    {
        std::unique_ptr<char[]> data;
        size_t size;
        bool is_final;

        DataChunk() : size(0), is_final(false) {}
        DataChunk(size_t buffer_size) : data(std::make_unique<char[]>(buffer_size + 1)), size(0), is_final(false) {}
    };

    // 线程安全的数据队列
    struct ThreadSafeQueue
    {
        std::queue<std::shared_ptr<DataChunk>> queue;
        std::mutex mtx;
        std::condition_variable cv;
        bool finished = false;
        std::exception_ptr exception_ptr = nullptr;

        void push(std::shared_ptr<DataChunk> chunk)
        {
            std::lock_guard<std::mutex> lock(mtx);
            queue.push(chunk);
            cv.notify_one();
        }

        std::shared_ptr<DataChunk> pop()
        {
            std::unique_lock<std::mutex> lock(mtx);
            cv.wait(lock, [this]
                    { return !queue.empty() || finished || exception_ptr; });

            if (exception_ptr)
            {
                std::rethrow_exception(exception_ptr);
            }

            if (queue.empty())
            {
                return nullptr; // 队列已空且生产者已完成
            }

            auto chunk = queue.front();
            queue.pop();
            return chunk;
        }

        void set_finished()
        {
            std::lock_guard<std::mutex> lock(mtx);
            finished = true;
            cv.notify_all();
        }

        void set_exception(std::exception_ptr ptr)
        {
            std::lock_guard<std::mutex> lock(mtx);
            exception_ptr = ptr;
            finished = true;
            cv.notify_all();
        }
    };
    // 文件读取线程函数
    void reader_thread_func(const std::string &file_name, ThreadSafeQueue &queue, const size_t buffer_size);

    // 数据处理线程函数
    void processor_thread_func(ThreadSafeQueue &queue, const std::string &tab_name, Context *context);

    // 线程安全版本的辅助方法
    size_t process_buffer_chunk_threaded(char *buffer, size_t buffer_size,
                                         std::string &leftover, const TabMeta &tab,
                                         int hidden_column_count, Context *context,
                                         bool skip_header);

    void process_csv_line_threaded(const std::string &line, const TabMeta &tab,
                                   int hidden_column_count, Context *context);

    void update_indexes_for_record_threaded(const RmRecord &rec, const Rid &rid,
                                            const TabMeta &tab, Context *context);
    void load_csv_data_page_batch(std::string &file_name, std::string &tab_name, Context *context);
    void batch_insert_records(const std::vector<std::unique_ptr<char[]>> &records,
                              std::vector<Rid> &rids,
                              const TabMeta &tab,
                              Context *context);
    void batch_update_indexes(const std::vector<std::unique_ptr<char[]>> &records,
                              const std::vector<Rid> &rids,
                              const TabMeta &tab,
                              Context *context);
    std::unique_ptr<char[]> parse_csv_to_record(const std::string &line,
                                                const TabMeta &tab,
                                                int hidden_column_count,
                                                Context *context);

public:
    SmManager(DiskManager_Final *disk_manager, BufferPoolManager_Final *buffer_pool_manager, RmManager_Final *rm_manager,
              IxManager *ix_manager)
        : disk_manager_(disk_manager),
          buffer_pool_manager_(buffer_pool_manager),
          rm_manager_(rm_manager),
          ix_manager_(ix_manager)
    {
    }

    ~SmManager() {}

    BufferPoolManager_Final *get_bpm() { return buffer_pool_manager_; }

    RmManager_Final *get_rm_manager() { return rm_manager_; }

    IxManager *get_ix_manager() { return ix_manager_; }

    inline std::shared_ptr<IxIndexHandle> get_index_handle(const std::string &index_name)
    {
        std::shared_lock lock(ihs_latch_);
        auto it = ihs_.find(index_name);
        if (it != ihs_.end())
        {
            return it->second;
        }
        return nullptr; // 如果没有找到对应的索引句柄，返回nullptr
    }

    inline std::shared_ptr<RmFileHandle_Final> get_table_handle(const std::string &table_name)
    {
        std::shared_lock lock(fhs_latch_);
        auto it = fhs_.find(table_name);
        if (it != fhs_.end())
        {
            return it->second;
        }
        return nullptr; // 如果没有找到对应的表文件句柄，返回nullptr
    }

    bool is_dir(const std::string &db_name);

    void create_db(const std::string &db_name);

    void drop_db(const std::string &db_name);

    void open_db(const std::string &db_name);

    void close_db();

    void flush_meta();

    void show_tables(Context *context);

    void desc_table(const std::string &tab_name, Context *context);

    void create_table(const std::string &tab_name, const std::vector<ColDef> &col_defs, Context *context);

    void drop_table(const std::string &tab_name, Context *context);

    void create_index(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context);

    void drop_index(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context);

    void drop_index(const std::string &tab_name, const std::vector<ColMeta> &col_names, Context *context);
    void show_index(const std::string &tab_name, Context *context);

    size_t process_buffer_chunk(char *buffer, size_t buffer_size,
                                std::string &leftover, const TabMeta &tab,
                                int hidden_column_count, Context *context,
                                bool skip_header);
    void process_csv_line(const std::string &line, const TabMeta &tab,
                          int hidden_column_count, Context *context);
    void parse_csv_fields(const std::string &line, std::vector<std::string> &fields);
    void process_final_line(const std::string &leftover, const TabMeta &tab,
                            int hidden_column_count, Context *context);
    int parse_int_safe(const std::string &str);
    float parse_float_safe(const std::string &str);
    void update_indexes_for_record(const RmRecord &rec, const Rid &rid,
                                   const TabMeta &tab, Context *context);
    void load_csv_data_threaded(std::string &file_name, std::string &tab_name, Context *context);

    std::vector<std::shared_ptr<RmFileHandle_Final>> get_all_table_handle()
    {
        std::shared_lock lock(fhs_latch_);
        std::vector<std::shared_ptr<RmFileHandle_Final>> handles;
        handles.reserve(fhs_.size());
        for (const auto &pair : fhs_)
        {
            handles.push_back(pair.second);
        }
        return handles;
    }
};
