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
#include <queue>
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class SortExecutor : public AbstractExecutor
{
private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<ColMeta> sort_cols_;   // 改为存储多个排序列
    std::vector<bool> is_desc_orders_; // 每列对应的排序方向
    int limit_;                        // LIMIT 限制
    size_t output_count_;              // 记录已输出的记录数
    std::string temp_dir_;             // 临时文件目录
    size_t block_size_;                // 每个块的大小（以字节为单位）
    Context *context_;
    std::vector<std::string> sorted_blocks_; // 已排序的块文件路径
    std::unique_ptr<RmRecord> current_tuple;
    std::vector<std::unique_ptr<RmRecord>> sorted_tuples_; // 用于内部排序的缓冲区
    size_t current_index_;                                 // 当前记录的索引
    size_t record_size;                                    // 记录的长度
    std::ifstream current_block_stream_;                   // 当前块的输入流
    std::unique_ptr<RmRecord> current_block_record_;       // 当前块的当前记录

    bool compareRecords(const RmRecord &a, const RmRecord &b)
    {
        for (size_t i = 0; i < sort_cols_.size(); ++i)
        {
            const auto &col_meta = sort_cols_[i];
            bool desc = is_desc_orders_[i];
            Value val_a = get_col_value(a, col_meta);
            Value val_b = get_col_value(b, col_meta);

            if (val_a != val_b)
            {
                return desc ? (val_a > val_b) : (val_a < val_b);
            }
            // 如果相等，继续比较下一列
        }
        return false; // 所有列都相等
    }

    Value get_col_value(const RmRecord &record, const ColMeta &col_meta)
    {
        Value value;
        const char *data = record.data + col_meta.offset;

        switch (col_meta.type)
        {
        case TYPE_INT:
            value.set_int(*reinterpret_cast<const int *>(data));
            break;
        case TYPE_FLOAT:
            value.set_float(*reinterpret_cast<const float *>(data));
            break;
        case TYPE_STRING:
            value.set_str(std::string(data, col_meta.len));
            break;
        default:
            throw RMDBError("Unsupported column type");
        }

        return value;
    }

    void sortExternallyWithInitialData()
    {
        // 先处理已经读入内存的数据
        if (sorted_tuples_.size())
        {
            sortAndWriteBlock(sorted_tuples_);
            sorted_tuples_.clear();
        }
        // 继续处理剩余数据
        std::vector<std::unique_ptr<RmRecord>> block;
        size_t current_block_size = 0;

        while (!prev_->is_end())
        {
            current_block_size += record_size;
            if (current_block_size > block_size_)
            {
                sortAndWriteBlock(block);
                block.clear();
                current_block_size = 0;
            }
            else
            {
                auto record = prev_->Next();
                block.emplace_back(std::move(record));
                prev_->nextTuple();
            }
        }
        if (block.size())
        {
            sortAndWriteBlock(block);
        }
        mergeSortedBlocks();
    }

    void sortAndWriteBlock(std::vector<std::unique_ptr<RmRecord>> &block)
    {
        std::sort(block.begin(), block.end(),
                  [this](const std::unique_ptr<RmRecord> &a, const std::unique_ptr<RmRecord> &b)
                  {
                      return compareRecords(*a, *b);
                  });

        std::string block_file = temp_dir_ + "/block_" + std::to_string(sorted_blocks_.size()) + ".dat";
        FILE *out = fopen(block_file.c_str(), "wb");
        if (!out)
        {
            throw RMDBError("Failed to open block file for writing: " + block_file);
        }
        for (const auto &record : block)
        {
            if (fwrite(record->data, record_size, 1, out) != 1)
            {
                fclose(out);
                throw RMDBError("Failed to write to block file: " + block_file);
            }
        }
        if (fclose(out) != 0)
        {
            throw RMDBError("Failed to close block file: " + block_file);
        }
        sorted_blocks_.emplace_back(block_file);
    }

    void mergeSortedBlocks()
    {
        // 打开所有块文件
        std::vector<FILE *> block_files;
        for (const auto &block_file : sorted_blocks_)
        {
            FILE *fp = fopen(block_file.c_str(), "rb");
            if (!fp)
            {
                // 关闭已打开的文件
                for (auto f : block_files)
                {
                    fclose(f);
                }
                throw RMDBError("Failed to open block file for reading: " + block_file);
            }
            block_files.emplace_back(fp);
        }

        // 定义比较器
        auto comp = [this](const std::pair<std::unique_ptr<RmRecord>, size_t> &a,
                           const std::pair<std::unique_ptr<RmRecord>, size_t> &b)
        {
            return !compareRecords(*a.first, *b.first);
        };

        // 使用自定义比较器初始化最小堆
        std::priority_queue<std::pair<std::unique_ptr<RmRecord>, size_t>,
                            std::vector<std::pair<std::unique_ptr<RmRecord>, size_t>>,
                            decltype(comp)>
            min_heap(comp);

        // 初始化堆
        for (size_t i = 0; i < block_files.size(); ++i)
        {
            auto record = readNextRecord(block_files[i]);
            if (record)
            {
                min_heap.emplace(std::move(record), i);
            }
        }

        // 归并排序
        sorted_tuples_.clear();
        while (min_heap.size() && (limit_ == -1 || sorted_tuples_.size() < static_cast<size_t>(limit_)))
        {
            auto top_pair = std::move(const_cast<std::pair<std::unique_ptr<RmRecord>, size_t> &>(min_heap.top()));
            min_heap.pop();
            sorted_tuples_.emplace_back(std::move(top_pair.first));

            auto next_record = readNextRecord(block_files[top_pair.second]);
            if (next_record)
            {
                min_heap.emplace(std::move(next_record), top_pair.second);
            }
        }

        // 关闭所有块文件
        for (auto &fp : block_files)
        {
            fclose(fp);
        }
    }

    std::unique_ptr<RmRecord> readNextRecord(FILE *fp)
    {
        auto record = std::make_unique<RmRecord>(record_size);
        size_t read = fread(record->data, record_size, 1, fp);
        if (read != 1)
        {
            if (feof(fp))
            {
                return nullptr;
            }
            else
            {
                throw RMDBError("Failed to read from block file");
            }
        }
        return record;
    }

public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols,
                 const std::vector<bool> &is_desc_orders, int limit, Context *context, size_t block_size = 8192)
        : prev_(std::move(prev)), is_desc_orders_(is_desc_orders), limit_(limit), output_count_(0), block_size_(block_size), context_(context)
    {
        txn_id_t txn_id = context_->txn_->get_transaction_id();
        temp_dir_ = "/tmp/rmdb_sort_" + std::to_string(txn_id);
        // 获取所有排序列的元数据
        for (const auto &col : sel_cols)
        {
            sort_cols_.emplace_back(*get_col(prev_->cols(), col, true));
        }
        record_size = prev_->tupleLen();
        if (mkdir(temp_dir_.c_str(), 0700) != 0 && errno != EEXIST)
        {
            throw RMDBError("Unable to create temporary sorting directory: " + temp_dir_);
        }
    }

    void beginTuple() override
    {
        output_count_ = 0;
        current_index_ = 0;
        if (sorted_tuples_.size())
            return;
        // 动态收集数据，根据实际数据量决定排序策略
        size_t current_size = 0;
        bool use_memory_sort = true;
        // 先尝试内存排序
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple())
        {
            current_size += record_size;
            // 如果超过内存限制，切换到外部排序
            if (current_size > block_size_)
            {
                use_memory_sort = false;
                break;
            }
            else
            {
                auto record = prev_->Next();
                sorted_tuples_.emplace_back(std::move(record));
            }
        }
        if (use_memory_sort)
        {
            // 如果全部数据都能放入内存，直接排序
            if (prev_->is_end())
            {
                std::sort(sorted_tuples_.begin(), sorted_tuples_.end(),
                          [this](const auto &a, const auto &b)
                          {
                              return compareRecords(*a, *b);
                          });
                if (limit_ != -1 && sorted_tuples_.size() > static_cast<size_t>(limit_))
                {
                    sorted_tuples_.resize(limit_);
                }
            }
            // 否则需要继续读取剩余数据并切换到外部排序
            else
            {
                sortExternallyWithInitialData();
            }
        }
        else
        {
            // 外部排序
            sortExternallyWithInitialData();
        }
    }

    void nextTuple() override
    {
        if (current_index_ < sorted_tuples_.size())
        {
            ++output_count_;
            ++current_index_;
        }
    }

    std::unique_ptr<RmRecord> Next() override
    {
        if (is_end())
        {
            return nullptr;
        }
        return std::make_unique<RmRecord>(*sorted_tuples_[current_index_]);
    }

    Rid &rid() override { return _abstract_rid; }

    bool is_end() const override
    {
        return sorted_tuples_.empty() ||
               current_index_ >= sorted_tuples_.size() ||
               (limit_ != -1 && output_count_ >= static_cast<size_t>(limit_));
    }

    const std::vector<ColMeta> &cols() const override { return prev_->cols(); }

    size_t tupleLen() const override { return record_size; }

    ~SortExecutor()
    {
        for (const auto &block_file : sorted_blocks_)
        {
            remove(block_file.c_str());
        }
        rmdir(temp_dir_.c_str());
    }

    ExecutionType type() const override
    {
        if (context_->hasAggFlag())
        {
            return ExecutionType::Agg_Sort;
        }
        return ExecutionType::Sort;
    }
};