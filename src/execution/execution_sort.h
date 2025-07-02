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
    std::unique_ptr<AbstractExecutor> prev_;       // 前驱执行器
    std::vector<ColMeta> sort_cols_;               // 排序列元数据
    std::vector<bool> is_desc_orders_;             // 是否降序排列
    int limit_;                                    // 返回记录数限制
    std::string temp_dir_;                         // 临时文件目录
    size_t block_size_;                             // 块大小(字节)
    std::vector<std::string> sorted_blocks_;        // 已排序的块文件
    std::vector<std::unique_ptr<RmRecord>> sorted_tuples_; // 内存中的排序结果
    size_t current_index_;                         // 当前记录索引
    size_t record_size;                            // 记录大小
    size_t batch_index_;                           // 当前批次索引
    std::vector<std::unique_ptr<RmRecord>> current_batch_; // 当前批次数据

    // 比较两条记录的大小
    bool compareRecords(const RmRecord &a, const RmRecord &b) {
        for (size_t i = 0; i < sort_cols_.size(); ++i) {
            const auto &col_meta = sort_cols_[i];
            bool desc = is_desc_orders_[i];
            Value val_a = get_col_value(a, col_meta);
            Value val_b = get_col_value(b, col_meta);

            if (val_a != val_b)
            {
                return desc ? (val_a > val_b) : (val_a < val_b);
            }
        }
        return false;
    }

    // 获取列值
    Value get_col_value(const RmRecord &record, const ColMeta &col_meta) {
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
        case TYPE_DATETIME:
            value.set_str(std::string(data, col_meta.len));
            break;
        default:
            throw RMDBError("Unsupported column type");
        }
        return value;
    }

    // 外部排序：初始数据处理
    void sortExternallyWithInitialData()
    {
        // 处理已读入内存的数据
        if (!sorted_tuples_.empty()) {
            sortAndWriteBlock(sorted_tuples_);
            sorted_tuples_.clear();
        }

        // 处理剩余数据
        std::vector<std::unique_ptr<RmRecord>> block;
        size_t current_block_size = 0;

        auto batch = prev_->next_batch();
        while (!batch.empty()) {
            for (auto &record : batch) {
                current_block_size += record_size;
                if (current_block_size > block_size_) {
                    sortAndWriteBlock(block);
                    block.clear();
                    current_block_size = 0;
                }
                block.emplace_back(std::move(record));
            }
            batch = prev_->next_batch();
        }

        if (!block.empty()) {
            sortAndWriteBlock(block);
        }

        mergeSortedBlocks();
    }

    // 排序并写入块文件
    void sortAndWriteBlock(std::vector<std::unique_ptr<RmRecord>> &block) {
        std::sort(block.begin(), block.end(),
            [this](const auto &a, const auto &b) {
                return compareRecords(*a, *b);
            });

        std::string block_file = temp_dir_ + "/block_" + std::to_string(sorted_blocks_.size()) + ".dat";
        std::ofstream out(block_file, std::ios::binary);
        if (!out) {
            throw RMDBError("无法打开块文件写入: " + block_file);
        }

        for (const auto &record : block) {
            out.write(record->data, record_size);
            if (!out) {
                throw RMDBError("写入块文件失败: " + block_file);
            }
        }

        sorted_blocks_.push_back(block_file);
    }

    // 合并已排序的块
    void mergeSortedBlocks() {
        std::vector<std::ifstream> block_streams;
        for (const auto &block_file : sorted_blocks_) {
            block_streams.emplace_back(block_file, std::ios::binary);
            if (!block_streams.back()) {
                throw RMDBError("无法打开块文件读取: " + block_file);
            }
        }

        auto comp = [this](const auto &a, const auto &b) {
            return !compareRecords(*a.first, *b.first);
        };

        std::priority_queue<
            std::pair<std::unique_ptr<RmRecord>, size_t>,
            std::vector<std::pair<std::unique_ptr<RmRecord>, size_t>>,
            decltype(comp)> min_heap(comp);

        // 初始化堆
        for (size_t i = 0; i < block_streams.size(); ++i) {
            auto record = readNextRecord(block_streams[i]);
            if (record) {
                min_heap.emplace(std::move(record), i);
            }
        }

        // 归并排序
        sorted_tuples_.clear();
        while (!min_heap.empty() && (limit_ == -1 || sorted_tuples_.size() < static_cast<size_t>(limit_))) {
            auto top = std::move(const_cast<std::pair<std::unique_ptr<RmRecord>, size_t>&>(min_heap.top()));
            min_heap.pop();
            sorted_tuples_.emplace_back(std::move(top.first));

            auto next_record = readNextRecord(block_streams[top.second]);
            if (next_record) {
                min_heap.emplace(std::move(next_record), top.second);
            }
        }
    }

    // 从文件流读取下一条记录
    std::unique_ptr<RmRecord> readNextRecord(std::ifstream &in) {
        auto record = std::make_unique<RmRecord>(record_size);
        in.read(record->data, record_size);
        if (in.gcount() != static_cast<std::streamsize>(record_size)) {
            if (in.eof()) return nullptr;
            throw RMDBError("读取块文件失败");
        }
        return record;
    }

public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, 
                const std::vector<TabCol> &sel_cols,
                const std::vector<bool> &is_desc_orders, 
                int limit, Context *context, 
                size_t block_size = 8192)
        : AbstractExecutor(context), 
          prev_(std::move(prev)), 
          is_desc_orders_(is_desc_orders), 
          limit_(limit), 
          block_size_(block_size),
          current_index_(0),
          batch_index_(0) {
        
        // 初始化临时目录
        txn_id_t txn_id = context_->txn_->get_transaction_id();
        temp_dir_ = "/tmp/rmdb_sort_" + std::to_string(txn_id);
        
        // 获取排序列元数据
        for (const auto &col : sel_cols) {
            sort_cols_.emplace_back(*get_col(prev_->cols(), col, true));
        }
        
        record_size = prev_->tupleLen();
        
        // 创建临时目录
        if (mkdir(temp_dir_.c_str(), 0700) != 0 && errno != EEXIST) {
            throw RMDBError("无法创建临时排序目录: " + temp_dir_);
        }
    }

    // 获取下一批记录
    std::vector<std::unique_ptr<RmRecord>> next_batch(size_t batch_size = BATCH_SIZE) override {
        if (sorted_tuples_.empty()) {
            begin_batch();
        }

        std::vector<std::unique_ptr<RmRecord>> batch;
        size_t count = 0;
        
        while (count < batch_size && current_index_ < sorted_tuples_.size() && 
               (limit_ == -1 || current_index_ < static_cast<size_t>(limit_))) {
            batch.emplace_back(std::make_unique<RmRecord>(*sorted_tuples_[current_index_]));
            current_index_++;
            count++;
        }
        
        return batch;
    }

    // 开始批处理
    void begin_batch() {
        current_index_ = 0;
        batch_index_ = 0;
        
        if (!sorted_tuples_.empty()) return;

        // 动态选择排序策略
        size_t current_size = 0;
        bool use_memory_sort = true;
        
        // 尝试内存排序
        auto batch = prev_->next_batch();
        while (!batch.empty()) {
            for (auto &record : batch) {
                current_size += record_size;
                if (current_size > block_size_) {
                    use_memory_sort = false;
                    break;
                }
                sorted_tuples_.emplace_back(std::move(record));
            }
            
            if (!use_memory_sort) break;
            batch = prev_->next_batch();
        }

        if (use_memory_sort) {
            // 内存排序
            std::sort(sorted_tuples_.begin(), sorted_tuples_.end(),
                [this](const auto &a, const auto &b) {
                    return compareRecords(*a, *b);
                });
            
            if (limit_ != -1 && sorted_tuples_.size() > static_cast<size_t>(limit_)) {
                sorted_tuples_.resize(limit_);
            }
        } else {
            // 外部排序
            sortExternallyWithInitialData();
        }
    }

    size_t tupleLen() const override { return record_size; }

    const std::vector<ColMeta> &cols() const override { return prev_->cols(); }

    ExecutionType type() const override {
        if (context_->hasAggFlag()) {
            return ExecutionType::Agg_Sort;
        }
        return ExecutionType::Sort;
    }

    ~SortExecutor() {
        // 清理临时文件
        for (const auto &block_file : sorted_blocks_) {
            remove(block_file.c_str());
        }
        rmdir(temp_dir_.c_str());
    }
};