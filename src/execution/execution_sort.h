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

class SortExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<ColMeta> sort_cols_;           // 排序列元数据
    std::vector<bool> is_desc_orders_;         // 每列的排序方向
    int limit_;                                // LIMIT 限制
    std::string temp_dir_;                     // 临时文件目录
    size_t block_size_;                        // 每个块的大小(字节)
    Context* context_;
    
    // 批处理相关状态
    std::vector<std::string> sorted_blocks_;   // 已排序的块文件路径
    std::vector<std::unique_ptr<RmRecord>> sorted_tuples_; // 内存中的排序结果
    size_t current_index_ = 0;                 // 当前批次中的索引
    size_t output_count_ = 0;                  // 已输出的记录数
    size_t record_size_;                       // 记录长度
    
    // 比较两个记录
    bool compareRecords(const RmRecord &a, const RmRecord &b) {
        for (size_t i = 0; i < sort_cols_.size(); ++i) {
            const auto &col_meta = sort_cols_[i];
            bool desc = is_desc_orders_[i];
            Value val_a = get_col_value(a, col_meta);
            Value val_b = get_col_value(b, col_meta);
            
            if (val_a != val_b) {
                return desc ? (val_a > val_b) : (val_a < val_b);
            }
        }
        return false;
    }

    // 获取列值
    Value get_col_value(const RmRecord &record, const ColMeta &col_meta) {
        Value value;
        const char *data = record.data + col_meta.offset;

        switch (col_meta.type) {
            case TYPE_INT:
                value.set_int(*reinterpret_cast<const int*>(data));
                break;
            case TYPE_FLOAT:
                value.set_float(*reinterpret_cast<const float*>(data));
                break;
            case TYPE_STRING:
                value.set_str(std::string(data, col_meta.len));
                break;
            default:
                throw RMDBError("Unsupported column type");
        }
        return value;
    }

    // 执行外部排序
    void performExternalSort() {
        // 生成排序后的块
        generateSortedBlocks();
        
        // 多路归并
        mergeSortedBlocks();
    }

    // 生成排序后的块文件
    void generateSortedBlocks() {
        std::vector<std::unique_ptr<RmRecord>> block;
        size_t current_block_size = 0;

        // 读取输入数据并分块排序
        prev_->beginTuple();
        while (true) {
            auto batch = prev_->next_batch();
            if (batch.empty()) break;
            for (auto& record : batch) {
                current_block_size += record_size_;
                block.emplace_back(std::move(record));
                
                if (current_block_size >= block_size_) {
                    sortAndWriteBlock(block);
                    block.clear();
                    current_block_size = 0;
                }
            }
        }
        
        // 处理最后一个块
        if (!block.empty()) {
            sortAndWriteBlock(block);
        }
    }

    // 排序内存中的块并写入文件
    void sortAndWriteBlock(std::vector<std::unique_ptr<RmRecord>> &block) {
        std::sort(block.begin(), block.end(), 
            [this](const auto &a, const auto &b) {
                return compareRecords(*a, *b);
            });

        std::string block_file = temp_dir_ + "/block_" + std::to_string(sorted_blocks_.size()) + ".dat";
        std::ofstream out(block_file, std::ios::binary);
        if (!out) {
            throw RMDBError("Failed to open block file for writing: " + block_file);
        }
        
        for (const auto &record : block) {
            out.write(record->data, record_size_);
            if (!out) {
                throw RMDBError("Failed to write to block file: " + block_file);
            }
        }
        
        sorted_blocks_.emplace_back(block_file);
    }

    // 多路归并排序
    void mergeSortedBlocks() {
        if (sorted_blocks_.empty()) return;

        // 打开所有块文件
        std::vector<std::ifstream> block_streams;
        for (const auto &block_file : sorted_blocks_) {
            block_streams.emplace_back(block_file, std::ios::binary);
            if (!block_streams.back()) {
                throw RMDBError("Failed to open block file for reading: " + block_file);
            }
        }

        // 定义比较器
        auto comp = [this](const std::pair<std::unique_ptr<RmRecord>, size_t> &a, 
                         const std::pair<std::unique_ptr<RmRecord>, size_t> &b) {
            return !compareRecords(*a.first, *b.first);
        };

        // 初始化最小堆
        std::priority_queue<std::pair<std::unique_ptr<RmRecord>, size_t>, 
                          std::vector<std::pair<std::unique_ptr<RmRecord>, size_t>>, 
                          decltype(comp)> min_heap(comp);

        // 从每个块读取第一条记录
        for (size_t i = 0; i < block_streams.size(); ++i) {
            auto record = readNextRecord(block_streams[i]);
            if (record) {
                min_heap.emplace(std::move(record), i);
            }
        }

        // 执行归并排序
        sorted_tuples_.clear();
        while (!min_heap.empty() && (limit_ == -1 || sorted_tuples_.size() < static_cast<size_t>(limit_))) {
            auto top = std::move(const_cast<std::pair<std::unique_ptr<RmRecord>, size_t>&>(min_heap.top()));
            min_heap.pop();
            
            sorted_tuples_.emplace_back(std::move(top.first));
            
            // 从同一块中读取下一条记录
            auto next_record = readNextRecord(block_streams[top.second]);
            if (next_record) {
                min_heap.emplace(std::move(next_record), top.second);
            }
        }
    }

    // 从流中读取下一条记录
    std::unique_ptr<RmRecord> readNextRecord(std::ifstream &in) {
        auto record = std::make_unique<RmRecord>(record_size_);
        in.read(record->data, record_size_);
        if (in.gcount() != static_cast<std::streamsize>(record_size_)) {
            if (in.eof()) return nullptr;
            throw RMDBError("Failed to read from block file");
        }
        return record;
    }

public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols, 
               const std::vector<bool> &is_desc_orders, int limit, Context* context, 
               size_t block_size = 8192) 
        : prev_(std::move(prev)), is_desc_orders_(is_desc_orders), limit_(limit), 
          block_size_(block_size), context_(context) {
        // 初始化临时目录
        txn_id_t txn_id = context_->txn_->get_transaction_id();
        temp_dir_ = "/tmp/rmdb_sort_" + std::to_string(txn_id);
        
        // 获取排序列元数据
        for (const auto &col : sel_cols) {
            sort_cols_.emplace_back(*get_col(prev_->cols(), col, true));
        }
        
        record_size_ = prev_->tupleLen();
        
        // 创建临时目录
        if (mkdir(temp_dir_.c_str(), 0700) != 0 && errno != EEXIST) {
            throw RMDBError("Unable to create temporary sorting directory: " + temp_dir_);
        }
    }

    void beginTuple() override {
        current_index_ = 0;
        output_count_ = 0;
        sorted_tuples_.clear();
        sorted_blocks_.clear();
        
        // 决定使用内存排序还是外部排序
        performExternalSort();
    }

    std::vector<std::unique_ptr<RmRecord>> next_batch(size_t batch_size = BATCH_SIZE) override {
        std::vector<std::unique_ptr<RmRecord>> batch;
        
        if (current_index_ >= sorted_tuples_.size()) {
            return batch;
        }
        
        // 考虑LIMIT限制
        size_t remaining = limit_ == -1 ? sorted_tuples_.size() - current_index_ : 
                         std::min(sorted_tuples_.size() - current_index_, 
                                 static_cast<size_t>(limit_) - output_count_);
        
        size_t take = std::min(remaining, batch_size);
        
        for (size_t i = 0; i < take; ++i) {
            batch.emplace_back(std::make_unique<RmRecord>(*sorted_tuples_[current_index_]));
            current_index_++;
            output_count_++;
        }
        
        return batch;
    }

    size_t tupleLen() const override { return record_size_; }

    const std::vector<ColMeta> &cols() const override { return prev_->cols(); }

    ~SortExecutor() {
        // 清理临时文件
        for (const auto &block_file : sorted_blocks_) {
            remove(block_file.c_str());
        }
        rmdir(temp_dir_.c_str());
    }

    ExecutionType type() const override { 
        return context_->hasAggFlag() ? ExecutionType::Agg_Sort : ExecutionType::Sort;
    }
};