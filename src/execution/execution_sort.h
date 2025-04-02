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
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class SortExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> prev_;
    ColMeta cols_;                              // 框架中只支持一个键排序，需要自行修改数据结构支持多个键排序
    bool is_desc_;
    std::string temp_dir_;                      // 临时文件目录
    size_t block_size_;                         // 每个块的大小（以字节为单位）
    std::vector<std::string> sorted_blocks_;    // 已排序的块文件路径
    std::unique_ptr<RmRecord> current_tuple;
    std::vector<std::unique_ptr<RmRecord>> sorted_tuples_; // 用于内部排序的缓冲区
    size_t current_index_;                      // 当前记录的索引
    size_t record_size;                         // 记录的长度
    std::ifstream current_block_stream_;        // 当前块的输入流
    std::unique_ptr<RmRecord> current_block_record_; // 当前块的当前记录
    

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

    void sortInMemory() {
        // 将所有记录读入内存
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            sorted_tuples_.emplace_back(prev_->Next());
        }
        if (sorted_tuples_.empty())
            return;

        // 对记录进行排序
        std::sort(sorted_tuples_.begin(), sorted_tuples_.end(), 
            [this](const std::unique_ptr<RmRecord> &a, const std::unique_ptr<RmRecord> &b) {
                Value val_a = get_col_value(*a, cols_);
                Value val_b = get_col_value(*b, cols_);
                if (is_desc_) {
                    return val_a >= val_b;
                } else {
                    return val_a < val_b;
                }
            });
    }

    void sortExternally() {
        // 分块排序
        std::vector<std::unique_ptr<RmRecord>> block;
        size_t current_block_size = 0;

        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            block.emplace_back(prev_->Next());
            current_block_size += prev_->Next()->size; 

            if (current_block_size >= block_size_) {
                sortAndWriteBlock(block);
                block.clear();
                current_block_size = 0;
            }
        }

        if (!block.empty()) {
            sortAndWriteBlock(block);
        }

        // 归并排序
        mergeSortedBlocks();
    }

    void sortAndWriteBlock(std::vector<std::unique_ptr<RmRecord>> &block) {
        std::sort(block.begin(), block.end(), 
            [this](const std::unique_ptr<RmRecord> &a, const std::unique_ptr<RmRecord> &b) {
                Value val_a = get_col_value(*a, cols_);
                Value val_b = get_col_value(*b, cols_);
                if (is_desc_) {
                    return val_a >= val_b;
                } else {
                    return val_a < val_b;
                }
            });

        std::string block_file = temp_dir_ + "/block_" + std::to_string(sorted_blocks_.size()) + ".dat";
        std::ofstream out(block_file, std::ios::binary);
        for (const auto &record : block) {
            out.write(record->data, record_size);
        }
        sorted_blocks_.emplace_back(block_file);
    }

    void mergeSortedBlocks() {
        // 打开所有块文件
        std::vector<std::ifstream> block_streams;
        for (const auto &block_file : sorted_blocks_) {
            block_streams.emplace_back(block_file, std::ios::binary);
        }

        // 定义比较器
        auto comp = [this](const std::pair<std::unique_ptr<RmRecord>, size_t> &a, 
                        const std::pair<std::unique_ptr<RmRecord>, size_t> &b) {
            Value val_a = get_col_value(*a.first, cols_);
            Value val_b = get_col_value(*b.first, cols_);
            if (is_desc_) {
                return val_a < val_b;
            } else {
                return val_a >= val_b;
            }
        };

        // 使用自定义比较器初始化最小堆
        std::priority_queue<std::pair<std::unique_ptr<RmRecord>, size_t>, 
                            std::vector<std::pair<std::unique_ptr<RmRecord>, size_t>>, 
                            decltype(comp)> min_heap(comp);

        // 初始化堆
        for (size_t i = 0; i < block_streams.size(); ++i) {
            auto record = readNextRecord(block_streams[i]);
            if (record) {
                min_heap.emplace(std::move(record), i); // 使用 emplace 直接构造 pair
            }
        }

        // 归并排序
        sorted_tuples_.clear();
        while (!min_heap.empty()) {
            auto top_pair = std::move(const_cast<std::pair<std::unique_ptr<RmRecord>, size_t>&>(min_heap.top()));
            min_heap.pop();
            sorted_tuples_.emplace_back(std::move(top_pair.first));

            auto next_record = readNextRecord(block_streams[top_pair.second]);
            if (next_record) {
                min_heap.emplace(std::move(next_record), top_pair.second);
            }
        }

        // 关闭所有块文件
        for (auto &stream : block_streams) {
            stream.close();
        }
    }

    std::unique_ptr<RmRecord> readNextRecord(std::ifstream &stream) {
        auto record = std::make_unique<RmRecord>(record_size);
        stream.read(record->data, record_size);
        if (stream.gcount() == 0) {
            return nullptr;
        }
        return record;
    }

public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, const TabCol &sel_cols, bool is_desc, 
                 const std::string &temp_dir = "/tmp", size_t block_size = 8192) 
        : prev_(std::move(prev)), is_desc_(is_desc), temp_dir_(temp_dir), block_size_(block_size) {
        cols_ = *get_col(prev_->cols(), sel_cols);
        record_size = prev_->tupleLen();
    }

    void beginTuple() override { 
        current_index_ = 0;
        if (!sorted_tuples_.empty())
            return;

        // 估算数据量
        size_t estimated_size = estimateDataSize();

        if (estimated_size <= block_size_) {
            sortInMemory();
        } else {
            sortExternally();
        }
    }

    void nextTuple() override {
        if (current_index_ < sorted_tuples_.size()) {
            ++current_index_;
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        return std::make_unique<RmRecord>(*sorted_tuples_[current_index_]);
    }

    Rid &rid() override { return _abstract_rid; }

    bool is_end() const override { return sorted_tuples_.empty() || current_index_ >= sorted_tuples_.size(); }
    
    const std::vector<ColMeta> &cols() const override { return prev_->cols(); }

    size_t tupleLen() const override { return record_size; }

    ~SortExecutor() {
        for (const auto &block_file : sorted_blocks_) {
            std::remove(block_file.c_str());
        }
    }

private:
    size_t estimateDataSize() {
        size_t size = 0;
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            auto record = prev_->Next();
            size += record->size;
        }
        return size;
    }
};