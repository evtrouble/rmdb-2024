/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_scan.h"
#include "rm_file_handle.h"
#include "storage/buffer_pool_manager.h"
#include "transaction/transaction_manager.h"

RmScan::RmScan(std::shared_ptr<RmFileHandle> file_handle, Context* context) :
    file_handle_(file_handle), 
    context_(context),
    rid_{RM_FILE_HDR_PAGE, -1},  // 初始化为第0页,slot_no为-1表示即将开始扫描
    current_record_idx_(0)
{
    // 预分配空间，避免后续resize
    current_records_.reserve(file_handle_->file_hdr_.num_records_per_page);
    load_next_page();  // 加载第一页数据
    rid_.slot_no = current_records_[current_record_idx_].second;
}

void RmScan::next() {
    if(current_record_idx_ < current_records_.size() - 1) {
        // 直接移动到下一条记录
        current_record_idx_++;
        rid_.slot_no = current_records_[current_record_idx_].second;
    } else {
        // 当前页处理完，读取下一页
        load_next_page();
        // 如果新页有记录，设置为第一条
        if(!current_records_.empty()) {
            current_record_idx_ = 0;
            rid_.slot_no = current_records_[0].second;
        }
    }
}

void RmScan::next_batch() {
    load_next_page();
}

void RmScan::load_next_page() {
    // 移动到下一页
    rid_.page_no++;
    current_record_idx_ = 0;
    
    // 检查是否到达文件末尾
    if(rid_.page_no >= file_handle_->file_hdr_.num_pages) {
        current_records_.clear();
        return;
    }
    
    // 直接使用移动赋值，避免拷贝
    current_records_ = file_handle_->get_records(rid_.page_no, context_);
    
    if(current_records_.empty()) {
        // 当前页没有记录，继续下一页
        load_next_page();
    }
}

bool RmScan::is_end() const {
    return rid_.page_no >= file_handle_->file_hdr_.num_pages ||
           current_records_.empty();
}

Rid RmScan::rid() const {
    return rid_;
}

std::vector<Rid> RmScan::rid_batch() const {
    std::vector<Rid> rids;
    rids.reserve(current_records_.size());
    
    for(size_t i = 0; i < current_records_.size(); i++) {
        // 返回所有记录的RID，包括需要在版本链上查找的记录
        rids.emplace_back(Rid{rid_.page_no, current_records_[i].second});
    }
    
    return rids;
}

std::vector<std::unique_ptr<RmRecord>> RmScan::record_batch() const {
    std::vector<std::unique_ptr<RmRecord>> records;
    records.reserve(current_records_.size());
    
    for(const auto& record_pair : current_records_) {
        if(record_pair.first != nullptr) {
            // 当前记录可见，直接移动所有权
            auto record = std::make_unique<RmRecord>(record_pair.first->size);
            memcpy(record->data, record_pair.first->data, record_pair.first->size);
            records.push_back(std::move(record));
        } else {
            // 尝试从版本链获取记录
            if(context_ && context_->txn_) {
                Rid current_rid{rid_.page_no, record_pair.second};
                auto visible_version = context_->txn_->get_txn_manager()->GetVisibleRecord(
                    file_handle_->GetFd(), current_rid, context_->txn_);
                if(visible_version) {
                    auto record = std::make_unique<RmRecord>(std::move(*visible_version));
                    records.push_back(std::move(record));
                }
            }
        }
    }
    
    return records;
}
