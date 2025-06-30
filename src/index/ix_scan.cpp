/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "ix_scan.h"
std::unique_ptr<RmRecord> IxScan::temp = nullptr;
void IxScan::next() {
    ++pos_;
    if (pos_ >= node_.get_size()) {
        // 需要切换到下一个叶子节点
        next_batch();
    }
}

void IxScan::next_batch() {
    page_id_t next_leaf = node_.get_next_leaf();
    if (next_leaf == IX_LEAF_HEADER_PAGE || max_pos_ < node_.get_size()) {
        ih_->unlock_shared(node_);
        pos_ = max_pos_;
        return;
    }
    // 先获取新节点并加锁，再释放当前节点锁，避免锁空窗
    IxNodeHandle new_node = ih_->fetch_node(next_leaf);
    ih_->lock_shared(new_node);
    ih_->unlock_shared(node_);
    node_ = std::move(new_node);
    pos_ = 0;
    update_max_pos();
}

std::vector<Rid> IxScan::rid_batch() const {
    std::vector<Rid> batch;
    batch.resize(max_pos_ - pos_);
    memcpy(batch.data(), node_.get_rid(pos_), (max_pos_ - pos_) * sizeof(Rid));
    return batch;
}

// RecScan标准批量接口：IxScan不支持直接返回记录，抛异常或返回空
std::vector<std::unique_ptr<RmRecord>> IxScan::record_batch() {
    // std::vector<std::unique_ptr<RmRecord>> batch;
    // int cur_pos = pos_;
    // int end_pos = close_ ? node_.upper_bound(max_key_.c_str()) : node_.lower_bound(max_key_.c_str());
    // end_pos = std::min(end_pos, node_.get_size());
    // // 若有RmRecord构造接口可用，否则留空
    // for (int i = cur_pos; i < end_pos; ++i) {
    //     // 伪代码: batch.push_back(std::make_unique<RmRecord>(node_.get_key(i), node_.get_rid(i)));
    //     // 实际实现需根据RmRecord定义调整
    // }
    // return batch;
    // mvcc下待支持
    return {}; // IxScan不支持直接返回记录，返回空
}