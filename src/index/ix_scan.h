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

#include "ix_defs.h"
#include "ix_index_handle.h"

// class IxIndexHandle;

// 用于遍历叶子结点
// 用于直接遍历叶子结点，而不用findleafpage来得到叶子结点
// TODO：对page遍历时，要加上读锁
class IxScan : public RecScan
{
    static std::unique_ptr<RmRecord> temp;
    std::shared_ptr<IxIndexHandle> ih_;
    IxNodeHandle node_;
    int pos_;
    int max_pos_; // 当前节点最大可访问位置
    std::string max_key_;
    bool close_;
    BufferPoolManager *bpm_;

public:
    // node已加读锁，max_key生命周期由调用者保证
    IxScan(const std::shared_ptr<IxIndexHandle>& ih, IxNodeHandle node, int start_pos, const std::string& max_key, bool close, BufferPoolManager *bpm)
        : ih_(ih), node_(std::move(node)), pos_(start_pos), max_key_(std::move(max_key)), 
            close_(close), bpm_(bpm) {
        update_max_pos();
        if(is_end()) {
            ih_->unlock_shared(node_);
        }
    }

    void next() override;

    // 批量推进游标，返回实际推进条数
    void next_batch() override;

    std::vector<Rid> rid_batch() const override;
    // RecScan标准批量接口：IxScan不支持直接返回记录，抛异常或返回空
    std::vector<std::unique_ptr<RmRecord>> record_batch() override;

    void record(std::unique_ptr<RmRecord> &out) override {}

    bool is_end() const override { return pos_ >= max_pos_; }

    Rid rid() const override {
        return *node_.get_rid(pos_);
    }

    // 单记录访问
    std::unique_ptr<RmRecord> &get_record() override {
        return temp;
    }

    const IxNodeHandle& node() const { return node_; }
    int pos() const { return pos_; }

private:
    inline void update_max_pos() {
        max_pos_ = close_ ? node_.upper_bound_adjust(max_key_.c_str()) : node_.lower_bound(max_key_.c_str());
        // max_pos_ = std::min(max_pos_, node_.get_size());
    }
};