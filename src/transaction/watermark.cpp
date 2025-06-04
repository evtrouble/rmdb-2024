/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction/watermark.h"

void Watermark::AddTxn(timestamp_t read_ts) {
    std::lock_guard lock(mutex_);
    current_reads_[read_ts]++;

    // 如果新的read_ts小于当前watermark,更新watermark
    if (read_ts < watermark_.load()) {
        watermark_.store(read_ts);
    }
}

void Watermark::RemoveTxn(timestamp_t read_ts) {
    std::lock_guard lock(mutex_);
    auto it = current_reads_.find(read_ts);
    if (it == current_reads_.end()) {
        return;
    }

    if (--(it->second) == 0) {
        current_reads_.erase(it);

        // 只有在删除的是最小时间戳时才需要更新watermark
        if (read_ts == watermark_.load()) {    
            timestamp_t new_watermark = current_reads_.empty() ? 
                commit_ts_.load() : current_reads_.begin()->first;
            watermark_.store(new_watermark);
        }
    }
}

void Watermark::UpdateCommitTs(timestamp_t commit_ts) {
    timestamp_t cur_commit_ts = commit_ts_.load();
    
    // 确保commit_ts单调递增
    while (commit_ts > cur_commit_ts) {
        if (commit_ts_.compare_exchange_strong(cur_commit_ts, commit_ts)) {
            break;  // CAS成功,更新完成
        }
        // CAS失败说明其他线程修改了commit_ts,重新读取并尝试
        cur_commit_ts = commit_ts_.load();
    }

    // 只有在没有活跃读事务时才更新watermark
    std::lock_guard lock(mutex_);
    if (current_reads_.empty()) {
        watermark_.store(commit_ts);
    }
}