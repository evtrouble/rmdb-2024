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

#include "common/context.h"
#include "rm_defs.h"
#include <memory>
#include <vector>

class RmFileHandle_Final;

class RmScan_Final : public RecScan
{
private:
    std::shared_ptr<RmFileHandle_Final> file_handle_;
    Context *context_; // 事务上下文
    Rid rid_;          // 当前扫描位置
    int page_num;

    // 批量扫描相关
    std::vector<std::pair<std::unique_ptr<RmRecord>, int>> current_records_; // 当前页面的记录批次
    size_t current_record_idx_;                                              // 当前批次中的位置

public:
    RmScan_Final(std::shared_ptr<RmFileHandle_Final> file_handle, Context *context);

    void next() override;         // 移动到下一条记录
    void next_batch();            // 移动到下一批次记录(下一页)
    bool is_end() const override; // 判断是否到达文件末尾
    Rid rid() const override;     // 获取当前记录的RID

    // 单记录访问
    std::unique_ptr<RmRecord> &get_record() override
    {
        return current_records_[current_record_idx_].first;
    }

    inline void record(std::unique_ptr<RmRecord> &out) override
    {
        out = std::move(current_records_[current_record_idx_].first);
    }

    // 批量访问接口
    std::vector<Rid> rid_batch() const override;                    // 获取当前页所有记录的RID
    std::vector<std::unique_ptr<RmRecord>> record_batch() override; // 获取当前页所有记录

private:
    void load_next_page(); // 加载下一页数据
};
