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

#include "rm_defs.h"

class RmFileHandle;

class RmScan : public RecScan
{
    static std::unique_ptr<RmRecord> temp;
    const RmFileHandle *file_handle_;
    Rid rid_;

public:
    RmScan(const RmFileHandle *file_handle);

    void next() override;

    bool is_end() const override;

    Rid rid() const override;
    // 单记录访问
    void record(std::unique_ptr<RmRecord> &out) {}
    std::unique_ptr<RmRecord> &get_record() { return temp; }

    void next_batch() {}

    // 批量访问接口
    std::vector<Rid> rid_batch() const { return {}; }
    std::vector<std::unique_ptr<RmRecord>> record_batch() {
        return {};
    }
};
