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
#include "common/context.h"

class RmFileHandle;

class RmScan {
private:
    std::shared_ptr<RmFileHandle> file_handle_;
    Rid rid_;
    Context* context_;  // 添加事务上下文

public:
    RmScan(std::shared_ptr<RmFileHandle> file_handle, Context* context);
    void next();
    void next_batch(); // 跳过当前页，移动到下一页
    bool is_end() const;
    Rid rid() const;
};
