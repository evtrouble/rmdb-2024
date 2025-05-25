/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */
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

#include "transaction/transaction.h"
#include "transaction/concurrency/lock_manager.h"
#include "recovery/log_manager.h"

// class TransactionManager;

// used for data_send
static int const_offset = -1;

// 封装标志位的结构体
struct QueryFlags {
    bool joinFlag = false;
    bool orderbyFlag = false;
};

class Context {
public:
    Context(LockManager* lock_mgr, LogManager* log_mgr,
        Transaction* txn, char* data_send = nullptr, int* offset = &const_offset)
        : lock_mgr_(lock_mgr), log_mgr_(log_mgr), txn_(txn),
        data_send_(data_send), offset_(offset) {
        ellipsis_ = false;
        // 初始化 QueryFlags
        queryFlags_ = QueryFlags();
    }

    // 设置 join 标志位的接口
    inline void setJoinFlag(bool value) {
        queryFlags_.joinFlag = value;
    }

    // 设置 orderby 标志位的接口
    inline void setOrderbyFlag(bool value) {
        queryFlags_.orderbyFlag = value;
    }

    // 判断 join 标志位的接口
    inline bool hasJoinFlag() const {
        return queryFlags_.joinFlag;
    }

    // 判断 orderby 标志位的接口
    inline bool hasOrderbyFlag() const {
        return queryFlags_.orderbyFlag;
    }

    void clearFlags() {
        queryFlags_.joinFlag = false;
        queryFlags_.orderbyFlag = false;
    }

    // TransactionManager *txn_mgr_;
    LockManager* lock_mgr_;
    LogManager* log_mgr_;
    Transaction* txn_;
    char* data_send_;
    int* offset_;
    bool ellipsis_;
    QueryFlags queryFlags_;  // 新增的标志位结构体成员
};