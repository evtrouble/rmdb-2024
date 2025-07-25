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

#include <map>
#include <unordered_map>
#include "log_manager.h"
#include "storage/disk_manager_final.h"
#include "system/sm_manager.h"

class RedoLogsInPage
{
public:
    RedoLogsInPage() { table_file_ = nullptr; }
    RmFileHandle_Final *table_file_;
    std::vector<lsn_t> redo_logs_; // 在该page上需要redo的操作的lsn
};

class RecoveryManager
{
public:
    RecoveryManager(DiskManager_Final *disk_manager, BufferPoolManager_Final *buffer_pool_manager,
                    SmManager *sm_manager, TransactionManager *txn_manager)
        : disk_manager_(disk_manager),
          buffer_pool_manager_(buffer_pool_manager),
          sm_manager_(sm_manager),
          txn_manager_(txn_manager) {}

    void analyze();
    void redo();
    void undo();

private:
    LogRecord *read_log(long long offset);

private:
    LogBuffer buffer_;                             // 读入日志
    DiskManager_Final *disk_manager_;              // 用来读写文件
    BufferPoolManager_Final *buffer_pool_manager_; // 对页面进行读写
    SmManager *sm_manager_;                        // 访问数据库元数据
    // std::unordered_map<txn_id_t, lsn_t> undo_list_;                 // 崩溃前未完成事务列表
    std::unordered_map<txn_id_t, std::unique_ptr<Transaction>> temp_txns_; // 临时事务列表
    TransactionManager *txn_manager_ = nullptr;                            // 事务管理器
};