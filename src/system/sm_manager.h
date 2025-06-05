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

#include "index/ix.h"
#include "record/rm_file_handle.h"
#include "sm_defs.h"
#include "sm_meta.h"
#include "common/context.h"

class Context;

struct ColDef
{
    std::string name; // Column name
    ColType type;     // Type of column
    int len;          // Length of column
};

/* 系统管理器，负责元数据管理和DDL语句的执行 */
class SmManager
{
public:
    DbMeta db_;                                                           // 当前打开的数据库的元数据
    std::unordered_map<std::string, std::shared_ptr<RmFileHandle>> fhs_;  // file name -> record file handle, 当前数据库中每张表的数据文件
    std::unordered_map<std::string, std::shared_ptr<IxIndexHandle>> ihs_; // file name -> index file handle, 当前数据库中每个索引的文件
private:
    DiskManager *disk_manager_;
    BufferPoolManager *buffer_pool_manager_;
    RmManager *rm_manager_;
    IxManager *ix_manager_;
    std::shared_mutex fhs_latch_; // 保护fhs_的读写锁，保证对文件句柄的并发访问安全
    std::shared_mutex ihs_latch_; // 保护ihs_的读写锁，保证对索引句柄的并发访问安全

public:
    SmManager(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, RmManager *rm_manager,
              IxManager *ix_manager)
        : disk_manager_(disk_manager),
          buffer_pool_manager_(buffer_pool_manager),
          rm_manager_(rm_manager),
          ix_manager_(ix_manager) {}

    ~SmManager() {}

    BufferPoolManager *get_bpm() { return buffer_pool_manager_; }

    RmManager *get_rm_manager() { return rm_manager_; }

    IxManager *get_ix_manager() { return ix_manager_; }

    inline std::shared_ptr<IxIndexHandle> get_index_handle(const std::string &index_name) {
        std::shared_lock lock(ihs_latch_);
        auto it = ihs_.find(index_name);
        if (it != ihs_.end()) {
            return it->second;
        } 
        return nullptr; // 如果没有找到对应的索引句柄，返回nullptr
    }

    inline std::shared_ptr<RmFileHandle> get_table_handle(const std::string &table_name) {
        std::shared_lock lock(fhs_latch_);
        auto it = fhs_.find(table_name);
        if (it != fhs_.end()) {
            return it->second;
        }
        return nullptr; // 如果没有找到对应的表文件句柄，返回nullptr
    }

    bool is_dir(const std::string &db_name);

    void create_db(const std::string &db_name);

    void drop_db(const std::string &db_name);

    void open_db(const std::string &db_name);

    void close_db();

    void flush_meta();

    void show_tables(Context *context);

    void desc_table(const std::string &tab_name, Context *context);

    void create_table(const std::string &tab_name, const std::vector<ColDef> &col_defs, Context *context);

    void drop_table(const std::string &tab_name, Context *context);

    void create_index(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context);

    void drop_index(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context);

    void drop_index(const std::string &tab_name, const std::vector<ColMeta> &col_names, Context *context);
    void show_index(const std::string &tab_name, Context *context);

    std::vector<std::shared_ptr<RmFileHandle>> get_all_table_handle() {
        std::shared_lock lock(fhs_latch_);
        std::vector<std::shared_ptr<RmFileHandle>> handles;
        handles.reserve(fhs_.size());
        for (const auto &pair : fhs_) {
            handles.push_back(pair.second);
        }
        return handles;
    }
};