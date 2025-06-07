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

#include <memory>
#include <string>

#include "system/sm_meta.h"
#include "ix_defs.h"

class IxIndexHandle;

class IxManager
{
    friend class IxIndexHandle;
private:
    DiskManager *disk_manager_;
    BufferPoolManager *buffer_pool_manager_;

public:
    IxManager(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager)
        : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager) {}

    std::string get_index_name(const std::string &filename, const std::vector<std::string> &index_cols)
    {
        std::string index_name = filename;
        for (size_t i = 0; i < index_cols.size(); ++i)
            index_name += "_" + index_cols[i];
        index_name += ".idx";

        return index_name;
    }

    std::string get_index_name(const std::string &filename, const std::vector<ColMeta> &index_cols)
    {
        std::string index_name = filename;
        for (size_t i = 0; i < index_cols.size(); ++i)
            index_name += "_" + index_cols[i].name;
        index_name += ".idx";

        return index_name;
    }

    bool exists(const std::string &filename, const std::vector<ColMeta> &index_cols)
    {
        auto ix_name = get_index_name(filename, index_cols);
        return disk_manager_->is_file(ix_name);
    }

    bool exists(const std::string &filename, const std::vector<std::string> &index_cols)
    {
        auto ix_name = get_index_name(filename, index_cols);
        return disk_manager_->is_file(ix_name);
    }

    void create_index(const std::string &filename, const std::vector<ColMeta> &index_cols);

    void destroy_index(const std::string &filename, const std::vector<ColMeta> &index_cols);

    void destroy_index(const std::string &filename, const std::vector<std::string> &index_cols);

    // 注意这里打开文件，创建并返回了index file handle的指针
    std::shared_ptr<IxIndexHandle> open_index(const std::string &filename, const std::vector<ColMeta> &index_cols);

    std::shared_ptr<IxIndexHandle> open_index(const std::string &filename, const std::vector<std::string> &index_cols);

    void close_index(const IxIndexHandle *ih, bool flush = true);
};