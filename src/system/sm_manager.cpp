/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string &db_name)
{
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string &db_name)
{
    // 检查数据库是否已存在
    if (disk_manager_->is_dir(db_name))
    {
        throw DatabaseExistsError(db_name);
    }

    // 创建数据库目录
    disk_manager_->create_dir(db_name);

    // 创建数据库元数据
    DbMeta *new_db = new DbMeta();
    new_db->name_ = db_name;

    // 创建并写入元数据文件
    std::string meta_path = db_name + "/" + DB_META_NAME;
    std::ofstream ofs(meta_path);
    ofs << *new_db;
    delete new_db;

    // 创建日志文件
    std::string log_path = db_name + "/" + LOG_FILE_NAME;
    disk_manager_->create_file(log_path);
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string &db_name)
{
    if (!disk_manager_->is_dir(db_name))
    {
        throw DatabaseNotFoundError(db_name);
    }
    disk_manager_->destroy_dir(db_name);
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string &db_name)
{
    if (!disk_manager_->is_dir(db_name))
    {
        throw DatabaseNotFoundError(db_name);
    }

    // 加载数据库元数据
    std::string meta_path = db_name + "/" + DB_META_NAME;
    std::ifstream ifs(meta_path);
    if (!ifs)
    {
        throw UnixError();
    }
    ifs >> db_;

    // 重新打开所有表文件
    for (const auto &table_entry : db_.tabs_)
    {
        const std::string &tab_name = table_entry.first;
        std::string table_path = db_name + "/" + tab_name;
        fhs_.emplace(tab_name, rm_manager_->open_file(table_path));
    }

    // 打开日志文件
    std::string log_path = db_name + "/" + LOG_FILE_NAME;
    disk_manager_->open_file(log_path);
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta()
{
    std::string meta_path = db_.name_ + "/" + DB_META_NAME;
    std::ofstream ofs(meta_path);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db()
{
    // 将元数据写入磁盘
    flush_meta();

    // 关闭所有表对应的文件句柄
    for (auto &file_handle : fhs_)
    {
        rm_manager_->close_file(file_handle.second.get());
    }
    fhs_.clear();

    // 关闭日志文件
    disk_manager_->close_file(disk_manager_->get_file_fd(LOG_FILE_NAME));
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context
 */
void SmManager::show_tables(Context *context)
{
    std::string output_path = db_.name_ + "/output.txt";
    std::fstream outfile;
    outfile.open(output_path, std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto &entry : db_.tabs_)
    {
        auto &tab = entry.second;
        printer.print_record({tab.name}, context);
        outfile << "| " << tab.name << " |\n";
    }
    printer.print_separator(context);
    outfile.close();
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context
 */
void SmManager::desc_table(const std::string &tab_name, Context *context)
{
    TabMeta &tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto &col : tab.cols)
    {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context
 */
void SmManager::create_table(const std::string &tab_name, const std::vector<ColDef> &col_defs, Context *context)
{
    if (db_.is_table(tab_name))
    {
        throw TableExistsError(tab_name);
    }

    std::string table_path = db_.name_ + "/" + tab_name;

    // 先检查文件是否存在
    if (disk_manager_->is_file(table_path))
    {
        throw FileExistsError(tab_name);
    }

    // Create table meta
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    for (auto &col_def : col_defs)
    {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset,
                       .index = false};
        curr_offset += col_def.len;
        tab.cols.push_back(col);
    }
    // Create & open record file
    int record_size = curr_offset;
    rm_manager_->create_file(table_path, record_size);
    db_.tabs_[tab_name] = tab;
    fhs_.emplace(tab_name, rm_manager_->open_file(table_path));

    flush_meta();
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string &tab_name, Context *context)
{
    // 检查表是否存在
    if (!db_.is_table(tab_name))
    {
        throw TableNotFoundError(tab_name);
    }

    std::string table_path = db_.name_ + "/" + tab_name;

    // 检查文件是否存在
    if (!disk_manager_->is_file(table_path))
    {
        throw InternalError("Table file not found: " + table_path);
    }

    // 删除表文件
    rm_manager_->close_file(fhs_[tab_name].get());
    rm_manager_->destroy_file(table_path);

    // 从文件句柄映射中删除
    fhs_.erase(tab_name);

    // 从数据库元数据中删除表信息
    db_.tabs_.erase(tab_name);

    // 将更新后的元数据写入磁盘
    flush_meta();
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::create_index(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context)
{
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context)
{
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<ColMeta>&} 索引包含的字段元数据
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string &tab_name, const std::vector<ColMeta> &cols, Context *context)
{
}