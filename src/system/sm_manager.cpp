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
#include "transaction/transaction_manager.h"

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
bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string &db_name)
{
    if (is_dir(db_name))
    {
        throw DatabaseExistsError(db_name);
    }
    // 为数据库创建一个子目录
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0)
    { // 创建一个名为db_name的目录
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0)
    { // 进入名为db_name的目录
        throw UnixError();
    }
    // 创建系统目录
    DbMeta *new_db = new DbMeta();
    new_db->name_ = db_name;

    // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
    std::ofstream ofs(DB_META_NAME);

    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << *new_db; // 注意：此处重载了操作符<<

    delete new_db;

    // 回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseExistsError(db_name);
    }
    // 如果已有数据库打开，则抛出数据库已存在异常
    if (!db_.name_.empty())
    {
        throw DatabaseExistsError(db_name);
    }

    if (chdir(db_name.c_str()) < 0)
    {
        throw UnixError();
    }

    // 加载数据库元数据
    std::ifstream ifs(DB_META_NAME);
    if (!ifs)
    {
        throw UnixError();
    }
    ifs >> db_;

    // 打开所有表和索引文件
    for (auto &[tab_name, tab_meta] : db_.tabs_)
    {
        fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));
        for (auto &index : tab_meta.indexes)
        {
            ihs_.emplace(ix_manager_->get_index_name(tab_name, index.cols),
                         ix_manager_->open_index(tab_name, index.cols));
        }
    }
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta() {
    // 默认清空文件
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db() {
    if (db_.name_.empty())
    {
        throw DatabaseNotFoundError("db not open");
    }

    // 刷新元数据到磁盘
    flush_meta();
    // 清空数据库元数据
    fhs_.clear();
    ihs_.clear();

    db_.name_.clear();
    db_.tabs_.clear();
    
    // 回到上一级目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context 
 */
void SmManager::show_tables(Context* context) {
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto &entry : db_.tabs_) {
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
void SmManager::desc_table(const std::string& tab_name, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);

    // std::vector<std::string> captions = {"Field", "Type", "Index"};
    std::vector<std::string> captions = {"Field", "Type"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto &col : tab.cols) {
        // std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        std::vector<std::string> field_info = {col.name, coltype2str(col.type)};
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
void SmManager::create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context) {
    if (db_.is_table(tab_name)) {
        throw TableExistsError(tab_name);
    }
    // Create table meta
    TabMeta tab;
    tab.name = tab_name;
    
    // 获取MVCC隐藏字段(如果需要)
    auto& hidden_cols = context->txn_->get_txn_manager()->get_hidden_columns();
    
    // 计算总列数(用户列 + 隐藏列)
    size_t total_cols = col_defs.size() + hidden_cols.size();
    tab.cols.reserve(total_cols);
    
    int curr_offset = 0;

    // 添加隐藏列
    for (auto &hidden_col : hidden_cols) {
        ColMeta col = {
            .tab_name = tab_name,
            .name = hidden_col.name,
            .type = hidden_col.type,
            .len = hidden_col.len,
            .offset = curr_offset,
        };
        curr_offset += hidden_col.len;
        tab.cols.emplace_back(std::move(col));
        tab.cols_map.emplace(tab.cols.back().name, tab.cols.size() - 1);
    }
    
    // 添加用户定义的列
    for (auto &col_def : col_defs) {
        ColMeta col = {
            .tab_name = tab_name,
            .name = std::move(col_def.name),
            .type = col_def.type,
            .len = col_def.len,
            .offset = curr_offset
        };
        curr_offset += col_def.len;
        tab.cols.emplace_back(std::move(col));
        tab.cols_map.emplace(tab.cols.back().name, tab.cols.size() - 1);
    }

    // Create & open record file
    rm_manager_->create_file(tab_name, curr_offset);
    
    {
        std::lock_guard lock(fhs_latch_);
        fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));
    }
    db_.tabs_.emplace(std::move(tab_name), std::move(tab));
    
    flush_meta();
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string& tab_name, Context* context) {
    if (!db_.is_table(tab_name)) 
        throw TableNotFoundError(tab_name);

    // delete index file
    auto &tab = db_.get_table(tab_name);

    {
        std::lock_guard lock(ihs_latch_);
        for(auto& index : tab.indexes) {
            auto index_iter = ihs_.find(ix_manager_->get_index_name(tab_name, index.cols));
            index_iter->second->mark_deleted(); // 标记为已删除
            ihs_.erase(index_iter);
        }
    }
    tab.indexes.clear();

    // delete record file
    auto record_iter = fhs_.find(tab_name);
    record_iter->second->mark_deleted(); // 标记为已删除

    db_.tabs_.erase(tab_name);

    {
        std::lock_guard lock(fhs_latch_);
        fhs_.erase(record_iter);
    }
    
    flush_meta();
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);
    auto index_name = ix_manager_->get_index_name(tab_name, col_names);
    if (ihs_.count(index_name))
        throw IndexExistsError(tab_name, col_names);
    
    // Create index meta
    std::vector<ColMeta> cols;
    cols.reserve(col_names.size());
    int tot_col_len = 0;
    for (auto &col_name : col_names)
    {
        cols.emplace_back(*tab.get_col(col_name));
        tot_col_len += cols.back().len;
    }
    ix_manager_->create_index(tab_name, cols);
    auto ih = ix_manager_->open_index(tab_name, cols);

    auto fh_ = get_table_handle(tab_name);

    // 向索引中插入表中已有数据
    auto insert_data = std::make_unique<char[]>(tot_col_len);
    for (RmScan rmScan(fh_, context); !rmScan.is_end(); rmScan.next_batch())
    {
        auto rids = rmScan.rid_batch();
        auto records = rmScan.record_batch();
        for(size_t id = 0; id < rids.size(); ++id)
        {
            auto &rid = rids[id];
            auto &record = records[id];
            int offset = 0;
            for (auto &col : cols)
            {
                std::memcpy(insert_data.get() + offset, record->data + col.offset, col.len);
                offset += col.len;
            }
            try
            {
                ih->insert_entry_without_lock(insert_data.get(), {rid.page_no, rid.slot_no});
            }
            catch (IndexEntryAlreadyExistError &) {}
        }
    }

    {
        std::lock_guard lock(ihs_latch_);
        ihs_.emplace(index_name, std::move(ih));
    }
    
    tab.indexes.emplace_back(tab_name, tot_col_len, static_cast<int>(cols.size()), cols);

    flush_meta();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    if (!db_.is_table(tab_name)) 
        throw TableNotFoundError(tab_name);

    // delete index file
    auto index_iter = ihs_.find(ix_manager_->get_index_name(tab_name, col_names));
    if(index_iter == ihs_.end())
        return;

    index_iter->second->mark_deleted(); // 标记为已删除

    {
        std::lock_guard lock(ihs_latch_);
        ihs_.erase(index_iter);
    }
    
    auto &tab = db_.get_table(tab_name);
    tab.indexes.erase(tab.get_index_meta(col_names));

    flush_meta();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<ColMeta>&} 索引包含的字段元数据
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<ColMeta>& cols, Context* context) {
    if (!db_.is_table(tab_name)) 
        throw TableNotFoundError(tab_name);

    // delete index file
    auto index_iter = ihs_.find(ix_manager_->get_index_name(tab_name, cols));
    if(index_iter == ihs_.end())
        return;

    index_iter->second->mark_deleted(); // 标记为已删除
    ihs_.erase(index_iter);  

    auto &tab = db_.get_table(tab_name);
    tab.indexes.erase(tab.get_index_meta(cols));

    flush_meta();
}

/**
 * @description: 显示表的所有索引
 * @param {string&} tab_name 表名称
 * @param {Context*} context
 * @return {*}
 */
void SmManager::show_index(const std::string &tab_name, Context *context)
{
    TabMeta &tab = db_.get_table(tab_name);
    int fd = open("output.txt", O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd == -1) return;
    
    // 64KB缓冲区
    std::string buffer;
    buffer.reserve(8096);
    [[maybe_unused]] ssize_t discard;

    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"index"}, context);
    printer.print_separator(context);
    for (auto &index : tab.indexes) {
        buffer.append("| ").append(tab_name).append(" | unique | (")
            .append(index.cols[0].name);
        for (size_t i = 1; i < index.cols.size(); ++i)
        {
            buffer.append(",").append(index.cols[i].name);
        }
        buffer.append(") |\n");
        // 缓冲区满时写入
        if (buffer.size() >= 8096) {
            discard = ::write(fd, buffer.data(), buffer.size());
            buffer.clear();
        }
        printer.print_record({ix_manager_->get_index_name(tab_name, index.cols)}, context);
    }
    printer.print_separator(context);
    // 写入剩余数据
    if (!buffer.empty()) {
        discard = ::write(fd, buffer.data(), buffer.size());
    }
    ::close(fd);
}