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
    if (chdir("..") < 0)
    {
        throw UnixError();
    }
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string &db_name)
{
    if (!is_dir(db_name))
    {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0)
    {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string &db_name)
{
    if (!is_dir(db_name))
    {
        throw DatabaseExistsError(db_name);
    }
    // 如果已有数据库打开，则抛出数据库已存在异常
    if (db_.name_.size())
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
void SmManager::flush_meta()
{
    // 默认清空文件
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db()
{
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
    if (chdir("..") < 0)
    {
        throw UnixError();
    }
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context
 */
void SmManager::show_tables(Context *context)
{
    std::fstream outfile;
    bool file_opened = false;

    // 只有当io_enabled_为true时才打开文件
    if (io_enabled_)
    {
        outfile.open("output.txt", std::ios::out | std::ios::app);
        if (outfile.is_open())
        {
            file_opened = true;
            outfile << "| Tables |\n";
        }
        // 如果文件打开失败，file_opened保持false，但不影响程序继续执行
    }

    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);

    for (auto &entry : db_.tabs_)
    {
        auto &tab = entry.second;

        // RecordPrinter 输出始终执行（显示到控制台/上下文）
        printer.print_record({tab.name}, context);

        // 只有当io_enabled_为true且文件成功打开时才写入文件
        if (file_opened)
        {
            outfile << "| " << tab.name << " |\n";
        }
    }

    printer.print_separator(context);

    // 关闭文件（如果已打开）
    if (file_opened)
    {
        outfile.close();
    }
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context
 */
void SmManager::desc_table(const std::string &tab_name, Context *context)
{
    TabMeta &tab = db_.get_table(tab_name);

    // std::vector<std::string> captions = {"Field", "Type", "Index"};
    std::vector<std::string> captions = {"Field", "Type"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto &col : tab.cols)
    {
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
void SmManager::create_table(const std::string &tab_name, const std::vector<ColDef> &col_defs, Context *context)
{
    if (db_.is_table(tab_name))
    {
        throw TableExistsError(tab_name);
    }
    // Create table meta
    TabMeta tab;
    tab.name = tab_name;

    // 获取MVCC隐藏字段(如果需要)
    auto &hidden_cols = context->txn_->get_txn_manager()->get_hidden_columns();

    // 计算总列数(用户列 + 隐藏列)
    size_t total_cols = col_defs.size() + hidden_cols.size();
    tab.cols.reserve(total_cols);

    int curr_offset = 0;

    // 添加隐藏列
    for (auto &hidden_col : hidden_cols)
    {
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
    for (auto &col_def : col_defs)
    {
        ColMeta col = {
            .tab_name = tab_name,
            .name = std::move(col_def.name),
            .type = col_def.type,
            .len = col_def.len,
            .offset = curr_offset};
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
void SmManager::drop_table(const std::string &tab_name, Context *context)
{
    if (!db_.is_table(tab_name))
    {
        throw TableNotFoundError(tab_name);
    }

    // delete index file
    auto &tab = db_.get_table(tab_name);

    {
        std::lock_guard lock(ihs_latch_);
        for (auto &index : tab.indexes)
        {
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
void SmManager::create_index(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context)
{
    TabMeta &tab = db_.get_table(tab_name);
    auto index_name = ix_manager_->get_index_name(tab_name, col_names);
    if (ihs_.count(index_name))
    {
        throw IndexExistsError(tab_name, col_names);
    }

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
        for (size_t id = 0; id < rids.size(); ++id)
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
                ih->insert_entry(insert_data.get(), {rid.page_no, rid.slot_no}, context->txn_, true);
            }
            catch (IndexEntryAlreadyExistError &)
            {
            }
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
void SmManager::drop_index(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context)
{
    if (!db_.is_table(tab_name))
    {
        throw TableNotFoundError(tab_name);
    }

    // delete index file
    auto index_iter = ihs_.find(ix_manager_->get_index_name(tab_name, col_names));
    if (index_iter == ihs_.end())
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
void SmManager::drop_index(const std::string &tab_name, const std::vector<ColMeta> &cols, Context *context)
{
    if (!db_.is_table(tab_name))
    {
        throw TableNotFoundError(tab_name);
    }

    // delete index file
    auto index_iter = ihs_.find(ix_manager_->get_index_name(tab_name, cols));
    if (index_iter == ihs_.end())
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

    int fd = -1;
    std::string buffer;
    [[maybe_unused]] ssize_t discard;

    // 只有当io_enabled_为true时才打开文件和准备缓冲区
    if (io_enabled_)
    {
        fd = open("output.txt", O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd == -1)
        {
            // 如果文件打开失败，继续执行但不写入文件
            io_enabled_ = false; // 临时禁用文件输出
        }
        else
        {
            // 64KB缓冲区
            buffer.reserve(8096);
        }
    }

    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"index"}, context);
    printer.print_separator(context);

    for (auto &index : tab.indexes)
    {
        // 只有当io_enabled_为true且文件打开成功时才写入缓冲区
        if (io_enabled_ && fd != -1)
        {
            buffer.append("| ").append(tab_name).append(" | unique | (").append(index.cols[0].name);
            for (size_t i = 1; i < index.cols.size(); ++i)
            {
                buffer.append(",").append(index.cols[i].name);
            }
            buffer.append(") |\n");

            // 缓冲区满时写入
            if (buffer.size() >= 8096)
            {
                discard = ::write(fd, buffer.data(), buffer.size());
                buffer.clear();
            }
        }

        // RecordPrinter 输出始终执行（显示到控制台/上下文）
        printer.print_record({ix_manager_->get_index_name(tab_name, index.cols)}, context);
    }

    printer.print_separator(context);

    // 写入剩余数据并关闭文件
    if (io_enabled_ && fd != -1)
    {
        if (buffer.size())
        {
            discard = ::write(fd, buffer.data(), buffer.size());
        }
        ::close(fd);
    }
}
void SmManager::load_csv_data(std::string &file_name, std::string &tab_name, Context *context)
{
    std::ifstream file(file_name);
    if (!file.is_open())
    {
        throw RMDBError("Failed to open file: " + file_name);
    }

    auto tab_ = db_.get_table(tab_name);
    auto fh_ = fhs_[tab_name].get();
    TransactionManager *txn_mgr = context->txn_->get_txn_manager();

    // Skip the first line (header)
    std::string line;
    std::getline(file, line, '\n');

    // 使用while循环，检查getline的返回值
    while (std::getline(file, line, '\n'))
    {
        if (line.empty())
        {
            continue; // 跳过空行
        }

        std::stringstream line_stream(line);
        std::string cell;

        // Make record buffer (参考Next方法)
        RmRecord rec(fh_->get_file_hdr().record_size);

        int hidden_column_count = txn_mgr->get_hidden_column_count();

        // 解析CSV数据并填充记录
        for (size_t i = hidden_column_count; i < tab_.cols.size(); ++i)
        {
            const auto &col = tab_.cols[i];
            std::getline(line_stream, cell, ',');

            switch (col.type)
            {
            case ColType::TYPE_INT:
            {
                int value = parse_int(cell);
                *(reinterpret_cast<int *>(rec.data + col.offset)) = value;
                break;
            }
            case ColType::TYPE_FLOAT:
            {
                float value = parse_float(cell);
                *(reinterpret_cast<float *>(rec.data + col.offset)) = value;
                break;
            }
            case ColType::TYPE_STRING:
            {
                std::memcpy(rec.data + col.offset, cell.c_str(), col.len);
                break;
            }
            case ColType::TYPE_DATETIME:
            {
                std::memcpy(rec.data + col.offset, cell.c_str(), col.len);
                break;
            }
            }
        }

        // 获取排他锁 (参考Next方法)
        // if (!context->lock_mgr_->lock_exclusive_on_key(context->txn_, fh_->GetFd(),
        //                                                rec.data + txn_mgr->get_start_offset()))
        // {
        //     throw TransactionAbortException(context->txn_->get_transaction_id(),
        //                                     AbortReason::UPGRADE_CONFLICT);
        // }

        // 设置事务ID (参考Next方法)
        txn_mgr->set_record_txn_id(rec.data, context->txn_, false);

        // 插入记录到文件
        Rid rid = fh_->insert_record(rec.data, context);

        // 添加写记录到事务日志 (参考Next方法)
        // context->txn_->append_write_record(new WriteRecord(WType::INSERT_TUPLE,
        //    tab_name, rid));

        // 更新索引 (参考Next方法的索引更新逻辑)
        for (size_t id = 0; id < tab_.indexes.size(); id++)
        {
            auto &index = tab_.indexes[id];
            std::unique_ptr<char[]> key(new char[index.col_tot_len]);
            int offset = 0;

            // 构造索引键：拼接索引涉及的所有列
            for (int i = 0; i < index.col_num; ++i)
            {
                // 找到索引列在表中的位置，然后获取其在记录中的偏移量
                int col_index = -1;
                for (size_t j = 0; j < tab_.cols.size(); ++j)
                {
                    if (tab_.cols[j].name == index.cols[i].name)
                    {
                        col_index = j;
                        break;
                    }
                }

                if (col_index != -1)
                {
                    memcpy(key.get() + offset, rec.data + tab_.cols[col_index].offset, index.cols[i].len);
                }
                offset += index.cols[i].len;
            }

            try
            {
                // 插入索引条目
                auto ih = get_index_handle(ix_manager_->get_index_name(tab_name, index.cols));
                ih->insert_entry(key.get(), rid, context->txn_, true);
            }
            catch (IndexEntryAlreadyExistError &)
            {
            }
        }

        // 记录日志 (参考Next方法)
        // InsertLogRecord log_record(context->txn_->get_transaction_id(), rec, rid, tab_name);
        // context->log_mgr_->add_log_to_buffer(&log_record);
    }

    file.close();
}
// 完整的双缓冲CSV加载实现
#include <vector>
#include <string>
#include <cstring>
#include <algorithm>

// 双缓冲方案主函数
void SmManager::load_csv_data_double_buffer(std::string &file_name, std::string &tab_name, Context *context)
{

    const size_t BUFFER_SIZE = 1024 * 1024; // 1MB缓冲区

    int fd = open(file_name.c_str(), O_RDONLY);
    if (fd == -1)
    {
        throw RMDBError("Failed to open file: " + file_name);
    }

    // 双缓冲区
    std::unique_ptr<char[]> buffer1(new char[BUFFER_SIZE + 1]); // +1 for null terminator
    std::unique_ptr<char[]> buffer2(new char[BUFFER_SIZE + 1]);

    char *read_buffer = buffer1.get();
    char *process_buffer = buffer2.get();

    auto tab_ = db_.get_table(tab_name);
    auto fh_ = fhs_[tab_name].get();
    TransactionManager *txn_mgr = context->txn_->get_txn_manager();

    RmRecord rec(fh_->get_file_hdr().record_size);
    int hidden_column_count = txn_mgr->get_hidden_column_count();

    ssize_t bytes_read;
    std::string leftover; // 处理跨缓冲区的行
    bool first_chunk = true;
    size_t total_records = 0;

    // 预分配批处理容器以提高性能
    std::vector<RmRecord> batch_records;
    std::vector<Rid> batch_rids;
    const size_t BATCH_SIZE = 500;
    batch_records.reserve(BATCH_SIZE);
    batch_rids.reserve(BATCH_SIZE);

    std::cout << "开始加载CSV文件: " << file_name << std::endl;
    auto start_time = std::chrono::high_resolution_clock::now();

    while ((bytes_read = read(fd, read_buffer, BUFFER_SIZE)) > 0)
    {
        // 添加null终止符
        read_buffer[bytes_read] = '\0';

        // 交换缓冲区（这里是关键：一个读，一个处理）
        std::swap(read_buffer, process_buffer);

        // 处理当前缓冲区中的数据
        size_t processed_records = process_buffer_chunk(
            process_buffer, bytes_read, leftover, tab_,
            hidden_column_count, context, first_chunk);

        total_records += processed_records;
        first_chunk = false;

        // 每处理一定数量记录输出进度
        if (total_records % 10000 == 0)
        {
            std::cout << "已处理记录数: " << total_records << std::endl;
        }
    }

    // 处理剩余数据
    if (!leftover.empty())
    {
        process_final_line(leftover, tab_, hidden_column_count, context);
        total_records++;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "CSV加载完成！" << std::endl;
    std::cout << "总记录数: " << total_records << std::endl;
    std::cout << "耗时: " << duration.count() << " ms" << std::endl;
    std::cout << "平均速度: " << (total_records * 1000.0 / duration.count()) << " 记录/秒" << std::endl;

    close(fd);
}

// 处理缓冲区数据块
size_t SmManager::process_buffer_chunk(char *buffer, size_t buffer_size,
                                       std::string &leftover, const TabMeta &tab,
                                       int hidden_column_count, Context *context,
                                       bool skip_header)
{
    char *ptr = buffer;
    char *end = buffer + buffer_size;
    size_t processed_count = 0;

    // 第一行如果是表头则跳过
    bool header_skipped = !skip_header;

    while (ptr < end)
    {
        // 找到一行的开始和结束
        char *line_start = ptr;
        char *line_end = ptr;

        // 寻找行结束符
        while (line_end < end && *line_end != '\n' && *line_end != '\r')
        {
            line_end++;
        }

        // 如果到达缓冲区末尾但没找到换行符，说明行被截断了
        if (line_end >= end)
        {
            // 将剩余部分保存到leftover中，等待下次处理
            size_t remaining_len = end - line_start;
            if (leftover.empty())
            {
                leftover.assign(line_start, remaining_len);
            }
            else
            {
                leftover.append(line_start, remaining_len);
            }
            break;
        }

        // 构造完整的行（包括之前的leftover）
        std::string complete_line;
        if (!leftover.empty())
        {
            complete_line = leftover + std::string(line_start, line_end - line_start);
            leftover.clear();
        }
        else
        {
            complete_line.assign(line_start, line_end - line_start);
        }

        // 跳过空行
        if (complete_line.empty())
        {
            ptr = line_end + 1;
            continue;
        }

        // 跳过表头
        if (!header_skipped)
        {
            header_skipped = true;
            ptr = line_end + 1;
            continue;
        }

        // 处理这一行数据
        try
        {
            process_csv_line(complete_line, tab, hidden_column_count, context);
            processed_count++;
        }
        catch (const std::exception &e)
        {
            std::cerr << "处理CSV行时出错: " << e.what() << std::endl;
            std::cerr << "问题行内容: " << complete_line << std::endl;
            // 继续处理下一行，不中断整个加载过程
        }

        // 移动到下一行
        ptr = line_end + 1;
        // 跳过可能的 \r\n 中的 \n
        if (ptr < end && *(ptr - 1) == '\r' && *ptr == '\n')
        {
            ptr++;
        }
    }

    return processed_count;
}

// 处理单行CSV数据
void SmManager::process_csv_line(const std::string &line, const TabMeta &tab,
                                 int hidden_column_count, Context *context)
{
    auto fh_ = fhs_[tab.name].get();
    TransactionManager *txn_mgr = context->txn_->get_txn_manager();

    // 创建记录缓冲区
    RmRecord rec(fh_->get_file_hdr().record_size);

    // 解析CSV字段
    std::vector<std::string> fields;
    parse_csv_fields(line, fields);

    // 检查字段数量是否匹配
    size_t expected_fields = tab.cols.size() - hidden_column_count;
    if (fields.size() != expected_fields)
    {
        throw RMDBError("CSV字段数量不匹配，期望: " + std::to_string(expected_fields) +
                        ", 实际: " + std::to_string(fields.size()));
    }

    // 填充记录数据
    for (size_t i = 0; i < fields.size(); ++i)
    {
        const auto &col = tab.cols[i + hidden_column_count];
        const std::string &field_value = fields[i];

        try
        {
            switch (col.type)
            {
            case ColType::TYPE_INT:
            {
                int value = parse_int_safe(field_value);
                *(reinterpret_cast<int *>(rec.data + col.offset)) = value;
                break;
            }
            case ColType::TYPE_FLOAT:
            {
                float value = parse_float_safe(field_value);
                *(reinterpret_cast<float *>(rec.data + col.offset)) = value;
                break;
            }
            case ColType::TYPE_STRING:
            {
                // 处理字符串字段，确保不超出长度限制
                int copy_len = std::min(field_value.length(), static_cast<size_t>(col.len));
                std::memcpy(rec.data + col.offset, field_value.c_str(), copy_len);
                // 如果字符串较短，用0填充剩余空间
                if (copy_len < col.len)
                {
                    std::memset(rec.data + col.offset + copy_len, 0, col.len - copy_len);
                }
                break;
            }
            case ColType::TYPE_DATETIME:
            {
                // 日期时间字段处理
                int copy_len = std::min(field_value.length(), static_cast<size_t>(col.len));
                std::memcpy(rec.data + col.offset, field_value.c_str(), copy_len);
                if (copy_len < col.len)
                {
                    std::memset(rec.data + col.offset + copy_len, 0, col.len - copy_len);
                }
                break;
            }
            default:
                throw RMDBError("不支持的列类型");
            }
        }
        catch (const std::exception &e)
        {
            throw RMDBError("解析字段 '" + col.name + "' 时出错: " + e.what() +
                            ", 值: '" + field_value + "'");
        }
    }

    // 设置事务ID
    txn_mgr->set_record_txn_id(rec.data, context->txn_, false);

    // 插入记录到文件
    Rid rid = fh_->insert_record(rec.data, context);

    // 更新所有索引
    update_indexes_for_record(rec, rid, tab, context);
}

// 解析CSV字段（处理引号和转义）
void SmManager::parse_csv_fields(const std::string &line, std::vector<std::string> &fields)
{
    fields.clear();

    size_t pos = 0;
    size_t len = line.length();

    while (pos < len)
    {
        std::string field;

        // 跳过前导空白
        while (pos < len && (line[pos] == ' ' || line[pos] == '\t'))
        {
            pos++;
        }

        if (pos >= len)
            break;

        // 检查是否以引号开始
        if (line[pos] == '"')
        {
            pos++; // 跳过开始引号

            // 处理带引号的字段
            while (pos < len)
            {
                if (line[pos] == '"')
                {
                    // 检查是否是转义的引号
                    if (pos + 1 < len && line[pos + 1] == '"')
                    {
                        field += '"';
                        pos += 2;
                    }
                    else
                    {
                        // 字段结束
                        pos++;
                        break;
                    }
                }
                else
                {
                    field += line[pos];
                    pos++;
                }
            }

            // 跳过到下一个逗号
            while (pos < len && line[pos] != ',')
            {
                pos++;
            }
        }
        else
        {
            // 处理普通字段
            while (pos < len && line[pos] != ',')
            {
                field += line[pos];
                pos++;
            }
        }

        // 移除尾部空白
        while (!field.empty() && (field.back() == ' ' || field.back() == '\t'))
        {
            field.pop_back();
        }

        fields.push_back(field);

        // 跳过逗号
        if (pos < len && line[pos] == ',')
        {
            pos++;
        }
    }
}

// 处理文件末尾的剩余行
void SmManager::process_final_line(const std::string &leftover, const TabMeta &tab,
                                   int hidden_column_count, Context *context)
{
    if (leftover.empty())
        return;

    try
    {
        process_csv_line(leftover, tab, hidden_column_count, context);
    }
    catch (const std::exception &e)
    {
        std::cerr << "处理最后一行时出错: " << e.what() << std::endl;
        std::cerr << "行内容: " << leftover << std::endl;
    }
}

// 安全的整数解析
int SmManager::parse_int_safe(const std::string &str)
{
    if (str.empty())
        return 0;

    try
    {
        size_t pos;
        int value = std::stoi(str, &pos);
        if (pos != str.length())
        {
            throw std::invalid_argument("包含非数字字符");
        }
        return value;
    }
    catch (const std::exception &e)
    {
        throw std::invalid_argument("无法解析整数: " + str);
    }
}

// 安全的浮点数解析
float SmManager::parse_float_safe(const std::string &str)
{
    if (str.empty())
        return 0.0f;

    try
    {
        size_t pos;
        float value = std::stof(str, &pos);
        if (pos != str.length())
        {
            throw std::invalid_argument("包含非数字字符");
        }
        return value;
    }
    catch (const std::exception &e)
    {
        throw std::invalid_argument("无法解析浮点数: " + str);
    }
}

// 为记录更新所有索引
void SmManager::update_indexes_for_record(const RmRecord &rec, const Rid &rid,
                                          const TabMeta &tab, Context *context)
{
    for (const auto &index : tab.indexes)
    {
        std::unique_ptr<char[]> key(new char[index.col_tot_len]);
        int offset = 0;

        // 构造索引键
        for (int i = 0; i < index.col_num; ++i)
        {
            // 找到索引列在表中的位置
            auto col_iter = tab.cols_map.find(index.cols[i].name);
            if (col_iter != tab.cols_map.end())
            {
                const auto &col = tab.cols[col_iter->second];
                std::memcpy(key.get() + offset, rec.data + col.offset, index.cols[i].len);
            }
            offset += index.cols[i].len;
        }

        try
        {
            auto ih = get_index_handle(ix_manager_->get_index_name(tab.name, index.cols));
            ih->insert_entry(key.get(), rid, context->txn_, true);
        }
        catch (IndexEntryAlreadyExistError &)
        {
            // 忽略重复键错误，继续处理
        }
    }
}
void SmManager::load_csv_data_threaded(std::string &file_name, std::string &tab_name, Context *context)
{
    const size_t BUFFER_SIZE = 1024 * 1024; // 1MB缓冲区
    const size_t MAX_QUEUE_SIZE = 5;        // 最大队列长度，控制内存使用

    // 创建线程安全队列
    ThreadSafeQueue data_queue;

    std::cout << "开始双线程加载CSV文件: " << file_name << std::endl;
    auto start_time = std::chrono::high_resolution_clock::now();

    // 启动读取线程
    std::thread reader_thread([this, &file_name, &data_queue, BUFFER_SIZE]()
                              {
        try {
            reader_thread_func(file_name, data_queue, BUFFER_SIZE);
        } catch (...) {
            data_queue.set_exception(std::current_exception());
        } });

    // 启动处理线程
    std::thread processor_thread([this, &data_queue, &tab_name, context]()
                                 {
        try {
            processor_thread_func(data_queue, tab_name, context);
        } catch (...) {
            data_queue.set_exception(std::current_exception());
        } });

    // 等待两个线程完成
    reader_thread.join();
    processor_thread.join();

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "双线程CSV加载完成！" << std::endl;
    std::cout << "耗时: " << duration.count() << " ms" << std::endl;
}

void SmManager::reader_thread_func(const std::string &file_name, ThreadSafeQueue &queue, const size_t buffer_size)
{
    int fd = open(file_name.c_str(), O_RDONLY);
    if (fd == -1)
    {
        throw RMDBError("Failed to open file: " + file_name);
    }

    // 创建读取缓冲区
    std::unique_ptr<char[]> read_buffer(new char[buffer_size]);
    ssize_t bytes_read;
    size_t total_read = 0;

    std::cout << "[读取线程] 开始读取文件..." << std::endl;

    while ((bytes_read = read(fd, read_buffer.get(), buffer_size)) > 0)
    {
        // 创建数据块
        auto chunk = std::make_shared<DataChunk>(buffer_size);

        // 复制数据到chunk
        std::memcpy(chunk->data.get(), read_buffer.get(), bytes_read);
        chunk->size = bytes_read;
        chunk->data[bytes_read] = '\0'; // null终止符

        // 将数据块放入队列
        queue.push(chunk);

        total_read += bytes_read;

        // 控制队列大小，避免内存使用过多
        std::this_thread::sleep_for(std::chrono::microseconds(10));
    }

    // 发送结束标志
    auto final_chunk = std::make_shared<DataChunk>();
    final_chunk->is_final = true;
    queue.push(final_chunk);
    queue.set_finished();

    close(fd);
    std::cout << "[读取线程] 文件读取完成，总计: " << total_read << " 字节" << std::endl;
}

void SmManager::processor_thread_func(ThreadSafeQueue &queue, const std::string &tab_name, Context *context)
{
    auto tab_ = db_.get_table(tab_name);
    auto fh_ = fhs_[tab_name].get();
    TransactionManager *txn_mgr = context->txn_->get_txn_manager();
    int hidden_column_count = txn_mgr->get_hidden_column_count();

    std::string leftover; // 处理跨缓冲区的行
    bool first_chunk = true;
    size_t total_records = 0;

    std::cout << "[处理线程] 开始处理数据..." << std::endl;

    while (true)
    {
        // 从队列获取数据块
        auto chunk = queue.pop();

        if (!chunk)
        {
            break; // 队列已空且读取线程已完成
        }

        if (chunk->is_final)
        {
            break; // 收到结束标志
        }

        // 处理这个数据块
        size_t processed_records = process_buffer_chunk_threaded(
            chunk->data.get(), chunk->size, leftover, tab_,
            hidden_column_count, context, first_chunk);

        total_records += processed_records;
        first_chunk = false;

        // 每处理一定数量记录输出进度
        if (total_records % 5000 == 0)
        {
            std::cout << "[处理线程] 已处理记录数: " << total_records << std::endl;
        }
    }

    // 处理剩余数据
    if (!leftover.empty())
    {
        try
        {
            process_csv_line_threaded(leftover, tab_, hidden_column_count, context);
            total_records++;
        }
        catch (const std::exception &e)
        {
            std::cerr << "[处理线程] 处理最后一行时出错: " << e.what() << std::endl;
        }
    }

    std::cout << "[处理线程] 数据处理完成，总记录数: " << total_records << std::endl;
}

// 线程安全版本的缓冲区处理函数
size_t SmManager::process_buffer_chunk_threaded(char *buffer, size_t buffer_size,
                                                std::string &leftover, const TabMeta &tab,
                                                int hidden_column_count, Context *context,
                                                bool skip_header)
{
    char *ptr = buffer;
    char *end = buffer + buffer_size;
    size_t processed_count = 0;
    bool header_skipped = !skip_header;

    while (ptr < end)
    {
        char *line_start = ptr;
        char *line_end = ptr;

        // 寻找行结束符
        while (line_end < end && *line_end != '\n' && *line_end != '\r')
        {
            line_end++;
        }

        // 如果到达缓冲区末尾但没找到换行符，说明行被截断了
        if (line_end >= end)
        {
            size_t remaining_len = end - line_start;
            if (leftover.empty())
            {
                leftover.assign(line_start, remaining_len);
            }
            else
            {
                leftover.append(line_start, remaining_len);
            }
            break;
        }

        // 构造完整的行
        std::string complete_line;
        if (!leftover.empty())
        {
            complete_line = leftover + std::string(line_start, line_end - line_start);
            leftover.clear();
        }
        else
        {
            complete_line.assign(line_start, line_end - line_start);
        }

        // 跳过空行
        if (complete_line.empty())
        {
            ptr = line_end + 1;
            continue;
        }

        // 跳过表头
        if (!header_skipped)
        {
            header_skipped = true;
            ptr = line_end + 1;
            continue;
        }

        // 处理这一行数据
        try
        {
            process_csv_line_threaded(complete_line, tab, hidden_column_count, context);
            processed_count++;
        }
        catch (const std::exception &e)
        {
            std::cerr << "[处理线程] 处理CSV行时出错: " << e.what() << std::endl;
            std::cerr << "问题行内容: " << complete_line << std::endl;
        }

        // 移动到下一行
        ptr = line_end + 1;
        if (ptr < end && *(ptr - 1) == '\r' && *ptr == '\n')
        {
            ptr++;
        }
    }

    return processed_count;
}

// 线程安全版本的CSV行处理函数
void SmManager::process_csv_line_threaded(const std::string &line, const TabMeta &tab,
                                          int hidden_column_count, Context *context)
{
    // 获取文件句柄时需要线程安全
    std::shared_ptr<RmFileHandle> fh;
    {
        std::shared_lock<std::shared_mutex> lock(fhs_latch_);
        auto it = fhs_.find(tab.name);
        if (it == fhs_.end())
        {
            throw RMDBError("表文件句柄未找到: " + tab.name);
        }
        fh = it->second;
    }

    TransactionManager *txn_mgr = context->txn_->get_txn_manager();

    // 创建记录缓冲区
    RmRecord rec(fh->get_file_hdr().record_size);

    // 解析CSV字段
    std::vector<std::string> fields;
    parse_csv_fields(line, fields);

    // 检查字段数量是否匹配
    size_t expected_fields = tab.cols.size() - hidden_column_count;
    if (fields.size() != expected_fields)
    {
        throw RMDBError("CSV字段数量不匹配，期望: " + std::to_string(expected_fields) +
                        ", 实际: " + std::to_string(fields.size()));
    }

    // 填充记录数据
    for (size_t i = 0; i < fields.size(); ++i)
    {
        const auto &col = tab.cols[i + hidden_column_count];
        const std::string &field_value = fields[i];

        try
        {
            switch (col.type)
            {
            case ColType::TYPE_INT:
            {
                int value = parse_int_safe(field_value);
                *(reinterpret_cast<int *>(rec.data + col.offset)) = value;
                break;
            }
            case ColType::TYPE_FLOAT:
            {
                float value = parse_float_safe(field_value);
                *(reinterpret_cast<float *>(rec.data + col.offset)) = value;
                break;
            }
            case ColType::TYPE_STRING:
            {
                size_t copy_len = std::min(field_value.length(), static_cast<size_t>(col.len));
                std::memcpy(rec.data + col.offset, field_value.c_str(), copy_len);
                if (copy_len < col.len)
                {
                    std::memset(rec.data + col.offset + copy_len, 0, col.len - copy_len);
                }
                break;
            }
            case ColType::TYPE_DATETIME:
            {
                size_t copy_len = std::min(field_value.length(), static_cast<size_t>(col.len));
                std::memcpy(rec.data + col.offset, field_value.c_str(), copy_len);
                if (copy_len < col.len)
                {
                    std::memset(rec.data + col.offset + copy_len, 0, col.len - copy_len);
                }
                break;
            }
            default:
                throw RMDBError("不支持的列类型");
            }
        }
        catch (const std::exception &e)
        {
            throw RMDBError("解析字段 '" + col.name + "' 时出错: " + e.what() +
                            ", 值: '" + field_value + "'");
        }
    }

    // 设置事务ID
    txn_mgr->set_record_txn_id(rec.data, context->txn_, false);

    // 插入记录到文件（这里需要线程安全）
    Rid rid = fh->insert_record(rec.data, context);

    // 更新所有索引（需要线程安全）
    update_indexes_for_record_threaded(rec, rid, tab, context);
}

// 线程安全版本的索引更新函数
void SmManager::update_indexes_for_record_threaded(const RmRecord &rec, const Rid &rid,
                                                   const TabMeta &tab, Context *context)
{
    for (const auto &index : tab.indexes)
    {
        std::unique_ptr<char[]> key(new char[index.col_tot_len]);
        int offset = 0;

        // 构造索引键
        for (int i = 0; i < index.col_num; ++i)
        {
            auto col_iter = tab.cols_map.find(index.cols[i].name);
            if (col_iter != tab.cols_map.end())
            {
                const auto &col = tab.cols[col_iter->second];
                std::memcpy(key.get() + offset, rec.data + col.offset, index.cols[i].len);
            }
            offset += index.cols[i].len;
        }

        try
        {
            // 获取索引句柄时需要线程安全
            std::shared_ptr<IxIndexHandle> ih;
            {
                std::shared_lock<std::shared_mutex> lock(ihs_latch_);
                auto index_name = ix_manager_->get_index_name(tab.name, index.cols);
                auto it = ihs_.find(index_name);
                if (it != ihs_.end())
                {
                    ih = it->second;
                }
            }

            if (ih)
            {
                ih->insert_entry(key.get(), rid, context->txn_, true);
            }
        }
        catch (IndexEntryAlreadyExistError &)
        {
            // 忽略重复键错误
        }
    }
}