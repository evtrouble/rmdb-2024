/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "log_recovery.h"
#include "transaction/transaction_manager.h"

/**
 * @description: analyze阶段，需要获得脏页表（DPT）和未完成的事务列表（ATT）
 */
void RecoveryManager::analyze()
{
}

/**
 * @description: 重做所有未落盘的操作
 */
void RecoveryManager::redo()
{
    // for(auto& pair : sm_manager_->fhs_) {
    //     // 确保每个表的文件大小足够
    //     pair.second->ensure_file_size();
    // }
    txn_id_t max_txn_id = -1;

    long long offset = 0;
    LogRecord *log_record = nullptr;
    
    while(true){
        log_record = read_log(offset);
        if(log_record == nullptr){
            break;
        }
        offset += log_record->log_tot_len_;
        switch(log_record->log_type_){
            case BEGIN:{
                BeginLogRecord* begin_log_record = static_cast<BeginLogRecord*>(log_record); 
                auto txn = std::make_unique<Transaction>(begin_log_record->log_tid_, txn_manager_);
                max_txn_id = std::max(max_txn_id, begin_log_record->log_tid_);
                temp_txns_[begin_log_record->log_tid_] = std::move(txn);
                break;
            }
            case COMMIT :{
                CommitLogRecord* commit_log_record = static_cast<CommitLogRecord*>(log_record);
                auto it = temp_txns_.find(commit_log_record->log_tid_);
                if(it == temp_txns_.end()) {
                    break;  // 事务不存在，可能是日志损坏，跳过
                }
                auto &txn = it->second;
                auto write_set = txn->get_write_set();
                for(auto write_record : *write_set){
                    delete write_record; // 清理写集中的记录
                }
                temp_txns_.erase(it);
                break;
            }
            case ABORT:{
                AbortLogRecord* abort_log_record = static_cast<AbortLogRecord*>(log_record);
                auto it = temp_txns_.find(abort_log_record->log_tid_);
                if(it == temp_txns_.end()) {
                    break;  // 事务不存在，跳过
                }
                auto &txn = it->second;
                auto write_set = txn->get_write_set();
                std::unordered_set<Rid> abort_set;
                while (write_set->size())
                {
                    auto write_record = write_set->front();
                    write_set->pop_front();
                    Rid rid = write_record->GetRid();
                    // 根据写操作类型进行回滚
                    switch (write_record->GetWriteType())
                    {
                        case WType::INSERT_TUPLE:
                            abort_set.emplace(rid);
                            sm_manager_->get_table_handle(write_record->GetTableName())
                                ->abort_insert_record(rid);
                            break;
                        case WType::DELETE_TUPLE: {
                            if(abort_set.count(rid))break;
                            abort_set.emplace(rid);
                            auto fh = sm_manager_->get_table_handle(write_record->GetTableName());
                            fh->abort_delete_record(rid, write_record->GetRecord().data);
                            break;
                        }
                        case WType::UPDATE_TUPLE:{
                            if(abort_set.count(rid))break;
                            abort_set.emplace(rid);
                            auto fh = sm_manager_->get_table_handle(write_record->GetTableName());
                            fh->abort_update_record(rid, write_record->GetRecord().data);
                            break;
                        }
                        default:
                            break;
                    }
                    delete write_record;
                }
                temp_txns_.erase(it);
                break;
            }
            case UPDATE:{
                UpdateLogRecord* update_log_record = static_cast<UpdateLogRecord*>(log_record);
                std::string table_name(update_log_record->table_name_, update_log_record->table_name_size_);
                
                // 检查表是否存在
                auto fh_ = sm_manager_->get_table_handle(table_name);
                if(fh_ == nullptr) {
                    break;  // 表不存在，跳过
                }
                while(fh_->file_hdr_.num_pages <= update_log_record->rid_.page_no) {
                    RmPageHandle page_handle = fh_->create_new_page_handle();
                    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
                }
                
                // 检查事务是否存在
                auto txn_it = temp_txns_.find(update_log_record->log_tid_);
                if(txn_it == temp_txns_.end()) {
                    break;  // 事务不存在，跳过
                }
                auto &txn = txn_it->second;
                
                fh_->recovery_insert_record(update_log_record->rid_, update_log_record->after_value_.data);
                txn->append_write_record(new WriteRecord(WType::UPDATE_TUPLE,
                            table_name, update_log_record->rid_, update_log_record->before_value_));
                break;
            }  
            case INSERT:{
                InsertLogRecord* insert_log_record = static_cast<InsertLogRecord*>(log_record);
                std::string table_name(insert_log_record->table_name_, insert_log_record->table_name_size_);
                
                // 检查表是否存在
                auto fh_ = sm_manager_->get_table_handle(table_name);
                if(fh_ == nullptr) {
                    break;  // 表不存在，跳过
                }

                while(fh_->file_hdr_.num_pages <= insert_log_record->rid_.page_no) {
                    RmPageHandle page_handle = fh_->create_new_page_handle();
                    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
                }
                
                // 检查事务是否存在
                auto txn_it = temp_txns_.find(insert_log_record->log_tid_);
                if(txn_it == temp_txns_.end()) {
                    break;  // 事务不存在，跳过
                }
                auto &txn = txn_it->second;
                
                fh_->recovery_insert_record(insert_log_record->rid_, insert_log_record->insert_value_.data);
                txn->append_write_record(new WriteRecord(WType::INSERT_TUPLE,
                            table_name, insert_log_record->rid_));
                break;
            }    
            case DELETE:{
                DeleteLogRecord* delete_log_record = static_cast<DeleteLogRecord*>(log_record);
                std::string table_name(delete_log_record->table_name_, delete_log_record->table_name_size_);
                
                // 检查表是否存在
                auto fh_ = sm_manager_->get_table_handle(table_name);
                if(fh_ == nullptr) {
                    break;  // 表不存在，跳过
                }

                while(fh_->file_hdr_.num_pages <= delete_log_record->rid_.page_no) {
                    RmPageHandle page_handle = fh_->create_new_page_handle();
                    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
                }
                
                // 检查事务是否存在
                auto txn_it = temp_txns_.find(delete_log_record->log_tid_);
                if(txn_it == temp_txns_.end()) {
                    break;  // 事务不存在，跳过
                }
                auto &txn = txn_it->second;
                
                fh_->recovery_delete_record(delete_log_record->rid_);
                txn->append_write_record(new WriteRecord(WType::DELETE_TUPLE,
                            table_name, delete_log_record->rid_, delete_log_record->delete_value_));
                break;
            }
        }
        delete log_record;
    }
    txn_manager_->set_txn_id(max_txn_id);
}

/**
 * @description: 回滚未完成的事务
 */
void RecoveryManager::undo() {
    for(auto& [txn_id, txn] : temp_txns_){
        auto write_set = txn->get_write_set();
        std::unordered_set<Rid> abort_set;
        while (write_set->size())
        {
            auto write_record = write_set->front();
            write_set->pop_front();
            Rid rid = write_record->GetRid();
            // 根据写操作类型进行回滚
            switch (write_record->GetWriteType())
            {
                case WType::INSERT_TUPLE:
                    abort_set.emplace(rid);
                    sm_manager_->get_table_handle(write_record->GetTableName())
                        ->abort_insert_record(rid);
                    break;
                case WType::DELETE_TUPLE: {
                    if(abort_set.count(rid))break;
                    abort_set.emplace(rid);
                    auto fh = sm_manager_->get_table_handle(write_record->GetTableName());
                    fh->abort_delete_record(rid, write_record->GetRecord().data);
                    break;
                }
                case WType::UPDATE_TUPLE:{
                    if(abort_set.count(rid))break;
                    abort_set.emplace(rid);
                    auto fh = sm_manager_->get_table_handle(write_record->GetTableName());
                    fh->abort_update_record(rid, write_record->GetRecord().data);
                    break;
                }
                default:
                    break;
            }
            delete write_record;
        }
    }

    temp_txns_.clear();
    disk_manager_->clear_log();

    txn_manager_->init_txn(); // 重新初始化事务管理器
    Transaction* start_txn = txn_manager_->get_start_txn();
    auto context = new Context(nullptr, nullptr, start_txn);
    for (auto &tab : sm_manager_->db_.tabs_)
    {
        std::vector<IndexMeta> indexes;
        indexes.reserve(tab.second.indexes.size());
        for (auto &index_ : tab.second.indexes)
        {
            indexes.emplace_back(index_);
        }
        for (auto &index_ : indexes)
        {
            sm_manager_->drop_index(index_.tab_name, index_.cols, nullptr);
            std::vector<std::string> col_names_;
            col_names_.reserve(index_.cols.size());
            for (auto &col : index_.cols)
            {
                col_names_.emplace_back(col.name);
            }
            sm_manager_->create_index(index_.tab_name, col_names_, context);
        }
    }
    start_txn->reset(); // 重置起始事务
    delete context; // 清理上下文
    buffer_pool_manager_->force_flush_all_pages(); // 确保所有脏页都被刷新到磁盘
}

LogRecord *RecoveryManager::read_log(long long offset)
{
    // 读取日志记录头
    char log_header[LOG_HEADER_SIZE];
    if (disk_manager_->read_log(log_header, LOG_HEADER_SIZE, offset) == 0)
    {
        return nullptr;
    }
    // 解析日志记录头
    LogType log_type = *reinterpret_cast<const LogType *>(log_header);
    uint32_t log_size = *reinterpret_cast<const uint32_t *>(log_header + OFFSET_LOG_TOT_LEN);
    // 读取日志记录
    char log_data[log_size];
    disk_manager_->read_log(log_data, log_size, offset);
    // 解析日志记录
    LogRecord *log_record = nullptr;
    switch (log_type)
    {
    case BEGIN:
        log_record = new BeginLogRecord();
        break;
    case COMMIT:
        log_record = new CommitLogRecord();
        break;
    case ABORT:
        log_record = new AbortLogRecord();
        break;
    case UPDATE:
        log_record = new UpdateLogRecord();
        break;
    case INSERT:
        log_record = new InsertLogRecord();
        break;
    case DELETE:
        log_record = new DeleteLogRecord();
        break;
    }

    if (log_record != nullptr)
    {
        log_record->deserialize(log_data);
    }
    return log_record;
}
