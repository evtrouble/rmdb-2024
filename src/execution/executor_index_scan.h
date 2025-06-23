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

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class IndexScanExecutor : public AbstractExecutor {
private:
    SmManager *sm_manager_;
    std::string tab_name_;                      // 表名称
    TabMeta tab_;                               // 表的元数据
    std::vector<Condition> fed_conds_;          // 扫描条件
    std::shared_ptr<RmFileHandle> fh_;          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度 
    std::vector<size_t> col_indices_;        // 在原始记录中的列索引

    IndexMeta index_meta_;                      // index scan涉及到的索引元数据
    Rid rid_;
    std::unique_ptr<RecScan> scan_;

    // 新增：用于存储扫描结果的缓存
    std::vector<std::unique_ptr<RmRecord>> result_cache_;
    mutable bool first_record_ = true;  // 是否是第一次读取记录
    size_t cache_index_ = 0;
    int max_match_col_count_;  // 最大匹配列数

public:
    IndexScanExecutor(SmManager *sm_manager, const std::string& tab_name, const std::vector<Condition> &conds, const IndexMeta& index_meta,
                      int max_match_col_count, Context *context) : AbstractExecutor(context), sm_manager_(sm_manager), 
        tab_name_(std::move(tab_name)), fed_conds_(std::move(conds)), index_meta_(std::move(index_meta)), max_match_col_count_(max_match_col_count) {

        // 增加错误检查
        try {
            tab_ = sm_manager_->db_.get_table(tab_name_);
        } catch (const std::exception &e) {
            throw InternalError("Failed to get table metadata: " + std::string(e.what()));
        }

        // 增加错误检查
        fh_ = sm_manager_->get_table_handle(tab_name_);

        // cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;

        // fed_conds_ = conds_;

        // 加表级意向共享锁
        // context_->lock_mgr_->lock_IS_on_table(context_->txn_, fh_->GetFd());
    }

    void setup_scan() {
        // 生成索引扫描的上下界
        char low_key[index_meta_.col_tot_len];
        char up_key[index_meta_.col_tot_len];
        generate_index_key(low_key, up_key);

        // 获取索引处理器
        auto index_handle = sm_manager_->get_index_handle(
            sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_meta_.cols));
        // 范围扫描设置边界
        Iid lower = index_handle->lower_bound(low_key);
        Iid upper = index_handle->upper_bound(up_key);

        scan_ = std::make_unique<IxScan>(index_handle, lower, upper, sm_manager_->get_bpm());
    }

    void generate_index_key(char* low_key, char* up_key) {
        std::unordered_map<std::string, int> index_names_map;
        std::vector<int> index_offsets;
        index_offsets.reserve(max_match_col_count_);
        int offset = 0;
        for (int id = 0; id < max_match_col_count_; ++id) {
            const auto &col = index_meta_.cols[id];
            index_names_map.emplace(col.name, id);
            index_offsets.push_back(offset);
            offset += col.len;
        }

        // for (size_t id = 0; id < index_meta_.cols.size(); ++id) {
        //     int offset = index_offsets[id];
        //     int col_len = index_meta_.cols[id].len;
        //     ColType col_type = index_meta_.cols[id].type;
        //     inject_max_value(up_key + offset, col_type, col_len);
        //     inject_min_value(low_key + offset, col_type, col_len);
        // }
        memcpy(low_key, index_meta_.min_val.get(), index_meta_.col_tot_len);
        memcpy(up_key, index_meta_.max_val.get(), index_meta_.col_tot_len);

        for (auto &cond : fed_conds_) {
            if(cond.is_rhs_val && cond.op != CompOp::OP_NE) {
                // 只处理右侧是值的条件
                auto iter = index_names_map.find(cond.lhs_col.col_name);
                if (iter == index_names_map.end()) {
                    continue; // 如果条件不涉及索引列，跳过
                }
                int offset = index_offsets[iter->second];
                int col_len = index_meta_.cols[iter->second].len;
                auto& rhs_val = cond.rhs_val;
                ColType col_type = index_meta_.cols[iter->second].type;

                switch (cond.op) {
                    case OP_EQ: {
                        // 等值条件设置精确边界
                        memcpy(low_key + offset, rhs_val.raw->data, col_len);
                        memcpy(up_key + offset, rhs_val.raw->data, col_len);
                        break;
                    }
                    case OP_LT: {
                        // 上界设置为条件值减1
                        memcpy(up_key + offset, rhs_val.raw->data, col_len);
                        decrement_key(up_key + offset, col_type, col_len);
                        break;
                    }
                    case OP_LE: {
                        // 直接使用条件值作为上界
                        memcpy(up_key + offset, rhs_val.raw->data, col_len);
                        break;
                    }
                    case OP_GT: {
                        // 下界设置为条件值加1
                        memcpy(low_key + offset, rhs_val.raw->data, col_len);
                        increment_key(low_key + offset, col_type, col_len);
                        break;
                    }
                    case OP_GE: {
                        // 直接使用条件值作为下界
                        memcpy(low_key + offset, rhs_val.raw->data, col_len);
                        break;
                    }
                    default:
                        throw InternalError("Unsupported operator for index scan");
                }
            }
        }

        // 使用双指针算法移除已经使用的条件
        auto left = fed_conds_.begin();
        auto right = fed_conds_.end();
        auto check = [&](const Condition &cond) {
            return cond.is_rhs_val && cond.op != CompOp::OP_NE &&
                   index_names_map.count(cond.lhs_col.col_name);
        };
        while (left < right) {
            if (check(*left)) {
                // 将右侧非目标元素换到左侧
                while (left < --right && check(*right));
                if (left >= right) break;
                *left = std::move(*right);
            }
            ++left;
        }
        fed_conds_.erase(left, fed_conds_.end());
    }

    // 辅助函数：键值操作
    // void inject_min_value(char* key, ColType type, int len) {
    //     switch (type) {
    //         case TYPE_INT:
    //             *reinterpret_cast<int*>(key) = std::numeric_limits<int>::min();
    //             break;
    //         case TYPE_FLOAT:
    //             *reinterpret_cast<float*>(key) = -std::numeric_limits<float>::infinity();
    //             break;
    //         case TYPE_STRING:
    //             memset(key, 0, len);
    //             break;
    //         case TYPE_DATETIME:
    //             memcpy(key, "0000-01-01 00:00:00", len);
    //             break;
    //     }
    // }

    // void inject_max_value(char* key, ColType type, int len) {
    //     switch (type) {
    //         case TYPE_INT:
    //             *reinterpret_cast<int*>(key) = std::numeric_limits<int>::max();
    //             break;
    //         case TYPE_FLOAT:
    //             *reinterpret_cast<float*>(key) = std::numeric_limits<float>::infinity();
    //             break;
    //         case TYPE_STRING:
    //             memset(key, 0xFF, len);
    //             break;
    //         case TYPE_DATETIME:
    //             memcpy(key, "9999-12-31 23:59:59", len);
    //             break;
    //     }
    // }

    void increment_key(char* key, ColType type, int len) {
        switch (type) {
            case TYPE_INT: {
                int& val = *reinterpret_cast<int*>(key);
                val = (val == std::numeric_limits<int>::max()) ? val : val + 1;
                break;
            }
            case TYPE_FLOAT: {
                float& val = *reinterpret_cast<float*>(key);
                val = std::nextafter(val, std::numeric_limits<float>::infinity());
                break;
            }
            case TYPE_STRING: {
                for (int i = len-1; i >= 0; --i) {
                    if (key[i] < 0xFF) {
                        ++key[i];
                        break;
                    }
                }
                break;
            }
            case TYPE_DATETIME:
                // 日期类型不支持自增，保持原值
                break;
        }
    }

    void decrement_key(char* key, ColType type, int len) {
        switch (type) {
            case TYPE_INT: {
                int& val = *reinterpret_cast<int*>(key);
                val = (val == std::numeric_limits<int>::min()) ? val : val - 1;
                break;
            }
            case TYPE_FLOAT: {
                float& val = *reinterpret_cast<float*>(key);
                val = std::nextafter(val, -std::numeric_limits<float>::infinity());
                break;
            }
            case TYPE_STRING: {
                for (int i = len-1; i >= 0; --i) {
                    if (key[i] > 0) {
                        --key[i];
                        break;
                    }
                }
                break;
            }
            case TYPE_DATETIME:
                // 日期类型不支持自减，保持原值
                break;
        }
    }

    // bool check_point_query(const char* low_key, const char* up_key) const {
    //     return memcmp(low_key, up_key, index_meta_.col_tot_len) == 0;
    // }

    /**
     * @brief 检查元组是否满足条件
     */
    bool check_con(Condition &cond, const RmRecord *record) {
        auto& lhs_col = get_col_meta(cond.lhs_col.col_name);
        char *lhs_data = record->data + lhs_col.offset;
        char *rhs_data = nullptr;
        ColType rhs_type;
        if (cond.is_rhs_val) {
            rhs_type = cond.rhs_val.type;
            rhs_data = cond.rhs_val.raw->data;
        } else {
            auto& rhs_col = get_col_meta(cond.rhs_col.col_name);
            rhs_type = rhs_col.type;
            rhs_data = record->data + rhs_col.offset;
        }
        int cmp = ix_compare(lhs_data, rhs_data, rhs_type, lhs_col.len);
        switch (cond.op) {
            case OP_EQ:
                return cmp == 0;
            case OP_NE:
                return cmp != 0;
            case OP_LT:
                return cmp < 0;
            case OP_LE:
                return cmp <= 0;
            case OP_GT:
                return cmp > 0;
            case OP_GE:
                return cmp >= 0;
            default:
                throw InternalError("Unknown op");
        }
    }

    ColMeta &get_col_meta(const std::string &col_name)
    {
        auto iter = tab_.cols_map.find(col_name);
        if (iter != tab_.cols_map.end())
            return tab_.cols.at(iter->second);
        throw ColumnNotFoundError(col_name);
    }

    /**
     * @brief 检查元组是否满足条件组
     */
    inline bool check_cons(std::vector<Condition> &conds, const RmRecord *record) {
        return std::all_of(conds.begin(), conds.end(), [&](auto &cond) {
            return check_con(cond, record);
        });
    }

    /**
     * @brief 获得第一个满足条件的元组，赋给rid_
     */
    void beginTuple() override {
        if (first_record_) {
            setup_scan();
            std::sort(fed_conds_.begin(), fed_conds_.end());
        } 
        cache_index_ = -1;
        nextTuple();
    }

    /**
     * @brief 获得下一个满足条件的元组，赋给rid_
     */
    void nextTuple() override {
        if (first_record_) {
            while (!scan_->is_end()) {
                rid_ = scan_->rid(); 
                auto record = fh_->get_record(rid_, context_);
                if (check_cons(fed_conds_, record.get())) {
                    if(context_->hasJoinFlag() || result_cache_.empty()) {
                        result_cache_.emplace_back(project(record));
                        ++cache_index_;
                    }
                    else
                    {
                        result_cache_[0] = project(record);
                    }
                    scan_->next();
                    return;
                }
                scan_->next();
            }
            rid_.page_no = INVALID_PAGE_ID;  // 如果没有找到满足条件的记录，设置为无效
        }
        ++cache_index_;
    }

    bool is_end() const override {
        if (first_record_) {
            bool end = (rid_.page_no == INVALID_PAGE_ID);
            if (end)
            {
                first_record_ = false;
                return true;
            }
            return false;
        }
        return cache_index_ >= result_cache_.size() || cache_index_ < 0;
    }

    std::unique_ptr<RmRecord> Next() override {
        if(cache_index_ >= 0 && cache_index_ < result_cache_.size()) {
            std::unique_ptr<RmRecord> record = std::make_unique<RmRecord>(*result_cache_[cache_index_]);
            return record;
        }
        return nullptr;  // 如果没有更多记录，返回空指针
    }

    const std::vector<ColMeta> &cols() const override {
        return (cols_.size() ? cols_ : tab_.cols); 
    }

    size_t tupleLen() const override {
        return len_;
    }

    Rid &rid() override { return rid_; }

     /**
     * @brief 批量获取满足条件的元组
     * @param batch_size 批量大小
     * @return 包含多个记录的向量
     */
    // std::vector<std::unique_ptr<RmRecord>> next_batch(size_t batch_size) {
    //     std::vector<std::unique_ptr<RmRecord>> batch;
    //     for (size_t i = 0; i < batch_size; ++i) {
    //         if (is_end()) {
    //             break;
    //         }
    //         auto record = Next();
    //         if (record) {
    //             batch.emplace_back(record);
    //             nextTuple();
    //         }
    //     }
    //     return batch;
    // }

    // /**
    //  * @brief 辅助方法，用于批量操作
    //  * @param batch_size 批量大小
    //  */
    // void next_tuple_batch(size_t batch_size) {
    //     for (size_t i = 0; i < batch_size; ++i) {
    //         if (is_end()) {
    //             break;
    //         }
    //         nextTuple();
    //     }
    // }

    ExecutionType type() const override { return ExecutionType::IndexScan; }

        void set_cols(const std::vector<TabCol> &sel_cols) override {
        auto &prev_cols = tab_.cols;
        cols_.reserve(sel_cols.size());
        col_indices_.reserve(sel_cols.size());

        for (auto &sel_col : sel_cols)
        {
            auto pos = get_col(prev_cols, sel_col);

            cols_.emplace_back(*pos);
            col_indices_.emplace_back(pos - prev_cols.begin());
        }
        len_ = 0;
        for (auto &col : cols_)
        {
            // 重新计算投影后的偏移
            col.offset = len_;
            len_ += col.len;
        }
    }

    std::unique_ptr<RmRecord> project(std::unique_ptr<RmRecord>& prev_record)
    {
        if(cols_.empty())
            return std::move(prev_record);
        
        // 创建新的投影记录
        auto projected_record = std::make_unique<RmRecord>(len_);

        // 获取原始记录的列信息
        const auto &prev_cols = tab_.cols;

        // 复制选定的列到新记录中
        for (size_t i = 0; i < cols_.size(); ++i)
        {
            const auto &src_col = prev_cols[col_indices_[i]];
            const auto &dst_col = cols_[i];

            // 从原记录复制数据到新记录
            memcpy(projected_record->data + dst_col.offset,
                   prev_record->data + src_col.offset,
                   src_col.len);
        }

        return projected_record;
    }
};