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

class IndexCacheScanExecutor : public AbstractExecutor {
private:
    SmManager *sm_manager_;
    std::string tab_name_;                      // 表名称
    TabMeta &tab_;                               // 表的元数据
    std::vector<Condition> fed_conds_;          // 扫描条件
    std::shared_ptr<RmFileHandle> fh_;          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度 
    std::vector<size_t> col_indices_;        // 在原始记录中的列索引

    IndexMeta index_meta_;                      // index scan涉及到的索引元数据
    std::unique_ptr<RecScan> scan_;

    int max_match_col_count_;  // 最大匹配列数
    size_t cache_index_ = INF;
    std::vector<std::unique_ptr<RmRecord>> result_cache_;

public:
    IndexCacheScanExecutor(SmManager *sm_manager, std::string &tab_name, std::vector<Condition> &conds, IndexMeta &index_meta,
                           int max_match_col_count, Context *context)
                            : AbstractExecutor(context), sm_manager_(sm_manager), tab_name_(std::move(tab_name)), 
                            tab_(sm_manager_->db_.get_table(tab_name_)), fed_conds_(std::move(conds)), index_meta_(std::move(index_meta)), max_match_col_count_(max_match_col_count)
    {
        // 增加错误检查
        fh_ = sm_manager_->get_table_handle(tab_name_);

        len_ = tab_.cols.back().offset + tab_.cols.back().len;

        // 加表级意向共享锁
        // context_->lock_mgr_->lock_IS_on_table(context_->txn_, fh_->GetFd());
        setup_scan();
    }

    void setup_scan() {
        // 生成索引扫描的上下界
        std::string low_key, up_key;
        low_key.resize(index_meta_.col_tot_len);
        up_key.resize(index_meta_.col_tot_len);

        generate_index_key(low_key.data(), up_key.data());

        // 获取索引处理器
        auto index_handle = sm_manager_->get_index_handle(
            sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_meta_.cols));
        // 范围扫描设置边界
        auto lower = index_handle->lower_bound(low_key.c_str());

        scan_ = std::make_unique<IxScan>(index_handle, lower.first, 
                lower.second, up_key, sm_manager_->get_bpm());
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
            case TYPE_STRING:
            case TYPE_DATETIME:
            {
                for (int i = len-1; i >= 0; --i) {
                    if (key[i] < 0xFF) {
                        ++key[i];
                        break;
                    }
                }
                break;
            }
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
            case TYPE_STRING:
            case TYPE_DATETIME:
            {
                for (int i = len-1; i >= 0; --i) {
                    if (key[i] > 0) {
                        --key[i];
                        break;
                    }
                }
                break;
            }
        }
    }

    // bool check_point_query(const char* low_key, const char* up_key) const {
    //     return memcmp(low_key, up_key, index_meta_.col_tot_len) == 0;
    // }

    /**
     * @brief 检查元组是否满足条件
     */
    bool check_con(const Condition &cond, const RmRecord *record) {
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
    inline bool check_cons(const std::vector<Condition> &conds, const RmRecord *record) {
        return std::all_of(conds.begin(), conds.end(), [&](auto &cond) {
            return check_con(cond, record);
        });
    }
    
    // 批量获取下一个batch_size个满足条件的元组，最少一页，最多batch_size且为页的整数倍
    std::vector<std::unique_ptr<RmRecord>> next_batch(size_t batch_size = BATCH_SIZE) override {
        std::vector<std::unique_ptr<RmRecord>> results;
        size_t num = std::min(batch_size, result_cache_.size() - cache_index_);
        if(num == 0)
            return results;
        results.reserve(num);
        for (size_t id = 0; id < num; id++) {
            auto &cache = result_cache_[cache_index_ + id];
            results.push_back(std::make_unique<RmRecord>(cache->data, cache->size, false));
        }
        cache_index_ += num;
        return results;
    }

    void beginTuple() override
    {
        if(cache_index_ == INF) {
            while (!scan_->is_end())
            {
                auto rid_batch = scan_->rid_batch();
                for (auto &rid : rid_batch) {
                    auto record = fh_->get_record(rid, context_);
                    if (check_cons(fed_conds_, record.get())) {
                        result_cache_.emplace_back(project(record));
                    }
                }
                scan_->next_batch();
            } 
        }
        cache_index_ = 0;
    }

    const std::vector<ColMeta> &cols() const override {
        return (cols_.size() ? cols_ : tab_.cols); 
    }

    size_t tupleLen() const override {
        return len_;
    }

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