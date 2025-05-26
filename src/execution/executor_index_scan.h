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
    std::vector<Condition> fed_conds_;              // 扫描条件
    RmFileHandle *fh_;                          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度 
    // std::vector<Condition> fed_conds_;          // 扫描条件，和conds_字段相同 
    std::vector<Condition> index_conds_;        // index scan涉及到的条件
    bool is_con_closed_;                        // 是否闭合区间
    Condition con_closed_;                      // 闭合区间的条件

    IndexMeta index_meta_;                      // index scan涉及到的索引元数据
    Rid rid_;
    std::unique_ptr<RecScan> scan_;

    // 新增：用于存储扫描结果的缓存
    std::vector<std::unique_ptr<RmRecord>> result_cache_;
    mutable bool first_record_ = true;  // 是否是第一次读取记录
    size_t cache_index_ = 0;

    // 筛选可用于索引的条件
    void filterIndexConditions() {
        std::vector<Condition> eq_conds;
        std::vector<Condition> other_conds;
        std::unordered_set<std::string> used_col_names;  // 记录已经使用的列名

        for (auto &cond : fed_conds_) {
            if (cond.op == OP_EQ && cond.is_rhs_val) {
                eq_conds.emplace_back(cond);
            } else if (cond.op != OP_NE && cond.is_rhs_val) {
                other_conds.emplace_back(cond);
            }
        }

        for (auto &index_col : index_meta_.cols) {
            bool found = false;
            for (auto &eq_cond : eq_conds) {
                if (eq_cond.lhs_col.col_name.compare(index_col.name) == 0) {
                    found = true;
                    index_conds_.emplace_back(eq_cond);
                    used_col_names.insert(eq_cond.lhs_col.col_name);
                    break;
                }
            }
            if (found) continue;
            for (auto &other_cond : other_conds) {
                if (other_cond.lhs_col.col_name.compare(index_col.name) == 0) {
                    found = true;
                    index_conds_.emplace_back(other_cond);
                    used_col_names.insert(other_cond.lhs_col.col_name);
                    break;
                }
            }
            if (found) {
                CompOp op = index_conds_.back().op;
                switch (op) {
                    case OP_EQ:
                    case OP_NE:
                        assert(false);
                        break;
                    case OP_LT:
                        is_con_closed_ = findCondition(OP_LT, other_conds);
                        break;
                    case OP_GT:
                        is_con_closed_ = findCondition(OP_GT, other_conds);
                        break;
                    case OP_LE:
                        is_con_closed_ = findCondition(OP_LE, other_conds);
                        break;
                    case OP_GE:
                        is_con_closed_ = findCondition(OP_GE, other_conds);
                        break;
                }
            }
            break;
        }

        // 使用双指针算法移除已经使用的条件
        auto left = fed_conds_.begin();
        auto right = fed_conds_.end();
        auto check = [&](const Condition &cond) {
            return cond.is_rhs_val && cond.op != CompOp::OP_NE &&
                   used_col_names.count(cond.lhs_col.col_name);
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

public:
    IndexScanExecutor(SmManager *sm_manager, const std::string& tab_name, const std::vector<Condition> &conds, const IndexMeta& index_meta,
                      Context *context) : AbstractExecutor(context), sm_manager_(sm_manager), 
        tab_name_(std::move(tab_name)), fed_conds_(std::move(conds)), index_meta_(std::move(index_meta)) {

        // 增加错误检查
        try {
            tab_ = sm_manager_->db_.get_table(tab_name_);
        } catch (const std::exception &e) {
            throw InternalError("Failed to get table metadata: " + std::string(e.what()));
        }

        // 增加错误检查
        try {
            fh_ = sm_manager_->fhs_.at(tab_name_).get();
        } catch (const std::out_of_range &e) {
            throw InternalError("Failed to get file handle: " + std::string(e.what()));
        }

        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        is_con_closed_ = false;

        // fed_conds_ = conds_;
        filterIndexConditions();
        std::sort(fed_conds_.begin(), fed_conds_.end());

        // 加表级意向共享锁
        // context_->lock_mgr_->lock_IS_on_table(context_->txn_, fh_->GetFd());
    }

    /**
     * @brief 找出otherCondition中匹配符号的条件（第一个即可）
     * @param  op  操作符
     * @param  otherConditions  条件组
     * @return  返回是否找到
     */
    bool findCondition(CompOp op, std::vector<Condition> &otherConditions) {
        switch (op) {
            case OP_LT:
            case OP_LE:
                for (auto &cond : otherConditions) {
                    if (cond.op == OP_GE || cond.op == OP_GT) {
                        con_closed_ = cond;
                        return true;
                    }
                }
                break;
            case OP_GT:
            case OP_GE:
                for (auto &cond : otherConditions) {
                    if (cond.op == OP_LE || cond.op == OP_LT) {
                        con_closed_ = cond;
                        return true;
                    }
                }
                break;
            default:
                break;
        }
        return false;
    }

    /**
     * @brief 注入查找上界极限key
     * @param  col_name 字段名称
     * @param  key     注入key指针
     * @param  offset  字段偏移量
     * @param  len     字段长度
     */
    void injectHighKey(std::string col_name, char *key, int offset, int len) {
        int max_int = std::numeric_limits<int>::max();
        float max_float = std::numeric_limits<float>::max();
        const char *max_datetime = "9999-12-31 23:59:59";
        switch (tab_.get_col(col_name)->type) {
            case TYPE_INT: {
                memcpy(key + offset, &max_int, len);
                break;
            }
            case TYPE_FLOAT: {
                memcpy(key + offset, &max_float, len);
                break;
            }
            case TYPE_STRING: {
                std::fill(key + offset, key + offset + len, 0xff);
                break;
            }
            case TYPE_DATETIME:
                memcpy(key + offset, max_datetime, len);
                break;
        }
    }

    /**
     * @brief 注入查找下界极限key
     * @param  col_name 字段名称
     * @param  key     注入key指针
     * @param  offset  字段偏移量
     * @param  len     字段长度
     */
    void injectLowKey(std::string col_name, char *key, int offset, int len) {
        int min_int = std::numeric_limits<int>::min();
        float min_float = std::numeric_limits<float>::min();
        const char *min_datetime = "0000-01-01 00:00:00";
        switch (tab_.get_col(col_name)->type) {
            case TYPE_INT: {
                memcpy(key + offset, &min_int, len);
                break;
            }
            case TYPE_FLOAT: {
                memcpy(key + offset, &min_float, len);
                break;
            }
            case TYPE_STRING: {
                std::fill(key + offset, key + offset + len, 0x00);
                break;
            }
            case TYPE_DATETIME:
                memcpy(key + offset, min_datetime, len);
                break;
        }
    }

    /**
     * @brief 写入定位索引的上下界key
     * */
    void generate_index_key(char *low_key, char *up_key) {
        size_t i = 0;
        int offset = 0;
        for (; i < index_conds_.size(); ++i) {
            int len = index_meta_.cols[i].len;
            switch (index_conds_[i].op) {
                case OP_EQ:
                    memcpy(low_key + offset, index_conds_[i].rhs_val.raw->data, len);
                    memcpy(up_key + offset, index_conds_[i].rhs_val.raw->data, len);
                    offset += len;
                    break;
                case OP_LT: {
                    if (index_meta_.cols[i].type == TYPE_STRING) {
                        // 对于字符串类型，复制值并减一个字符
                        memcpy(up_key + offset, index_conds_[i].rhs_val.raw->data, len);
                        // 找到最后一个非空字符并减1
                        for (int j = len - 1; j >= 0; --j) {
                            if (up_key[offset + j] > 0) {
                                --up_key[offset + j];
                                break;
                            }
                        }
                    } else if (index_meta_.cols[i].type == TYPE_INT) {
                        int value;
                        memcpy(&value, index_conds_[i].rhs_val.raw->data, len);
                        value--;
                        memcpy(up_key + offset, &value, len);
                    } else if (index_meta_.cols[i].type == TYPE_FLOAT) {
                        float value;
                        memcpy(&value, index_conds_[i].rhs_val.raw->data, len);
                        value -= std::numeric_limits<float>::epsilon();
                        memcpy(up_key + offset, &value, len);
                    } else {
                        memcpy(up_key + offset, index_conds_[i].rhs_val.raw->data, len);
                    }
                    if (is_con_closed_) {
                        memcpy(low_key + offset, con_closed_.rhs_val.raw->data, len);
                    } else {
                        injectLowKey(index_conds_[i].lhs_col.col_name, low_key, offset, len);
                    }
                    offset += len;
                    break;
                }
                case OP_LE:
                    memcpy(up_key + offset, index_conds_[i].rhs_val.raw->data, len);
                    if (is_con_closed_) {
                        memcpy(low_key + offset, con_closed_.rhs_val.raw->data, len);
                    } else {
                        injectLowKey(index_conds_[i].lhs_col.col_name, low_key, offset, len);
                    }
                    offset += len;
                    break;
                case OP_GT: {
                    if (index_meta_.cols[i].type == TYPE_STRING) {
                        // 对于字符串类型，复制值并加一个字符
                        memcpy(low_key + offset, index_conds_[i].rhs_val.raw->data, len);
                        // 找到最后一个非空字符并加1
                        for (int j = len - 1; j >= 0; --j) {
                            if (low_key[offset + j] < 0xff) {
                                ++low_key[offset + j];
                                break;
                            }
                        }
                    } else if (index_meta_.cols[i].type == TYPE_INT) {
                        int value;
                        memcpy(&value, index_conds_[i].rhs_val.raw->data, len);
                        value++;
                        memcpy(low_key + offset, &value, len);
                    } else if (index_meta_.cols[i].type == TYPE_FLOAT) {
                        float value;
                        memcpy(&value, index_conds_[i].rhs_val.raw->data, len);
                        value += std::numeric_limits<float>::epsilon();
                        memcpy(low_key + offset, &value, len);
                    } else {
                        memcpy(low_key + offset, index_conds_[i].rhs_val.raw->data, len);
                    }
                    if (is_con_closed_) {
                        memcpy(up_key + offset, con_closed_.rhs_val.raw->data, len);
                    } else {
                        injectHighKey(index_conds_[i].lhs_col.col_name, up_key, offset, len);
                    }
                    offset += len;
                    break;
                }
                case OP_GE:
                    memcpy(low_key + offset, index_conds_[i].rhs_val.raw->data, len);
                    if (is_con_closed_) {
                        memcpy(up_key + offset, con_closed_.rhs_val.raw->data, len);
                    } else {
                        injectHighKey(index_conds_[i].lhs_col.col_name, up_key, offset, len);
                    }
                    offset += len;
                    break;
                default:
                    throw InternalError("Unknown op");
            }
        }
        for (; i < index_meta_.cols.size(); ++i) {
            int len = index_meta_.cols[i].len;
            injectLowKey(index_meta_.cols[i].name, low_key, offset, len);
            injectHighKey(index_meta_.cols[i].name, up_key, offset, len);
            offset += len;
        }
    }

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
        std::cout << "nm\n";
        return std::all_of(conds.begin(), conds.end(), [&](auto &cond) {
            return check_con(cond, record);
        });
    }

    /**
     * @brief 获得第一个满足条件的元组，赋给rid_
     */
    void beginTuple() override {
        if (first_record_) {
            auto index_handle = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_meta_.cols)).get();
            char low_key[index_meta_.col_tot_len];
            char up_key[index_meta_.col_tot_len];
            generate_index_key(low_key, up_key);
            int offset = 0;
            int res = 0;
            for (size_t i = 0; i < index_meta_.cols.size(); ++i) {
                res = ix_compare(low_key + offset, up_key + offset, index_meta_.cols[i].type, index_meta_.cols[i].len);
                if (res != 0)
                    break;
                offset += index_meta_.cols[i].len;
            }
            // 检查区间不为空集
            if (res > 0) {
                scan_ = std::make_unique<IxScan>(index_handle, index_handle->leaf_end(), index_handle->leaf_end(), sm_manager_->get_bpm());
            } else {
                scan_ = std::make_unique<IxScan>(index_handle, index_handle->lower_bound(low_key), index_handle->upper_bound(up_key), sm_manager_->get_bpm());
            }
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
                        result_cache_.emplace_back(std::move(record));
                        ++cache_index_;
                    }
                    else
                    {
                        result_cache_[0] = std::move(record);
                    }
                    scan_->next();
                    sm_manager_->get_bpm()->unpin_page({fh_->GetFd(), rid_.page_no}, false);
                    // context_->lock_mgr_->lock_shared_on_record(context_->txn_, rid_, fh_->GetFd());
                    return;
                }
                sm_manager_->get_bpm()->unpin_page({fh_->GetFd(), rid_.page_no}, false);
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
        return cols_;
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
};