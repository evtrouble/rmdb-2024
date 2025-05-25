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

class IndexScanExecutor : public AbstractExecutor
{
private:
    std::string tab_name_;             // 表名称
    TabMeta tab_;                      // 表的元数据
    std::vector<Condition> conds_;     // 扫描条件
    RmFileHandle *fh_;                 // 表的数据文件句柄
    std::vector<ColMeta> cols_;        // 需要读取的字段
    size_t len_;                       // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_; // 扫描条件，和conds_字段相同

    std::vector<std::string> index_col_names_; // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                     // index scan涉及到的索引元数据
    IxIndexHandle *index_handle_;
    Iid lower_position_{}; // 用于存储索引扫描的起始位置
    Iid upper_position_{}; // 用于存储索引扫描的结束位置

    Rid rid_;
    std::unique_ptr<RecScan> scan_;

    SmManager *sm_manager_;
    bool effective = true;
    bool first = true;
    bool is_point = false; // 点查
    // 获取指定列的值
    inline char *get_col_value(const RmRecord *rec, const ColMeta &col)
    {
        return rec->data + col.offset;
    }
    // 查找列的元数据
    ColMeta &get_col_meta(const std::string &col_name)
    {
        auto iter = tab_.cols_map.find(col_name);
        if (iter != tab_.cols_map.end())
            return tab_.cols[iter->second];
        throw ColumnNotFoundError(col_name);
    }

    // 检查记录是否满足所有条件
    bool satisfy_conditions(const RmRecord *rec)
    {
        return std::all_of(conds_.begin(), conds_.end(), [&](auto &cond)
                           {
            ColMeta &left_col = get_col_meta(cond.lhs_col.col_name);
            char *lhs_value = get_col_value(rec, left_col);
            char *rhs_value;
            ColType rhs_type;

            if (cond.is_rhs_val)
            {
                // 如果右侧是值
                rhs_value = const_cast<char *>(cond.rhs_val.raw->data);
                rhs_type = cond.rhs_val.type;
            }
            else
            {
                // 如果右侧是列
                ColMeta &right_col = get_col_meta(cond.rhs_col.col_name);
                rhs_value = get_col_value(rec, right_col);
                rhs_type = right_col.type;
            }
            return check_condition(lhs_value, left_col.type, rhs_value, rhs_type, cond.op); });
    }

public:
    IndexScanExecutor(SmManager *sm_manager, const std::string &tab_name,
                      const std::vector<Condition> &conds, const IndexMeta &index_meta,
                      Context *context)
        : AbstractExecutor(context), tab_name_(std::move(tab_name)), conds_(std::move(conds)),
          index_meta_(std::move(index_meta)), sm_manager_(sm_manager), first(true)
    {
        tab_ = sm_manager_->db_.get_table(tab_name_);
        // index_no_ = index_no;
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        len_ = tab_.cols.back().offset + tab_.cols.back().len;

        index_handle_ = sm_manager_->ihs_.at(
                                             sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_meta_.cols))
                            .get();
    }

    void beginTuple() override
    {
        if (!effective)
        {
            rid_.page_no = RM_NO_PAGE; // 设置 rid_ 以指示结束
            return;
        }
        if (first)
        {
            set_index_scan();

            if (is_point)
                return;
            std::sort(conds_.begin(), conds_.end());

#ifdef DEBUG
            std::cout << lower_position_.page_no << ' ' << lower_position_.slot_no << "\n"
                      << upper_position_.page_no << ' ' << upper_position_.slot_no << "\n";
#endif
        }
        scan_ = std::make_unique<IxScan>(index_handle_, lower_position_, upper_position_, sm_manager_->get_bpm());
        find_next_valid_tuple();
    }

    void nextTuple() override
    {
        if (is_point)
        {
            rid_.page_no = RM_NO_PAGE; // 设置 rid_ 以指示结束
            return;
        }

        scan_->next();
        find_next_valid_tuple();
    }
    bool is_end() const override { return rid_.page_no == RM_NO_PAGE; }

    std::unique_ptr<RmRecord> Next() override
    {
        if (!is_end())
        {
            std::unique_ptr<RmRecord> rid_record = fh_->get_record(rid_, context_);
            std::unique_ptr<RmRecord> ret = std::make_unique<RmRecord>(rid_record->size, rid_record->data);
            sm_manager_->get_bpm()->unpin_page({fh_->GetFd(), rid_.page_no}, false);
            return ret;
        }
        return nullptr;
    }
    Rid &rid() override { return rid_; }
    const std::vector<ColMeta> &cols() const override
    {
        return tab_.cols;
    };

private:
    void find_next_valid_tuple()
    {
        while (!scan_->is_end())
        {
            rid_ = scan_->rid();
            std::unique_ptr<RmRecord> rid_record = fh_->get_record(rid_, context_);
            if (satisfy_conditions(rid_record.get()))
            {
                sm_manager_->get_bpm()->unpin_page({fh_->GetFd(), rid_.page_no}, false);
                return;
            }
            sm_manager_->get_bpm()->unpin_page({fh_->GetFd(), rid_.page_no}, false);
            scan_->next();
        }
        rid_.page_no = RM_NO_PAGE; // 设置 rid_ 以指示结束
    }

    void set_index_scan()
    {
        lower_position_ = index_handle_->leaf_begin();
        upper_position_ = index_handle_->leaf_end();

        char low_key[index_meta_.col_tot_len];
        char up_key[index_meta_.col_tot_len];
        std::memcpy(low_key, index_meta_.min_val.get(), index_meta_.col_tot_len);
        std::memcpy(up_key, index_meta_.max_val.get(), index_meta_.col_tot_len);
        std::unordered_set<std::string> erase_cond;
        int offset = 0;
        bool init = false;

        // 直接使用索引的最小值和最大值
        for (auto &col : index_meta_.cols)
        {
            erase_cond.emplace(col.name);
            offset += col.len;
        }

        // 设置扫描范围
        set_position(low_key, true, up_key, false);
        init = true;

        is_point = !init;
        if (is_point && !index_handle_->get_value(low_key, rid_, nullptr))
        {
            rid_.page_no = RM_NO_PAGE; // 设置 rid_ 以指示结束
        }

        auto left = conds_.begin();
        auto right = conds_.end();
        auto check = [&](const Condition &cond)
        {
            return cond.is_rhs_val && cond.op != CompOp::OP_NE &&
                   erase_cond.count(cond.lhs_col.col_name);
        };
        while (left < right)
        {
            if (check(*left))
            {
                // 将右侧非目标元素换到左侧
                while (left < --right && check(*right))
                    ;
                if (left >= right)
                    break;
                *left = std::move(*right);
            }
            ++left;
        }
        conds_.erase(left, conds_.end());
    }

    void set_position(char *lower, bool is_lower_closed, char *upper, bool is_upper_closed)
    {
        if (is_lower_closed)
            lower_position_ = index_handle_->lower_bound(lower);
        else
            lower_position_ = index_handle_->upper_bound(lower);

        if (is_upper_closed)
            upper_position_ = index_handle_->upper_bound(upper);
        else
            upper_position_ = index_handle_->lower_bound(upper);
    }

    ExecutionType type() const override { return ExecutionType::IndexScan; }
};