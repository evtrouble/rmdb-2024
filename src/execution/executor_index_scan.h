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
    std::string tab_name_;                      // 表名称
    TabMeta tab_;                               // 表的元数据
    std::vector<Condition> conds_;              // 扫描条件
    RmFileHandle *fh_;                          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;          // 扫描条件，和conds_字段相同

    IndexMeta index_meta_;                      // index scan涉及到的索引元数据
    bool range_scan_;
    char *lower_key_;
    char *upper_key_;

    Iid lower_position_{}; // 用于存储索引扫描的起始位置
    Iid upper_position_{}; // 用于存储索引扫描的结束位置

    Rid rid_;
    std::unique_ptr<RecScan> scan_;

    SmManager *sm_manager_;

   public:
    IndexScanExecutor(SmManager *sm_manager, const std::string &tab_name, 
        const std::vector<Condition> &conds, const IndexMeta &index_meta,
        Context *context) 
        : AbstractExecutor(context), tab_name_(std::move(tab_name)), 
        index_meta_(std::move(index_meta)), sm_manager_(sm_manager)
    {
        tab_ = sm_manager_->db_.get_table(tab_name_);
        // index_no_ = index_no;
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        
        // std::unordered_map<std::string, std::vector<size_t>> conds_map;
        // for (size_t id = 0; id < conds.size(); id++) {
        //     auto iter = conds_map.try_emplace(
        //         std::move(conds.at(id).lhs_col.col_name), std::vector<size_t>()).first;
        //     iter->second.emplace_back(id);
        // }

        // size_t col_id;
        // for (col_id = 0; col_id < index_col_names_.size(); col_id++)
        // {
        //     auto iter = conds_map.find(index_col_names_[col_id]);
        //     if (iter == conds_map.end())
        //         break;
        //     for(auto id : iter->second) {

        //     }
        // }
        // for (auto &cond : conds_)
        //     for (auto &cond : conds_)
        //     {
        //         if (cond.lhs_col.tab_name != tab_name_)
        //         {
        //             // lhs is on other table, now rhs must be on this table
        //             assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
        //             // swap lhs and rhs
        //             std::swap(cond.lhs_col, cond.rhs_col);
        //             cond.op = swap_op.at(cond.op);
        //         }
        //     }
        fed_conds_ = conds_;
    }

    void beginTuple() override {
        
    }

    void nextTuple() override {
        
    }

    std::unique_ptr<RmRecord> Next() override {
        return nullptr;
    }

    Rid &rid() override { return rid_; }

    const std::vector<ColMeta> &cols() const override {
        return cols_;
    };
};