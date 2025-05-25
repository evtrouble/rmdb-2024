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

class UpdateExecutor : public AbstractExecutor
{
private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;
    std::unordered_set<int> changes;

public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, const std::vector<SetClause> &set_clauses,
                   const std::vector<Condition> &conds, const std::vector<Rid> &rids, Context *context)
        : AbstractExecutor(context), rids_(rids), tab_name_(tab_name),
          set_clauses_(set_clauses), conds_(conds), sm_manager_(sm_manager)
    {
        tab_ = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();

        // 检查每个要更新的列的类型是否匹配
        for (auto &set_clause : set_clauses_)
        {
            // 找到要更新的列的元数据
            auto col = tab_.get_col(set_clause.lhs.col_name);
            // 检查类型是否匹配
            if (col->type != set_clause.rhs.type)
            {
                throw IncompatibleTypeError(coltype2str(col->type), coltype2str(set_clause.rhs.type));
            }
            changes.insert(col->offset);
        }
    }
    std::unique_ptr<RmRecord> Next() override
    {

        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};