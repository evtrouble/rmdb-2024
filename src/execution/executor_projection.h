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

class ProjectionExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;        // 投影节点的儿子节点
    std::vector<ColMeta> cols_;                     // 需要投影的字段
    size_t len_;                                    // 字段总长度
    std::vector<size_t> sel_idxs_;                  

   public:
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols) 
        : prev_(std::move(prev)), cols_(sel_cols.size()), sel_idxs_(sel_cols.size())
    {
        size_t curr_offset = 0;
        auto &prev_cols = prev_->cols();
        for (size_t id = 0; id < sel_cols.size(); id++)
        {
            auto pos = get_col(prev_cols, sel_cols[id]);
            sel_idxs_[id] = pos - prev_cols.begin();
            cols_[id] = *pos;
            cols_[id].offset = curr_offset;
            curr_offset += cols_[id].len;
        }
        len_ = curr_offset;
    }

    void beginTuple() override {}

    void nextTuple() override {}

    std::unique_ptr<RmRecord> Next() override {
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};