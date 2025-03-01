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
#include "common/common.h"
#include "index/ix.h"
#include "system/sm.h"

class AbstractExecutor
{
public:
    Rid _abstract_rid;

    Context *context_;

    virtual ~AbstractExecutor() = default;

    virtual size_t tupleLen() const { return 0; };

    virtual const std::vector<ColMeta> &cols() const
    {
        std::vector<ColMeta> *_cols = nullptr;
        return *_cols;
    };

    virtual std::string getType() { return "AbstractExecutor"; };

    virtual void beginTuple() {};

    virtual void nextTuple() {};

    virtual bool is_end() const { return true; };

    virtual Rid &rid() = 0;

    virtual std::unique_ptr<RmRecord> Next() = 0;

    virtual ColMeta get_col_offset(const TabCol &target) { return ColMeta(); };

    std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target)
    {
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col)
                                { return col.tab_name == target.tab_name && col.name == target.col_name; });
        if (pos == rec_cols.end())
        {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return pos;
    }

protected:
    // 检查条件是否满足的通用函数
    bool check_condition(char *lhs_value, ColType lhs_type, char *rhs_value, ColType rhs_type, CompOp op)
    {
        int cmp;

        // 处理INT和FLOAT类型之间的比较
        if ((lhs_type == TYPE_INT && rhs_type == TYPE_FLOAT) ||
            (lhs_type == TYPE_FLOAT && rhs_type == TYPE_INT))
        {
            float lhs_float, rhs_float;

            // 将INT转换为FLOAT进行比较
            if (lhs_type == TYPE_INT) {
                lhs_float = static_cast<float>(*(int *)lhs_value);
                rhs_float = *(float *)rhs_value;
            } else {
                lhs_float = *(float *)lhs_value;
                rhs_float = static_cast<float>(*(int *)rhs_value);
            }

            cmp = (lhs_float < rhs_float) ? -1 : ((lhs_float > rhs_float) ? 1 : 0);
        }
        else if (lhs_type != rhs_type)
        {
            throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
        }
        else
        {
            // 相同类型的比较
            switch (lhs_type)
            {
            case TYPE_INT:
            {
                int lhs = *(int *)lhs_value;
                int rhs = *(int *)rhs_value;
                cmp = lhs - rhs;
                break;
            }
            case TYPE_FLOAT:
            {
                float lhs = *(float *)lhs_value;
                float rhs = *(float *)rhs_value;
                cmp = (lhs < rhs) ? -1 : ((lhs > rhs) ? 1 : 0);
                break;
            }
            case TYPE_STRING:
            {
                cmp = memcmp(lhs_value, rhs_value, strlen(rhs_value));
                break;
            }
            default:
                throw InternalError("Unsupported column type");
            }
        }

        switch (op)
        {
        case OP_EQ:
            return cmp == 0;
        case OP_NE:
            return cmp != 0;
        case OP_LT:
            return cmp < 0;
        case OP_GT:
            return cmp > 0;
        case OP_LE:
            return cmp <= 0;
        case OP_GE:
            return cmp >= 0;
        default:
            throw InternalError("Unsupported comparison operator");
        }
    }
};