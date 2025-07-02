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

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include <cmath>
#include <sstream> // 用于 std::ostringstream
#include <iomanip> // 用于 std::setprecision, std::fixed

#include "defs.h"
#include "record/rm_defs.h"
#include "parser/ast.h"

// 浮点数精度常量
const int FLOAT_PRECISION = 6;
const float FLOAT_PRECISION_MULTIPLIER = std::pow(10, FLOAT_PRECISION);

struct TabCol
{
    std::string tab_name;
    std::string col_name;
    ast::AggFuncType aggFuncType;
    std::string alias;

    TabCol() = default;
    TabCol(const std::string &tab_name, const std::string &col_name,
           ast::AggFuncType agg_type = ast::AggFuncType::NO_TYPE, const std::string &alias = "")
        : tab_name(std::move(tab_name)), col_name(std::move(col_name)), aggFuncType(agg_type), alias(std::move(alias)) {}

    friend bool operator<(const TabCol &x, const TabCol &y)
    {
        return std::make_pair(x.tab_name, x.col_name) < std::make_pair(y.tab_name, y.col_name);
    }
};
inline int parse_int(const std::string &str)
{
    int result = 0;
    const char *cstr = str.c_str();
    bool negative = (*cstr == '-');
    if (negative)
        ++cstr;
    while (*cstr)
    {
        result = (result << 3) + (result << 1) + (*cstr - '0');
        ++cstr;
    }
    return negative ? -result : result;
}

inline float parse_float(const std::string &str)
{
    float result = 0.0f;
    float factor = 1.0f;
    const char *cstr = str.c_str();
    bool negative = (*cstr == '-');
    if (negative)
        ++cstr;
    bool decimal_found = false;
    while (*cstr)
    {
        if (*cstr == '.')
        {
            decimal_found = true;
            ++cstr;
            continue;
        }
        if (decimal_found)
        {
            factor *= 0.1f;
            result += (*cstr - '0') * factor;
        }
        else
        {
            result = result * 10.0f + (*cstr - '0');
        }
        ++cstr;
    }
    return negative ? -result : result;
}

struct Value
{
    ColType type; // type of value
    union
    {
        int int_val;     // int value
        float float_val; // float value
    };
    std::string str_val; // string value

    std::shared_ptr<RmRecord> raw; // raw record buffer

    void set_int(int int_val_)
    {
        type = TYPE_INT;
        int_val = int_val_;
    }

    void set_float(float float_val_)
    {
        type = TYPE_FLOAT;
        // 对浮点数进行精度处理
        double temp = std::round(static_cast<double>(float_val_) * FLOAT_PRECISION_MULTIPLIER) / FLOAT_PRECISION_MULTIPLIER;
        float_val = static_cast<float>(temp);
    }

    void set_str(const std::string &str_val_)
    {
        type = TYPE_STRING;
        str_val = std::move(str_val_);
    }

    // void set_datetime(const std::string &datetime_val_)
    // {
    //     type = TYPE_DATETIME;
    //     str_val = std::move(datetime_val_);
    // }
    void set_datetime()
    {
        type = TYPE_DATETIME;
        // str_val = std::move(datetime_val_);
    }

    void init_raw(int len)
    {
        assert(raw == nullptr);
        raw = std::make_shared<RmRecord>(len);
        switch (type)
        {
        case TYPE_INT:
            assert(len == sizeof(int));
            *(int *)(raw->data) = int_val;
            break;
        case TYPE_FLOAT:
            assert(len == sizeof(float));
            *(float *)(raw->data) = std::round(float_val * multiplier) / multiplier;
            break;
        case TYPE_DATETIME:
            assert(len == 19);
        case TYPE_STRING:
            if (len < (int)str_val.size())
            {
                throw StringOverflowError();
            }
            memcpy(raw->data, str_val.c_str(), str_val.size());
            if (len > (int)str_val.size())
                memset(raw->data + str_val.size(), 0, len - str_val.size());
            break;
        default:
            throw InternalError("Unsupported value type");
        }
    }

    void export_val(char *dest, int len)
    {
        switch (type)
        {
        case TYPE_INT:
            assert(len == sizeof(int));
            *(int *)(dest) = int_val;
            break;
        case TYPE_FLOAT:
            assert(len == sizeof(float));
            *(float *)(dest) = std::round(float_val * multiplier) / multiplier;
            break;
        case TYPE_DATETIME:
            assert(len == 19);
        case TYPE_STRING:
            if (len < (int)str_val.size())
            {
                throw StringOverflowError();
            }
            memcpy(dest, str_val.c_str(), str_val.size());
            if (len > (int)str_val.size())
                memset(dest + str_val.size(), 0, len - str_val.size());
            break;
        default:
            throw InternalError("Unsupported value type");
        }
    }

    bool operator<(const Value &other) const
    {
        assert(type == other.type);
        switch (type)
        {
        case TYPE_INT:
            return int_val < other.int_val;
        case TYPE_FLOAT:
            return float_val < other.float_val;
        case TYPE_STRING:
        case TYPE_DATETIME:
            return str_val < other.str_val;
        }
        return false;
    }
    bool operator==(const Value &other) const
    {
        assert(type == other.type);
        switch (type)
        {
        case TYPE_INT:
            return int_val == other.int_val;
        case TYPE_FLOAT:
            return float_val == other.float_val;
        case TYPE_STRING:
        case TYPE_DATETIME:
            return str_val == other.str_val;
        }
        return false;
    }

    bool operator>=(const Value &other) const
    {
        return !(*this < other);
    }

    bool operator<=(const Value &other) const
    {
        return (*this < other) || (*this == other);
    }

    bool operator>(const Value &other) const
    {
        return !(*this <= other);
    }

    bool operator!=(const Value &other) const
    {
        return !(*this == other);
    }

    void set_max(ColType type, int len)
    {
        this->type = type;
        switch (type)
        {
        case TYPE_INT:
            int_val = std::numeric_limits<int>::max();
            break;
        case TYPE_FLOAT:
            float_val = std::numeric_limits<float>::max();
            break;
        case TYPE_STRING:
            str_val.append(len, 255);
            break;
        case TYPE_DATETIME:
            str_val = "9999-12-31 23:59:59";
            break;
        }
    }
    void set_min(ColType type, int len)
    {
        this->type = type;
        switch (type)
        {
        case TYPE_INT:
            int_val = std::numeric_limits<int>::min();
            break;
        case TYPE_FLOAT:
            float_val = std::numeric_limits<float>::min();
            break;
        case TYPE_STRING:
            str_val.append(len, 0);
            break;
        case TYPE_DATETIME:
            str_val = "0000-01-01 00:00:00";
            break;
        }
    }
    Value() = default;
};
// 在 std 命名空间中特化 hash<Value>
namespace std
{
    template <>
    struct hash<Value>
    {
        size_t operator()(const Value &v) const
        {
            // 基于 Value 的类型和值计算哈希值
            size_t h = hash<int>()(static_cast<int>(v.type)); // 哈希类型
            switch (v.type)
            {
            case TYPE_INT:
                // 结合 int_val 的哈希值
                // 使用 boost::hash_combine 的思想来组合哈希值
                h ^= hash<int>()(v.int_val) + 0x9e3779b9 + (h << 6) + (h >> 2);
                break;
            case TYPE_FLOAT:
                // 结合 float_val 的哈希值
                h ^= hash<float>()(v.float_val) + 0x9e3779b9 + (h << 6) + (h >> 2);
                break;
            case TYPE_STRING:
            case TYPE_DATETIME:
                // 结合 str_val 的哈希值
                h ^= hash<string>()(v.str_val) + 0x9e3779b9 + (h << 6) + (h >> 2);
                break;
                // 可以添加对其他可能类型的处理
            }
            return h;
        }
    };
} // namespace std
class Query;
class Plan;
class PortalStmt;
// struct SubQuery
// {
//     std::shared_ptr<ast::SelectStmt> stmt;
//     std::shared_ptr<Query> query;
//     std::shared_ptr<Plan> plan;
//     std::shared_ptr<PortalStmt> portalStmt;

//     bool is_scalar = false;

//     ColType subquery_type;
//     std::unordered_set<Value> result;
// };
enum CompOp
{
    OP_EQ,
    OP_NE,
    OP_LT,
    OP_GT,
    OP_LE,
    OP_GE,
    // OP_IN,
    // OP_NOT_IN
};

struct Condition
{
    TabCol lhs_col;        // left-hand side column
    CompOp op;             // comparison operator
    bool is_rhs_val;       // true if right-hand side is a value (not a column)
    TabCol rhs_col;        // right-hand side column
    Value rhs_val;         // right-hand side value
    bool needSwap = false; // explain输出时使用
    // subquery
    // bool is_subquery = false;
    // std::shared_ptr<SubQuery> subQuery;

    int get_priority(CompOp op) const
    {
        switch (op)
        {
        case OP_EQ:
            return 0;
        case OP_LT:
            return 1;
        case OP_GT:
            return 2;
        case OP_LE:
            return 3;
        case OP_GE:
            return 4;
        case OP_NE:
            return 5;
        default:
            return 6; // 兜底
        }
    }
    bool operator<(const Condition &other) const
    {
        // 优先按操作符优先级排序
        int prio = get_priority(op);
        int other_prio = get_priority(other.op);
        if (prio != other_prio)
        {
            return prio < other_prio;
        }

        // 优先级相同则按 is_rhs_val 排序（值为 true 的排前面）
        if (is_rhs_val != other.is_rhs_val)
        {
            return is_rhs_val > other.is_rhs_val;
        }
        return false;
    }
};

struct SetClause
{
    TabCol lhs;
    Value rhs;
    ast::UpdateOp op;
};

struct JoinExpr
{
    std::string left;
    std::string right;
    std::vector<Condition> conds;
    JoinType type;
    JoinExpr() = default;
};