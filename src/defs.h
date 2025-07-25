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

#include <iostream>
#include <map>
#include <memory>
#include <vector>

// 此处重载了<<操作符，在ColMeta中进行了调用
template <typename T, typename = typename std::enable_if<std::is_enum<T>::value, T>::type>
std::ostream &operator<<(std::ostream &os, const T &enum_val)
{
    os << static_cast<int>(enum_val);
    return os;
}

template <typename T, typename = typename std::enable_if<std::is_enum<T>::value, T>::type>
std::istream &operator>>(std::istream &is, T &enum_val)
{
    int int_val;
    is >> int_val;
    enum_val = static_cast<T>(int_val);
    return is;
}

struct Rid
{
    int page_no;
    int slot_no;

    friend bool operator==(const Rid &x, const Rid &y)
    {
        return x.page_no == y.page_no && x.slot_no == y.slot_no;
    }

    friend bool operator!=(const Rid &x, const Rid &y) { return !(x == y); }
};

namespace std {
    template<>
    struct hash<Rid> {
        size_t operator()(const Rid& x) const {
            return (x.page_no << 16) | x.slot_no;
        }
    };
}
// inline std::ostream &operator<<(std::ostream &os, const Rid &rid)
// {
//     os << "(" << rid.page_no << ", " << rid.slot_no << ")";
//     return os;
// }

// inline std::ostream &operator<<(std::ostream &os, const Rid &rid)
// {
//     os << "(" << rid.page_no << ", " << rid.slot_no << ")";
//     return os;
// }

enum ColType
{
    TYPE_INT,
    TYPE_FLOAT,
    TYPE_STRING,
    TYPE_DATETIME
};

inline std::string coltype2str(ColType type)
{
    std::map<ColType, std::string> m = {
        {TYPE_INT, "INT"},
        {TYPE_FLOAT, "FLOAT"},
        {TYPE_STRING, "STRING"},
        {TYPE_DATETIME, "DATETIME"}};
    return m.at(type);
}

struct RmRecord;

class RecScan
{
public:
    virtual ~RecScan() = default;

    virtual void next() = 0;

    virtual bool is_end() const = 0;

    virtual Rid rid() const = 0;

    // 单记录访问
    virtual void record(std::unique_ptr<RmRecord> &out) = 0;
    virtual std::unique_ptr<RmRecord> &get_record() = 0;

    virtual void next_batch() = 0;

    // 批量访问接口
    virtual std::vector<Rid> rid_batch() const = 0;
    virtual std::vector<std::unique_ptr<RmRecord>> record_batch() = 0;
};