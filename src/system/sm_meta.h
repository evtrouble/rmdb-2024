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

#include <algorithm>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "errors.h"
#include "sm_defs.h"

/* 字段元数据 */
struct ColMeta {
    std::string tab_name;   // 字段所属表名称
    std::string name;       // 字段名称
    ColType type;           // 字段类型
    int len;                // 字段长度
    int offset;             // 字段位于记录中的偏移量

    friend std::ostream &operator<<(std::ostream &os, const ColMeta &col) {
        // ColMeta中有各个基本类型的变量，然后调用重载的这些变量的操作符<<（具体实现逻辑在defs.h）
        return os << col.tab_name << ' ' << col.name << ' ' << col.type << ' ' << col.len << ' ' << col.offset;
    }

    friend std::istream &operator>>(std::istream &is, ColMeta &col) {
        return is >> col.tab_name >> col.name >> col.type >> col.len >> col.offset;
    }
};

/* 索引元数据 */
struct IndexMeta {
    std::string tab_name;           // 索引所属表名称
    int col_tot_len;                // 索引字段长度总和
    int col_num;                    // 索引字段数量
    std::vector<ColMeta> cols;      // 索引包含的字段
    std::shared_ptr<char> max_val;
    std::shared_ptr<char> min_val;

    IndexMeta() = default;
    IndexMeta(const std::string &tab_name, int col_tot_len, int col_num, const std::vector<ColMeta>& cols)
        : tab_name(std::move(tab_name)), col_tot_len(col_tot_len), col_num(col_num),
        cols(std::move(cols)) {
            init();
    }

    friend std::ostream &operator<<(std::ostream &os, const IndexMeta &index)
    {
        os << index.tab_name << " " << index.col_tot_len << " " << index.col_num;
        for(auto& col: index.cols) {
            os << "\n" << col;
        }
        return os;
    }

    friend std::istream &operator>>(std::istream &is, IndexMeta &index) {
        is >> index.tab_name >> index.col_tot_len >> index.col_num;
        index.cols.reserve(index.col_num);
        for (int i = 0; i < index.col_num; ++i)
        {
            ColMeta col;
            is >> col;
            index.cols.emplace_back(std::move(col));
        }
        index.init();
        return is;
    }

    private:
    void init(){
        max_val = std::shared_ptr<char>(new char[col_tot_len], array_deleter);
        min_val = std::shared_ptr<char>(new char[col_tot_len], array_deleter);
        int offset = 0;
        for (auto &col : cols)
        {
            switch (col.type)
            {
                case ColType::TYPE_INT:
                    *reinterpret_cast<int*>(max_val.get() + offset) = std::numeric_limits<int>::max();
                    *reinterpret_cast<int*>(min_val.get() + offset) = std::numeric_limits<int>::min();  
                    break;  
                case ColType::TYPE_FLOAT:
                    *reinterpret_cast<float*>(max_val.get() + offset) = std::numeric_limits<float>::max();
                    *reinterpret_cast<float*>(min_val.get() + offset) = std::numeric_limits<float>::min();  
                    break;
                case ColType::TYPE_STRING:
                    std::memset(max_val.get() + offset, 0xff, col.len);
                    std::memset(min_val.get() + offset, 0, col.len);
                    break; 
                case ColType::TYPE_DATETIME:
                    static const char *max_datatime = "9999-12-31 23:59:59";
                    static const char *min_datatime = "0000-01-01 00:00:00";
                    std::memcpy(max_val.get() + offset, max_datatime, col.len);
                    std::memcpy(min_val.get() + offset, min_datatime, col.len);
                    break;
            }
            offset += col.len;
        }
    }

    static void array_deleter(char* p) {
        delete[] p;
    }
};

/* 表元数据 */
struct TabMeta {
    std::string name;                   // 表名称
    std::vector<ColMeta> cols;          // 表包含的字段
    std::vector<IndexMeta> indexes;     // 表上建立的索引
    std::unordered_map<std::string, size_t> cols_map;

    /* 判断当前表中是否存在名为col_name的字段 */
    bool is_col(const std::string &col_name) const {
        return cols_map.count(col_name);
    }

    /* 判断当前表上是否建有指定索引，索引包含的字段为col_names */
    bool is_index(const std::vector<std::string>& col_names) const {
        for(auto& index: indexes) {
            if(index.col_num == (int)col_names.size()) {
                int i = 0;
                for(; i < index.col_num; ++i) {
                    if(index.cols[i].name.compare(col_names[i]) != 0)
                        break;
                }
                if(i == index.col_num) return true;
            }
        }

        return false;
    }

    /* 根据字段名称集合获取索引元数据 */
    std::vector<IndexMeta>::iterator get_index_meta(const std::vector<ColMeta>& compare_index_cols) {
        for(auto index = indexes.begin(); index != indexes.end(); ++index) {
            if((*index).col_num != (int)compare_index_cols.size()) continue;
            auto& index_cols = (*index).cols;
            size_t i = 0;
            for(; i < index_cols.size(); ++i) {
                if(index_cols[i].name.compare(compare_index_cols[i].name) != 0) 
                    break;
            }
            if(i == compare_index_cols.size()) return index;
        }
        std::vector<std::string> col_names(compare_index_cols.size());
        for (size_t id = 0; id < compare_index_cols.size(); ++id)
            col_names[id] = std::move(compare_index_cols[id].name);
        throw IndexNotFoundError(name, col_names);
    }

    std::vector<IndexMeta>::iterator get_index_meta(const std::vector<std::string>& col_names) {
        for(auto index = indexes.begin(); index != indexes.end(); ++index) {
            if((*index).col_num != (int)col_names.size()) continue;
            auto& index_cols = (*index).cols;
            size_t i = 0;
            for(; i < col_names.size(); ++i) {
                if(index_cols[i].name.compare(col_names[i]) != 0) 
                    break;
            }
            if(i == col_names.size()) return index;
        }
        throw IndexNotFoundError(name, col_names);
    }

    /* 根据字段名称获取字段元数据 */
    std::vector<ColMeta>::iterator get_col(const std::string &col_name) {
        auto iter = cols_map.find(col_name);
        if (iter == cols_map.end()) {
            throw ColumnNotFoundError(col_name);
        }
        return cols.begin() + iter->second;
    }

    friend std::ostream &operator<<(std::ostream &os, const TabMeta &tab) {
        os << tab.name << '\n' << tab.cols.size() << '\n';
        for (auto &col : tab.cols) {
            os << col << '\n';  // col是ColMeta类型，然后调用重载的ColMeta的操作符<<
        }
        os << tab.indexes.size() << "\n";
        for (auto &index : tab.indexes) {
            os << index << "\n";
        }
        return os;
    }

    friend std::istream &operator>>(std::istream &is, TabMeta &tab) {
        size_t n;
        is >> tab.name >> n;
        tab.cols.reserve(n);
        for (size_t i = 0; i < n; ++i)
        {
            ColMeta col;
            is >> col;
            tab.cols.emplace_back(std::move(col));
            tab.cols_map.emplace(tab.cols.back().name, tab.cols.size() - 1);
        }
        is >> n;
        tab.indexes.reserve(n);
        for (size_t i = 0; i < n; ++i)
        {
            IndexMeta index;
            is >> index;
            tab.indexes.emplace_back(std::move(index));
        }
        return is;
    }

    size_t get_col_num() const {
        return cols.size();
    }
};

// 注意重载了操作符 << 和 >>，这需要更底层同样重载TabMeta、ColMeta的操作符 << 和 >>
/* 数据库元数据 */
class DbMeta {
    friend class SmManager;

   private:
    std::string name_;                      // 数据库名称
    std::map<std::string, TabMeta> tabs_;   // 数据库中包含的表

   public:
    // DbMeta(std::string name) : name_(name) {}

    /* 判断数据库中是否存在指定名称的表 */
    bool is_table(const std::string &tab_name) const { return tabs_.find(tab_name) != tabs_.end(); }

    void SetTabMeta(const std::string &tab_name, const TabMeta &meta) {
        tabs_.emplace(tab_name, meta);
    }

    /* 获取指定名称表的元数据 */
    TabMeta &get_table(const std::string &tab_name) {
        auto pos = tabs_.find(tab_name);
        if (pos == tabs_.end()) {
            throw TableNotFoundError(tab_name);
        }

        return pos->second;
    }

    // 重载操作符 <<
    friend std::ostream &operator<<(std::ostream &os, const DbMeta &db_meta) {
        os << db_meta.name_ << '\n' << db_meta.tabs_.size() << '\n';
        for (auto &entry : db_meta.tabs_) {
            os << entry.second << '\n';
        }
        return os;
    }

    friend std::istream &operator>>(std::istream &is, DbMeta &db_meta) {
        size_t n;
        is >> db_meta.name_ >> n;
        for (size_t i = 0; i < n; ++i) {
            TabMeta tab;
            is >> tab;
            db_meta.tabs_.emplace(tab.name, std::move(tab));
        }
        return is;
    }
};
