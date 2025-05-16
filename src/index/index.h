#pragma once

#include "ix_defs.h"
#include "transaction/transaction.h"
#include <string>
#include <vector>

class Index {
public:
    // 析构函数声明为虚函数，确保正确释放派生类对象
    virtual ~Index() = default;

    // 查找操作，返回键对应的值是否存在
    virtual bool get_value(const std::string &key, Rid &value, Transaction *transaction) = 0;

    // 批量查找操作
    virtual bool get_value(const std::vector<std::string> &keys, std::vector<Rid> &values, Transaction *transaction) = 0;

    // 插入操作
    virtual void insert_entry(const std::string &key, const Rid &value, Transaction *transaction) = 0;

    // 批量插入操作
    virtual void insert_entry(const std::vector<std::pair<std::string, Rid>> &kvs, Transaction *transaction) = 0;

    // 删除操作
    virtual void remove(const std::string &key, Transaction *transaction) = 0;

    // 批量删除操作
    virtual void remove_batch(const std::vector<std::string> &keys, Transaction *transaction) = 0;
};