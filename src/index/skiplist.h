#pragma once
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "transaction.h"
#include "bloom_filter.h"
#include "comparator.h"
#include "lsmtree.h"

struct SkipListNode {
    std::string key_;
    Rid value_;
    txn_id_t txn_id_;
    std::vector<std::shared_ptr<SkipListNode>> next_;

    SkipListNode(std::string key, const Rid& value, int height, txn_id_t txn_id)
        : key_(std::move(key)), 
          value_(value), 
          txn_id_(txn_id),
          next_(height, nullptr) {}
};

class SkipListIterator {
public:
    explicit SkipListIterator(std::shared_ptr<SkipListNode> node) 
        : current_(node) {}
    
    void Next() { 
        if (current_) {
            current_ = current_->next_[0];
        }
    }
    
    bool Valid() const { return current_ != nullptr && !current_->key_.empty(); }
    bool IsEnd() const { return current_ == nullptr; }
    
    std::string Key() const { return current_->key_; }
    Rid Value() const { return current_->value_; }
    txn_id_t Txn_id() const { return current_->txn_id_; }
    
private:
    std::shared_ptr<SkipListNode> current_;
};

// 当前实现key和value的长度是固定的，且key在同一个跳表中是唯一的
// 储存txn_id用于合并时删除不需要的版本（不同跳表中含有的不同版本数据）
// 当前操作能访问的所有节点对于当前事务都是可见的，由上层范围锁表保证，
// 任何大于当前时间戳的操作都不会影响当前操作需要访问的节点，任何小于当前时间戳的操作都已提交
// 故不需要储存时间戳来区分版本数据
// 当前上层读取和写入时都锁住了整个跳表，TODO:可以考虑分段锁
class SkipList {
public:
    static constexpr int kMaxHeight = 12;
    
    explicit SkipList(LsmFileHdr *file_hdr, int max_height = kMaxHeight, size_t expected_num_items = 1000000);
    
    void put(const std::string& key, const Rid& value, txn_id_t txn_id);
    bool get(const std::string& key, Rid& value, txn_id_t txn_id) const;
    void remove(const std::string& key, txn_id_t txn_id);
    
    SkipListIterator begin() const;
    SkipListIterator end() const;
    SkipListIterator find(const std::string& key, txn_id_t txn_id) const;
    size_t SkipList::get_size() { return size_bytes; }
    std::vector<std::tuple<std::string, std::string, uint64_t>> flush();
    SkipListIterator lower_bound(const std::string &key);
    SkipListIterator upper_bound(const std::string &key);

private:
    int RandomHeight();
    std::shared_ptr<SkipListNode> FindGreaterOrEqual(const std::string& key, 
                                                    std::vector<std::shared_ptr<SkipListNode>>* prev) const;
    inline int compare_key(const std::string &key1, const std::string &key2) {
        return ix_compare(key1.c_str(), key2.c_str(), file_hdr_->col_types_, file_hdr_->col_lens_);
    }

    std::shared_ptr<SkipListNode> head_;
    int max_height_;
    int current_height_;
    std::shared_ptr<BloomFilter> bloom_filter_;
    LsmFileHdr *file_hdr_;

    std::mt19937 gen_;
    std::uniform_int_distribution<> dist_;
    size_t size_bytes = 0;
};

