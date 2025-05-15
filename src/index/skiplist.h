#pragma once
#include <memory>
#include <random>
#include <string>
#include <vector>
#include <optional>

#include "transaction.h"
#include "storage/bloom_filter.h"
#include "comparator.h"
#include "ix_defs.h"
#include "iterator.h"

struct SkipListNode {
    std::string key_;
    Rid value_;
    std::vector<std::shared_ptr<SkipListNode>> next_;

    SkipListNode(std::string key, const Rid& value, int height)
        : key_(std::move(key)), 
          value_(value), 
          next_(height, nullptr) {}
};

class SkipListIterator : public BaseIterator {
public:
    SkipListIterator(const std::shared_ptr<SkipListNode>& node, const std::shared_ptr<SkipList>& skiplist) 
        : current_(node), lock_(std::move(skiplist)) {}
    SkipListIterator(const std::shared_ptr<SkipListNode>& node, 
        const std::shared_ptr<SkipList>& skiplist, const std::string& upper, bool is_closed) 
        : current_(node), right_key(upper), is_closed(is_closed), lock_(std::move(skiplist)) {}
    SkipListIterator() : current_(nullptr) {}
    virtual BaseIterator &operator++() override;
    virtual T& operator*() const override;
    virtual T* operator->() const override;
    virtual IteratorType get_type() const override;
    virtual bool is_end() const override;
private:
    std::shared_ptr<SkipListNode> current_;
    std::string right_key;// 右键
    bool is_closed;
    std::shared_ptr<SkipList> lock_;       // 引用计数
    mutable std::optional<T> cached_value; // 缓存当前值
};

// 当前实现key和value的长度是固定的，且key在同一个跳表中是唯一的
// 当前操作能访问的所有节点对于当前事务都是可见的，由上层范围锁表保证，
// 任何大于当前时间戳的操作都不会影响当前操作需要访问的节点，任何小于当前时间戳的操作都已提交
// 故不需要储存时间戳来区分版本数据
// 当前上层读取和写入时都锁住了整个跳表，TODO:需要采用无锁实现
class SkipList : public std::enable_shared_from_this<SkipList>
{
    friend class SkipListIterator;

public:
    static constexpr int kMaxHeight = 12;
    
    explicit SkipList(LsmFileHdr *file_hdr, int max_height = kMaxHeight, size_t expected_num_items = 1000000);
    
    void put(const std::string& key, const Rid& value);
    bool get(const std::string& key, Rid& value) const;
    void remove(const std::string& key);
    
    std::shared_ptr<SkipListIterator> find(const std::string& key, bool is_closed);
    std::shared_ptr<SkipListIterator> find(const std::string &lower, bool is_lower_closed, 
        const std::string &upper, bool is_upper_closed);
    size_t SkipList::get_size() { return size_bytes; }
    std::vector<std::pair<std::string, Rid>> flush();

    std::shared_ptr<BloomFilter> bloom_filter() const {
        return bloom_filter_;
    }

private:
    int RandomHeight();
    std::shared_ptr<SkipListNode> FindGreaterOrEqual(const std::string& key, 
                                                    std::vector<std::shared_ptr<SkipListNode>>* prev) const;
    inline int compare_key(const std::string &key1, const std::string &key2) const {
        return ix_compare(key1.c_str(), key2.c_str(), file_hdr_->col_types_, file_hdr_->col_lens_);
    }

    std::shared_ptr<SkipListNode> head_;
    int max_height_;
    int current_height_;
    std::shared_ptr<BloomFilter> bloom_filter_;
    LsmFileHdr *file_hdr_;
    size_t entry_size;

    std::mt19937 gen_;
    std::uniform_int_distribution<> dist_;
    size_t size_bytes = 0;
};

