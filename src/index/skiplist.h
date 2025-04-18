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
    timestamp_t ts_;
    std::vector<std::shared_ptr<SkipListNode>> next_;

    SkipListNode(std::string key, const Rid& value, int height, timestamp_t ts)
        : key_(std::move(key)), 
          value_(value), 
          ts_(ts),
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
    timestamp_t Ts() const { return current_->ts_; }
    
private:
    std::shared_ptr<SkipListNode> current_;
};

class SkipList {
public:
    static constexpr int kMaxHeight = 12;
    
    explicit SkipList(LsmFileHdr *file_hdr, int max_height = kMaxHeight, size_t expected_num_items = 1000000);
    
    void put(const std::string& key, const Rid& value, timestamp_t ts);
    bool get(const std::string& key, Rid& value, timestamp_t ts) const;
    void remove(const std::string& key, timestamp_t ts);
    
    SkipListIterator begin() const;
    SkipListIterator end() const;
    SkipListIterator find(const std::string& key, timestamp_t ts) const;
    size_t SkipList::get_size() { return size_bytes; }
    
private:
    int RandomHeight();
    std::shared_ptr<SkipListNode> FindGreaterOrEqual(const std::string& key, 
                                                    std::vector<std::shared_ptr<SkipListNode>>* prev) const;
    int Compare(const SkipListNode& a, const SkipListNode& b) const {
        int res = ix_compare(a.key_.c_str(), b.key_.c_str(), file_hdr_->col_types_, file_hdr_->col_lens_);
        if(res == 0)
        {
            if(a.txn_id_ < b.txn_id_)
                return 1;
            if(a.txn_id_ > b.txn_id_)
                return -1;
            return 0;
        }
        return res;
    }

    std::shared_ptr<SkipListNode> head_;
    int max_height_;
    int current_height_;
    BloomFilter bloom_filter_;
    LsmFileHdr *file_hdr_;

    std::mt19937 gen_;
    std::uniform_int_distribution<> dist_;
    size_t size_bytes = 0;
};

