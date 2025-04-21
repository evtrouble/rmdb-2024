#include "skiplist.h"
#include <algorithm>

SkipList::SkipList(LsmFileHdr *file_hdr, int max_height, size_t expected_num_items)
    : max_height_(max_height),
      current_height_(1),
      bloom_filter_(make_shared<BloomFilter>(expected_num_items)),
      file_hdr_(file_hdr),
      head_(std::make_shared<SkipListNode>("", Rid(), max_height_, 0)),
      gen_(std::random_device()()),
      dist_(0, 1) {}

int SkipList::RandomHeight() {
    int height = 1;
    while (height < max_height_ && dist_(gen_) == 1) {
        height++;
    }
    return height;
}

std::shared_ptr<SkipListNode> SkipList::FindGreaterOrEqual(
    const std::string& key,
    std::vector<std::shared_ptr<SkipListNode>>* prev) const {
    
    auto current = head_;
    int level = current_height_ - 1;
    
    while (true) {
        auto next = current->next_[level];
        if (next && compare_key(next->key, key) < 0) {
            current = next;
        } else {
            if (prev) {
                (*prev)[level] = current;
            }
            if (level == 0) {
                return next;
            }
            level--;
        }
    }
}

void SkipList::put(const std::string& key, const Rid& value, txn_id_t txn_id) {
    std::vector<std::shared_ptr<SkipListNode>> prev(max_height_);
    auto node = FindGreaterOrEqual(key, &prev);
    
    if (node && compare_key(node->key_, key) == 0) {
        if(value.is_valid() && node->value_.is_valid()) {
            throw IndexEntryAlreadyExistError();
        }
        node->value_ = value;
        node->txn_id_ = txn_id;
        return;
    }
    
    int height = RandomHeight();
    auto new_node = std::make_shared<SkipListNode>(key, value, height, txn_id);
    
    if (height > current_height_) {
        for (int i = current_height_; i < height; i++) {
            prev[i] = head_;
        }
        current_height_ = height;
    }
    
    for (int i = 0; i < height; i++) {
        new_node->next_[i] = prev[i]->next_[i];
        prev[i]->next_[i] = new_node;
    }
    
    bloom_filter_.Add(key);
    size_bytes += key.size() + sizeof(Rid) + sizeof(txn_id_t);
    return;
}

bool SkipList::get(const std::string& key, Rid& value, txn_id_t txn_id) const {
    if (!bloom_filter_.MayContain(key)) {
        return false;
    }
    
    auto current = head_;
    for (int i = current_height_ - 1; i >= 0; i--) {
        while (current->next_[i]) {
            auto next = current->next_[i];
            if (compare_key(next->key, key) >= 0) break;
            current = next;
        }
    }
    
    current = current->next_[0];
    // while (current && current->key_ == key) {
    //     if (current->txn_id_ <= txn_id) {
    //         value = current->value_;
    //         return true;
    //     }
    //     current = current->next_[0];
    // }
    
    if (current && compare_key(current->key_, key) == 0) {
        return true;
    }

    return false;
}

void SkipList::remove(const std::string& key, txn_id_t txn_id) {
    put(key, Rid(), txn_id);  // 使用空值标记删除
}

SkipListIterator SkipList::begin() const {
    return SkipListIterator(head_->next_[0]);
}

SkipListIterator SkipList::end() const {
    return SkipListIterator(nullptr);
}

SkipListIterator SkipList::find(const std::string& key, txn_id_t txn_id) const {
    auto node = FindGreaterOrEqual(key, nullptr);
    // while (node && node->key_ == key && node->txn_id_ > txn_id) {
    //     node = node->next_[0];
    // }
    return SkipListIterator(node);
}

// 找到前缀的起始位置
// 返回第一个前缀匹配或者大于前缀的迭代器
SkipListIterator SkipList::lower_bound(const std::string &key)
{
    auto node = FindGreaterOrEqual(key, nullptr);
    return SkipListIterator(node);
}
  
  // 找到前缀的终结位置
SkipListIterator SkipList::upper_bound(const std::string &key) 
{
    auto node = FindGreaterOrEqual(key, nullptr);
    
    while (node && compare_key(node->key_, key) == 0) {
        node = node->next_[0];
    }
    return SkipListIterator(node);
  }