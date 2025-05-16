#include "skiplist.h"
#include <algorithm>

BaseIterator &SkipListIterator::operator++() {
    if (current_) {
        current_ = current_->next_[0];
        cached_value.reset();
    }
    return *this;
}

SkipListIterator::T& SkipListIterator::operator*() const {
    if (!current_)
        throw std::runtime_error("Dereferencing invalid iterator");
    if(!cached_value.has_value()) {
        cached_value = std::make_pair(current_->key_, current_->value_);
    }
    return cached_value.value();
}

IteratorType SkipListIterator::get_type() const {
    return IteratorType::SkipListIterator;
}

SkipListIterator::T *SkipListIterator::operator->() const
{
	return &operator*();//通过调用operator*来实现operator->
}

bool SkipListIterator::is_end() const 
{ 
    if(current_ == nullptr)
        return true;
    if(right_key.empty())
        return false;
    int cmp = lock_->compare_key(current_->key_, right_key);
    return (is_closed && cmp > 0) || (!is_closed && cmp >= 0);
}

SkipList::SkipList(LsmFileHdr *file_hdr, int max_height, size_t expected_num_items)
    : head_(std::make_shared<SkipListNode>("", Rid(), max_height_)), 
      max_height_(max_height),
      current_height_(1),
      bloom_filter_(std::make_shared<BloomFilter>(expected_num_items)),
      file_hdr_(file_hdr),
      entry_size(file_hdr->col_tot_len_ + sizeof(Rid)),
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
        if (next && compare_key(next->key_, key) < 0) {
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

void SkipList::put(const std::string& key, const Rid& value) {
    assert((int)key.size() == file_hdr_->col_tot_len_);
    std::vector<std::shared_ptr<SkipListNode>> prev(max_height_);
    auto node = FindGreaterOrEqual(key, &prev);
    
    if (node && compare_key(node->key_, key) == 0) {
        if(value.is_valid() && node->value_.is_valid()) {
            throw IndexEntryAlreadyExistError();
        }
        node->value_ = value;
        return;
    }
    
    int height = RandomHeight();
    auto new_node = std::make_shared<SkipListNode>(key, value, height);
    
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
    
    bloom_filter_->Add(key);
    size_bytes += entry_size;
    return;
}

bool SkipList::get(const std::string& key, Rid& value) const {
    if (!bloom_filter_->MayContain(key)) {
        return false;
    }
    
    auto current = head_;
    for (int i = current_height_ - 1; i >= 0; i--) {
        while (current->next_[i]) {
            auto next = current->next_[i];
            if (compare_key(next->key_, key) >= 0) break;
            current = next;
        }
    }
    
    current = current->next_[0];
    if (current && compare_key(current->key_, key) == 0) {
        return true;
    }

    return false;
}

void SkipList::remove(const std::string& key) {
    put(key, Rid());  // 使用空值标记删除
}

std::shared_ptr<SkipListIterator> SkipList::find(const std::string& key, bool is_closed) {
    auto node = FindGreaterOrEqual(key, nullptr);
    if(!is_closed && node != nullptr && compare_key(node->key_, key) == 0)
        node = node->next_[0];
    return std::make_shared<SkipListIterator>(node, shared_from_this());
}

std::shared_ptr<SkipListIterator> SkipList::find(const std::string &lower, bool is_lower_closed, 
    const std::string &upper, bool is_upper_closed) {
    auto node = FindGreaterOrEqual(lower, nullptr);
    if(!is_lower_closed && node != nullptr && compare_key(node->key_, lower) == 0)
        node = node->next_[0];
    return std::make_shared<SkipListIterator>(node, shared_from_this(), upper, is_upper_closed);
}

// 刷盘时可以直接遍历最底层链表
std::vector<std::pair<std::string, Rid>> SkipList::flush() {
    // std::shared_lock<std::shared_mutex> slock(rw_mutex);
  
    std::vector<std::pair<std::string, Rid>> data;
    auto node = head_->next_[0];
    data.reserve(size_bytes / entry_size);
    while (node)
    {
        data.emplace_back(node->key_, node->value_);
        node = node->next_[0];
    }
    return data;
}