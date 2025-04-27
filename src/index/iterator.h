#pragma once

#include <cstdint>
#include <vector>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <utility>
#include "defs.h"
#include "ix_defs.h"
#include "comparator.h"

enum class IteratorType {
  SkipListIterator,
  MemTableIterator,
  SstIterator,
  BlockIterator,
  HeapIterator,
  TwoMergeIterator,
  ConcactIterator,
  LevelIterator,
  MergeIterator
};

class BaseIterator {
public:
  using key_type = std::string;
  using value_type = Rid;
  using pointer = value_type *;
  using reference = value_type &;
  using T = std::pair<key_type, value_type>;

  virtual BaseIterator &operator++() = 0;
  virtual bool operator==(const BaseIterator &other) const = 0;
  virtual bool operator!=(const BaseIterator &other) const = 0;
  virtual T& operator*() const = 0;
  virtual T* operator->() const = 0;
  virtual IteratorType get_type() const = 0;
  virtual bool is_end() const = 0;
};

class MergeIterator : public BaseIterator
{
  constexpr static int block_size_ = 8192;
  // 多路归并
  std::vector<std::shared_ptr<BaseIterator>> iters_;
  LsmFileHdr *file_hdr_;

public:
  virtual BaseIterator &operator++() override;
  virtual bool operator==(const BaseIterator &other) const override;
  virtual bool operator!=(const BaseIterator &other) const override;
  virtual T& operator*() const override;
  virtual T* operator->() const override;
  virtual IteratorType get_type() const override;
  virtual bool is_end() const override;

  MergeIterator(std::vector<std::shared_ptr<BaseIterator>> &iters, LsmFileHdr *file_hdr, bool filter = false);

  private:
    struct Entry
    {
      std::string key;
      Rid value;
      size_t id;
    };
  bool compare_key(const Entry &entry1, const Entry &entry2) const {
    int res = ix_compare(entry1.key.c_str(), entry2.key.c_str(), file_hdr_->col_types_, file_hdr_->col_lens_);
    if(res == 0)
      return entry1.id > entry2.id;
    return res > 0;
  }
  std::priority_queue<Entry, std::vector<Entry>,
                      decltype(compare_key)> min_heap;

  inline int compare_key(const std::string &key1, const std::string &key2) const {
        return ix_compare(key1.c_str(), key2.c_str(), file_hdr_->col_types_, file_hdr_->col_lens_);
  }

  void fill(int num)
  {
    for (; cur_pos < iters_.size();cur_pos++)
    {
      while(min_heap.size() < num)
      {
        auto &iter = iters_[cur_pos];
        if (iter->is_end())
            break;
        min_heap.emplace(std::move((*iter)->first), (*iter)->second, cur_pos);
        ++(*iter);
      }
      if(min_heap.size() == num)
        return;
    }
  }

  mutable std::optional<T> cached_value; // 缓存当前值
  int cur_pos;
  bool filter_; // 是否过滤被删除的值
};