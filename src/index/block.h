#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "ix_defs.h"
#include "transaction.h"
#include "iterator.h"
#include "comparator.h"

/*
key和value的存储格式（长度固定）
----------------------------------------------
|          Data Section     |     Extra      |
----------------------------------------------
|Entry#1|Entry#2|...|Entry#N|num_of_elements |
----------------------------------------------

-------------------------------
|              Entry #1 | ... |
------------------------------|
|key(keylen)|val(vallen)| ... |
-------------------------------
*/

class BlockIterator : public BaseIterator {
  public:
    // 构造函数
    BlockIterator(const std::shared_ptr<Block>& b, size_t index);
    BlockIterator(const std::shared_ptr<Block>& b, size_t index, 
      const std::string &right_key, bool is_closed);
    BlockIterator()
        : block(nullptr), current_index(0) {} // end iterator
  
    // 迭代器操作
    virtual BaseIterator &operator++() override;
    virtual T& operator*() const override;
    virtual T* operator->() const override;
    virtual IteratorType get_type() const override;
    virtual bool is_end() const override;
    void set_high_key(const std::string &high_key, bool is_closed);

  private:
    std::shared_ptr<Block> block;                   // 指向所属的 Block
    size_t current_index;                           // 当前位置的索引
    size_t upper_id;
    mutable std::optional<T> cached_value; // 缓存当前值
};

class LsmFileHdr;

class Block : public std::enable_shared_from_this<Block> {
  friend class BlockIterator;
  friend class SSTable;

private:
  std::vector<uint8_t> data;
  LsmFileHdr *file_hdr_;
  uint16_t num_elements = 0; // 元素个数
  size_t capacity;
  size_t entry_size = 0; // 每个entry的大小
    std::string get_key_at(size_t offset) const;
    Rid get_value_at(size_t offset) const;
    int compare_key_at(size_t offset, const std::string &target) const;
  
    bool is_same_key(size_t idx, const std::string &target_key) const;

    inline int compare_key(const std::string &key1, const std::string &key2) const {
      return ix_compare(key1.c_str(), key2.c_str(), file_hdr_->col_types_, file_hdr_->col_lens_);
    }
  
  public:
    Block(size_t capacity, LsmFileHdr *file_hdr);
    // ! 这里的编码函数不包括 hash
    std::vector<uint8_t> encode();
    // ! 这里的解码函数可指定切片是否包括 hash
    static std::shared_ptr<Block> decode(std::vector<uint8_t> &encoded,
                                         bool with_hash = false);
    std::string get_first_key();
    size_t get_offset_at(size_t idx) const;
    bool add_entry(const std::string &key, const Rid &value);
    Rid get_value_binary(const std::string &key);
  
    inline size_t size() const;
    inline size_t cur_size() const;
    inline bool is_empty() const;
    int get_idx_binary(const std::string &key);
    int lower_bound(const std::string &key);
    std::shared_ptr<BlockIterator> find(const std::string &key, bool is_closed);
    std::shared_ptr<BlockIterator> find(const std::string &lower, bool is_lower_closed, 
        const std::string &upper, bool is_upper_closed);
    inline std::shared_ptr<BlockIterator> begin() { return std::make_shared<BlockIterator>(shared_from_this(), 0); }
};