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

class Block : public std::enable_shared_from_this<Block> {
  friend class BlockIterator;
  
  private:
    std::vector<uint8_t> data;
    LsmFileHdr *file_hdr_;
    uint16_t num_elements = 0; // 元素个数
    size_t capacity;
    size_t entry_size = 0; // 每个entry的大小

    struct Entry {
      std::string key;
      Rid value;
    };
    Entry get_entry_at(size_t offset) const;
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
  
    // 按照谓词返回迭代器, 左闭右开
    // std::optional<
    //     std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
    // get_monotony_predicate_iters(uint64_t tranc_id,
    //                              std::function<int(const std::string &)> func);
  
    BlockIterator begin();
  
    // std::optional<
    //     std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
    // iters_preffix(uint64_t tranc_id,const std::string &preffix);
  
    BlockIterator end();
  };

class BlockIterator : public BaseIterator {
  public:
    // 构造函数
    BlockIterator(std::shared_ptr<Block> b, size_t index);
    BlockIterator(std::shared_ptr<Block> b, const std::string &key);
    BlockIterator()
        : block(nullptr), current_index(0) {} // end iterator
  
    // 迭代器操作
    virtual BaseIterator &operator++() override;
    virtual bool operator==(const BaseIterator &other) const override;
    virtual bool operator!=(const BaseIterator &other) const override;
    virtual T& operator*() const override;
    virtual T* operator->() const override;
    virtual IteratorType get_type() const override;
    virtual bool is_end() const override;
  
  private:
    std::shared_ptr<Block> block;                   // 指向所属的 Block
    size_t current_index;                           // 当前位置的索引
    mutable std::optional<T> cached_value; // 缓存当前值
};