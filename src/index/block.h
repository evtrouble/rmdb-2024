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
#include "lsmtree.h"
#include "transaction.h"

/*
key和value的存储格式（长度固定）
----------------------------------------------
|          Data Section     |     Extra      |
----------------------------------------------
|Entry#1|Entry#2|...|Entry#N|num_of_elements |
----------------------------------------------

------------------------------------------
|              Entry #1 |            ... |
-----------------------------------|-----|
|key(keylen)|val(vallen)|txn_id(8B)| ... |
------------------------------------------
*/

class Block : public std::enable_shared_from_this<Block> {
    // friend BlockIterator;
  
  private:
    std::vector<uint8_t> data;
    LsmFileHdr *file_hdr_;
    uint16_t num_elements = 0; // 元素个数
    size_t capacity;
    size_t entry_size = 0; // 每个entry的大小

    struct Entry {
      std::string key;
      Rid value;
      txn_id_t txn_id;
    };
    Entry get_entry_at(size_t offset) const;
    std::string get_key_at(size_t offset) const;
    Rid get_value_at(size_t offset) const;
    txn_id_t get_txn_id_at(size_t offset) const;
    int compare_key_at(size_t offset, const std::string &target) const;
  
    bool is_same_key(size_t idx, const std::string &target_key) const;

    inline int compare_key(const std::string &key1, const std::string &key2) {
      return ix_compare(key1.c_str(), key2.c_str(), file_hdr_->col_types_, file_hdr_->col_lens_);
    }
  
  public:
    Block(size_t capacity, LsmFileHdr *file_hdr);
    // ! 这里的编码函数不包括 hash
    std::vector<uint8_t> encode();
    // ! 这里的解码函数可指定切片是否包括 hash
    static std::shared_ptr<Block> decode(const std::vector<uint8_t> &encoded,
                                         bool with_hash = false);
    std::string get_first_key();
    size_t get_offset_at(size_t idx) const;
    bool add_entry(const std::string &key, const Rid &value, txn_id_t txn_id);
    Rid get_value_binary(const std::string &key,
      txn_id_t txn_id);
  
    inline size_t size() const;
    inline size_t cur_size() const;
    inline bool is_empty() const;
    int get_idx_binary(const std::string &key, txn_id_t txn_id);
  
    // 按照谓词返回迭代器, 左闭右开
    // std::optional<
    //     std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
    // get_monotony_predicate_iters(uint64_t tranc_id,
    //                              std::function<int(const std::string &)> func);
  
    // BlockIterator begin(uint64_t tranc_id = 0);
  
    // std::optional<
    //     std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
    // iters_preffix(uint64_t tranc_id,const std::string &preffix);
  
    // BlockIterator end();
  };