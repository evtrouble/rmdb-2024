#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "lsmtree.h"

/**
 * SST文件的结构, 参考自 https://skyzh.github.io/mini-lsm/week1-04-sst.html
 * -------------------------------------------------------------------------------------------
 * |         Block Section         |          Meta Section         | Extra                  |
 * -------------------------------------------------------------------------------------------
 * | data block | ... | data block |            metadata           | meta block offset (32) |
 * -------------------------------------------------------------------------------------------

 * 其中, metadata 是一个数组加上一些描述信息, 数组每个元素由一个 BlockMeta 编码形成 MetaEntry,
 MetaEntry 结构如下:
 * ---------------------------------------------------------------------
 * | offset (32) | first_key (first_key_len) | last_key (last_key_len) |
 * ---------------------------------------------------------------------
 * Meta Section 的结构如下:
 * --------------------------------------------------------------------------------------------------------------
 * | num_entries (32) | MetaEntry | ... | MetaEntry | Hash (32) |
 * --------------------------------------------------------------------------------------------------------------
 * 其中, num_entries 表示 metadata 数组的长度, Hash 是 metadata
 数组的哈希值(只包括数组部分, 不包括 num_entries ), 用于校验 metadata 的完整性
 */

class BlockMeta {
  friend class BlockMetaTest;
public:
  size_t offset;       // 块在文件中的偏移量
  std::string first_key; // 块的第一个key
  std::string last_key;  // 块的最后一个key
  static void encode_meta_to_slice(std::vector<BlockMeta> &meta_entries, uint8_t *ptr);
  static std::vector<BlockMeta> decode_meta_from_slice(const std::vector<uint8_t> &metadata, int col_tot_len);
  BlockMeta();
  BlockMeta(size_t offset, const std::string& first_key, const std::string& last_key);
  static size_t size(std::vector<BlockMeta> &meta_entries);
};
