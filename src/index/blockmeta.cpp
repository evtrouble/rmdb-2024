#include "blockmeta.h"
#include <cstring>

BlockMeta::BlockMeta(int col_tot_len) : offset(0), first_key(""), last_key(""), 
    col_tot_len(col_tot_len){}

BlockMeta::BlockMeta(int col_tot_len, size_t offset, const std::string &first_key,
                     const std::string &last_key)
    : offset(offset), first_key(first_key), last_key(last_key), col_tot_len(col_tot_len) {}

void BlockMeta::encode_meta_to_slice(std::vector<BlockMeta> &meta_entries,
                                     uint8_t *ptr) {
  // 1. 写入元素个数
  memcpy(ptr, &num_entries, sizeof(uint32_t));
  ptr += sizeof(uint32_t);

  // 2. 写入每个entry
  for (const auto &meta : meta_entries) {
    // 写入 offset
    uint32_t offset32 = static_cast<uint32_t>(meta.offset);
    memcpy(ptr, &offset32, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    // 写入 first_key
    uint16_t first_key_len = meta.first_key.size();
    memcpy(ptr, meta.first_key.data(), first_key_len);
    ptr += first_key_len;

    // 写入 last_key
    uint16_t last_key_len = meta.last_key.size();
    memcpy(ptr, meta.last_key.data(), last_key_len);
    ptr += last_key_len;
  }

  // 3. 计算并写入hash
  const uint8_t *data_start = metadata.data() + sizeof(uint32_t);
  const uint8_t *data_end = ptr;
  size_t data_len = data_end - data_start;

  // 使用 std::hash 计算哈希值
  uint32_t hash = std::hash<std::string_view>{}(
      std::string_view(reinterpret_cast<const char *>(data_start), data_len));

  memcpy(ptr, &hash, sizeof(uint32_t));
}

size_t BlockMeta::size(std::vector<BlockMeta> &meta_entries)
{
    // 计算总大小：num_entries(32) + 所有entries的大小 + hash(32)
    uint32_t num_entries = meta_entries.size();
    size_t total_size = sizeof(uint32_t); // num_entries
  
    // 计算所有entries的大小
    for (const auto &meta : meta_entries) {
      total_size += sizeof(uint32_t) +      // offset
                    meta.first_key.size() + // first_key
                    meta.last_key.size();   // last_key
    }
    total_size += sizeof(uint32_t); // hash
    return total_size;
}

std::vector<BlockMeta>
BlockMeta::decode_meta_from_slice(const std::vector<uint8_t> &metadata) {
  std::vector<BlockMeta> meta_entries;

  // 1. 验证最小长度
  if (metadata.size() < sizeof(uint32_t) * 2) { // 至少要有num_entries和hash
    throw std::runtime_error("Invalid metadata size");
  }

  // 2. 读取元素个数
  uint32_t num_entries;
  const uint8_t *ptr = metadata.data();
  memcpy(&num_entries, ptr, sizeof(uint32_t));
  ptr += sizeof(uint32_t);
  meta_entries.reserve(num_entries); // 预分配空间

  // 3. 读取entries
  for (uint32_t i = 0; i < num_entries; ++i) {
    BlockMeta meta;

    // 读取 offset
    uint32_t offset32;
    memcpy(&offset32, ptr, sizeof(uint32_t));
    meta.offset = offset32;
    ptr += sizeof(uint32_t);

    // 读取 first_key
    meta.first_key.assign(reinterpret_cast<const char *>(ptr), col_tot_len);
    ptr += col_tot_len;

    // 读取 last_key
    meta.last_key.assign(reinterpret_cast<const char *>(ptr), col_tot_len);
    ptr += col_tot_len;

    meta_entries.emplace_back(std::move(meta));
  }

  // 4. 验证hash
  uint32_t stored_hash;
  memcpy(&stored_hash, ptr, sizeof(uint32_t));

  const uint8_t *data_start = metadata.data() + sizeof(uint32_t);
  const uint8_t *data_end = ptr;
  size_t data_len = data_end - data_start;

  // 使用与编码时相同的 std::hash 计算哈希值
  uint32_t computed_hash = std::hash<std::string_view>{}(
      std::string_view(reinterpret_cast<const char *>(data_start), data_len));

  if (stored_hash != computed_hash) {
    throw std::runtime_error("Metadata hash mismatch");
  }

  return meta_entries;
}
