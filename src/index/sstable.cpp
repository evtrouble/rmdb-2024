#include "sstable.h"
// #include "../../include/consts.h"
// #include "../../include/sst/sst_iterator.h"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>

SSTable::SSTable(LsmFileHdr *file_hdr, DiskManager *disk_manager, size_t sst_id, const std::string &file_path,
    std::shared_ptr<BlockCache> block_cache)
    : file_path_(file_path), disk_manager_(disk_manager), sst_id(sst_id), 
    block_cache(block_cache), file_hdr_(file_hdr), fd(-1){
    
    file_size = disk_manager->get_file_size(file_path);
    // 读取文件末尾的元数据块
    if (file_size < tail_size) {
        throw std::runtime_error("Invalid SST file: too small");
    }
    fd = disk_manager.open_file(file_path);

    int offset = 0;
    // 0. 读取元数据块的偏移量, 最后8字节: 2个 uint32_t,
    // 分别是 meta 和 bloom 的 offset
    vector<uint8_t> data(tail_size);
    disk_manager.read_page_bytes(fd, file_size - tail_size, data.data(),
                                 tail_size);
    memcpy(&meta_block_offset, data.data() + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    memcpy(&bloom_offset, data.data() + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    // 1. 读取最大和最小的timestamp
    // memcpy(&min_ts_, data.data() + offset, sizeof(txn_id_t));
    // offset += sizeof(timestamp_t);
    // memcpy(&max_ts_, data.data() + offset, sizeof(txn_id_t));

    // 2. 读取 bloom filter
    if (bloom_offset + tail_size < file_size)
    {
        // 布隆过滤器偏移量 + 2*uint32_t 的大小小于文件大小
        // 表示存在布隆过滤器
        uint32_t bloom_size = file_size - tail_size - bloom_offset;
        data.resize(bloom_size);
        disk_manager.read_page_bytes(fd, bloom_offset, data.data(), data);

        auto bloom = BloomFilter::decode(data);
        sst->bloom_filter = std::make_shared<BloomFilter>(std::move(bloom));
    }

    // 3. 读取并解码元数据块
    uint32_t meta_size = sst->bloom_offset - sst->meta_block_offset;
    data.resize(meta_size);
    disk_manager.read_page_bytes(fd, meta_block_offset, data.data(), meta_size);
    meta_entries = BlockMeta::decode_meta_from_slice(data);

    // 4. 设置首尾key
    if (!sst->meta_entries.empty()) {
        sst->first_key = sst->meta_entries.front().first_key;
        sst->last_key = sst->meta_entries.back().last_key;
    }
}

void SSTable::delete_file() 
{ 
    disk_manager->close_file(fd);
    disk_manager->delete_file(file.get_path()); 
}

std::shared_ptr<Block> SSTable::read_block(size_t block_idx) {
  if (block_idx >= meta_entries.size()) {
    throw std::out_of_range("Block index out of range");
  }

  // 先从缓存中查找
  if (block_cache != nullptr) {
    auto cache_ptr = block_cache->get(sst_id, block_idx);
    if (cache_ptr != nullptr) {
      return cache_ptr;
    }
  } else {
    throw std::runtime_error("Block cache not set");
  }

  const auto &meta = meta_entries[block_idx];
  size_t block_size;

  // 计算block大小
  if (block_idx == meta_entries.size() - 1) {
    block_size = meta_block_offset - meta.offset;
  } else {
    block_size = meta_entries[block_idx + 1].offset - meta.offset;
  }

  // 读取block数据
  vector<uint8_t> data(block_size);
  disk_manager.read_page_bytes(fd, meta.offset, data.data(), block_size);
  auto block_res = Block::decode(block_data, true);

  // 更新缓存
  if (block_cache != nullptr) {
    block_cache->put(sst_id, block_idx, block_res);
  } else {
    throw std::runtime_error("Block cache not set");
  }
  return block_res;
}

int SSTable::find_block_idx(const std::string &key) {
  // 先在布隆过滤器判断key是否存在
  // if (bloom_filter != nullptr && !bloom_filter->possibly_contains(key)) {
  //   return -1;
  // }

  // 二分查找
  size_t left = 0;
  size_t right = meta_entries.size();

  while (left < right) {
    size_t mid = (left + right) / 2;
    const auto &meta = meta_entries[mid];

    if (compare_key(key, meta.first_key) < 0) {
      right = mid;
    } else if (compare_key(key, meta.last_key) > 0) {
      left = mid + 1;
    } else {
      return mid;
    }
  }

  if (left >= meta_entries.size()) {
    // 如果没有找到完全匹配的块，返回-1
    return -1;
  }
  return left;
}

bool SSTable::get(const std::string &key, Rid& value, txn_id_t txn_id) {
  if (compare_key(key, first_key) < 0 || compare_key(key, last_key) > 0) {
    return false;
  }

  // 在布隆过滤器判断key是否存在
  if (bloom_filter != nullptr && !bloom_filter->possibly_contains(key)) {
    return false;
  }
  try {
    auto m_block_idx = find_block_idx(key);
    if (m_block_idx == -1 || m_block_idx >= m_sst->num_blocks()) {
      return false;
    }
    auto block = read_block(m_block_idx);
    if (!block) {
      return false;
    }
    auto key_idx_ops = block->get_idx_binary(key, txn_id);
    if (key_idx_ops == -1 || key_idx_ops >= block->size()) {
      return false;
    }
    value = block->get_value_at(block->get_offset_at(key_idx_ops));
  } catch (const std::exception &) {
    return false;
  }
  return true;
}

size_t SSTable::num_blocks() const { return meta_entries.size(); }

std::string SSTable::get_first_key() const { return first_key; }

std::string SSTable::get_last_key() const { return last_key; }

size_t SSTable::sst_size() const { return file_size; }

size_t SSTable::get_sst_id() const { return sst_id; }

SstIterator SST::begin(uint64_t tranc_id) {
  return SstIterator(shared_from_this(), tranc_id);
}

SstIterator SST::end() {
  SstIterator res(shared_from_this(), 0);
  res.m_block_idx = meta_entries.size();
  res.m_block_it = nullptr;
  return res;
}

// **************************************************
// SSTBuilder
// **************************************************

SSTBuilder::SSTBuilder(DiskManager *disk_manager, LsmFileHdr *file_hdr, size_t block_size,std::shared_ptr<BloomFilter> bloom_filter)
 : block(block_size, file_hdr), bloom_filter(bloom_filter), disk_manager_(disk_manager), 
  file_hdr_(file_hdr){}

void SSTBuilder::add(const std::string &key, const Rid &value,
                     txn_id_t txn_id) {
  // 记录第一个key
  if (first_key.empty()) {
    first_key = key;
  }

  if (block.add_entry(key, value, txn_id)) {
    // block 满足容量限制, 插入成功
    last_key = std::move(key);
    return;
  }

  finish_block(); // 将当前 block 写入

  block.add_entry(key, value, txn_id);
  first_key = key;
  last_key = std::move(key); // 更新最后一个key
}

size_t SSTBuilder::estimated_size() const { return data.size(); }

void SSTBuilder::finish_block() {
  auto old_block = std::move(block);
  auto encoded_block = old_block.encode();

  meta_entries.emplace_back(data.size(), std::move(first_key), std::move(last_key));

  // 计算block的哈希值
  auto block_hash = static_cast<uint32_t>(std::hash<std::string_view>{}(
      std::string_view(reinterpret_cast<const char *>(encoded_block.data()),
                       encoded_block.size())));

  // 预分配空间并添加数据
  data.reserve(data.size() + encoded_block.size() +
               sizeof(uint32_t)); // 加上的是哈希值
  data.insert(data.end(), encoded_block.begin(), encoded_block.end());
  data.resize(data.size() + sizeof(uint32_t));
  memcpy(data.data() + data.size() - sizeof(uint32_t), &block_hash,
         sizeof(uint32_t));
}

std::shared_ptr<SSTable>
SSTBuilder::build(size_t sst_id, const std::string &path,
                  std::shared_ptr<BlockCache> block_cache) {
  // 完成最后一个block
  if (!block.is_empty()) {
    finish_block();
  }

  // 如果没有数据，抛出异常
  if (meta_entries.empty()) {
    throw std::runtime_error("Cannot build empty SST");
  }

  // 编码元数据块
  std::vector<uint8_t> meta_block;
  size_t meta_block_size = BlockMeta::size(meta_entries);
  size_t bloom_filter_size = 0;
  size_t total_size = meta_block_size + tail_size;
  if (bloom_filter != nullptr)
  {
    bloom_filter_size = bloom_filter->size();
    total_size += bloom_filter_size;
  }

  // 计算元数据块的偏移量
  uint32_t meta_offset = data.size();

  // 构建完整的文件内容
  // 1. 已有的数据块
  std::vector<uint8_t> file_content = std::move(data);
  file_content.resize(file_content.size() + total_size);
  uint8_t *ptr = file_content.data() + meta_offset;

  // 2. 添加元数据块
  BlockMeta::encode_meta_to_slice(meta_entries, ptr);
  ptr += meta_block_size;

  // 3. 编码布隆过滤器
  uint32_t bloom_offset = meta_offset + meta_block_size;
  if (bloom_filter != nullptr) {
    bloom_filter->encode(ptr);
    ptr += bloom_filter_size;
  }

  // sizeof(uint32_t) * 2  表示: 元数据块的偏移量, 布隆过滤器偏移量,

  // 4. 添加元数据块偏移量
  memcpy(ptr, &meta_offset, sizeof(uint32_t));
  ptr += sizeof(uint32_t);

  // 5. 添加布隆过滤器偏移量
  memcpy(ptr, &bloom_offset, sizeof(uint32_t));

  // 创建文件
  disk_manager->create_file(path)
  int fd = disk_manager->open_file(path);
  disk_manager.write_page_bytes(fd, 0, file_content.data(), file_content.size());

  // 返回SST对象
  auto res = std::make_shared<SSTable>();

  res->sst_id = sst_id;
  res->fd = fd;
  res->first_key = meta_entries.front().first_key;
  res->last_key = meta_entries.back().last_key;
  res->meta_block_offset = meta_offset;
  res->bloom_filter = bloom_filter;
  res->bloom_offset = bloom_offset;
  res->meta_entries = std::move(meta_entries);
  res->block_cache = block_cache;

  return res;
}
