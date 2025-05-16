#include "sstable.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>

static constexpr size_t tail_size = sizeof(uint32_t) * 2;

SstIterator::SstIterator(const std::shared_ptr<SSTable> &sst, 
  const std::string &lower, bool is_lower_closed, 
  const std::string &upper, bool is_upper_closed)
  : right_key(upper), is_closed(is_upper_closed)
{
  if (m_sst) {
    seek(lower, is_closed);
    set_upper_id(upper, is_closed);
  }
}

SstIterator::SstIterator(const std::shared_ptr<SSTable> &sst, const std::string &key, bool is_closed)
    : m_sst(std::move(sst)), m_block_idx(0), m_block_it(nullptr) {
  if (m_sst) {
    seek(key, is_closed);
  }
}

SstIterator::T *SstIterator::operator->() const
{
	return &operator*();//通过调用operator*来实现operator->
}

void SstIterator::seek(const std::string &key, bool is_closed) {
  if (!m_sst) {
    m_block_it = nullptr;
    return;
  }

  try {
    m_block_idx = m_sst->lower_bound(key, is_closed);
    if ((int)m_block_idx == -1 || m_block_idx >= m_sst->num_blocks()) {
      // 置为 end
      // TODO: 这个边界情况需要添加单元测试
      m_block_it = nullptr;
      m_block_idx = m_sst->num_blocks();
      return;
    }
    auto block = m_sst->read_block(m_block_idx);
    if (!block) {
      m_block_it = nullptr;
      return;
    }
    m_block_it = block->find(key, is_closed);
    if (m_block_it->is_end()) {
      // block 中找不到
      m_block_idx++;
      m_block_it = nullptr;     
      return;
    }
  } catch (const std::exception &) {
    m_block_it = nullptr;
    return;
  }
}

void SstIterator::set_upper_id(const std::string &key, bool is_closed)
{
  try {
    upper_block_idx = m_sst->lower_bound(key, !is_closed);
    if ((int)upper_block_idx == -1 || upper_block_idx >= m_sst->num_blocks()) {
      upper_block_idx = m_sst->num_blocks();
    }
    if(m_block_idx == upper_block_idx && upper_block_idx != m_sst->num_blocks())
      m_block_it->set_high_key(key, is_closed);
  } catch (const std::exception &) {
    return;
  }
}

BaseIterator& SstIterator::operator++()
{
  if (!m_block_it) { // 添加空指针检查
    return *this;
  }
  ++(*m_block_it);
  
  if (m_block_it->is_end()) {
    m_block_idx++;
    if(m_block_idx < upper_block_idx)
    {
      auto next_block = m_sst->read_block(m_block_idx);
      m_block_it = next_block->begin();
    } 
    else if(m_block_idx == upper_block_idx)
    {
      if(upper_block_idx == m_sst->num_blocks())
        m_block_it = nullptr;
      else {
        auto next_block = m_sst->read_block(m_block_idx);
        m_block_it = next_block->begin();
        m_block_it->set_high_key(right_key, is_closed);
      }
    } else {
      // 没有下一个block
      m_block_it = nullptr;
    }
  }
  return *this;
}

void SstIterator::set_high_key(const std::string &high_key, bool is_closed)
{
  right_key = high_key;
  this->is_closed = is_closed;
  if (m_sst)
  {
    set_upper_id(high_key, is_closed);
  }
}

SstIterator::T& SstIterator::operator*() const
{
  if (!m_block_it) {
    throw std::runtime_error("Iterator is invalid");
  }
  return (**m_block_it);
}

IteratorType SstIterator::get_type() const
{
  return IteratorType::SstIterator;
}

bool SstIterator::is_end() const 
{ 
  return !m_block_it || m_block_it->is_end(); 
}

SSTable::SSTable(LsmFileHdr *file_hdr, DiskManager *disk_manager, size_t sst_id, const std::string &file_path,
    std::shared_ptr<BlockCache> block_cache)
    : file_path_(file_path), fd(-1), sst_id(sst_id), block_cache(block_cache), 
    disk_manager_(disk_manager), file_hdr_(file_hdr), is_delete(false) {
    
    file_size = disk_manager_->get_file_size(file_path);
    // 读取文件末尾的元数据块
    if (file_size < tail_size) {
        throw std::runtime_error("Invalid SST file: too small");
    }
    fd = disk_manager_->open_file(file_path);

    int offset = 0;
    // 0. 读取元数据块的偏移量, 最后8字节: 2个 uint32_t,
    // 分别是 meta 和 bloom 的 offset
    std::vector<uint8_t> data(tail_size);
    disk_manager_->read_page_bytes(fd, file_size - tail_size, data.data(),
                                 tail_size);
    memcpy(&meta_block_offset, data.data() + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    memcpy(&bloom_offset, data.data() + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    // 1. 读取 bloom filter
    if (bloom_offset + tail_size < file_size)
    {
        // 布隆过滤器偏移量 + 2*uint32_t 的大小小于文件大小
        // 表示存在布隆过滤器
        uint32_t bloom_size = file_size - tail_size - bloom_offset;
        data.resize(bloom_size);
        disk_manager->read_page_bytes(fd, bloom_offset, data.data(), bloom_size);

        auto bloom = BloomFilter::decode(data);
        bloom_filter = std::make_shared<BloomFilter>(std::move(bloom));
    }

    // 2. 读取并解码元数据块
    uint32_t meta_size = bloom_offset - meta_block_offset;
    data.resize(meta_size);
    disk_manager_->read_page_bytes(fd, meta_block_offset, data.data(), meta_size);
    meta_entries = BlockMeta::decode_meta_from_slice(data, file_hdr_->col_tot_len_);

    // 3. 设置首尾key
    if (!meta_entries.empty()) {
        first_key = meta_entries.front().first_key;
        last_key = meta_entries.back().last_key;
    }
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
  std::vector<uint8_t> block_data(block_size);
  disk_manager_->read_page_bytes(fd, meta.offset, block_data.data(), block_size);
  auto block_res = Block::decode(block_data, file_hdr_, true);

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
  if (bloom_filter != nullptr && !bloom_filter->MayContain(key)) {
    return -1;
  }

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

  return -1;
}

int SSTable::lower_bound(const std::string &key, bool is_closed) {
    // 二分查找
  size_t left = 0;
  size_t right = meta_entries.size();

  while (left < right) {
    size_t mid = (left + right) / 2;
    const auto &meta = meta_entries[mid];
    int cmp = compare_key(key, meta.first_key);

    if (cmp < 0) {
      right = mid;
    } else if ((cmp = compare_key(key, meta.last_key)) > 0 || (cmp == 0 && !is_closed)) {
      left = mid + 1;
    } else {
      return mid;
    }
  }
  return left;
}

bool SSTable::get(const std::string &key, Rid& value) 
{
  if (compare_key(key, first_key) < 0 || compare_key(key, last_key) > 0) {
    return false;
  }

  // 在布隆过滤器判断key是否存在
  if (bloom_filter != nullptr && !bloom_filter->MayContain(key)) {
    return false;
  }
  try {
    auto m_block_idx = find_block_idx(key);
    if (m_block_idx == -1 || m_block_idx >= (int)num_blocks()) {
      return false;
    }
    auto block = read_block(m_block_idx);
    if (!block) {
      return false;
    }
    auto key_idx_ops = block->get_idx_binary(key);
    if (key_idx_ops == -1 || key_idx_ops >= (int)block->size()) {
      return false;
    }
    value = block->get_value_at(block->get_offset_at(key_idx_ops));
  } catch (const std::exception &) {
    return false;
  }
  return true;
}

inline size_t SSTable::num_blocks() const { return meta_entries.size(); }

inline std::string SSTable::get_first_key() const { return first_key; }

inline std::string SSTable::get_last_key() const { return last_key; }

size_t SSTable::sst_size() const { return file_size; }

inline size_t SSTable::get_sst_id() const { return sst_id; }

std::shared_ptr<SstIterator> SSTable::begin() {
  return std::make_shared<SstIterator>(shared_from_this());
}

std::shared_ptr<SstIterator> SSTable::find(const std::string& key, bool is_closed)
{
  return std::make_shared<SstIterator>(shared_from_this(), key, is_closed);
}

std::shared_ptr<SstIterator> SSTable::find(const std::string &lower, bool is_lower_closed, 
    const std::string &upper, bool is_upper_closed)
{
  return std::make_shared<SstIterator>(shared_from_this(), lower, is_lower_closed, upper, is_upper_closed);
}

// **************************************************
// SSTBuilder
// **************************************************

SSTBuilder::SSTBuilder(DiskManager *disk_manager, LsmFileHdr *file_hdr, size_t block_size, bool need_bloom)
 : block(block_size, file_hdr), disk_manager_(disk_manager), 
  file_hdr_(file_hdr){
    if (need_bloom) {
      bloom_filter_ = std::make_shared<BloomFilter>(1000000);
    }
  }

void SSTBuilder::add(const std::string &key, const Rid &value) 
{
  // 记录第一个key
  if (first_key.empty()) {
    first_key = key;
  }
   // 在 布隆过滤器 中添加key
  if (bloom_filter_ != nullptr) {
    bloom_filter_->Add(key);
  }

  if (block.add_entry(key, value)) {
    // block 满足容量限制, 插入成功
    last_key = std::move(key);
    return;
  }

  finish_block(); // 将当前 block 写入

  block.add_entry(key, value);
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
  const std::shared_ptr<BlockCache> &block_cache, std::shared_ptr<BloomFilter> bloom_filter) 
{
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
  std::vector<uint8_t> file_content(std::move(data));
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
  disk_manager_->create_file(path);
  int fd = disk_manager_->open_file(path);
  disk_manager_->write_page_bytes(fd, 0, file_content.data(), file_content.size());

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

std::shared_ptr<SSTable>
SSTBuilder::build(size_t sst_id, const std::string &path, 
  const std::shared_ptr<BlockCache> &block_cache) 
{
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
  if (bloom_filter_ != nullptr)
  {
    bloom_filter_size = bloom_filter_->size();
    total_size += bloom_filter_size;
  }

  // 计算元数据块的偏移量
  uint32_t meta_offset = data.size();

  // 构建完整的文件内容
  // 1. 已有的数据块
  std::vector<uint8_t> file_content(std::move(data));
  file_content.resize(file_content.size() + total_size);
  uint8_t *ptr = file_content.data() + meta_offset;

  // 2. 添加元数据块
  BlockMeta::encode_meta_to_slice(meta_entries, ptr);
  ptr += meta_block_size;

  // 3. 编码布隆过滤器
  uint32_t bloom_offset = meta_offset + meta_block_size;
  if (bloom_filter_ != nullptr) {
    bloom_filter_->encode(ptr);
    ptr += bloom_filter_size;
  }

  // sizeof(uint32_t) * 2  表示: 元数据块的偏移量, 布隆过滤器偏移量,

  // 4. 添加元数据块偏移量
  memcpy(ptr, &meta_offset, sizeof(uint32_t));
  ptr += sizeof(uint32_t);

  // 5. 添加布隆过滤器偏移量
  memcpy(ptr, &bloom_offset, sizeof(uint32_t));

  // 创建文件
  disk_manager_->create_file(path);
  int fd = disk_manager_->open_file(path);
  disk_manager_->write_page_bytes(fd, 0, file_content.data(), file_content.size());

  // 返回SST对象
  auto res = std::make_shared<SSTable>();

  res->sst_id = sst_id;
  res->fd = fd;
  res->first_key = meta_entries.front().first_key;
  res->last_key = meta_entries.back().last_key;
  res->meta_block_offset = meta_offset;
  res->bloom_filter = bloom_filter_;
  res->bloom_offset = bloom_offset;
  res->meta_entries = std::move(meta_entries);
  res->block_cache = block_cache;

  return res;
}
