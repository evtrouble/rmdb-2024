#include "block.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>

Block::Block(size_t capacity, LsmFileHdr *file_hdr) : file_hdr_(file_hdr), num_elements(0), capacity(capacity), 
  entry_size(file_hdr_->col_tot_len_ + sizeof(Rid))
{
  data.reserve(capacity);
}

std::vector<uint8_t> Block::encode() {
  // 计算总大小：数据段 + 元素个数(2字节)
  size_t total_bytes = data.size() * sizeof(uint8_t) + sizeof(uint16_t);
  std::vector<uint8_t> encoded(total_bytes, 0);

  // 1. 复制数据段
  memcpy(encoded.data(), data.data(), data.size() * sizeof(uint8_t));

  // 2. 写入元素个数
  size_t num_pos =
      data.size() * sizeof(uint8_t) + num_elements * sizeof(uint16_t);
  memcpy(encoded.data() + num_pos, &num_elements, sizeof(uint16_t));

  return encoded;
}

std::shared_ptr<Block> Block::decode(std::vector<uint8_t> &encoded,
                                     bool with_hash) {
  // 使用 make_shared 创建对象
  auto block = std::make_shared<Block>();

  // 1. 安全性检查
  if (encoded.size() < sizeof(uint16_t)) {
    throw std::runtime_error("Encoded data too small");
  }

  // 2. 读取元素个数
  size_t num_elements_pos = encoded.size() - sizeof(uint16_t);
  if (with_hash) {
    num_elements_pos -= sizeof(uint32_t);
    auto hash_pos = encoded.size() - sizeof(uint32_t);
    uint32_t hash_value;
    memcpy(&hash_value, encoded.data() + hash_pos, sizeof(uint32_t));

    uint32_t compute_hash = std::hash<std::string_view>{}(
        std::string_view(reinterpret_cast<const char *>(encoded.data()),
                         encoded.size() - sizeof(uint32_t)));
    if (hash_value != compute_hash) {
      throw std::runtime_error("Block hash verification failed");
    }
  }
  memcpy(&block->num_elements, encoded.data() + num_elements_pos, sizeof(uint16_t));

  // 3. 复制数据段
  block->data.swap(encoded);
  block->data.resize(num_elements_pos); // 调整大小
  // block->data.reserve(num_elements_pos); // 优化内存分配
  // block->data.assign(encoded.begin(), encoded.begin() + num_elements_pos);

  return block;
}

std::string Block::get_first_key() {
  if (data.empty()) {
    return "";
  }

  // 读取key
  std::string key(reinterpret_cast<char*>(data.data()), file_hdr_->col_tot_len_);
  return key;
}

size_t Block::get_offset_at(size_t idx) const {
  if (idx > num_elements) {
    throw std::runtime_error("idx out of offsets range");
  }
  return entry_size * idx;
}

bool Block::add_entry(const std::string &key, const Rid &value) 
{
  assert(key.size() == file_hdr_->col_tot_len_);
  if ((cur_size() + entry_size > capacity) && num_elements) {
    return false;
  }
  // 计算entry大小：key长度(2B) + key + value长度(2B) + value
  size_t old_size = data.size();
  data.resize(old_size + entry_size);

  // 写入key
  memcpy(data.data() + old_size, key.data(), key.size());
  old_size += key.size();

  // 写入value
  memcpy(data.data() + old_size, &value, sizeof(Rid));
  old_size += sizeof(Rid);

  num_elements++;
  return true;
}

// 从指定偏移量获取entry的key
std::string Block::get_key_at(size_t offset) const {
  return std::string(
      reinterpret_cast<const char *>(data.data() + offset),
      file_hdr_->col_tot_len_); // 直接返回key
}

// 从指定偏移量获取entry的value
Rid Block::get_value_at(size_t offset) const {
  // 返回value
  Rid rid;
  memcpy(&rid, data.data() + offset + file_hdr_->col_tot_len_, sizeof(Rid));
  return rid;
}

// 比较指定偏移量处的key与目标key
int Block::compare_key_at(size_t offset, const std::string &target) const {
  std::string key = get_key_at(offset);
  return compare_key(key, target);
}

bool Block::is_same_key(size_t idx, const std::string &target_key) const
{
    if (idx >= num_elements)
    {
        return false; // 索引超出范围
    }
    return compare_key(get_key_at(get_offset_at(idx)), target_key.c_str()) == 0;
}

// 使用二分查找获取value
// 要求在插入数据时有序插入
Rid Block::get_value_binary(const std::string &key) 
{
  auto idx = get_idx_binary(key);
  if (idx == -1) {
    return Rid();
  }

  return get_value_at(get_offset_at(idx));
}

int Block::get_idx_binary(const std::string &key) 
{
  if(num_elements == 0) {
    return -1;
  }
  // 二分查找
  int left = 0;
  int right = num_elements - 1;

  while (left <= right) {
    int mid = left + (right - left) / 2;
    size_t mid_offset = get_offset_at(mid);

    int cmp = compare_key_at(mid_offset, key);

    if (cmp == 0) {
      // 找到key，返回对应的value
      // 还需要判断事务id可见性
      // return adjust_idx_by_tranc_id(mid, tranc_id);
      return mid;
    } else if (cmp < 0) {
      // 中间的key小于目标key，查找右半部分
      left = mid + 1;
    } else {
      // 中间的key大于目标key，查找左半部分
      right = mid - 1;
    }
  }

  return -1;
}

int Block::lower_bound(const std::string &key) 
{
  int left = 0;
  int right = num_elements - 1;

  while (left <= right) {
    int mid = left + (right - left) / 2;
    size_t mid_offset = get_offset_at(mid);
    auto mid_key = get_key_at(mid_offset);
    int direction = compare_key(key, mid_key);
    if (direction <= 0) { // 目标在 mid 左侧
      right = mid - 1;
    } else // 目标在mid右侧
      left = mid + 1;
  }
  return left;
}

size_t Block::size() const { return num_elements; }

size_t Block::cur_size() const {
  return data.size() + sizeof(uint16_t);
}

bool Block::is_empty() const { return num_elements == 0; }

std::shared_ptr<BlockIterator> Block::find(const std::string& key, bool is_closed)
{
  int id = lower_bound(key);
  if(!is_closed && id < num_elements && 
    compare_key(key, get_key_at(get_offset_at(id))) == 0)
    id++;
  return std::make_shared<BlockIterator>(shared_from_this(), id);
}

std::shared_ptr<BlockIterator> Block::find(const std::string &lower, bool is_lower_closed, 
    const std::string &upper, bool is_upper_closed)
{
  int lower_id = lower_bound(lower);
  if(!is_lower_closed && lower_id < num_elements && 
    compare_key(lower, get_key_at(get_offset_at(lower_id))) == 0)
    lower_id++;
  
  return std::make_shared<BlockIterator>(shared_from_this(), lower_id, upper, is_upper_closed);
}

BlockIterator::BlockIterator(const std::shared_ptr<Block>& b, size_t index)
  : block(std::move(b)), current_index(index) {
  upper_id = block->num_elements;
}

BlockIterator::BlockIterator(const std::shared_ptr<Block>& b, size_t index, 
  const std::string &right_key, bool is_closed) 
  : block(std::move(b)), current_index(index)
{
  upper_id = block->lower_bound(right_key);
  set_high_key(right_key, is_closed);
}

void BlockIterator::set_high_key(const std::string &high_key, bool is_closed)
{
  upper_id = block->lower_bound(high_key);
  if(is_closed && upper_id < block->num_elements && 
    block->compare_key(high_key, block->get_key_at(block->get_offset_at(upper_id))) == 0)
    upper_id++;
}

BaseIterator& BlockIterator::operator++()
{
  if (block && current_index < block->size()) {
    ++current_index;
    cached_value.reset(); // 清除缓存
  }
  return *this;
}

BlockIterator::T& BlockIterator::operator*() const
{
  if (!block || current_index >= block->size()) {
    throw std::out_of_range("Iterator out of range");
  }

  // 使用缓存避免重复解析
  if (!cached_value.has_value()) {
    size_t offset = block->get_offset_at(current_index);
    cached_value =
        std::make_pair(block->get_key_at(offset), block->get_value_at(offset));
  }
  return cached_value.value();
}

BlockIterator::T* BlockIterator::operator->() const
{
	return &operator*();//通过调用operator*来实现operator->
}

IteratorType BlockIterator::get_type() const
{
  return IteratorType::BlockIterator;
}

bool BlockIterator::is_end() const 
{ 
  return current_index >= upper_id; 
}