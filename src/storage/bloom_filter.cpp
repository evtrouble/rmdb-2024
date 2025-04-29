#include "bloom_filter.h"

void BloomFilter::encode(uint8_t *ptr) const {
    memcpy(ptr, &bits_per_key_, sizeof(bits_per_key_));
    ptr += sizeof(bits_per_key_);

    memcpy(ptr, &num_hash_functions_, sizeof(num_hash_functions_));
    ptr += sizeof(num_hash_functions_);

    size_t num_bits = bits_.size();
    memcpy(ptr, &num_bits, sizeof(num_bits));
    ptr += sizeof(num_bits);
    
    memcpy(ptr, bits_.data(), bits_.size() * sizeof(uint8_t));
}

// 从 std::vector<uint8_t> 解码布隆过滤器
BloomFilter BloomFilter::decode(const std::vector<uint8_t> &data) {
  size_t index = 0;
  BloomFilter bf;

  std::memcpy(&bf.bits_per_key_, &data[index], sizeof(bf.bits_per_key_));
  index += sizeof(bf.bits_per_key_);

  std::memcpy(&bf.num_hash_functions_, &data[index], sizeof(bf.num_hash_functions_));
  index += sizeof(bf.num_hash_functions_);

  // 解码 num_bits_
  size_t num_bits;
  std::memcpy(&num_bits, &data[index], sizeof(num_bits));
  index += sizeof(num_bits);

  bf.bits_.resize(num_bits);
  // 解码 bits_
  memcpy(bf.bits_.data(), &data[index], num_bits * sizeof(uint8_t));

  return bf;
}