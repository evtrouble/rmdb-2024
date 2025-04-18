#pragma once
#include <vector>
#include <string>
#include <cmath>

class BloomFilter {
    public:
        BloomFilter(size_t expected_num_items, double false_positive_rate = 0.01) 
            : bits_per_key_(OptimalNumOfBits(false_positive_rate)),
              num_hash_functions_(OptimalNumOfHashFunctions(bits_per_key_)) {
            size_t num_bits = expected_num_items * bits_per_key_;
            bits_.resize((num_bits + 7) / 8, 0);
        }
    
        void Add(const std::string& key) {
            size_t hash1 = Hash(key);
            size_t hash2 = Hash2(key);
            
            for (size_t i = 0; i < num_hash_functions_; i++) {
                // 使用双重哈希产生多个哈希值
                size_t bit_pos = (hash1 + i * hash2) % (bits_.size() * 8);
                bits_[bit_pos / 8] |= (1 << (bit_pos % 8));
            }
        }
    
        bool MayContain(const std::string& key) const {
            size_t hash1 = Hash(key);
            size_t hash2 = Hash2(key);
            
            for (size_t i = 0; i < num_hash_functions_; i++) {
                size_t bit_pos = (hash1 + i * hash2) % (bits_.size() * 8);
                if ((bits_[bit_pos / 8] & (1 << (bit_pos % 8))) == 0) {
                    return false;
                }
            }
            return true;
        }
    
    private:
        std::vector<uint8_t> bits_;
        size_t bits_per_key_;
        size_t num_hash_functions_;
    
        static size_t Hash(const std::string& key) {
            // MurmurHash或其他高质量哈希函数
            size_t hash = 0;
            for (char c : key) {
                hash = hash * 131 + c;
            }
            return hash;
        }
    
        static size_t Hash2(const std::string& key) {
            size_t hash = 0;
            for (char c : key) {
                hash = hash * 137 + c;
            }
            return hash | 1;  // 确保hash2不为0
        }
    
        static size_t OptimalNumOfBits(double false_positive_rate) {
            return size_t(-std::log(false_positive_rate) / (std::log(2) * std::log(2)));
        }
    
        static size_t OptimalNumOfHashFunctions(size_t bits_per_key) {
            return size_t(bits_per_key * std::log(2));
        }
 };
