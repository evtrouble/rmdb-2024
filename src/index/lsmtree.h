#include <vector>
#include "defs.h"
#include "storage/buffer_pool_manager.h"
#include "transaction/transaction.h"
#include "memtable.h"

class LsmFileHdr {
    public: 
        int col_num_;                       // 索引包含的字段数量
        std::vector<ColType> col_types_;    // 字段的类型
        std::vector<int> col_lens_;         // 字段的长度
        int col_tot_len_;                   // 索引包含的字段的总长度
        int keys_size_;                     // keys_size = (btree_order + 1) * col_tot_len
        int tot_len_;                       // 记录结构体的整体长度
    
        LsmFileHdr() : col_num_(0), tot_len_(0) {}
    
        LsmFileHdr(int col_num, int col_tot_len, int keys_size)
                    : col_num_(col_num), col_tot_len_(col_tot_len), 
                    keys_size_(keys_size), tot_len_(0) {} 
    
        void update_tot_len() {
            tot_len_ = sizeof(int) * 4;
            tot_len_ += sizeof(ColType) * col_num_ + sizeof(int) * col_num_;
        }
    
        void serialize(char* dest) {
            int offset = 0;
            memcpy(dest + offset, &tot_len_, sizeof(int));
            offset += sizeof(int);
            memcpy(dest + offset, &col_num_, sizeof(int));
            offset += sizeof(int);
            for(int i = 0; i < col_num_; ++i) {
                memcpy(dest + offset, &col_types_[i], sizeof(ColType));
                offset += sizeof(ColType);
            }
            for(int i = 0; i < col_num_; ++i) {
                memcpy(dest + offset, &col_lens_[i], sizeof(int));
                offset += sizeof(int);
            }
            memcpy(dest + offset, &col_tot_len_, sizeof(int));
            offset += sizeof(int);
            memcpy(dest + offset, &keys_size_, sizeof(int));
            offset += sizeof(int);
            assert(offset == tot_len_);
        }
    
        void deserialize(char* src) {
            int offset = 0;
            tot_len_ = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);
            col_num_ = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);
            col_types_.reserve(col_num_);
            for (int i = 0; i < col_num_; ++i)
            {
                // col_types_[i] = *reinterpret_cast<const ColType*>(src + offset);
                ColType type = *reinterpret_cast<const ColType*>(src + offset);
                offset += sizeof(ColType);
                col_types_.emplace_back(type);
            }
            col_lens_.reserve(col_num_);
            for (int i = 0; i < col_num_; ++i)
            {
                // col_lens_[i] = *reinterpret_cast<const int*>(src + offset);
                int len = *reinterpret_cast<const int*>(src + offset);
                offset += sizeof(int);
                col_lens_.emplace_back(len);
            }
            col_tot_len_ = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);
            keys_size_ = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);
            assert(offset == tot_len_);
        }
    };

class LsmTree
{
    LsmFileHdr* file_hdr_;
    MemTable memtable;

    LsmTree(LsmFileHdr* file_hdr) : file_hdr_(file_hdr), 
        memtable(file_hdr) {

    }
};