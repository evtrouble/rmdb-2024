/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <vector>

#include "defs.h"
#include "storage/buffer_pool_manager.h"
#include "sstable.h"

constexpr int IX_NO_PAGE = -1;
constexpr int IX_FILE_HDR_PAGE = 0;
constexpr int IX_LEAF_HEADER_PAGE = 1;
constexpr int IX_INIT_ROOT_PAGE = 2;
constexpr int IX_INIT_NUM_PAGES = 3;
constexpr int IX_MAX_COL_LEN = 512;

class IxFileHdr {
public: 
    page_id_t root_page_;               // B+树根节点对应的页面号
    int col_num_;                       // 索引包含的字段数量
    std::vector<ColType> col_types_;    // 字段的类型
    std::vector<int> col_lens_;         // 字段的长度
    int col_tot_len_;                   // 索引包含的字段的总长度
    int btree_order_;                   // # children per page 每个结点最多可插入的键值对数量
    int keys_size_;                     // keys_size = (btree_order + 1) * col_tot_len
    int tot_len_;                       // 记录结构体的整体长度

    IxFileHdr() : col_num_(0), tot_len_(0) {}

    IxFileHdr(page_id_t root_page, int col_num, int col_tot_len, 
            int btree_order, int keys_size)
                : root_page_(root_page), col_num_(col_num),
                col_tot_len_(col_tot_len), btree_order_(btree_order), 
                keys_size_(keys_size), tot_len_(0) {} 

    void update_tot_len() {
        tot_len_ = 0;
        tot_len_ += sizeof(page_id_t) + sizeof(int) * 5;
        tot_len_ += sizeof(ColType) * col_num_ + sizeof(int) * col_num_;
    }

    void serialize(char* dest) {
        int offset = 0;
        memcpy(dest + offset, &tot_len_, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, &root_page_, sizeof(page_id_t));
        offset += sizeof(page_id_t);
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
        memcpy(dest + offset, &btree_order_, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, &keys_size_, sizeof(int));
        offset += sizeof(int);
        assert(offset == tot_len_);
    }

    void deserialize(char* src) {
        int offset = 0;
        tot_len_ = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        root_page_ = *reinterpret_cast<const page_id_t*>(src + offset);
        offset += sizeof(page_id_t);
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
        btree_order_ = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        keys_size_ = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        assert(offset == tot_len_);
    }
};

class IxPageHdr {
public:
    page_id_t parent;               // 父亲节点所在页面的叶号
    int num_key;                    // # current keys (always equals to #child - 1) 已插入的keys数量，key_idx∈[0,num_key)
    bool is_leaf;                   // 是否为叶节点
    page_id_t next_leaf;            // next leaf node's page_no, effective only when is_leaf is true
};

class Iid {
public:
    int page_no;
    int slot_no;

    friend bool operator==(const Iid &x, const Iid &y) { return x.page_no == y.page_no && x.slot_no == y.slot_no; }

    friend bool operator!=(const Iid &x, const Iid &y) { return !(x == y); }
};

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

#define LSM_TOL_MEM_SIZE_LIMIT (16 * 1024 * 1024) // 内存表的总大小限制, 16MB
#define LSM_PER_MEM_SIZE_LIMIT (1 * 1024 * 1024) // 内存表的大小限制, 4MB
#define LSM_BLOCK_SIZE (32 * 1024)               // BLOCK的大小, 32KB

#define LSM_SST_LEVEL_RATIO 4 // 不同层级的sst的大小比例

struct FlushThread
{
    struct ToFlush
    {
        std::shared_ptr<SkipList> skiplist;
        std::shared_ptr<LsmTree> lsm;
        ToFlush(std::shared_ptr<SkipList> &skiplist, std::shared_ptr<LsmTree> &lsm)
            : skiplist(std::move(skiplist)), lsm(std::move(lsm))
        {}
    };
    std::queue<ToFlush> flush_queue_;
    std::mutex queue_mutex_;
    std::thread flush_thread_;
    std::condition_variable flush_cond_;
    std::atomic<bool> terminate_{false};

    ~FlushThread(){
        terminate_ = true;
        flush_cond_.notify_all();
        if (flush_thread_.joinable()) {
            flush_thread_.join();
        }
    }

    void add(std::shared_ptr<SkipList> &to_flush, std::shared_ptr<LsmTree> lsm)
    {
        std::lock_guard lock(queue_mutex_);
        flush_queue_.emplace(to_flush, lsm);
    }

    void background_flush()
    {
        while (!terminate_.load()) {
            std::vector<ToFlush> batch;
            {
                std::unique_lock lock(queue_mutex_);
                flush_cond_.wait_for(lock, std::chrono::seconds(1),
                    [this] { return !flush_queue_.empty() || terminate_; });

                batch.reserve(flush_queue_.size());
                while (!flush_queue_.empty())
                {
                    batch.emplace_back(std::move(flush_queue_.front()));
                    flush_queue_.pop();
                }
            }
    
            if(batch.empty())continue;
            for (auto& [table, lsm] : batch) {
                size_t new_sst_id = lsm->next_sst_id++;

                // 3. 准备 SSTBuilder
                SSTBuilder builder(lsm->disk_manager_, lsm->file_hdr_, LSM_BLOCK_SIZE); // 4KB block size
              
                // 4. 将 memtable 中最旧的表写入 SST
                auto sst_path = lsm->get_sst_path(new_sst_id, 0);
                std::vector<std::pair<std::string, Rid>> flush_data = table->flush();
                for (auto &[k, v] : flush_data) {
                    builder.add(k, v);
                }

                auto sst = builder.build(new_sst_id, sst_path, lsm->block_cache, table->bloom_filter());
                lsm->set_new_sst_id(new_sst_id, sst);
            }
        }
    }
};