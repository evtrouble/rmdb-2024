#pragma once

#include <vector>
#include <cstddef>
#include <deque>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <iomanip>

#include "defs.h"
#include "block_cache.h"
#include "transaction/transaction.h"
#include "memtable.h"
#include "sstable.h"
#include "transaction/transaction.h"
#include "ix_defs.h"

// 参考RocksDB的实现，至少拥有3个后台线程分别负责垃圾回收，Compaction，flush
// 对于每个sst都需要维护引用计数，增加待删除队列，删除前需要检查引用计数是否为0
#ifndef BPLUS
class LsmTree;

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

    void background_flush();
};

struct CompactionThread {
    struct ToCompact {
        std::shared_ptr<LsmTree> lsm;
        size_t src_level;
        ToCompact(std::shared_ptr<LsmTree> lsm, size_t src_level)
            : lsm(std::move(lsm)), src_level(src_level) {}
    };

    std::queue<ToCompact> compact_queue_;
    std::mutex queue_mutex_;
    std::thread compact_thread_;
    std::condition_variable compact_cond_;
    std::atomic<bool> terminate_{false};

    ~CompactionThread() {
        terminate_ = true;
        compact_cond_.notify_all();
        if (compact_thread_.joinable()) {
            compact_thread_.join();
        }
    }

    void add(std::shared_ptr<LsmTree> lsm, size_t src_level) {
        std::lock_guard lock(queue_mutex_);
        compact_queue_.emplace(lsm, src_level);
        compact_cond_.notify_one();
    }

    void background_compact();
};

class LevelIterator : public BaseIterator
{
  friend class SSTable;

  private:
    std::vector<std::shared_ptr<SSTable>> ssts_;
    size_t m_sst_idx;
    std::shared_ptr<SstIterator> m_sst_it;
    size_t upper_sst_idx;
    std::string right_key;
    bool is_closed;
  
  public:
  LevelIterator(const std::vector<std::shared_ptr<SSTable>> &ssts, 
      const std::string &lower, bool is_lower_closed, 
      const std::string &upper, bool is_upper_closed);
    // 创建迭代器, 并移动到第指定key
    LevelIterator(const std::vector<std::shared_ptr<SSTable>> &ssts, const std::string& key, bool is_closed);
    LevelIterator(const std::vector<std::shared_ptr<SSTable>> &ssts) : ssts_(std::move(ssts)), m_sst_idx(0)
    {
      upper_sst_idx = ssts_.size();
      if (m_sst_idx < upper_sst_idx)
      {
        m_sst_it = ssts_[m_sst_idx]->begin();
      }
    }
  
    int lower_bound(const std::string &key, bool is_closed);

    virtual BaseIterator &operator++() override;
    virtual T& operator*() const override;
    virtual T* operator->() const override;
    virtual IteratorType get_type() const override;
    virtual bool is_end() const override;
};

class LsmTree : public std::enable_shared_from_this<LsmTree>
{
    friend struct FlushThread;
    friend struct CompactionThread;

    bool is_delete;

    LsmFileHdr *file_hdr_;
    MemTable memtable;
    DiskManager* disk_manager_;
    std::string data_dir;
    std::atomic<size_t> next_sst_id = 0;
    static std::shared_ptr<BlockCache> block_cache;
    static FlushThread flush_thread;
    static CompactionThread compaction_thread;
    std::shared_mutex ssts_mtx;
    std::map<size_t, std::deque<size_t>> level_sst_ids;
    std::unordered_map<size_t, std::shared_ptr<SSTable>> ssts;
    size_t cur_max_level = 0;

    LsmTree(LsmFileHdr *file_hdr, const std::string &path);

  private:
    void full_compact(size_t src_level);
    std::vector<std::shared_ptr<SSTable>> full_l0_l1_compact(std::vector<std::shared_ptr<SSTable>> &l0_ssts, 
      std::vector<std::shared_ptr<SSTable>> &l1_ssts);

    std::vector<std::shared_ptr<SSTable>> full_common_compact(std::vector<std::shared_ptr<SSTable>> &lx_ssts, 
      std::vector<std::shared_ptr<SSTable>> &ly_ssts, size_t level_y);

    std::vector<std::vector<std::shared_ptr<SSTable>>> get_all_sstables();

    inline int compare_key(const std::string &key1, const std::string &key2) {
        return ix_compare(key1.c_str(), key2.c_str(), file_hdr_->col_types_, file_hdr_->col_lens_);
    }

    public:

    bool get(const std::string &key, Rid &value, Transaction *transaction);
    bool get_batch(const std::vector<std::string> &keys, std::vector<Rid> &values, Transaction *transaction);

    void put(const std::string &key, const Rid &value, Transaction *transaction);
    void put_batch(const std::vector<std::pair<std::string, Rid>> &kvs, Transaction *transaction);

    void remove(const std::string &key, Transaction *transaction);
    void remove_batch(const std::vector<std::string> &keys, Transaction *transaction);

    // using LSMIterator = TwoMergeIterator;
    // LSMIterator begin(uint64_t tranc_id);
    // LSMIterator end();
    // std::optional<std::pair<TwoMergeIterator, TwoMergeIterator>>
    // lsm_iters_monotony_predicate(
    //     uint64_t tranc_id, std::function<int(const std::string &)> predicate);
    void clear();
    void flush();
    void flush_all();
    std::string get_sst_path(size_t sst_id, size_t target_level) {
        // sst的文件路径格式为: data_dir/sst_<sst_id>，sst_id格式化为32位数字
        std::stringstream ss;
        ss << data_dir << "/sst_" << std::setfill('0') << std::setw(32) << sst_id
           << '.' << target_level;
        return ss.str();
    }

    MergeIterator find(const std::string &key, bool is_closed);
    MergeIterator find(const std::string &lower, bool is_lower_closed,
                       const std::string &upper, bool is_upper_closed);

    //   static size_t get_sst_size(size_t level);

    std::vector<std::shared_ptr<SSTable>>
    gen_sst_from_iter(std::shared_ptr<MergeIterator> &iter, size_t target_sst_size,
                      size_t target_level);

    void set_new_sst(size_t new_sst_id, std::shared_ptr<SSTable> &new_sst);
};
#endif