#pragma once

#include <vector>
#include <cstddef>
#include <deque>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "defs.h"
#include "storage/buffer_pool_manager.h"
#include "transaction/transaction.h"
#include "memtable.h"
#include "sstable.h"
// #include "compact.h"
#include "transaction.h"
// #include "two_merge_iterator.h"


class LSMEngine {
public:
  std::string data_dir;
  MemTable memtable;
  std::unordered_map<size_t, std::deque<size_t>> level_sst_ids;
  std::unordered_map<size_t, std::shared_ptr<SST>> ssts;
  std::shared_mutex ssts_mtx;
  std::shared_ptr<BlockCache> block_cache;
  size_t next_sst_id = 0;
  size_t cur_max_level = 0;

public:
  LSMEngine(std::string path);
  ~LSMEngine();

  std::optional<std::pair<std::string, uint64_t>> get(const std::string &key,
                                                      uint64_t tranc_id);
  std::vector<
      std::pair<std::string, std::optional<std::pair<std::string, uint64_t>>>>
  get_batch(const std::vector<std::string> &keys, uint64_t tranc_id);

  std::optional<std::pair<std::string, uint64_t>>
  sst_get_(const std::string &key, uint64_t tranc_id);

  // 如果触发了刷盘, 返回当前刷入sst的最大事务id
  uint64_t put(const std::string &key, const std::string &value,
               uint64_t tranc_id);

  uint64_t
  put_batch(const std::vector<std::pair<std::string, std::string>> &kvs,
            uint64_t tranc_id);

  uint64_t remove(const std::string &key, uint64_t tranc_id);
  uint64_t remove_batch(const std::vector<std::string> &keys,
                        uint64_t tranc_id);
  void clear();
  uint64_t flush();

  std::string get_sst_path(size_t sst_id, size_t target_level);

  std::optional<std::pair<TwoMergeIterator, TwoMergeIterator>>
  lsm_iters_monotony_predicate(
      uint64_t tranc_id, std::function<int(const std::string &)> predicate);

  TwoMergeIterator begin(uint64_t tranc_id);
  TwoMergeIterator end();

  static size_t get_sst_size(size_t level);

private:
  void full_compact(size_t src_level);
  std::vector<std::shared_ptr<SST>>
  full_l0_l1_compact(std::vector<size_t> &l0_ids, std::vector<size_t> &l1_ids);

  std::vector<std::shared_ptr<SST>>
  full_common_compact(std::vector<size_t> &lx_ids, std::vector<size_t> &ly_ids,
                      size_t level_y);

  std::vector<std::shared_ptr<SST>> gen_sst_from_iter(BaseIterator &iter,
                                                      size_t target_sst_size,
                                                      size_t target_level);
};

class LsmTree
{
    LsmFileHdr* file_hdr_;
    MemTable memtable;
    DiskManager* disk_manager_;

    LsmTree(LsmFileHdr* file_hdr) : file_hdr_(file_hdr), 
        memtable(file_hdr) {

    }

    private:
    void full_compact(size_t src_level);
    std::vector<std::shared_ptr<SSTable>>
    full_l0_l1_compact(std::vector<size_t> &l0_ids, std::vector<size_t> &l1_ids);

    std::vector<std::shared_ptr<SSTable>>
    full_common_compact(std::vector<size_t> &lx_ids, std::vector<size_t> &ly_ids,
                      size_t level_y);

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

    using LSMIterator = TwoMergeIterator;
    LSMIterator begin(uint64_t tranc_id);
    LSMIterator end();
    std::optional<std::pair<TwoMergeIterator, TwoMergeIterator>>
    lsm_iters_monotony_predicate(
        uint64_t tranc_id, std::function<int(const std::string &)> predicate);
    void clear();
    void flush();
    void flush_all();
};