// #include "../../include/lsm/engine.h"
// #include "../../include/consts.h"
// #include "../../include/sst/concact_iterator.h"
// #include "../../include/sst/sst.h"
// #include "../../include/sst/sst_iterator.h"
#include <algorithm>
#include <cstddef>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>
#include "lsmtree.h"
#include "memtable.h"
#include "sstable.h"
#include "transaction.h"

// *********************** LSM ***********************
LSM::LSM(std::string path)
    : engine(std::make_shared<LSMEngine>(path)),
      tran_manager_(std::make_shared<TranManager>(path)) {
  tran_manager_->set_engine(engine);
  auto check_recover_res = tran_manager_->check_recover();
  for (auto &[tranc_id, records] : check_recover_res) {
    tran_manager_->update_max_finished_tranc_id(tranc_id);
    for (auto &record : records) {
      if (record.getOperationType() == OperationType::PUT) {
        engine->put(record.getKey(), record.getValue(), tranc_id);
      } else if (record.getOperationType() == OperationType::DELETE) {
        engine->remove(record.getKey(), tranc_id);
      }
    }
  }
  tran_manager_->init_new_wal();
}

LSM::~LSM() {
  flush_all();
}

bool LsmTree::get(const std::string &key, Rid& value, Transaction *transaction) {
    auto txn_id = transaction->get_txn_id();
    // 1. 先查找 memtable
    if(memtable.get(key, value, txn_id))
    {
        return value.is_valid();
    }
  
    // 2. l0 sst中查询
    std::shared_lock<std::shared_mutex> rlock(ssts_mtx); // 读锁
  
    for (auto &sst_id : level_sst_ids[0]) {
      //  中的 sst_id 是按从大到小的顺序排列,
      // sst_id 越大, 表示是越晚刷入的, 优先查询
      auto &sst = ssts[sst_id];
      if(sst->get(key, value, txn_id))
        return value.is_valid();
    }
  
    // 3. 其他level的sst中查询
    for (size_t level = 1; level <= cur_max_level; level++) {
      std::deque<size_t>& l_sst_ids = level_sst_ids[level];
      // 二分查询
      size_t left = 0;
      size_t right = l_sst_ids.size();
      while (left < right) {
        size_t mid = left + (right - left) / 2;
        auto &sst = ssts[l_sst_ids[mid]];
        
        if (compare_key(sst->get_first_key(), key) <= 0
         && compare_key(key, sst->get_last_key()) <= 0) {
            // 如果sst_id在中, 则在sst中查询
            if(sst->get(key, value, txn_id))
                return value.is_valid();
            break;
        } else if (compare_key(sst->get_last_key(), key) < 0) {
          left = mid + 1;
        } else {
          right = mid;
        }
      }
    }

    return false;
}

bool LsmTree::get_batch(const std::vector<std::string> &keys, std::vector<Rid> &values,
                        Transaction *transaction) 
{
  // 1. 获取事务ID
  auto txn_id = transaction->get_txn_id();

  // 2. 先从 memtable 中批量查找，如果所有键都在memtable 中找到，直接返回
  auto results = memtable.get_batch(keys, txn_id);
  if(results.empty())
      return true;

  // 2. 从 L0 层 SST 文件中批量查找未命中的键
  std::shared_lock<std::shared_mutex> rlock(ssts_mtx); // 加读锁
  for (auto &sst_id : level_sst_ids[0]){
    auto &sst = ssts[sst_id];
    vector<size_t> new_results;
    for (auto &id : results)
    {
        if (!sst->get(key[id], value[id], txn_id))
            new_results.emplace_back(id);
    }
    if(new_results.empty())
        return true;
    results = new_results;
  }

  // 3. 从其他层级 SST 文件中批量查找未命中的键
  for (size_t level = 1; level <= cur_max_level; level++) {
    std::deque<size_t>& l_sst_ids = level_sst_ids[level];
    vector<size_t> new_results;

    for (auto &id : results) {
      // 二分查找确定键可能所在的 SST 文件
      size_t left = 0;
      size_t right = l_sst_ids.size();
      while (left < right) {
        size_t mid = left + (right - left) / 2;
        auto &sst = ssts[l_sst_ids[mid]];

        if (compare_key(sst->get_first_key(), key[id]) <= 0 && 
            compare_key(key[id], sst->get_last_key()) <= 0) {
          // 如果键在当前 SST 文件范围内，则在 SST 中查找
          if(!sst->get(key[id], value[id], txn_id))
            new_results.emplace_back(id);
          break; // 停止继续查找
        } else if (sst->get_last_key() < key) {
          left = mid + 1;
        } else {
          right = mid;
        }
      }
    }
    if(new_results.empty())
        return true;
    results = new_results;
  }

  return true;
}

void LsmTree::put(const std::string &key, const Rid& value, Transaction *transaction) 
{
    auto txn_id = transaction->get_txn_id();
    memtable.put(key, value, txn_id);

    // 如果 memtable 太大，需要刷新到磁盘
    if (memtable.get_total_size() >= LSM_TOL_MEM_SIZE_LIMIT) {
        return flush();
    }
}

void LsmTree::put_batch(const std::vector<std::pair<std::string, Rid>> &kvs, Transaction *transaction) 
{
    auto txn_id = transaction->get_txn_id();
    memtable.put_batch(kvs, txn_id);
    // 如果 memtable 太大，需要刷新到磁盘
    if (memtable.get_total_size() >= LSM_TOL_MEM_SIZE_LIMIT) {
      return flush();
    }
}
void LsmTree::remove(const std::string &key, Transaction *transaction) 
{
    auto txn_id = transaction->get_txn_id();
      // 在 LSM 中，删除实际上是插入一个空值
    memtable.remove(key, tranc_id);
    // 如果 memtable 太大，需要刷新到磁盘
    if (memtable.get_total_size() >= LSM_TOL_MEM_SIZE_LIMIT) {
        return flush();
    }
}

void LsmTree::remove_batch(const std::vector<std::string> &keys, Transaction *transaction) 
{
    auto txn_id = transaction->get_txn_id();
      // 在 LSM 中，删除实际上是插入一个空值
    memtable.remove_batch(keys, tranc_id);
    // 如果 memtable 太大，需要刷新到磁盘
    if (memtable.get_total_size() >= LSM_TOL_MEM_SIZE_LIMIT) {
        return flush();
    }
}

void LSM::clear() { engine->clear(); }

void LsmTree::full_compact(size_t src_level)
{
  // 将 src_level 的 sst 全体压缩到 src_level + 1

  // 递归地判断下一级 level 是否需要 full compact
  if (level_sst_ids[src_level + 1].size() >= LSM_SST_LEVEL_RATIO) {
    full_compact(src_level + 1);
  }

  // 获取源level和目标level的 sst_id
  auto old_level_id_x = level_sst_ids[src_level];
  auto old_level_id_y = level_sst_ids[src_level + 1];
  std::vector<std::shared_ptr<SSTable>> new_ssts;
  std::vector<size_t> lx_ids(old_level_id_x.begin(), old_level_id_x.end());
  std::vector<size_t> ly_ids(old_level_id_y.begin(), old_level_id_y.end());
  if (src_level == 0) {
    // l0这一层不同sst的key有重叠, 需要额外处理
    new_ssts = full_l0_l1_compact(lx_ids, ly_ids);
  } else {
    new_ssts = full_common_compact(lx_ids, ly_ids, src_level + 1);
  }
  // 完成 compact 后移除旧的sst记录
  for (auto &old_sst_id : old_level_id_x) {
    ssts[old_sst_id]->del_sst();
    ssts.erase(old_sst_id);
  }
  for (auto &old_sst_id : old_level_id_y) {
    ssts[old_sst_id]->del_sst();
    ssts.erase(old_sst_id);
  }
  level_sst_ids[src_level].clear();
  level_sst_ids[src_level + 1].clear();

  cur_max_level = std::max(cur_max_level, src_level + 1);

  // 添加新的sst
  for (auto &new_sst : new_ssts) {
    level_sst_ids[src_level + 1].push_back(new_sst->get_sst_id());
    ssts[new_sst->get_sst_id()] = new_sst;
  }
  // 此处没必要reverse了
  std::sort(level_sst_ids[src_level + 1].begin(),
            level_sst_ids[src_level + 1].end());
}

std::vector<std::shared_ptr<SST>>
LsmTree::full_l0_l1_compact(std::vector<size_t> &l0_ids,
                              std::vector<size_t> &l1_ids) {
  // TODO: 这里需要补全的是对已经完成事务的删除
  std::vector<SstIterator> l0_iters;
  std::vector<std::shared_ptr<SST>> l1_ssts;

  for (auto id : l0_ids) {
    auto sst_it = ssts[id]->begin(0);
    l0_iters.push_back(sst_it);
  }
  for (auto id : l1_ids) {
    l1_ssts.push_back(ssts[id]);
  }
  // l0 的sst之间的key有重叠, 需要合并
  auto [l0_begin, l0_end] = SstIterator::merge_sst_iterator(l0_iters, 0);

  std::shared_ptr<HeapIterator> l0_begin_ptr = std::make_shared<HeapIterator>();
  *l0_begin_ptr = l0_begin;

  std::shared_ptr<ConcactIterator> old_l1_begin_ptr =
      std::make_shared<ConcactIterator>(l1_ssts, 0);

  TwoMergeIterator l0_l1_begin(l0_begin_ptr, old_l1_begin_ptr, 0);

  return gen_sst_from_iter(l0_l1_begin,
                           LSM_PER_MEM_SIZE_LIMIT * LSM_SST_LEVEL_RATIO, 1);
}

void LsmTree::flush() 
{ 
  if (memtable.get_total_size() == 0) {
    return;
  }

  std::unique_lock lock(ssts_mtx); // 写锁

  // 1. 先判断 l0 sst 是否数量超限需要concat到 l1
  if (level_sst_ids.find(0) != level_sst_ids.end() &&
      level_sst_ids[0].size() >= LSM_SST_LEVEL_RATIO) {
    full_compact(0);
  }

  // 2. 创建新的 SST ID
  // 链表头部存储的是最新刷入的sst, 其sst_id最大
  size_t new_sst_id = next_sst_id++;

  // 3. 准备 SSTBuilder
  SSTBuilder builder(disk_manager_, file_hdr_, LSM_BLOCK_SIZE); // 4KB block size

  // 4. 将 memtable 中最旧的表写入 SST
  auto sst_path = get_sst_path(new_sst_id, 0);
  auto new_sst =
      memtable.flush_last(builder, sst_path, new_sst_id, block_cache);
}

void LSMTree::set_new_sst_id(size_t new_sst_id, std::shared_ptr<SSTable>& new_sst)
{
  std::unique_lock lock(ssts_mtx); // 写锁
  // 更新内存索引
  ssts[new_sst_id] = std::move(new_sst);

  // 更新 sst_ids
  level_sst_ids[0].push_front(new_sst_id);
}

void LSM::flush_all() {
  while (engine->memtable.get_total_size() > 0) {
    auto max_tranc_id = engine->flush();
    tran_manager_->update_max_flushed_tranc_id(max_tranc_id);
  }
}

LSM::LSMIterator LSM::begin(uint64_t tranc_id) {
  return engine->begin(tranc_id);
}

LSM::LSMIterator LSM::end() { return engine->end(); }

std::optional<std::pair<TwoMergeIterator, TwoMergeIterator>>
LSM::lsm_iters_monotony_predicate(
    uint64_t tranc_id, std::function<int(const std::string &)> predicate) {
  return engine->lsm_iters_monotony_predicate(tranc_id, predicate);
}

// 开启一个事务
std::shared_ptr<TranContext>
LSM::begin_tran(const IsolationLevel &isolation_level) {
  auto tranc_context = tran_manager_->new_tranc(isolation_level);
  return tranc_context;
}