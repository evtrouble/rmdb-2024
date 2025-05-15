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

LevelIterator::LevelIterator(const std::vector<std::shared_ptr<SSTable>> &ssts, 
  const std::string &lower, bool is_lower_closed, 
  const std::string &upper, bool is_upper_closed)
  : ssts_(std::move(ssts)), right_key(upper), is_closed(is_upper_closed)
{
  m_sst_idx = lower_bound(lower, is_lower_closed);
  if(m_sst_idx < ssts_.size())
    m_sst_it = ssts_[m_sst_idx]->find(lower, is_closed);
  upper_sst_idx = lower_bound(upper, !is_upper_closed);
  if(m_sst_idx == upper_sst_idx && upper_sst_idx != ssts_.size())
    m_sst_it->set_high_key(upper, is_closed);
}

LevelIterator::LevelIterator(const std::vector<std::shared_ptr<SSTable>> &ssts, const std::string& key, bool is_closed)
  : ssts_(std::move(ssts))
{
  upper_sst_idx = ssts_.size();
  m_sst_idx = lower_bound(key, is_closed);
  if(m_sst_idx < ssts_.size())
    m_sst_it = ssts_[m_sst_idx]->find(key, is_closed);
}

int LevelIterator::lower_bound(const std::string &key, bool is_closed) {
    // 二分查找
  size_t left = 0;
  size_t right = ssts_.size();

  while (left < right) {
    size_t mid = (left + right) / 2;
    const auto &sst = ssts_[mid];
    int cmp = sst->compare_key(key, sst->first_key);

    if (cmp < 0) {
      right = mid;
    } else if ((cmp = sst->compare_key(key, sst->last_key)) > 0 || (cmp == 0 && !is_closed)) {
      left = mid + 1;
    } else {
      return mid;
    }
  }
  return left;
}

BaseIterator &LevelIterator::operator++()
{
  if (!m_sst_it) { // 添加空指针检查
    return *this;
  }
  ++(*m_sst_it);

  if (m_sst_it->is_end()) {
    m_sst_idx++;
    if(m_sst_idx < upper_sst_idx)
    {
      m_sst_it = ssts_[m_sst_idx]->begin();
    } 
    else if(m_sst_idx == upper_sst_idx)
    {
      if(upper_sst_idx == ssts_.size())
        m_sst_it = nullptr;
      else {
        m_sst_it = ssts_[m_sst_idx]->begin();
        m_sst_it->set_high_key(right_key, is_closed);
      }
    } else {
      // 没有下一个sstable
      m_sst_it = nullptr;
    }
  }
  return *this;
}

LevelIterator::T& LevelIterator::operator*() const
{
  if (!m_sst_it) {
    throw std::runtime_error("Iterator is invalid");
  }
  return (**m_sst_it);
}

LevelIterator::T* LevelIterator::operator->() const
{
  return &operator*();
}

IteratorType LevelIterator::get_type() const
{
  return IteratorType::LevelIterator;
}

bool LevelIterator::is_end() const
{
  return !m_sst_it || m_sst_it->is_end(); 
}


// *********************** LsmTree ***********************
LsmTree::LsmTree(LsmFileHdr* file_hdr, const std::string& path) : file_hdr_(file_hdr), 
    memtable(file_hdr), data_dir(path) {
  // auto check_recover_res = tran_manager_->check_recover();
  // for (auto &[tranc_id, records] : check_recover_res) {
  //   tran_manager_->update_max_finished_tranc_id(tranc_id);
  //   for (auto &record : records) {
  //     if (record.getOperationType() == OperationType::PUT) {
  //       engine->put(record.getKey(), record.getValue(), tranc_id);
  //     } else if (record.getOperationType() == OperationType::DELETE) {
  //       engine->remove(record.getKey(), tranc_id);
  //     }
  //   }
  // }
  // tran_manager_->init_new_wal();
}

LsmTree::~LsmTree() {
  flush_all();
}

bool LsmTree::get(const std::string &key, Rid& value, Transaction *transaction) {
    // auto txn_id = transaction->get_transaction_id();
    // 1. 先查找 memtable
    if(memtable.get(key, value))
    {
        return value.is_valid();
    }
  
    // 2. l0 sst中查询
    auto ssts = get_all_sstables();
  
    for (auto &sst : ssts[0]) {
      //  中的 sst_id 是按从大到小的顺序排列,
      // sst_id 越大, 表示是越晚刷入的, 优先查询
      if(sst->get(key, value))
        return value.is_valid();
    }
  
    // 3. 其他level的sst中查询
    for (size_t level = 1; level <= cur_max_level; level++) {
      auto& level_ssts = ssts[level];
      // 二分查询
      size_t left = 0;
      size_t right = level_ssts.size();
      while (left < right) {
        size_t mid = left + (right - left) / 2;
        auto &sst = level_ssts[mid];
        
        if (compare_key(sst->get_first_key(), key) <= 0
         && compare_key(key, sst->get_last_key()) <= 0) {
            // 如果sst_id在中, 则在sst中查询
            if(sst->get(key, value))
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
  // 1. 先从 memtable 中批量查找，如果所有键都在memtable 中找到，直接返回
  auto results = memtable.get_batch(keys, values);
  if(results.empty())
      return true;

  // 2. 从 L0 层 SST 文件中批量查找未命中的键
  auto ssts = get_all_sstables();
  for (auto &sst : ssts[0]){
    std::vector<size_t> new_results;
    for (auto &id : results)
    {
        if (!sst->get(keys[id], values[id]))
            new_results.emplace_back(id);
    }
    if(new_results.empty())
        return true;
    results = new_results;
  }

  // 3. 从其他层级 SST 文件中批量查找未命中的键
  for (size_t level = 1; level <= cur_max_level; level++) {
    auto& level_ssts = ssts[level];
    std::vector<size_t> new_results;

    for (auto &id : results) {
      // 二分查找确定键可能所在的 SST 文件
      size_t left = 0;
      size_t right = level_ssts.size();
      while (left < right) {
        size_t mid = left + (right - left) / 2;
        auto &sst = level_ssts[mid];

        if (compare_key(sst->get_first_key(), keys[id]) <= 0 && 
            compare_key(keys[id], sst->get_last_key()) <= 0) {
          // 如果键在当前 SST 文件范围内，则在 SST 中查找
          if(!sst->get(keys[id], values[id]))
            new_results.emplace_back(id);
          break; // 停止继续查找
        } else if (sst->get_last_key() < keys[id]) {
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
    memtable.put(key, value);
    // transaction->append_write_record

    // 如果 memtable 太大，需要刷新到磁盘
    if (memtable.get_total_size() >= LSM_TOL_MEM_SIZE_LIMIT) {
        return flush();
    }
}

void LsmTree::put_batch(const std::vector<std::pair<std::string, Rid>> &kvs, Transaction *transaction) 
{
    memtable.put_batch(kvs);
    // 如果 memtable 太大，需要刷新到磁盘
    if (memtable.get_total_size() >= LSM_TOL_MEM_SIZE_LIMIT) {
      return flush();
    }
}
void LsmTree::remove(const std::string &key, Transaction *transaction) 
{
      // 在 LSM 中，删除实际上是插入一个空值
    memtable.remove(key);
    // 如果 memtable 太大，需要刷新到磁盘
    if (memtable.get_total_size() >= LSM_TOL_MEM_SIZE_LIMIT) {
        return flush();
    }
}

void LsmTree::remove_batch(const std::vector<std::string> &keys, Transaction *transaction) 
{
      // 在 LSM 中，删除实际上是插入一个空值
    memtable.remove_batch(keys);
    // 如果 memtable 太大，需要刷新到磁盘
    if (memtable.get_total_size() >= LSM_TOL_MEM_SIZE_LIMIT) {
        return flush();
    }
}

// void LSM::clear() { engine->clear(); }

void LsmTree::full_compact(size_t src_level)
{
  // 将 src_level 的 sst 全体压缩到 src_level + 1

  // 递归地判断下一级 level 是否需要 full compact
  if (level_sst_ids[src_level + 1].size() >= LSM_SST_LEVEL_RATIO) {
    full_compact(src_level + 1);
  }

  // 获取源level和目标level的 sst_id
  std::vector<std::shared_ptr<SSTable>> old_level_x, old_level_y;
  auto &old_level_id_x = level_sst_ids[src_level];
  old_level_x.reserve(old_level_id_x.size());
  for (auto id : old_level_id_x)
    old_level_x.emplace_back(ssts[id]);

  auto &old_level_id_y = level_sst_ids[src_level + 1];
  old_level_y.reserve(old_level_id_y.size());
  for(auto id : old_level_id_y)
    old_level_y.emplace_back(ssts[id]);

  if (src_level == 0) {
    // l0这一层不同sst的key有重叠, 需要额外处理
    full_l0_l1_compact(old_level_x, old_level_y);
  } else {
    full_common_compact(old_level_x, old_level_y, src_level + 1);
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

void LsmTree::full_l0_l1_compact(std::vector<std::shared_ptr<SSTable>> &l0_ssts, 
  std::vector<std::shared_ptr<SSTable>> &l1_ssts)
{
  // TODO: 这里需要补全的是对已经完成事务的删除
  std::vector<SstIterator> l0_iters;
  // l0 的sst之间的key有重叠, 需要合并
  std::vector<std::shared_ptr<BaseIterator>> merge_its;
  merge_its.reserve(2);
  std::vector<std::shared_ptr<SstIterator>> l_its;
  l_its.reserve(l0_ssts.size());
  for (auto &sst : l0_ssts)
  {
    l_its.emplace_back(std::make_shared<SstIterator>(sst));
  }
  merge_its.emplace_back(std::make_shared<MergeIterator>(l_its, file_hdr_, false));

  auto compact = std::make_shared<MergeIterator>(merge_its, file_hdr_, true);

  l_its.reserve(l1_ssts.size());
  merge_its.emplace_back(std::make_shared<LevelIterator>(l1_ssts));

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

  std::shared_lock lock(ssts_mtx); // 读锁

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

std::vector<std::vector<std::shared_ptr<SSTable>>> LsmTree::get_all_sstables()
{
  std::vector<std::vector<std::shared_ptr<SSTable>>> ssts;
  ssts.reserve(cur_max_level);
  std::shared_lock<std::shared_mutex> rlock(ssts_mtx); // 加读锁
  for (auto it = level_sst_ids.begin(); it != level_sst_ids.end(); it++)
  {
    std::vector<std::shared_ptr<SSTable>> level;
    level.reserve(it->second.size());
    for (auto id : it->second)
    {
      level.emplace_back(ssts[id]);
    }
    ssts.emplace_back(std::move(level));
  }
  return ssts;
}

MergeIterator LsmTree::find(const std::string &key, bool is_closed)
{
  auto it = memtable.find(key, is_closed);
  if(cur_max_level == 0)
    return MergeIterator(std::move(*it));

  std::vector<std::shared_ptr<BaseIterator>> merge_its;
  merge_its.emplace_back(std::move(it));

  // 从 L0 层 SST 文件中查找
  auto ssts = get_all_sstables();
  merge_its.reserve(cur_max_level + 1);
  std::vector<std::shared_ptr<SstIterator>> its;
  its.reserve(ssts[0].size());
  for(auto& sst : ssts[0])
  {
    its.emplace_back(std::make_shared<SstIterator>(sst, key, is_closed));
  }
  merge_its.emplace_back(std::make_shared<MergeIterator>(its, file_hdr_, false));
  for (size_t id = 1; id < ssts.size(); id++)
  {
    merge_its.emplace_back(std::make_shared<LevelIterator>(ssts[id], key, is_closed));
  }
  return MergeIterator(merge_its, file_hdr_, true);
}

MergeIterator LsmTree::find(const std::string &lower, bool is_lower_closed,
                   const std::string &upper, bool is_upper_closed)
{
  auto it = memtable.find(lower, is_lower_closed, upper, is_upper_closed);
  if(cur_max_level == 0)
    return MergeIterator(std::move(*it));

  std::vector<std::shared_ptr<BaseIterator>> merge_its;
  merge_its.emplace_back(std::move(it));

  std::vector<std::vector<std::shared_ptr<SSTable>>> ssts;
  ssts.reserve(cur_max_level);
  merge_its.reserve(cur_max_level + 1);
  {
    std::shared_lock<std::shared_mutex> rlock(ssts_mtx); // 加读锁
    for (auto it = level_sst_ids.begin(); it != level_sst_ids.end(); it++)
    {
      std::vector<std::shared_ptr<SSTable>> level;
      level.reserve(it->second.size());
      for (auto id : it->second)
      {
        level.emplace_back(ssts[id]);
      }
      ssts.emplace_back(std::move(level));
    }
  }

  // 从 L0 层 SST 文件中查找
  std::vector<std::shared_ptr<SstIterator>> its;
  its.reserve(ssts[0].size());
  for(auto& sst : ssts[0])
  {
    its.emplace_back(std::make_shared<SstIterator>(sst, lower, 
      is_lower_closed, upper, is_upper_closed));
  }
  merge_its.emplace_back(std::make_shared<MergeIterator>(its, file_hdr_, false));

  // 从 L1 层 SST 文件中查找
  for (size_t id = 1; id < ssts.size(); id++)
  {
    merge_its.emplace_back(std::make_shared<LevelIterator>(ssts[id], lower, 
      is_lower_closed, upper, is_upper_closed));
  }
  return MergeIterator(merge_its, file_hdr_, true);
}

void LsmTree::set_new_sst(size_t new_sst_id, std::shared_ptr<SSTable>& new_sst)
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