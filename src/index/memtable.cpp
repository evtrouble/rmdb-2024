#include "memtable.h"

void MemTable::put(const std::string &key, const Rid &value, txn_id_t txn_id)
{
  std::unique_lock lock1(cur_mtx);
  active_memtable_->put(key, value, txn_id);
  if (active_memtable_->get_size() > LSM_PER_MEM_SIZE_LIMIT) {
    // 冻结当前表还需要获取frozen_mtx的写锁
    std::unique_lock lock2(frozen_mtx);
    frozen_table();
  }
}

void MemTable::put_batch(const std::vector<std::pair<std::string, Rid>> &kvs,
  txn_id_t txn_id)
{
  std::unique_lock<std::shared_mutex> lock1(cur_mtx);
  for (auto &[key, value] : kvs) {
    active_memtable_->put(key, value, txn_id);
  }
  if (active_memtable_->get_size() > LSM_PER_MEM_SIZE_LIMIT) {
    // 冻结当前表还需要获取frozen_mtx的写锁
    std::unique_lock<std::shared_mutex> lock2(frozen_mtx);
    frozen_table();
  }
}

bool MemTable::get(const std::string &key, Rid& value, txn_id_t txn_id)
{
  {
    std::shared_lock lock(cur_mtx);
    if(active_memtable_->get(key, value, txn_id))
      return true;
  }
  
  std::shared_lock lock(frozen_mtx);
  return frozen_get(key, value, txn_id);
}

bool MemTable::frozen_get(const std::string &key, Rid& value, txn_id_t txn_id)
{
  // 检查frozen memtable
  for (auto &table : immutable_memtables_) {
    if(table->get(key, value, txn_id))
      return true;
  }
  return false;
}

void MemTable::remove(const std::string &key, txn_id_t txn_id)
{
  std::unique_lock lock(cur_mtx);
  active_memtable_->put(key, Rid(), txn_id);
  if (active_memtable_->get_size() > LSM_PER_MEM_SIZE_LIMIT) {
    // 冻结当前表还需要获取frozen_mtx的写锁
    std::unique_lock lock2(frozen_mtx);
    frozen_table();
  }
}

void MemTable::remove_batch(const std::vector<std::string> &keys,
  txn_id_t txn_id) {
  std::unique_lock lock(cur_mtx);
  // 删除的方式是写入空值
  for (auto &key : keys) {
    active_memtable_->put(key, Rid(), txn_id);
  }
  if (active_memtable_->get_size() > LSM_PER_MEM_SIZE_LIMIT) {
    // 冻结当前表还需要获取frozen_mtx的写锁
    std::unique_lock lock2(frozen_mtx);
    frozen_table();
  }
}

void MemTable::frozen_table()
{
  frozen_bytes += active_memtable_->get_size();
  immutable_memtables_.push_front(std::move(active_memtable_));
  active_memtable_ = std::make_unique<SkipList>(comparator);
}