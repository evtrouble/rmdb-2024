#include "memtable.h"

void MemTable::put(const std::string &key, const Rid &value)
{
  std::unique_lock lock1(cur_mtx);
  active_memtable_->put(key, value);
  if (active_memtable_->get_size() > LSM_PER_MEM_SIZE_LIMIT) {
    // 冻结当前表还需要获取frozen_mtx的写锁
    std::unique_lock lock2(frozen_mtx);
    frozen_table();
  }
}

void MemTable::put_batch(const std::vector<std::pair<std::string, Rid>> &kvs)
{
  std::unique_lock lock1(cur_mtx);
  for (auto &[key, value] : kvs) {
    active_memtable_->put(key, value);
  }
  if (active_memtable_->get_size() > LSM_PER_MEM_SIZE_LIMIT) {
    // 冻结当前表还需要获取frozen_mtx的写锁
    std::unique_lock lock2(frozen_mtx);
    frozen_table();
  }
}

bool MemTable::get(const std::string &key, Rid& value)
{
  std::shared_ptr<SkipList> active_memtable;
  {
    std::shared_lock lock(cur_mtx);
    active_memtable = active_memtable_;
  }
  if (active_memtable->get(key, value))
    return true;

  return frozen_get(key, value);
}

std::vector<size_t> MemTable::get_batch(const std::vector<std::string> &keys, std::vector<Rid> &value)
{
  std::vector<size_t> ret;
  value.reserve(keys.size());
  {
    std::shared_lock lock(cur_mtx);
    for (auto &key : keys) {
      Rid val;
      if(active_memtable_->get(key, val)) {
        value.emplace_back(val);
      } else {
        value.emplace_back(Rid());
      }
    }
  }

  bool need_search = false;
  for(auto &val : value) {
    if(val.is_valid()) continue;
    need_search = true;
    break;
  }
  if(!need_search) return ret;

  std::shared_lock lock2(frozen_mtx);
  for (size_t i = 0; i < keys.size(); ++i) {
    if(value[i].is_valid()) continue;
    Rid val;
    if(frozen_get(keys[i], val)) {
      value[i] = val;
    } else {
      ret.emplace_back(i);
    }
  }
  return ret;
}

bool MemTable::frozen_get(const std::string &key, Rid& value)
{
  std::vector<std::shared_ptr<SkipList>> immutable_memtables;
  {
    std::shared_lock lock(frozen_mtx);
    immutable_memtables.reserve(immutable_memtables_.size());
    for (auto &immutable_memtable : immutable_memtables_)
      immutable_memtables.emplace_back(immutable_memtable);
  }

  // 检查frozen memtable
  for (auto &table : immutable_memtables)
  {
    if (table->get(key, value))
      return true;
  }
  return false;
}

void MemTable::remove(const std::string &key)
{
  std::unique_lock lock(cur_mtx);
  active_memtable_->put(key, Rid());
  if (active_memtable_->get_size() > LSM_PER_MEM_SIZE_LIMIT) {
    // 冻结当前表还需要获取frozen_mtx的写锁
    std::unique_lock lock2(frozen_mtx);
    frozen_table();
  }
}

void MemTable::remove_batch(const std::vector<std::string> &keys) {
  std::unique_lock lock(cur_mtx);
  // 删除的方式是写入空值
  for (auto &key : keys) {
    active_memtable_->put(key, Rid());
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
  active_memtable_ = std::make_shared<SkipList>(file_hdr_);
}

std::shared_ptr<SkipList> MemTable::get_last() 
{
  std::shared_lock lock(frozen_mtx);

  if (immutable_memtables_.empty()) {
    // 如果当前表为空，直接返回nullptr
    return nullptr;
  }

  // 将最老的 memtable 写入 SST
  std::shared_ptr<SkipList> table = immutable_memtables_.back();
  frozen_bytes -= table->get_size();
  return table;
}

void MemTable::remove_last()
{
  std::unique_lock lock(frozen_mtx);
  if(immutable_memtables_.size())
    immutable_memtables_.pop_back();
}

std::shared_ptr<MergeIterator> MemTable::find(const std::string &key, bool is_closed)
{
  std::shared_ptr<SkipList> active_memtable;
  {
    std::shared_lock lock(cur_mtx);
    active_memtable = active_memtable_;
  }
  std::vector<std::shared_ptr<SkipListIterator>> merge_its;
  merge_its.emplace_back(active_memtable->find(key, is_closed));
  std::vector<std::shared_ptr<SkipList>> immutable_memtables;
  {
    std::shared_lock lock(frozen_mtx);
    immutable_memtables.reserve(immutable_memtables_.size());
    for (auto &immutable_memtable : immutable_memtables_)
      immutable_memtables.emplace_back(immutable_memtable);
  }
  for (auto &immutable_memtable : immutable_memtables)
    merge_its.emplace_back(immutable_memtable->find(key, is_closed));
  return std::make_shared<MergeIterator>(merge_its, file_hdr_, false);
}

std::shared_ptr<MergeIterator> MemTable::find(const std::string &lower, bool is_lower_closed,
                   const std::string &upper, bool is_upper_closed)
{
  std::shared_ptr<SkipList> active_memtable;
  {
    std::shared_lock lock(cur_mtx);
    active_memtable = active_memtable_;
  }
  std::vector<std::shared_ptr<SkipListIterator>> merge_its;
  merge_its.emplace_back(active_memtable->find(lower, is_lower_closed, upper, is_upper_closed));
  std::vector<std::shared_ptr<SkipList>> immutable_memtables;
  {
    std::shared_lock lock(frozen_mtx);
    immutable_memtables.reserve(immutable_memtables_.size());
    for (auto &immutable_memtable : immutable_memtables_)
      immutable_memtables.emplace_back(immutable_memtable);
  }
  for (auto &immutable_memtable : immutable_memtables)
    merge_its.emplace_back(immutable_memtable->find(lower, is_lower_closed, upper, is_upper_closed));
  return std::make_shared<MergeIterator>(merge_its, file_hdr_, false);
}