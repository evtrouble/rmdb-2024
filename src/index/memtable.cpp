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
  std::unique_lock<std::shared_mutex> lock1(cur_mtx);
  for (auto &[key, value] : kvs) {
    active_memtable_->put(key, value);
  }
  if (active_memtable_->get_size() > LSM_PER_MEM_SIZE_LIMIT) {
    // 冻结当前表还需要获取frozen_mtx的写锁
    std::unique_lock<std::shared_mutex> lock2(frozen_mtx);
    frozen_table();
  }
}

bool MemTable::get(const std::string &key, Rid& value)
{
  {
    std::shared_lock lock(cur_mtx);
    if(active_memtable_->get(key, value))
      return true;
  }
  
  std::shared_lock lock(frozen_mtx);
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
  // 检查frozen memtable
  for (auto &table : immutable_memtables_) {
    if(table->get(key, value))
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
  active_memtable_ = std::make_unique<SkipList>(file_hdr_);
}

// 将最老的 memtable 写入 SST, 并返回控制类
std::shared_ptr<SSTable>
MemTable::flush_last(SSTBuilder &builder, std::string &sst_path, size_t sst_id,
                     std::shared_ptr<BlockCache> block_cache) {
  // 由于 flush 后需要移除最老的 memtable, 因此需要加写锁
  std::unique_lock<std::shared_mutex> lock(frozen_mtx);

  if (frozen_tables.empty()) {
    // 如果当前表为空，直接返回nullptr
    if (current_table->get_size() == 0) {
      return nullptr;
    }
    // 将当前表加入到frozen_tables头部
    frozen_tables.push_front(current_table);
    frozen_bytes += current_table->get_size();
    // 创建新的空表作为当前表
    current_table = std::make_shared<SkipList>(file_hdr_);
  }

  // 将最老的 memtable 写入 SST
  std::shared_ptr<SkipList> table = frozen_tables.back();
  frozen_tables.pop_back();
  frozen_bytes -= table->get_size();

  std::vector<std::tuple<std::string, std::string, uint64_t>> flush_data =
      table->flush();
  for (auto &[k, v, t] : flush_data) {
    builder.add(k, v, t);
  }
  return builder.build(sst_id, sst_path, block_cache, table->bloom_filter());
}