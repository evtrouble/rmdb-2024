#pragma once

#include <string>
#include "ix_defs.h"
#include "transaction/transaction.h"
#include "skiplist.h"
#include "sstable.h"

class MemTable {
    friend class HeapIterator;

  private:
    bool frozen_get(const std::string &key, Rid& value);
    void frozen_table();
  
  public:
    MemTable(LsmFileHdr* file_hdr) 
      : active_memtable_(std::make_unique<SkipList>(file_hdr)), file_hdr_(file_hdr) {}

    ~MemTable() = default;

    void put(const std::string &key, const Rid &value);
    void put_batch(const std::vector<std::pair<std::string, Rid>> &kvs);

    bool get(const std::string &key, Rid &value);
    std::vector<size_t> get_batch(const std::vector<std::string> &keys, std::vector<Rid> &value);
    void remove(const std::string &key);
    void remove_batch(const std::vector<std::string> &keys);

    // void clear();
    std::shared_ptr<SkipList> get_last();
    void remove_last();
    //TODO 无锁
    size_t get_total_size() { return frozen_bytes + active_memtable_->get_size(); }

    std::shared_ptr<MergeIterator> find(const std::string &key, bool is_closed);
    std::shared_ptr<MergeIterator> find(const std::string &lower, bool is_lower_closed,
                       const std::string &upper, bool is_upper_closed);

  private:
    std::shared_ptr<SkipList> active_memtable_; // 活跃 SkipList
    std::list<std::shared_ptr<SkipList>> immutable_memtables_; // 冻结 SkipList
    size_t frozen_bytes;
    std::shared_mutex frozen_mtx; // 冻结表的锁
    std::shared_mutex cur_mtx;    // 活跃表的锁
    LsmFileHdr *file_hdr_;
};