#include <string>
#include "ix_defs.h"
#include "transaction.h"
#include "skiplist.h"
#include "sstable.h"

class MemTable {
    friend class TranContext;
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
    std::shared_ptr<SSTable> flush_last(SSTBuilder &builder, std::string &sst_path,
                                    size_t sst_id,
                                    std::shared_ptr<BlockCache> block_cache);

    // HeapIterator begin(uint64_t tranc_id);
    // HeapIterator iters_preffix(const std::string &preffix, uint64_t tranc_id);
  
    // std::optional<std::pair<HeapIterator, HeapIterator>>
    // iters_monotony_predicate(uint64_t tranc_id,
    //                          std::function<int(const std::string &)> predicate);
  
    // HeapIterator end();
  
  private:
    std::unique_ptr<SkipList> active_memtable_; // 活跃 SkipList
    std::list<std::unique_ptr<SkipList>> immutable_memtables_; // 冻结 SkipList
    size_t frozen_bytes;
    std::shared_mutex frozen_mtx; // 冻结表的锁
    std::shared_mutex cur_mtx;    // 活跃表的锁
    LsmFileHdr *file_hdr_;
};