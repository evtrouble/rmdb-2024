#include <string>
#include "lsmtree.h"
#include "transaction.h"
#include "skiplist.h"

class MemTable {
    friend class TranContext;
    friend class HeapIterator;
    static constexpr int LSM_PER_MEM_SIZE_LIMIT = 4 * 1024 * 1024; // 内存表的大小限制, 4MB

  private:
    bool frozen_get(const std::string &key, Rid& value, txn_id_t txn_id);
    void frozen_table();
  
  public:
    MemTable(LsmFileHdr* file_hdr_) 
      : active_memtable_(std::make_unique<SkipList>(file_hdr_)) {}

    ~MemTable() = default;

    void put(const std::string &key, const Rid &value, txn_id_t txn_id);
    void put_batch(const std::vector<std::pair<std::string, Rid>> &kvs,
                   txn_id_t txn_id);

    bool get(const std::string &key, Rid &value, txn_id_t txn_id);
    // std::vector<std::pair<std::string, std::optional<std::pair<std::string, uint64_t>>>>
    //                   get_batch(const std::vector<std::string> &keys, Transaction* trn);
    void remove(const std::string &key, txn_id_t txn_id);
    void remove_batch(const std::vector<std::string> &keys, txn_id_t txn_id);

    // void clear();
    // std::shared_ptr<SST> flush_last(SSTBuilder &builder, std::string &sst_path,
    //                                 size_t sst_id,
    //                                 std::shared_ptr<BlockCache> block_cache);

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
    std::shared_ptr<IComparator> comparator;
};