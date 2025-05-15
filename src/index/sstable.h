#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <optional>

#include "storage/disk_manager.h"
#include "ix_defs.h"
#include "block.h"
#include "block_cache.h"
#include "storage/blockmeta.h"
#include "storage/bloom_filter.h"

/**
 * SST文件的结构
 * ------------------------------------------------------------------------
 * |         Block Section         |  Meta Section | Extra                |
 * ------------------------------------------------------------------------
 * | data block | ... | data block |    metadata   | metadata offset (32) |
 * ------------------------------------------------------------------------

 * 其中, metadata 是一个数组加上一些描述信息, 数组每个元素由一个 BlockMeta
 编码形成 MetaEntry, MetaEntry 结构如下:
 * ---------------------------------------------------------------
 * | offset(32) | 1st_key(1st_key_len)  | last_key(last_key_len) |
 * ---------------------------------------------------------------
 * Meta Section 的结构如下:
 * ---------------------------------------------------------------
 * | num_entries (32) | MetaEntry | ... | MetaEntry | Hash (32) |
 * ---------------------------------------------------------------
 * 其中, num_entries 表示 metadata 数组的长度, Hash 是 metadata
 数组的哈希值(只包括数组部分, 不包括 num_entries ), 用于校验 metadata 的完整性
 */

class SstIterator : public BaseIterator {
  friend class SSTable;

private:
  std::shared_ptr<SSTable> m_sst;
  size_t m_block_idx;
  std::shared_ptr<BlockIterator> m_block_it;
  size_t upper_block_idx;
  std::string right_key;
  bool is_closed;

public:
  SstIterator(const std::shared_ptr<SSTable> &sst, 
    const std::string &lower, bool is_lower_closed, 
    const std::string &upper, bool is_upper_closed);
  // 创建迭代器, 并移动到第指定key
  SstIterator(const std::shared_ptr<SSTable> &sst, const std::string& key, bool is_closed);
  SstIterator(const std::shared_ptr<SSTable> &sst) : m_sst(std::move(sst)), m_block_idx(0)
  {
    upper_block_idx = m_sst->num_blocks();
    if (m_block_idx < upper_block_idx)
    {
      auto next_block = m_sst->read_block(m_block_idx);
      m_block_it = next_block->begin();
    }
  }

  void seek(const std::string &key, bool is_closed);
  void set_upper_id(const std::string &key, bool is_closed);
  void set_high_key(const std::string &high_key, bool is_closed);

  virtual BaseIterator &operator++() override;
  virtual T& operator*() const override;
  virtual T* operator->() const override;
  virtual IteratorType get_type() const override;
  virtual bool is_end() const override;
};

class SSTable : public std::enable_shared_from_this<SSTable> {
  friend class SSTBuilder;
  friend class LevelIterator;
  static constexpr size_t tail_size = sizeof(uint32_t) * 2;

private:
  std::string file_path_;
  int fd;
  std::vector<BlockMeta> meta_entries;
  uint32_t bloom_offset;
  uint32_t meta_block_offset;
  size_t sst_id;
  std::string first_key;
  std::string last_key;
  std::shared_ptr<BloomFilter> bloom_filter;
  std::shared_ptr<BlockCache> block_cache;
  DiskManager* disk_manager_;
  LsmFileHdr *file_hdr_;
  size_t file_size;
  bool is_delete;

  inline int compare_key(const std::string &key1, const std::string &key2) {
    return ix_compare(key1.c_str(), key2.c_str(), file_hdr_->col_types_, file_hdr_->col_lens_);
  }

public:
  SSTable(LsmFileHdr *file_hdr, DiskManager *disk_manager, size_t sst_id, const std::string &file_path,
          std::shared_ptr<BlockCache> block_cache);
  SSTable() = default;
  ~SSTable()
  {
    disk_manager_->close_file(fd);
    if(is_delete)
      disk_manager_->destroy_file(file_path_);
  }
  // 删除sst文件
  void mark_delete() { is_delete = true; }
  // 创建一个sst, 只包含首尾key的元数据

  // 根据索引读取block
  std::shared_ptr<Block> read_block(size_t block_idx);

  // 找到key所在的block的idx
  int find_block_idx(const std::string &key);

  int lower_bound(const std::string &key, bool is_closed);

  // 根据key返回迭代器
  bool get(const std::string &key, Rid& value);

  // 返回sst中block的数量
  inline size_t num_blocks() const;

  // 返回sst的首key
  inline std::string get_first_key() const;

  // 返回sst的尾key
  inline std::string get_last_key() const;

  // 返回sst的大小
  inline size_t sst_size() const;

  // 返回sst的id
  inline size_t get_sst_id() const;

  inline std::shared_ptr<SstIterator> find(const std::string& key, bool is_closed);
  inline std::shared_ptr<SstIterator> find(const std::string &lower, bool is_lower_closed, 
      const std::string &upper, bool is_upper_closed);

  std::shared_ptr<SstIterator> begin();
};

class SSTBuilder {
private:
  Block block;
  std::string first_key;
  std::string last_key;
  std::vector<BlockMeta> meta_entries;
  std::vector<uint8_t> data;
  size_t block_size;
  DiskManager *disk_manager_;
  LsmFileHdr *file_hdr_;

public:
  // 创建一个sst构建器, 指定目标block的大小
  SSTBuilder(DiskManager *disk_manager, LsmFileHdr *file_hdr, size_t block_size); // 添加一个key-value对
  void add(const std::string &key, const Rid &value);
  // 估计sst的大小
  size_t estimated_size() const;
  // 完成当前block的构建, 即将block写入data, 并创建新的block
  void finish_block();
  // 构建sst, 将sst写入文件并返回SST描述类
  std::shared_ptr<SSTable> build(size_t sst_id, const std::string &path,
    const std::shared_ptr<BlockCache> &block_cache, std::shared_ptr<BloomFilter> bloom_filter);
};