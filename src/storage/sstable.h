#pragma once

#include <string>
#include <vector>
#include "storage/disk_manager.h"
#include "index/skiplist.h"

namespace rmdb {

// SSTable文件格式:
// | Footer | Index Block | Data Block1 | Data Block2 | ... |
// Footer包含指向Index Block的偏移量和其他元数据

struct BlockHandle {
    uint64_t offset;    // 块在文件中的偏移
    uint64_t size;      // 块的大小
};

class SSTableBuilder {
public:
    SSTableBuilder(DiskManager* disk_manager);
    
    // 添加键值对到SSTable
    void Add(const std::string& key, const InternalValue& value);
    
    // 完成SSTable构建并写入磁盘
    void Finish();
    
    // 获取当前预估大小
    uint64_t FileSize() const;

private:
    static const size_t kBlockSize = 4096;  // 4KB
    
    void FlushDataBlock();  // 将数据块写入磁盘
    void WriteIndexBlock(); // 写入索引块
    void WriteFooter();     // 写入footer
    
    DiskManager* disk_manager_;
    std::string filename_;
    
    // 当前数据块
    std::vector<std::pair<std::string, InternalValue>> current_block_;
    uint64_t current_block_size_;
    
    // 索引块
    std::vector<std::pair<std::string, BlockHandle>> index_entries_;
    
    uint64_t offset_;      // 当前文件偏移
};

class SSTableIterator {
public:
    SSTableIterator(DiskManager* disk_manager, const std::string& filename);
    
    bool Valid() const;
    void SeekToFirst();
    void SeekToLast();
    void Seek(const std::string& key);
    void Next();
    
    std::string key() const;
    InternalValue value() const;

private:
    // ...迭代器实现细节
};

class SSTable {
public:
    // 在SSTable中查询key
    bool Get(const char* key, std::string& value, Transaction* txn) {
        std::string target_key(key);
        
        // 1. 通过索引块快速定位数据块
        auto block = FindTargetBlock(target_key);
        if (!block) return false;
        
        // 2. 在数据块中查找具体记录
        return block->Get(key, value, txn);
    }

    // 创建SSTable实例
    static std::unique_ptr<SSTable> Open(const std::string& filename, 
                                       DiskManager* disk_manager);

private:
    // 根据key查找目标数据块
    std::unique_ptr<DataBlock> FindTargetBlock(const std::string& key);
};

} // namespace rmdb
