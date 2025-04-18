#include "storage/sstable.h"

namespace rmdb {

SSTableBuilder::SSTableBuilder(DiskManager* disk_manager)
    : disk_manager_(disk_manager),
      current_block_size_(0),
      offset_(0) {
    // 生成新的SSTable文件名
    filename_ = "sstable_" + std::to_string(time(nullptr)) + ".sst";
}

void SSTableBuilder::Add(const std::string& key, const InternalValue& value) {
    current_block_.emplace_back(key, value);
    current_block_size_ += key.size() + sizeof(InternalValue);
    
    if (current_block_size_ >= kBlockSize) {
        FlushDataBlock();
    }
}

void SSTableBuilder::FlushDataBlock() {
    if (current_block_.empty()) return;
    
    // 序列化数据块
    std::string block_data;
    for (const auto& entry : current_block_) {
        // 写入key长度、key、value
        block_data.append(/* serialized data */);
    }
    
    // 记录块信息到索引
    BlockHandle handle;
    handle.offset = offset_;
    handle.size = block_data.size();
    
    // 写入数据块
    disk_manager_->write_page(filename_, offset_, block_data);
    offset_ += block_data.size();
    
    // 更新索引
    if (!current_block_.empty()) {
        index_entries_.emplace_back(current_block_.back().first, handle);
    }
    
    current_block_.clear();
    current_block_size_ = 0;
}

void SSTableBuilder::WriteIndexBlock() {
    std::string index_block;
    for (const auto& entry : index_entries_) {
        // 序列化索引项
        index_block.append(/* serialized index */);
    }
    
    // 写入索引块
    BlockHandle index_handle;
    index_handle.offset = offset_;
    index_handle.size = index_block.size();
    
    disk_manager_->WritePage(filename_, offset_, index_block);
    offset_ += index_block.size();
    
    // 写入footer (包含索引块位置)
    std::string footer;
    // 序列化index_handle到footer
    disk_manager_->WritePage(filename_, offset_, footer);
}

void SSTableBuilder::Finish() {
    FlushDataBlock();    // 刷新最后的数据块
    WriteIndexBlock();   // 写入索引块
    WriteFooter();       // 写入footer
}

} // namespace rmdb
