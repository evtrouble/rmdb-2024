#pragma once

#include <shared_mutex>
#include <atomic>
#include <condition_variable>
#include <vector>
#include <thread>
#include <cassert>
#include <queue>
#include "disk_manager_final.h"
#include "page_final.h"
#include "replacer/lru_replacer_final.h"
#include "replacer/clock_replacer_final.h"

class BufferPoolManager_Final
{
public:
    BufferPoolManager_Final(size_t pool_size, DiskManager_Final *disk_manager);
    ~BufferPoolManager_Final();

    Page_Final *fetch_page(const PageId_Final &page_id);
    bool unpin_page(const PageId_Final &page_id, bool is_dirty);
    bool flush_page(const PageId_Final &page_id);
    Page_Final *new_page(PageId_Final *page_id);
    bool delete_page(const PageId_Final &page_id);
    void remove_all_pages(int fd, bool flush = true);
    void force_flush_all_pages(); // 强制刷新所有脏页到磁盘

private:
    std::shared_mutex table_latch_; // 保护 page_table
    std::unordered_map<PageId_Final, frame_id_t, PageIdHash_Final> page_table_;
    std::vector<Page_Final> pages_;
    std::mutex free_list_mutex_;      // 保护 free_list_
    std::list<frame_id_t> free_list_; // 空闲帧编号的链表
    DiskManager_Final *disk_manager_;
    Replacer *replacer_;

    // 异步刷盘相关
    std::atomic<size_t> dirty_page_count_{0}; // 脏页计数
    size_t last_scan_pos_{0};                 // 上次扫描结束的位置
    std::mutex flush_mutex_;                  // 保护条件变量
    std::condition_variable flush_cond_;
    std::atomic<bool> terminate_{false};
    std::thread flush_thread_;

    void background_flush();
    bool find_victim_page(frame_id_t *frame_id);
    void update_page(Page_Final *page, const PageId_Final &new_page_id, frame_id_t new_frame_id);

    // 新增的辅助方法
    void collect_dirty_pages(std::vector<frame_id_t> &batch);
    void flush_batch(const std::vector<frame_id_t> &batch);
};