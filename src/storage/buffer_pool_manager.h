#pragma once

#include <shared_mutex>
#include <atomic>
#include <condition_variable>
#include <vector>
#include <thread>
#include <cassert>
#include <queue>
#include "disk_manager.h"
#include "page.h"
#include "replacer/lru_replacer.h"
#include "replacer/clock_replacer.h"

class BufferPoolManager {
public:
    BufferPoolManager(size_t pool_size, DiskManager* disk_manager);
    ~BufferPoolManager();

    Page* fetch_page(PageId page_id);
    bool unpin_page(PageId page_id, bool is_dirty);
    bool flush_page(PageId page_id);
    Page* new_page(PageId* page_id);
    bool delete_page(PageId page_id);
    void remove_all_pages(int fd, bool flush = true);
    void force_flush_all_pages();  // 强制刷新所有脏页到磁盘

private:
    std::shared_mutex table_latch_;    // 保护 page_table
    std::unordered_map<PageId, frame_id_t, PageIdHash> page_table_;
    std::vector<Page> pages_;
    std::mutex free_list_mutex_;  // 保护 free_list_
    std::list<frame_id_t> free_list_; // 空闲帧编号的链表
    DiskManager* disk_manager_;
    Replacer* replacer_;
    
    // 异步刷盘相关
    std::atomic<size_t> dirty_page_count_{0};  // 脏页计数
    size_t last_scan_pos_{0};     // 上次扫描结束的位置
    std::mutex flush_mutex_;                    // 保护条件变量
    std::condition_variable flush_cond_;
    std::atomic<bool> terminate_{false};
    std::thread flush_thread_;

    void background_flush();
    bool find_victim_page(frame_id_t* frame_id);
    void update_page(Page* page, PageId new_page_id, frame_id_t new_frame_id);

    // 新增的辅助方法
    void collect_dirty_pages(std::vector<frame_id_t>& batch);
    void flush_batch(const std::vector<frame_id_t>& batch);
};