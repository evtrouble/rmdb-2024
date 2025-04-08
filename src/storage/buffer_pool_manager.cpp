#include "buffer_pool_manager.h"
#include <chrono>

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager* disk_manager)
    : buckets_(BUCKET_COUNT), pages_(pool_size), 
      disk_manager_(disk_manager)
{
    // 可以被Replacer改变
    if (REPLACER_TYPE.compare("LRU"))
        replacer_ = new LRUReplacer(pool_size);
    else if (REPLACER_TYPE.compare("CLOCK"))
        replacer_ = new ClockReplacer(pool_size);
    else {
        replacer_ = new LRUReplacer(pool_size);
    }
    for (size_t i = 0; i < pool_size; ++i) {
        free_list_.emplace_back(i);
    }
    flush_thread_ = std::thread(&BufferPoolManager::background_flush, this);
}

BufferPoolManager::~BufferPoolManager() {
    terminate_ = true;
    flush_cond_.notify_all();
    if (flush_thread_.joinable()) {
        flush_thread_.join();
    }
    delete replacer_;
    flush_all_pages(-1); // 确保所有脏页刷盘
}

Page* BufferPoolManager::fetch_page(PageId page_id) {
    auto& bucket = get_bucket(page_id);
    // 第一重检查：共享锁
    {
        std::shared_lock read_lock(bucket.latch);
        auto it = bucket.page_table.find(page_id);
        if (it != bucket.page_table.end()) {
            frame_id_t frame_id = it->second;
            replacer_->pin(frame_id);
            ++pages_[frame_id].pin_count_;
            return &pages_[frame_id];
        }
    }

    // 第二重检查：独占锁
    std::unique_lock write_lock(bucket.latch);
    auto it = bucket.page_table.find(page_id);
    if (it != bucket.page_table.end()) {
        frame_id_t frame_id = it->second;
        replacer_->pin(frame_id);
        ++pages_[frame_id].pin_count_;
        return &pages_[frame_id];
    }

    // 获取可替换的帧
    frame_id_t frame_id;
    if (!find_victim_page(&frame_id)) return nullptr;

    // 替换页面
    Page& old_page = pages_[frame_id];

    // 更新元数据
    auto& old_bucket = get_bucket(old_page.id_);
    {
        std::unique_lock lock(old_bucket.latch);
        old_bucket.page_table.erase(old_page.id_);
    }
    update_page(&old_page, page_id, frame_id);

    // 读取新页面数据
    disk_manager_->read_page(page_id.fd, page_id.page_no, old_page.data_, PAGE_SIZE);
    bucket.page_table[page_id] = frame_id;
    replacer_->pin(frame_id);
    old_page.pin_count_.store(1);
    return &old_page;
}

bool BufferPoolManager::unpin_page(PageId page_id, bool is_dirty) {
    auto& bucket = get_bucket(page_id);
    std::shared_lock read_lock(bucket.latch);
    auto it = bucket.page_table.find(page_id);
    if (it == bucket.page_table.end()) return false;

    frame_id_t frame_id = it->second;
    Page& page = pages_[frame_id];

    int prev = page.pin_count_.fetch_sub(1);
    if (prev == 1) {
        replacer_->unpin(frame_id);
    } else if(prev <= 0) [[unlikely]] {
        page.pin_count_.fetch_add(1);
        return false;
    }
    if (is_dirty) page.is_dirty_.store(true);
    return true;
}

bool BufferPoolManager::flush_page(PageId page_id) {
    auto& bucket = get_bucket(page_id);
    std::shared_lock read_lock(bucket.latch);
    auto it = bucket.page_table.find(page_id);
    if (it == bucket.page_table.end()) return false;

    frame_id_t frame_id = it->second;
    Page& page = pages_[frame_id];
    if (page.is_dirty_.exchange(false)) {
        disk_manager_->write_page(page_id.fd, page_id.page_no, page.data_, PAGE_SIZE);
    }
    return true;
}

Page* BufferPoolManager::new_page(PageId* page_id) {
    frame_id_t frame_id;
    if (!find_victim_page(&frame_id)) return nullptr;

    Page& page = pages_[frame_id];
    // 分配新页面ID
    page_id->page_no = disk_manager_->allocate_page(page_id->fd);

    // 更新元数据
    auto& old_bucket = get_bucket(page.id_);
    {
        std::unique_lock lock(old_bucket.latch);
        old_bucket.page_table.erase(page.id_);
    }
    update_page(&page, *page_id, frame_id);

    auto& new_bucket = get_bucket(*page_id);
    {
        std::unique_lock lock(new_bucket.latch);
        new_bucket.page_table[*page_id] = frame_id;
    }
    replacer_->pin(frame_id);
    page.pin_count_.store(1);
    return &page;
}

bool BufferPoolManager::delete_page(PageId page_id) {
    auto& bucket = get_bucket(page_id);
    std::unique_lock lock(bucket.latch);
    auto it = bucket.page_table.find(page_id);
    if (it == bucket.page_table.end()) return true;

    frame_id_t frame_id = it->second;
    Page& page = pages_[frame_id];
    if (page.pin_count_.load() > 0) return false;

    // 标记删除并异步刷盘
    if (page.is_dirty_.load()) {
        std::lock_guard queue_lock(queue_mutex_);
        flush_queue_.push(frame_id);
        flush_cond_.notify_one();
    }

    bucket.page_table.erase(it);
    return true;
}

void BufferPoolManager::flush_all_pages(int fd) {
    for (auto& bucket : buckets_) {
        std::unique_lock lock(bucket.latch);
        for (auto &[pid, frame_id] : bucket.page_table)
        {
            if (fd != -1 && pid.fd != fd)
                continue;
            Page& page = pages_[frame_id];
            if (page.is_dirty_.exchange(false)) {
                disk_manager_->write_page(pid.fd, pid.page_no, page.data_, PAGE_SIZE);
            }
        }
    }
}

void BufferPoolManager::background_flush() {
    while (!terminate_.load()) {
        std::vector<frame_id_t> batch;
        {
            std::unique_lock lock(queue_mutex_);
            flush_cond_.wait_for(lock, std::chrono::seconds(1),
                [this] { return !flush_queue_.empty() || terminate_; });
            
            while (!flush_queue_.empty()) {
                batch.emplace_back(flush_queue_.front());
                flush_queue_.pop();
            }
        }

        if(batch.empty())continue;
        std::lock_guard<std::mutex> list_lock(free_list_mutex_);
        for (frame_id_t fid : batch) {
            Page& page = pages_[fid];
            if (page.is_dirty_.exchange(false)) {
                disk_manager_->write_page(page.id_.fd, page.id_.page_no, page.data_, PAGE_SIZE);
                free_list_.push_back(fid);
            }
        }
    }
}

bool BufferPoolManager::find_victim_page(frame_id_t* frame_id) {
    {
        std::lock_guard<std::mutex> lock(free_list_mutex_);
        if (!free_list_.empty()) {
        
            *frame_id = free_list_.front();
            free_list_.pop_front();
            return true;
        }
    }
    return replacer_->victim(frame_id);
}

void BufferPoolManager::update_page(Page* page, PageId new_page_id, frame_id_t new_frame_id) {
    if (page->is_dirty_.load()) {
        disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
        page->is_dirty_ = false;
    }
    page->reset_memory();
    page->id_ = new_page_id;
    page->pin_count_.store(0);
}