#include "buffer_pool_manager.h"
#include <chrono>
#include <algorithm>

// replacer
static std::string REPLACER_TYPE;
static constexpr size_t FLUSH_BATCH_SIZE = 32;  // 批量刷盘大小
static constexpr auto FLUSH_INTERVAL = std::chrono::seconds(1);  // 定期刷盘间隔
static constexpr size_t DIRTY_THRESHOLD = 1024;  // 脏页阈值

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
        : buckets_(BUCKET_COUNT), pages_(pool_size), 
      disk_manager_(disk_manager),
      dirty_page_count_(0)
{
    // REPLACER_TYPE = "CLOCK";
    // 可以被Replacer改变
    if (REPLACER_TYPE.compare("LRU") == 0)
        replacer_ = new LRUReplacer(pool_size);
    else if (REPLACER_TYPE.compare("CLOCK") == 0)
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
    for(auto& page : pages_) {
        if (page.is_dirty_) {
            disk_manager_->write_page(page.id_.fd, page.id_.page_no, page.data_, PAGE_SIZE);
        }
    }
}

Page* BufferPoolManager::fetch_page(PageId page_id) {
    auto& bucket = get_bucket(page_id);
    // 第一重检查：共享锁
    {
        std::shared_lock read_lock(bucket.latch);
        auto it = bucket.page_table.find(page_id);
        if (it != bucket.page_table.end()) {
            frame_id_t frame_id = it->second;
            int prev = pages_[frame_id].pin_count_.fetch_add(1);
            if(prev == 0) {
                // 如果之前没有被pin，则将其pin到replacer中
                replacer_->pin(frame_id);
            } 
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
        if(&old_bucket != &bucket)
        {
            std::unique_lock lock(old_bucket.latch);
            old_bucket.page_table.erase(old_page.id_);
        }
        else old_bucket.page_table.erase(old_page.id_);
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
    
    // 如果页面变脏，增加脏页计数
    if (is_dirty && !page.is_dirty_.exchange(true)) {
        dirty_page_count_.fetch_add(1);
        // 如果脏页数量超过阈值，唤醒后台刷盘线程
        if (dirty_page_count_.load() > DIRTY_THRESHOLD) {
            flush_cond_.notify_one();
        }
    }
    return true;
}

bool BufferPoolManager::flush_page(PageId page_id) {
    auto& bucket = get_bucket(page_id);
    std::shared_lock read_lock(bucket.latch);
    auto it = bucket.page_table.find(page_id);
    if (it == bucket.page_table.end()) return false;

    frame_id_t frame_id = it->second;
    read_lock.unlock(); // 释放读锁，避免持有锁过长时间
    Page& page = pages_[frame_id];
    std::lock_guard lock(page.latch_); // 确保页面解锁
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
    page.reset_memory();

    auto& new_bucket = get_bucket(*page_id);
    {
        if(&old_bucket != &new_bucket)
        {
            std::unique_lock lock(new_bucket.latch);
            new_bucket.page_table[*page_id] = frame_id;
        }
        else new_bucket.page_table[*page_id] = frame_id;        
    }
    replacer_->pin(frame_id);
    page.pin_count_.store(1);
    return &page;
}

bool BufferPoolManager::delete_page(PageId page_id) {
    auto& bucket = get_bucket(page_id);
    std::unique_lock lock(bucket.latch);
    auto it = bucket.page_table.find(page_id);
    if (it == bucket.page_table.end())
        return true;

    frame_id_t frame_id = it->second;
    Page& page = pages_[frame_id];
    if (page.pin_count_.load() > 0) return false;
    
    // 从page table移除并加入free list
    bucket.page_table.erase(it);
    {
        std::lock_guard free_lock(free_list_mutex_);
        free_list_.push_back(frame_id);
    }
    return true;
}

void BufferPoolManager::flush_all_pages(int fd, bool flush) {
    for (auto& bucket : buckets_) {
        std::unique_lock lock(bucket.latch);
        auto it = bucket.page_table.begin();
        while (it != bucket.page_table.end()) {
            auto& [pid, frame_id] = *it;
            if (fd != -1 && pid.fd != fd) {
                ++it;
                continue;
            }
            Page& page = pages_[frame_id];
            if(!flush) {
                page.is_dirty_.store(false);
                dirty_page_count_.fetch_sub(1);
            }
            std::lock_guard page_lock(free_list_mutex_);
            free_list_.push_back(frame_id);
            page.pin_count_.store(0);
            replacer_->unpin(frame_id);
            // 删除对应bucket的记录
            it = bucket.page_table.erase(it);
        }
    }
}

void BufferPoolManager::background_flush() {
    std::vector<frame_id_t> batch;
    batch.reserve(FLUSH_BATCH_SIZE);
    
    while (!terminate_.load()) {
        // 周期性检查或等待条件触发
        {
            std::unique_lock lock(flush_mutex_);
            flush_cond_.wait_for(lock, FLUSH_INTERVAL,
                [this] { 
                    return dirty_page_count_.load() > DIRTY_THRESHOLD ||
                           terminate_; 
                });
        }

        // 检查是否需要退出
        if (terminate_.load()) break;

        // 收集并处理脏页
        if (dirty_page_count_.load() > 0) {
            collect_dirty_pages(batch);
            if (!batch.empty()) {
                flush_batch(batch);
                batch.clear();
            }
        }
    }
}

void BufferPoolManager::collect_dirty_pages(std::vector<frame_id_t>& batch) {
    size_t start_pos = last_scan_pos_;
    size_t current_pos = start_pos;
    size_t pool_size = pages_.size();
    
    // 从上次结束的位置开始扫描,最多扫描一轮
    do {
        if(pages_[current_pos].is_dirty_.load()) {
            batch.push_back(current_pos);
            if(batch.size() >= FLUSH_BATCH_SIZE) {
                break;
            }
        }
        current_pos++;
        if (current_pos >= pool_size) current_pos = 0; // 循环扫描
    } while (current_pos != start_pos && batch.size() < FLUSH_BATCH_SIZE);

    // 更新下次扫描的起始位置
    last_scan_pos_ = current_pos;
}

void BufferPoolManager::flush_batch(const std::vector<frame_id_t>& batch) {
    if(batch.empty())[[unlikely]] return;
    
    // 直接遍历所有页面进行刷盘
    for (frame_id_t fid : batch) {
        Page& page = pages_[fid];
        std::lock_guard lock(page.latch_);
        
        // 检查是否仍然是脏页
        if (page.is_dirty_.exchange(false)) {
            disk_manager_->write_page(page.id_.fd, page.id_.page_no, page.data_, PAGE_SIZE);
            dirty_page_count_.fetch_sub(1);
        }
    }
}

bool BufferPoolManager::find_victim_page(frame_id_t* frame_id) {
    {
        std::lock_guard lock(free_list_mutex_);
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
        std::lock_guard lock(page->latch_);
        if(page->is_dirty_.load()) {
            disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
            page->is_dirty_ = false;
            dirty_page_count_.fetch_sub(1);
        }
    }
    // page->reset_memory();
    page->id_ = new_page_id;
    page->pin_count_.store(0);
}