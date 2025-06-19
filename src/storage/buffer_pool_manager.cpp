#include "buffer_pool_manager.h"
#include <chrono>
#include <algorithm>

// replacer
static std::string REPLACER_TYPE;
static constexpr size_t FLUSH_BATCH_SIZE = 32;  // 批量刷盘大小
static constexpr auto FLUSH_INTERVAL = std::chrono::seconds(1);  // 定期刷盘间隔
static constexpr size_t DIRTY_THRESHOLD = 1024;  // 脏页阈值

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
        : pages_(pool_size), disk_manager_(disk_manager),
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
    page_table_.reserve(pool_size);
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

Page* BufferPoolManager::fetch_page(const PageId& page_id) {
    frame_id_t frame_id;
    
    // 先用读锁检查页面是否存在
    {
        std::shared_lock read_lock(table_latch_);
        auto it = page_table_.find(page_id);
        if (it != page_table_.end()) {
            frame_id = it->second;
            Page& page = pages_[frame_id];
            int prev = page.pin_count_.fetch_add(1);
            if(prev == 0) {
                replacer_->pin(frame_id);
            }
            return &page;
        }
    }

    // 页面不存在，获取写锁进行页面替换
    std::unique_lock write_lock(table_latch_);
    
    // 再次检查页面是否存在（双重检查，避免竞态）
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        frame_id = it->second;
        Page& page = pages_[frame_id];
        int prev = page.pin_count_.fetch_add(1);
        if(prev == 0) {
            replacer_->pin(frame_id);
        }
        return &page;
    }

    // 获取可替换的帧
    if (!find_victim_page(&frame_id)) {
        return nullptr;
    }

    Page& old_page = pages_[frame_id];
    PageId old_page_id = old_page.id_;

    // 从page table中移除旧页面
    if (old_page_id.fd != -1) {
        page_table_.erase(old_page_id);
    }

    // 先获取页面锁，然后释放表锁
    std::lock_guard page_lock(old_page.latch_);
    
    // 更新page table并释放表锁
    page_table_[page_id] = frame_id;
    // replacer_->pin(frame_id);
    old_page.pin_count_.store(1);
    write_lock.unlock();
    
    // 在表锁外进行IO操作，只持有页面锁
    update_page(&old_page, page_id, frame_id);
    
    // 读取新页面数据
    disk_manager_->read_page(page_id.fd, page_id.page_no, old_page.data_, PAGE_SIZE);
    return &old_page;
}

bool BufferPoolManager::unpin_page(const PageId& page_id, bool is_dirty) {
    std::shared_lock lock(table_latch_);
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) return false;

    frame_id_t frame_id = it->second;
    Page& page = pages_[frame_id];

    int prev = page.pin_count_.fetch_sub(1);
    if (prev == 1) {
        replacer_->unpin(frame_id);
    } else if(prev <= 0)  {
        assert(false && "Pin count should not be negative");
        page.pin_count_.fetch_add(1);
        return false;
    }
    
    if (is_dirty && !page.is_dirty_.exchange(true)) {
        dirty_page_count_.fetch_add(1);
        if (dirty_page_count_.load() > DIRTY_THRESHOLD) {
            flush_cond_.notify_one();
        }
    }
    return true;
}

bool BufferPoolManager::flush_page(const PageId& page_id) {
    std::shared_lock read_lock(table_latch_);
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) return false;

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

    // 分配新页面ID
    page_id->page_no = disk_manager_->allocate_page(page_id->fd);

    Page& page = pages_[frame_id];
    PageId old_page_id = page.id_;
    std::unique_lock lock(table_latch_);

    // 处理页面替换
    if (old_page_id.fd != -1) {
        page_table_.erase(old_page_id);
    }
    page_table_[*page_id] = frame_id;
    // replacer_->pin(frame_id);
    page.pin_count_.store(1);
    std::lock_guard page_lock(page.latch_);
    lock.unlock(); // 释放表锁，避免长时间持有
    
    update_page(&page, *page_id, frame_id);

    page.reset_memory();    
    return &page;
}

bool BufferPoolManager::delete_page(const PageId& page_id) {
    std::lock_guard lock(table_latch_);
    auto it = page_table_.find(page_id);
    if (it == page_table_.end())
        return true;

    frame_id_t frame_id = it->second;
    Page& page = pages_[frame_id];
    if (page.pin_count_.load() > 0) return false;
    
    // 从page table移除并加入free list
    page_table_.erase(it);
    {
        std::lock_guard free_lock(free_list_mutex_);
        free_list_.push_back(frame_id);
    }
    return true;
}

void BufferPoolManager::remove_all_pages(int fd, bool flush) {
    if(fd < 0) {
        return; // 无效的文件描述符
    }
    std::lock_guard lock(table_latch_);
    auto it = page_table_.begin();
    while (it != page_table_.end()) {
        auto& [pid, frame_id] = *it;
        if (pid.fd != fd) {
            ++it;
            continue;
        }
        Page& page = pages_[frame_id];
        if (!flush) {
            page.is_dirty_.store(false);
            dirty_page_count_.fetch_sub(1);
        }
        
        {
            std::lock_guard free_lock(free_list_mutex_);
            free_list_.push_back(frame_id);
        }
        
        assert(page.pin_count_.load() == 0 && "Cannot remove a pinned page");
        // page.pin_count_.store(0);
        
        // 删除记录
        it = page_table_.erase(it);
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
            if (batch.size()) {
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
    if(batch.empty()) return;
    
    // 直接遍历所有页面进行刷盘
    for (frame_id_t fid : batch) {
        Page& page = pages_[fid];
        std::shared_lock lock(page.latch_);
        
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
        if (free_list_.size()) {
        
            *frame_id = free_list_.front();
            free_list_.pop_front();
            return true;
        }
    }
    return replacer_->victim(frame_id);
}

void BufferPoolManager::update_page(Page* page, const PageId& new_page_id, frame_id_t new_frame_id) {
    if (page->is_dirty_.load()) {
        disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
        page->is_dirty_ = false;
        dirty_page_count_.fetch_sub(1);
    }
    page->id_ = new_page_id;
}

void BufferPoolManager::force_flush_all_pages() {
    // 逐个刷新脏页
    for(auto& page : pages_) {
        std::shared_lock lock(page.latch_);
        auto& page_id = page.id_;
        // 检查是否是脏页并刷新
        if (page.is_dirty_.exchange(false)) {
            disk_manager_->write_page(page_id.fd, page_id.page_no, page.data_, PAGE_SIZE);
            dirty_page_count_.fetch_sub(1);
        }
    }
}