/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "buffer_pool_manager.h"
#include <fstream>

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : buckets_(BUCKET_COUNT), pages_(pool_size),
      disk_manager_(disk_manager)
{
    // 可以被Replacer改变
    if (REPLACER_TYPE.compare("LRU") == 0)
        replacer_ = new LRUReplacer(pool_size);
    else if (REPLACER_TYPE.compare("CLOCK") == 0)
        replacer_ = new ClockReplacer(pool_size);
    else
    {
        replacer_ = new LRUReplacer(pool_size);
    }
    for (size_t i = 0; i < pool_size; ++i)
    {
        free_list_.emplace_back(i);
    }
    flush_thread_ = std::thread(&BufferPoolManager::background_flush, this);
}
BufferPoolManager::~BufferPoolManager()
{
    terminate_ = true;
    flush_cond_.notify_all();
    if (flush_thread_.joinable())
    {
        flush_thread_.join();
    }
    delete replacer_;
    flush_all_pages(-1); // 确保所有脏页刷盘
}
/**
 * @description: 从free_list或replacer中得到可淘汰帧页的 *frame_id
 * @return {bool} true: 可替换帧查找成功 , false: 可替换帧查找失败
 * @param {frame_id_t*} frame_id 帧页id指针,返回成功找到的可替换帧id
 */
bool BufferPoolManager::find_victim_page(frame_id_t *frame_id)
{
    {
        std::lock_guard<std::mutex> lock(free_list_mutex_);
        if (!free_list_.empty())
        {

            *frame_id = free_list_.front();
            free_list_.pop_front();
            return true;
        }
    }
    return replacer_->victim(frame_id);
}

/**
 * @description: 更新页面数据, 如果为脏页则需写入磁盘，再更新为新页面，更新page元数据(data, is_dirty, page_id)和page table
 * @param {Page*} page 写回页指针
 * @param {PageId} new_page_id 新的page_id
 * @param {frame_id_t} new_frame_id 新的帧frame_id
 */
// 用一个新的页面替换掉当前帧中的页面。
void BufferPoolManager::update_page(Page *page, PageId new_page_id, frame_id_t new_frame_id)
{
    if (page->is_dirty_.load())
    {
        disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
        page->is_dirty_ = false;
    }
    page->reset_memory();
    page->id_ = new_page_id;
    page->pin_count_.store(0);
}

/**
 * @description: 从buffer pool获取需要的页。
 *              如果页表中存在page_id（说明该page在缓冲池中），并且pin_count++。
 *              如果页表不存在page_id（说明该page在磁盘中），则找缓冲池victim page，将其替换为磁盘中读取的page，pin_count置1。
 * @return {Page*} 若获得了需要的页则将其返回，否则返回nullptr
 * @param {PageId} page_id 需要获取的页的PageId
 */
Page *BufferPoolManager::fetch_page(PageId page_id)
{
    auto &bucket = get_bucket(page_id);
    // 第一重检查：共享锁
    {
        std::shared_lock read_lock(bucket.latch);
        auto it = bucket.page_table.find(page_id);
        if (it != bucket.page_table.end())
        {
            frame_id_t frame_id = it->second;
            replacer_->pin(frame_id);
            ++pages_[frame_id].pin_count_;
            return &pages_[frame_id];
        }
    }

    // 第二重检查：独占锁
    std::unique_lock write_lock(bucket.latch);
    auto it = bucket.page_table.find(page_id);
    if (it != bucket.page_table.end())
    {
        frame_id_t frame_id = it->second;
        replacer_->pin(frame_id);
        ++pages_[frame_id].pin_count_;
        return &pages_[frame_id];
    }

    // 获取可替换的帧
    frame_id_t frame_id;
    if (!find_victim_page(&frame_id))
    {
        return nullptr;
    }

    // 替换页面
    Page &old_page = pages_[frame_id];

    // 如果旧页面有效，需要处理它
    if (old_page.id_.fd != -1)
    {
        if (old_page.pin_count_ > 0)
        {
            return nullptr;
        }

        // 如果是脏页，写回磁盘
        if (old_page.is_dirty_)
        {
            disk_manager_->write_page(old_page.id_.fd, old_page.id_.page_no, old_page.data_, PAGE_SIZE);
            old_page.is_dirty_ = false;
        }

        // 更新元数据
        auto &old_bucket = get_bucket(old_page.id_);
        if (&old_bucket != &bucket)
        {
            std::unique_lock lock(old_bucket.latch);
            old_bucket.page_table.erase(old_page.id_);
        }
        else
        {
            old_bucket.page_table.erase(old_page.id_);
        }
    }

    // 读取新页面数据
    disk_manager_->read_page(page_id.fd, page_id.page_no, old_page.data_, PAGE_SIZE);

    // 更新页面元数据
    old_page.id_ = page_id;
    old_page.is_dirty_ = false;
    old_page.pin_count_.store(1);

    // 更新页表和replacer
    bucket.page_table[page_id] = frame_id;
    replacer_->pin(frame_id);

    return &old_page;
}

/**
 * @description: 取消固定pin_count>0的在缓冲池中的page
 * @return {bool} 如果目标页的pin_count<=0则返回false，否则返回true
 * @param {PageId} page_id 目标page的page_id
 * @param {bool} is_dirty 若目标page应该被标记为dirty则为true，否则为false
 */
bool BufferPoolManager::unpin_page(PageId page_id, bool is_dirty)
{
    // 获取对应的桶并加共享锁
    auto &bucket = get_bucket(page_id);
    std::shared_lock read_lock(bucket.latch);

    // 在页表中查找目标页
    auto it = bucket.page_table.find(page_id);
    if (it == bucket.page_table.end())
    {
        return false; // 页面不在缓冲池中
    }

    frame_id_t frame_id = it->second;
    Page &page = pages_[frame_id];

    // 原子操作减少 pin_count
    int prev = page.pin_count_.fetch_sub(1);
    if (prev == 1)
    {
        // pin_count 变为0，通知replacer
        replacer_->unpin(frame_id);
    }
    else if (prev <= 0) [[unlikely]]
    {
        // pin_count 已经是0或负数，恢复并返回false
        page.pin_count_.fetch_add(1);
        return false;
    }

    // 如果需要标记为脏页，使用原子操作设置
    if (is_dirty)
    {
        page.is_dirty_.store(true);
    }

    return true;
}

/**
 * @description: 将目标页写回磁盘，不考虑当前页面是否正在被使用
 * @return {bool} 成功则返回true，否则返回false(只有page_table_中没有目标页时)
 * @param {PageId} page_id 目标页的page_id，不能为INVALID_PAGE_ID
 */
bool BufferPoolManager::flush_page(PageId page_id)
{
    // 获取对应的桶并加共享锁
    auto &bucket = get_bucket(page_id);
    std::shared_lock read_lock(bucket.latch);

    // 查找目标页
    auto it = bucket.page_table.find(page_id);
    if (it == bucket.page_table.end())
    {
        return false; // 页面不在缓冲池中
    }

    // 获取页面
    frame_id_t frame_id = it->second;
    Page &page = pages_[frame_id];

    // 原子操作设置dirty标志，并在原来是脏页的情况下写回磁盘
    if (page.is_dirty_.exchange(false))
    {
        disk_manager_->write_page(page_id.fd, page_id.page_no, page.data_, PAGE_SIZE);
    }

    return true;
}

/**
 * @description: 创建一个新的page，即从磁盘中移动一个新建的空page到缓冲池某个位置。
 * @return {Page*} 返回新创建的page，若创建失败则返回nullptr
 * @param {PageId*} page_id 当成功创建一个新的page时存储其page_id
 */
Page *BufferPoolManager::new_page(PageId *page_id)
{
    // 获取一个可用帧
    frame_id_t frame_id;
    if (!find_victim_page(&frame_id))
    {
        return nullptr;
    }

    Page &page = pages_[frame_id];

    // 如果旧页面有效，需要处理它
    if (page.id_.fd != -1)
    {
        // 如果页面被固定，不能替换
        if (page.pin_count_ > 0)
        {
            return nullptr;
        }

        // 如果是脏页，写回磁盘
        if (page.is_dirty_)
        {
            disk_manager_->write_page(page.id_.fd, page.id_.page_no, page.data_, PAGE_SIZE);
        }

        // 从旧桶的页表中删除旧页面
        auto &old_bucket = get_bucket(page.id_);
        {
            std::unique_lock lock(old_bucket.latch);
            old_bucket.page_table.erase(page.id_);
        }
    }

    // 分配新页面ID
    page_id->page_no = disk_manager_->allocate_page(page_id->fd);

    // 初始化新页面
    page.reset_memory();
    page.id_ = *page_id;
    page.is_dirty_ = false;

    // 更新新桶的页表
    auto &new_bucket = get_bucket(*page_id);
    {
        std::unique_lock lock(new_bucket.latch);
        new_bucket.page_table[*page_id] = frame_id;
    }

    // 更新replacer和pin_count
    replacer_->pin(frame_id);
    page.pin_count_.store(1);

    return &page;
}

/**
 * @description: 从buffer_pool删除目标页
 * @return {bool} 如果目标页不存在于buffer_pool或者成功被删除则返回true，若其存在于buffer_pool但无法删除则返回false
 * @param {PageId} page_id 目标页
 */
bool BufferPoolManager::delete_page(PageId page_id)
{
    // 获取对应的桶并加独占锁
    auto &bucket = get_bucket(page_id);
    std::unique_lock lock(bucket.latch);

    // 查找目标页
    auto it = bucket.page_table.find(page_id);
    if (it == bucket.page_table.end())
    {
        return true; // 页面不在缓冲池中，视为删除成功
    }

    // 获取页面并检查pin_count
    frame_id_t frame_id = it->second;
    Page &page = pages_[frame_id];
    if (page.pin_count_.load() > 0)
    {
        return false; // 页面正在使用中，无法删除
    }

    // 如果是脏页，加入异步刷盘队列
    if (page.is_dirty_.load())
    {
        std::lock_guard queue_lock(queue_mutex_);
        flush_queue_.push(frame_id);
        flush_cond_.notify_one();
    }

    // 释放磁盘空间
    disk_manager_->deallocate_page(page_id.page_no);

    // 从页表中删除
    bucket.page_table.erase(it);

    // 重置页面元数据
    page.reset_memory();
    page.id_ = PageId{-1, -1};
    page.pin_count_.store(0);
    page.is_dirty_.store(false);

    // 将帧加入空闲列表
    {
        std::lock_guard<std::mutex> free_lock(free_list_mutex_);
        free_list_.push_back(frame_id);
    }

    return true;
}

/**
 * @description: 将buffer_pool中的所有页写回到磁盘
 * @param {int} fd 文件句柄
 */
void BufferPoolManager::flush_all_pages(int fd)
{
    std::scoped_lock lock{latch_};

    for (size_t i = 0; i < pool_size_; i++)
    {
        Page *page = &pages_[i];
        if (page->id_.fd == fd)
        { // 只刷新指定文件的页面
            disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
            page->is_dirty_ = false;
        }
    }
}
void BufferPoolManager::background_flush()
{
    while (!terminate_.load())
    {
        std::vector<frame_id_t> batch;
        {
            std::unique_lock lock(queue_mutex_);
            flush_cond_.wait_for(lock, std::chrono::seconds(1),
                                 [this]
                                 { return !flush_queue_.empty() || terminate_; });

            while (!flush_queue_.empty())
            {
                batch.emplace_back(flush_queue_.front());
                flush_queue_.pop();
            }
        }

        if (batch.empty())
            continue;
        std::lock_guard<std::mutex> list_lock(free_list_mutex_);
        for (frame_id_t fid : batch)
        {
            Page &page = pages_[fid];
            if (page.is_dirty_.exchange(false))
            {
                disk_manager_->write_page(page.id_.fd, page.id_.page_no, page.data_, PAGE_SIZE);
                free_list_.push_back(fid);
            }
        }
    }
}