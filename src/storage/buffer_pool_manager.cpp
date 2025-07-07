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
/**
 * @description: 从free_list或replacer中得到可淘汰帧页的 *frame_id
 * @return {bool} true: 可替换帧查找成功 , false: 可替换帧查找失败
 * @param {frame_id_t*} frame_id 帧页id指针,返回成功找到的可替换帧id
 */
bool BufferPoolManager::find_victim_page(frame_id_t *frame_id)
{
    std::ofstream debug_log("storage_test.log", std::ios::out | std::ios::app);

    if (!free_list_.empty())
    {
        *frame_id = free_list_.front();
        free_list_.pop_front();
        debug_log.close();
        return true;
    }

    bool found = replacer_->victim(frame_id);
    if (found)
    {
    }
    else
    {
    }
    debug_log.close();
    return found;
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
    std::scoped_lock lock{latch_};
    std::ofstream debug_log("storage_test.log", std::ios::out | std::ios::app);

    if (page->is_dirty_)
    {
        disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
        page->is_dirty_ = false;
    }

    page_table_.erase(page->id_);
    page_table_[new_page_id] = new_frame_id;
    page->id_ = new_page_id;
    debug_log.close();
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
    std::scoped_lock lock{latch_};

    auto it = page_table_.find(page_id);
    if (it != page_table_.end())
    {
        frame_id_t frame_id = it->second;
        Page *page = &pages_[frame_id];
        page->pin_count_++;
        replacer_->pin(frame_id);
        return page;
    }

    frame_id_t frame_id;
    if (!find_victim_page(&frame_id))
    {
        return nullptr;
    }

    Page *page = &pages_[frame_id];
    if (page->id_.fd != -1)
    {
        if (page->pin_count_ > 0)
        {
            return nullptr;
        }

        if (page->is_dirty_)
        {
            disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
        }
        page_table_.erase(page->id_);
    }

    disk_manager_->read_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);

    page->id_ = page_id;
    page->pin_count_ = 1;
    page->is_dirty_ = false;
    page_table_[page_id] = frame_id;
    replacer_->pin(frame_id);

    return page;
}

/**
 * @description: 取消固定pin_count>0的在缓冲池中的page
 * @return {bool} 如果目标页的pin_count<=0则返回false，否则返回true
 * @param {PageId} page_id 目标page的page_id
 * @param {bool} is_dirty 若目标page应该被标记为dirty则为true，否则为false
 */
bool BufferPoolManager::unpin_page(PageId page_id, bool is_dirty)
{
    // Todo:
    // 0. lock latch
    // 1. 尝试在page_table_中搜寻page_id对应的页P
    // 1.1 P在页表中不存在 return false
    // 1.2 P在页表中存在，获取其pin_count_
    // 2.1 若pin_count_已经等于0，则返回false
    // 2.2 若pin_count_大于0，则pin_count_自减一
    // 2.2.1 若自减后等于0，则调用replacer_的Unpin
    // 3 根据参数is_dirty，更改P的is_dirty_
    std::scoped_lock lock{latch_};

    // 1. 查找目标页
    auto it = page_table_.find(page_id);
    if (it == page_table_.end())
    {
        return false; // 页面不在缓冲池中
    }

    // 1.2 获取页面
    Page *page = &pages_[it->second];

    // 2.1 检查pin_count
    if (page->pin_count_ <= 0)
    {
        return false;
    }

    // 2.2 减少pin_count
    page->pin_count_--;

    // 2.2.1 如果pin_count变为0，通知replacer
    if (page->pin_count_ == 0)
    {
        replacer_->unpin(it->second);
    }

    // 3. 更新dirty标志
    if (is_dirty)
    {
        page->is_dirty_ = true;
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
    // Todo:
    // 0. lock latch
    // 1. 查找页表,尝试获取目标页P
    // 1.1 目标页P没有被page_table_记录 ，返回false
    // 2. 无论P是否为脏都将其写回磁盘。
    // 3. 更新P的is_dirty_
    std::scoped_lock lock{latch_};

    // 1. 查找目标页
    auto it = page_table_.find(page_id);
    if (it == page_table_.end())
    {
        return false; // 页面不在缓冲池中
    }

    // 2. 获取页面并写回磁盘
    Page *page = &pages_[it->second];
    disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);

    // 3. 更新dirty标志
    page->is_dirty_ = false;

    return true;
}

/**
 * @description: 创建一个新的page，即从磁盘中移动一个新建的空page到缓冲池某个位置。
 * @return {Page*} 返回新创建的page，若创建失败则返回nullptr
 * @param {PageId*} page_id 当成功创建一个新的page时存储其page_id
 */
Page *BufferPoolManager::new_page(PageId *page_id)
{

    std::scoped_lock lock{latch_};

    // 1. 获取一个可用帧
    frame_id_t frame_id;
    if (!find_victim_page(&frame_id))
    {
        return nullptr;
    }

    // 2. 检查当前帧中的页面状态
    Page *page = &pages_[frame_id];
    if (page->id_.fd != -1)
    {
        // 如果页面被固定，不能替换
        if (page->pin_count_ > 0)
        {
            return nullptr;
        }

        // 如果是脏页，写回磁盘
        if (page->is_dirty_)
        {
            disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
        }

        // 从页表中删除旧页面
        page_table_.erase(page->id_);
    }

    // 3. 分配新的页面ID
    page_id->page_no = disk_manager_->allocate_page(page_id->fd);

    // 4. 初始化新页面
    page->reset_memory();
    page->id_ = *page_id;
    page->pin_count_ = 1;
    page->is_dirty_ = false;

    // 5. 更新页表和replacer
    page_table_[*page_id] = frame_id;
    replacer_->pin(frame_id);

    return page;
}

/**
 * @description: 从buffer_pool删除目标页
 * @return {bool} 如果目标页不存在于buffer_pool或者成功被删除则返回true，若其存在于buffer_pool但无法删除则返回false
 * @param {PageId} page_id 目标页
 */
bool BufferPoolManager::delete_page(PageId page_id)
{
    // 1.   在page_table_中查找目标页，若不存在返回true
    // 2.   若目标页的pin_count不为0，则返回false
    // 3.   将目标页数据写回磁盘，从页表中删除目标页，重置其元数据，将其加入free_list_，返回true
    std::scoped_lock lock{latch_};

    // 1. 查找目标页
    auto it = page_table_.find(page_id);
    if (it == page_table_.end())
    {
        return true; // 页面不在缓冲池中，视为删除成功
    }

    // 2. 检查pin_count
    frame_id_t frame_id = it->second;
    Page *page = &pages_[frame_id];
    if (page->pin_count_ > 0)
    {
        return false; // 页面正在使用中，无法删除
    }

    // 3. 删除页面
    // 如果是脏页，写回磁盘
    if (page->is_dirty_)
    {
        disk_manager_->write_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);
    }

    // 释放磁盘空间
    disk_manager_->deallocate_page(page_id.page_no);

    // 从页表中删除
    page_table_.erase(page_id);

    // 重置页面元数据
    page->reset_memory();
    page->id_ = PageId{-1, -1};
    page->pin_count_ = 0;
    page->is_dirty_ = false;

    // 将帧加入空闲列表
    free_list_.push_back(frame_id);

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