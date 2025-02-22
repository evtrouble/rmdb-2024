/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lru_replacer.h"

/**
 * @description: LRU替换器的构造函数
 * @param {size_t} num_pages 缓冲池的最大页面数，用于初始化替换器的最大容量
 */
LRUReplacer::LRUReplacer(size_t num_pages) { max_size_ = num_pages; }

/**
 * @description: 析构函数，使用默认实现即可
 */
LRUReplacer::~LRUReplacer() = default;

/**
 * @description: 使用LRU策略删除一个victim frame，并返回该frame的id
 * @param {frame_id_t*} frame_id 被移除的frame的id，如果没有frame被移除返回nullptr
 * @return {bool} 如果成功淘汰了一个页面则返回true，否则返回false
 * @note: 实现思路：
 *       1. 从LRUlist_尾部获取最久未使用的页面
 *       2. 从LRUhash_中删除对应记录
 *       3. 从LRUlist_中移除该页面
 */
bool LRUReplacer::victim(frame_id_t *frame_id)
{
    // 使用RAII方式加锁，保证线程安全
    std::scoped_lock lock{latch_};

    // 如果没有可替换的页面，返回false
    if (LRUlist_.empty())
    {
        return false;
    }

    // 获取最久未使用的页面（链表尾部）
    *frame_id = LRUlist_.back();
    // 从哈希表和链表中删除该页面
    LRUhash_.erase(*frame_id);
    LRUlist_.pop_back();

    return true;
}

/**
 * @description: 固定指定的frame，即该页面无法被淘汰
 * @param {frame_id_t} frame_id 需要固定的frame的id
 * @note: 实现思路：
 *       1. 在LRUhash_中查找该页面
 *       2. 如果找到，从LRUlist_中删除该页面
 *       3. 同时从LRUhash_中删除对应记录
 */
void LRUReplacer::pin(frame_id_t frame_id)
{
    std::scoped_lock lock{latch_};

    // 查找该页面是否在可替换列表中
    auto it = LRUhash_.find(frame_id);
    if (it != LRUhash_.end())
    {
        // 从链表中删除
        LRUlist_.erase(it->second);
        // 从哈希表中删除
        LRUhash_.erase(it);
    }
}

/**
 * @description: 取消固定一个frame，代表该页面可以被淘汰
 * @param {frame_id_t} frame_id 取消固定的frame的id
 * @note: 实现思路：
 *       1. 检查该页面是否已经在可替换列表中
 *       2. 如果不在，将其添加到LRUlist_头部（最近使用）
 *       3. 在LRUhash_中记录其位置
 */
void LRUReplacer::unpin(frame_id_t frame_id)
{
    std::scoped_lock lock{latch_};

    // 如果该页面已经在可替换列表中，则不需要重复添加
    if (LRUhash_.find(frame_id) != LRUhash_.end())
    {
        return;
    }

    // 将页面添加到链表头部（最近使用）
    LRUlist_.push_front(frame_id);
    // 在哈希表中记录其位置
    LRUhash_[frame_id] = LRUlist_.begin();
}

/**
 * @description: 获取当前replacer中可以被淘汰的页面数量
 * @return {size_t} 返回当前可被淘汰的页面数量
 */
size_t LRUReplacer::Size() { return LRUlist_.size(); }
