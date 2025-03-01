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
bool LRUReplacer::victim(frame_id_t* frame_id) {
    std::lock_guard lock{latch_};  

    //  利用lru_replacer中的LRUlist_,LRUHash_实现LRU策略
    //  选择合适的frame指定为淘汰页面,赋值给*frame_id
    if(LRUlist_.empty()) {
        frame_id = nullptr;
        return false;
    }

    *frame_id = LRUlist_.back();
    LRUhash_.erase(*frame_id);
    LRUlist_.pop_back();
    return true;
}

/**
 * @description: 固定指定的frame，即该页面无法被淘汰
 * @param {frame_id_t} 需要固定的frame的id
 * @note: 实现思路：
 *       1. 在LRUhash_中查找该页面
 *       2. 如果找到，从LRUlist_中删除该页面
 *       3. 同时从LRUhash_中删除对应记录
 */
void LRUReplacer::pin(frame_id_t frame_id) {
    std::lock_guard lock{latch_};
    // 固定指定id的frame
    // 在数据结构中移除该frame
    auto iter = LRUhash_.find(frame_id);
    if (iter != LRUhash_.end()){
        LRUlist_.erase(iter->second);
        LRUhash_.erase(iter);
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
void LRUReplacer::unpin(frame_id_t frame_id) {
    //  支持并发锁
    //  选择一个frame取消固定
    std::lock_guard lock{latch_};
    if(!LRUhash_.count(frame_id)) {
        // 将页面添加到链表头部（最近使用）
        LRUlist_.emplace_front(frame_id);
        // 在哈希表中记录其位置
        LRUhash_.emplace(frame_id, LRUlist_.begin());
    }
}

/**
 * @description: 获取当前replacer中可以被淘汰的页面数量
 * @return {size_t} 返回当前可被淘汰的页面数量
 */
size_t LRUReplacer::Size() { return LRUlist_.size(); }
