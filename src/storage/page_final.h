/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "common/config.h"

/**
 * @description: 存储层每个Page的id的声明
 */
struct PageId_Final
{
    int fd; //  Page所在的磁盘文件开启后的文件描述符, 来定位打开的文件在内存中的位置
    page_id_t page_no = INVALID_PAGE_ID;

    friend bool operator==(const PageId_Final &x, const PageId_Final &y) { return x.fd == y.fd && x.page_no == y.page_no; }
    bool operator<(const PageId_Final &x) const
    {
        if (fd < x.fd)
            return true;
        return page_no < x.page_no;
    }

    std::string toString()
    {
        return "{fd: " + std::to_string(fd) + " page_no: " + std::to_string(page_no) + "}";
    }

    inline int64_t Get() const
    {
        return (static_cast<int64_t>(fd << 16) | page_no);
    }
};

// PageId的自定义哈希算法, 用于构建unordered_map<PageId_Final, frame_id_t, PageIdHash_Final>
struct PageIdHash_Final
{
    size_t operator()(const PageId_Final &x) const { return (x.fd << 16) | x.page_no; }
};

template <>
struct std::hash<PageId_Final>
{
    size_t operator()(const PageId_Final &obj) const { return std::hash<int64_t>()(obj.Get()); }
};

/**
 * @description: Page类声明, Page是RMDB数据块的单位、是负责数据操作Record模块的操作对象，
 * Page对象在磁盘上有文件存储, 若在Buffer中则有帧偏移, 并非特指Buffer或Disk上的数据
 */
class Page_Final
{
    friend class BufferPoolManager_Final;

public:
    std::shared_mutex latch_;

    Page_Final() { reset_memory(); }

    ~Page_Final() = default;

    inline void lock()
    {
        // std::cout << id_.page_no<<" is lock\n";
        latch_.lock();
    }
    inline void lock_shared()
    {
        // std::cout << id_.page_no<<" is lock share\n";
        latch_.lock_shared();
    }
    inline void unlock()
    {
        // std::cout << id_.page_no<<" is unlock\n";
        latch_.unlock();
    }
    inline void unlock_shared()
    {
        // std::cout << id_.page_no<<" is unlock share\n";
        latch_.unlock_shared();
    }

    PageId_Final get_page_id() const { return id_; }

    inline char *get_data() { return data_; }

    bool is_dirty() const { return is_dirty_; }

    static constexpr size_t OFFSET_PAGE_START = 0;
    static constexpr size_t OFFSET_LSN = 0;
    static constexpr size_t OFFSET_PAGE_HDR = 4;

    inline lsn_t get_page_lsn() { return *reinterpret_cast<lsn_t *>(get_data() + OFFSET_LSN); }

    inline void set_page_lsn(lsn_t page_lsn) { memcpy(get_data() + OFFSET_LSN, &page_lsn, sizeof(lsn_t)); }
    /*

                +-------------------+
            | LSN (4 bytes)    | <- OFFSET_PAGE_START (0)
            |                  | <- OFFSET_LSN (0)
            +-------------------+
            | Actual Page_Final Data | <- OFFSET_PAGE_HDR (4)
            |                  |
            |                  |
            +-------------------+
    */
private:
    void reset_memory() { memset(data_, OFFSET_PAGE_START, PAGE_SIZE); } // 将data_的PAGE_SIZE个字节填充为0

    /** page的唯一标识符 */
    PageId_Final id_ = {.fd = -1, .page_no = INVALID_PAGE_ID};
    /** The actual data that is stored within a page.
     *  该页面在bufferPool中的偏移地址
     */
    char data_[PAGE_SIZE] = {};

    /** 脏页判断 */
    std::atomic<bool> is_dirty_{false};

    /** The pin count of this page. */
    std::atomic<int> pin_count_{0};
};