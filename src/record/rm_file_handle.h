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

#include <assert.h>

#include <memory>

#include "bitmap.h"
#include "common/context.h"
#include "rm_defs.h"
#include "rm_manager.h"

class RmManager;

/* 对表数据文件中的页面进行封装 */
struct RmPageHandle
{
    const RmFileHdr *file_hdr; // 当前页面所在文件的文件头指针
    Page *page;                // 页面的实际数据，包括页面存储的数据、元信息等
    RmPageHdr *page_hdr;       // page->data的第一部分，存储页面元信息，指针指向首地址，长度为sizeof(RmPageHdr)
    char *bitmap;              // page->data的第二部分，存储页面的bitmap，指针指向首地址，长度为file_hdr->bitmap_size
    char *slots;               // page->data的第三部分，存储表的记录，指针指向首地址，每个slot的长度为file_hdr->record_size

    RmPageHandle(const RmFileHdr *fhdr_, Page *page_) : file_hdr(fhdr_), page(page_)
    {
        page_hdr = reinterpret_cast<RmPageHdr *>(page->get_data() + page->OFFSET_PAGE_HDR);
        bitmap = page->get_data() + sizeof(RmPageHdr) + page->OFFSET_PAGE_HDR;
        slots = bitmap + file_hdr->bitmap_size;
    }

    // 返回指定slot_no的slot存储首地址
    char *get_slot(int slot_no) const
    {
        return slots + slot_no * file_hdr->record_size; // slots的首地址 + slot个数 * 每个slot的大小(每个record的大小)
    }
};

/* 每个RmFileHandle对应一个表的数据文件，里面有多个page，每个page的数据封装在RmPageHandle中 */
class RmFileHandle
{
    friend class RmScan;
    friend class RmManager;

private:
    RmManager *rm_manager_; // 记录管理器，用于管理表的数据文件
    int fd_;             // 打开文件后产生的文件句柄
    RmFileHdr file_hdr_; // 文件头，维护当前表文件的元数据
    std::mutex lock_;    // 锁，用于保护文件头的读写操作
    bool is_deleted_ = false; // 标记文件是否被删除

public:
    RmFileHandle(RmManager *rm_manager, int fd)
        : rm_manager_(rm_manager), fd_(fd)
    {
        DiskManager *disk_manager_ = rm_manager_->disk_manager_;
        // 注意：这里从磁盘中读出文件描述符为fd的文件的file_hdr，读到内存中
        // 这里实际就是初始化file_hdr，只不过是从磁盘中读出进行初始化
        // init file_hdr_
        disk_manager_->read_page(fd, RM_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
        // disk_manager管理的fd对应的文件中，设置从file_hdr_.num_pages开始分配page_no
        disk_manager_->set_fd2pageno(fd, file_hdr_.num_pages);
    }

    ~RmFileHandle()
    {
        DiskManager *disk_manager_ = rm_manager_->disk_manager_;
        if(is_deleted_) {
            std::string file_name = disk_manager_->get_file_name(fd_);
            // 如果文件被标记为已删除，则不需要写回file_hdr到磁盘
            rm_manager_->close_file(this, false);
            disk_manager_->destroy_file(file_name);
        }
        else
            rm_manager_->close_file(this);
    }

    RmFileHdr get_file_hdr() const { return file_hdr_; }
    int GetFd() { return fd_; }
    inline void mark_deleted() { is_deleted_ = true; } // 标记文件为已删除
    inline BufferPoolManager *get_buffer_pool_manager() const
    {
        return rm_manager_->buffer_pool_manager_;
    }

    /* 判断指定位置上是否已经存在一条记录，通过Bitmap来判断 */
    bool is_record(const Rid &rid) const
    {
        RmPageHandle page_handle = fetch_page_handle(rid.page_no);
        return Bitmap::is_set(page_handle.bitmap, rid.slot_no); // page的slot_no位置上是否有record
    }

    std::unique_ptr<RmRecord> get_record(const Rid &rid, Context *context) const;
    std::vector<std::pair<std::unique_ptr<RmRecord>, int>> get_records(int page_no, Context *context) const;

    Rid insert_record(char *buf, Context *context);

    void insert_record(const Rid &rid, char *buf);

    void delete_record(const Rid &rid, Context *context);

    void update_record(const Rid &rid, char *buf, Context *context);

    RmPageHandle create_new_page_handle();

    RmPageHandle fetch_page_handle(int page_no) const;

    // 获取记录的可见版本
    std::unique_ptr<RmRecord> get_record_version(const Rid &rid, Context *context) const;

private:
    RmPageHandle create_page_handle();

    void release_page_handle(RmPageHandle &page_handle);
};