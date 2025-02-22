/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_file_handle.h"

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid &rid, Context *context) const
{
    // 1. 获取指定记录所在的page handle
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);

    // 2. 检查记录是否存在
    if (!is_record(rid))
    {
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }

    // 3. 初始化一个指向RmRecord的指针（赋值其内部的data和size）
    auto record = std::make_unique<RmRecord>(file_hdr_.record_size);
    // 将slot中的数据复制到record中
    memcpy(record->data, page_handle.get_slot(rid.slot_no), file_hdr_.record_size);

    return record;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char *buf, Context *context)
{
    // 1. 获取当前未满的page handle
    RmPageHandle page_handle = create_page_handle();

    // 2. 在page handle中找到空闲slot位置
    int slot_no = page_handle.page_hdr->num_records;

    // 3. 将buf复制到空闲slot位置
    char *slot = page_handle.get_slot(slot_no);
    memcpy(slot, buf, file_hdr_.record_size);

    // 4. 更新page_handle.page_hdr中的数据结构
    page_handle.page_hdr->num_records++;
    Bitmap::set(page_handle.bitmap, slot_no); // 标记slot被使用

    // 5. 如果页面已满，更新空闲页面链表
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page)
    {
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
    }

    // 6. 返回新记录的RID
    Rid rid;
    rid.page_no = page_handle.page->get_page_id().page_no;
    rid.slot_no = slot_no;
    return rid;
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 */
void RmFileHandle::insert_record(const Rid &rid, char *buf)
{
    // 1. 获取页面句柄
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);

    // 2. 检查该位置是否已经有记录
    bool is_occupied = is_record(rid);
    if (is_occupied)
    {
        throw RMDBError("Cannot insert record: slot is already occupied");
    }

    // 3. 复制数据到指定slot
    char *slot = page_handle.get_slot(rid.slot_no);
    memcpy(slot, buf, file_hdr_.record_size);

    // 4. 更新bitmap和记录数
    Bitmap::set(page_handle.bitmap, rid.slot_no);
    page_handle.page_hdr->num_records++;

    // 5. 如果这是页面的第一条记录，需要更新空闲页面链表
    if (page_handle.page_hdr->num_records == 1)
    {
        if (file_hdr_.first_free_page_no == rid.page_no)
        {
            file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
        }
    }
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(const Rid &rid, Context *context)
{
    // 1. 获取指定记录所在的page handle
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);

    // 2. 检查记录是否存在
    if (!is_record(rid))
    {
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }

    // 3. 更新bitmap，标记slot为空闲
    Bitmap::reset(page_handle.bitmap, rid.slot_no);
    page_handle.page_hdr->num_records--;

    // 4. 如果页面从满状态变为未满状态，需要更新空闲页面链表
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page - 1)
    {
        release_page_handle(page_handle);
    }
}

/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record(const Rid &rid, char *buf, Context *context)
{
    // 1. 获取指定记录所在的page handle
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);

    // 2. 检查记录是否存在
    if (!is_record(rid))
    {
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }

    // 3. 更新记录数据
    char *slot = page_handle.get_slot(rid.slot_no);
    memcpy(slot, buf, file_hdr_.record_size);
}

/**
 * 以下函数为辅助函数，仅提供参考，可以选择完成如下函数，也可以删除如下函数，在单元测试中不涉及如下函数接口的直接调用
 */
/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const
{
    // 检查page_no的有效性
    if (page_no >= file_hdr_.num_pages)
    {
        throw PageNotExistError(std::string("Record File"), page_no);
    }

    // 使用缓冲池获取指定页面
    Page *page = buffer_pool_manager_->fetch_page(PageId{fd_, page_no});
    if (page == nullptr)
    {
        throw PageNotExistError(std::string("Record File"), page_no);
    }

    // 生成并返回page handle
    return RmPageHandle(&file_hdr_, page);
}

/**
 * @description: 创建一个新的page handle
 * @return {RmPageHandle} 新的PageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle()
{
    // 1.使用缓冲池来创建一个新page
    PageId new_page_id = {fd_, static_cast<int>(file_hdr_.num_pages)};
    Page *page = buffer_pool_manager_->new_page(&new_page_id);
    if (page == nullptr)
    {
        throw std::runtime_error("Failed to create new page");
    }

    // 2.更新page handle中的相关信息
    RmPageHandle page_handle(&file_hdr_, page);

    // 初始化页头信息
    page_handle.page_hdr->next_free_page_no = RM_NO_PAGE; // 新页面的下一个空闲页面初始化为-1
    page_handle.page_hdr->num_records = 0;                // 当前记录数为0

    // 初始化bitmap，将所有位都设置为0（表示所有slot都是空闲的）
    memset(page_handle.bitmap, 0, file_hdr_.bitmap_size);

    // 3.更新file_hdr_
    file_hdr_.num_pages++;

    return page_handle;
}

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::create_page_handle()
{
    // 1. 判断file_hdr_中是否还有空闲页
    if (file_hdr_.first_free_page_no == RM_NO_PAGE)
    {
        // 1.1 没有空闲页：使用缓冲池来创建一个新page
        RmPageHandle new_handle = create_new_page_handle();
        // 将新页面设置为第一个空闲页
        file_hdr_.first_free_page_no = file_hdr_.num_pages - 1;
        return new_handle;
    }
    else
    {
        // 1.2 有空闲页：直接获取第一个空闲页
        return fetch_page_handle(file_hdr_.first_free_page_no);
    }
}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle &page_handle)
{
    // 当page从已满变成未满，考虑如何更新：
    // 1. 将这个新的空闲页面插入到空闲页面链表的头部
    // 2. 让这个页面指向原来的第一个空闲页面
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    file_hdr_.first_free_page_no = page_handle.page->get_page_id().page_no;
}