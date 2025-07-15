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
#include "transaction/transaction_manager.h"

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid &rid, Context *context)
{
    // 1. 获取指定记录所在的page handle
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    std::shared_lock lock(page_handle.page->latch_);

    // 2. 检查记录是否存在
    if (!is_record(page_handle, rid))
    {
        rm_manager_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }

    // 3. 初始化一个指向RmRecord的指针（赋值其内部的data和size）
    auto record = std::make_unique<RmRecord>(file_hdr_.record_size);
    // 将slot中的数据复制到record中
    memcpy(record->data, page_handle.get_slot(rid.slot_no), file_hdr_.record_size);
    rm_manager_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);

    return record;
}

std::vector<std::pair<std::unique_ptr<RmRecord>, int>> RmFileHandle::get_records(int page_no, Context *context)
{
    // 1. 获取指定记录所在的page handle
    std::vector<std::pair<std::unique_ptr<RmRecord>, int>> records;
    int slot_no = -1;
    records.reserve(file_hdr_.num_records_per_page); // 预分配空间，避免多次扩容
    TransactionManager *txn_manager = context->txn_->get_txn_manager();

    RmPageHandle page_handle = fetch_page_handle(page_no);
    std::shared_lock lock(page_handle.page->latch_);

    while (true)
    {
        slot_no = Bitmap::next_bit(true, page_handle.bitmap, file_hdr_.num_records_per_page, slot_no);

        if (slot_no >= file_hdr_.num_records_per_page)
        {
            // 如果没有找到空闲位置，则退出循环
            break;
        }

        char *data = page_handle.get_slot(slot_no);
        txn_id_t txn_id = txn_manager->get_record_txn_id(data);
        Transaction *record_txn = txn_manager->get_or_create_transaction(txn_id);

        // 1. 检查是否需要查找版本链
        if (txn_manager->need_find_version_chain(record_txn, context->txn_))
        {
            // 需要查找版本链，让上层处理
            records.emplace_back(std::make_pair(nullptr, slot_no));
            continue;
        }

        // 2. 当前版本可见，检查是否是删除记录
        bool is_deleted = txn_manager->is_deleted(txn_id);
        if (is_deleted)
        {
            // 记录已被删除，跳过
            continue;
        }

        // 3. 记录可见且未删除，复制数据
        auto record = std::make_unique<RmRecord>(file_hdr_.record_size);
        memcpy(record->data, data, file_hdr_.record_size);
        records.emplace_back(std::make_pair(std::move(record), slot_no));
    }
    rm_manager_->buffer_pool_manager_->unpin_page({fd_, page_no}, false);
    return records;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char *buf, Context *context)
{
    while (true)
    { // 循环尝试，直到插入成功
        // 1. 获取一个可能有空闲空间的页面
        RmPageHandle page_handle = create_page_handle();

        // 2. 获取页面锁
        std::lock_guard lock(page_handle.page->latch_);

        // 3. 在page handle中尝试找到空闲slot位置
        int slot_no = Bitmap::first_bit(false, page_handle.bitmap, file_hdr_.num_records_per_page);

        // 如果这个页面已经没有空闲slot，释放这个页面并继续尝试
        if (slot_no >= file_hdr_.num_records_per_page)
        {
            rm_manager_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
            continue;
        }

        // 4. 找到空闲位置，设置bitmap
        Bitmap::set(page_handle.bitmap, slot_no); // 立即标记为已使用，防止其他进程选中同一个slot

        // 5. 复制数据到slot
        char *slot = page_handle.get_slot(slot_no);
        memcpy(slot, buf, file_hdr_.record_size);

        // 6. 更新记录数
        page_handle.page_hdr->num_records++;

        // 7. 如果页面已满，更新空闲页面链表
        if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page)
        {
            std::lock_guard file_lock(lock_);
            if (file_hdr_.first_free_page_no == page_handle.page->get_page_id().page_no)
            {
                file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
            }
        }

        // 8. 创建返回的RID
        Rid rid{page_handle.page->get_page_id().page_no, slot_no};

        // 9. 解除页面固定
        rm_manager_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);

        return rid;
    }
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
    std::lock_guard lock(page_handle.page->latch_);

    // 2. 检查该位置是否已经有记录
    bool is_occupied = is_record(page_handle, rid);
    if (is_occupied)
    {
        rm_manager_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw RMDBError("Cannot insert record: slot is already occupied");
    }

    // 3. 复制数据到指定slot
    memcpy(page_handle.get_slot(rid.slot_no), buf, file_hdr_.record_size);

    // 4. 更新bitmap和记录数
    Bitmap::set(page_handle.bitmap, rid.slot_no);
    ++page_handle.page_hdr->num_records;

    // {
    //     std::lock_guard lock(lock_);
    //     if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page)
    //         file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
    // }
    rm_manager_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

void RmFileHandle::recovery_insert_record(const Rid &rid, char *buf)
{
    // 1. 获取页面句柄
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    std::lock_guard page_lock(page_handle.page->latch_);

    bool is_occupied = is_record(page_handle, rid);
    // 2. 复制数据到指定slot
    memcpy(page_handle.get_slot(rid.slot_no), buf, file_hdr_.record_size);

    if (!is_occupied)
    {
        // 3. 更新bitmap和记录数
        Bitmap::set(page_handle.bitmap, rid.slot_no);
        ++page_handle.page_hdr->num_records;

        // if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page)
        // {
        //     std::lock_guard file_lock(lock_);
        //     if (file_hdr_.first_free_page_no == page_handle.page->get_page_id().page_no) {
        //         file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
        //     }
        // }
    }
    rm_manager_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
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
    std::lock_guard lock(page_handle.page->latch_);

    // 2. 检查记录是否存在
    if (!is_record(page_handle, rid))
    {
        rm_manager_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }

    // 3. 更新bitmap，标记slot为空闲
    // Bitmap::reset(page_handle.bitmap, rid.slot_no);
    // page_handle.page_hdr->num_records--;

    TransactionManager *txn_mgr = context->txn_->get_txn_manager();
    char *data = page_handle.get_slot(rid.slot_no);
    if (txn_mgr->get_concurrency_mode() == ConcurrencyMode::MVCC)
    {
        txn_id_t txn_id = txn_mgr->get_record_txn_id(data);
        Transaction *record_txn = txn_mgr->get_or_create_transaction(txn_id);
        if (txn_mgr->is_write_conflict(record_txn, context->txn_))
        {
            rm_manager_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
            throw TransactionAbortException(context->txn_->get_transaction_id(),
                                            AbortReason::UPGRADE_CONFLICT);
        }

        if (record_txn != context->txn_)
        {
            UndoLog *undolog = new UndoLog(RmRecord(data, file_hdr_.record_size),
                                           record_txn);
            UpdateUndoLink(rid, undolog);
            txn_mgr->set_record_txn_id(data, context->txn_, true);

            auto write_record = new WriteRecord(WType::DELETE_TUPLE, rm_manager_->disk_manager_->get_file_name(fd_),
                                                rid, undolog);
            context->txn_->append_write_record(write_record);
        }
        else
        {
            txn_mgr->set_record_txn_id_without_dup(data, context->txn_, true);
        }
    }
    else
    {
        Bitmap::reset(page_handle.bitmap, rid.slot_no);
        page_handle.page_hdr->num_records--;
        // 4. 如果页面从满状态变为未满状态，需要更新空闲页面链表
        if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page - 1)
            release_page_handle(page_handle);
    }

    rm_manager_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

void RmFileHandle::recovery_delete_record(const Rid &rid)
{
    // 1. 获取页面句柄
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    std::lock_guard lock(page_handle.page->latch_);

    bool is_occupied = is_record(page_handle, rid);
    if (is_occupied)
    {
        page_handle.page_hdr->num_records--;
        // 2. 更新bitmap和记录数
        Bitmap::reset(page_handle.bitmap, rid.slot_no);

        // 3. 如果页面从满状态变为未满状态，需要更新空闲页面链表
        if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page - 1)
            release_page_handle(page_handle);
    }
    rm_manager_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), is_occupied);
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
    std::lock_guard lock(page_handle.page->latch_);

    // 2. 检查记录是否存在
    if (!is_record(page_handle, rid))
    {
        rm_manager_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }

    TransactionManager *txn_mgr = context->txn_->get_txn_manager();
    char *data = page_handle.get_slot(rid.slot_no);
    if (txn_mgr->get_concurrency_mode() == ConcurrencyMode::MVCC)
    {
        txn_id_t txn_id = txn_mgr->get_record_txn_id(data);
        Transaction *record_txn = txn_mgr->get_or_create_transaction(txn_id);
        if (txn_mgr->is_write_conflict(record_txn, context->txn_))
        {
            rm_manager_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
            throw TransactionAbortException(context->txn_->get_transaction_id(),
                                            AbortReason::UPGRADE_CONFLICT);
        }
        if (record_txn != context->txn_)
        {
            txn_mgr->set_record_txn_id(buf, context->txn_);
            UndoLog *undolog = new UndoLog(RmRecord(data, file_hdr_.record_size),
                                           record_txn);
            UpdateUndoLink(rid, undolog);

            auto write_record = new WriteRecord(WType::UPDATE_TUPLE, rm_manager_->disk_manager_->get_file_name(fd_),
                                                rid, undolog);
            context->txn_->append_write_record(write_record);
            memcpy(page_handle.get_slot(rid.slot_no), buf, file_hdr_.record_size);
        }
        else
        {
            txn_mgr->set_record_txn_id_without_dup(buf, context->txn_);
        }
    }

    memcpy(page_handle.get_slot(rid.slot_no), buf, file_hdr_.record_size);
    rm_manager_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * 以下函数为辅助函数，仅提供参考，可以选择完成如下函数，也可以删除如下函数，在单元测试中不涉及如下函数接口的直接调用
 */
/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no)
{
    // 检查page_no的有效性
    {
        std::shared_lock lock(lock_); // 添加共享锁保护file_hdr_访问
        if (page_no >= file_hdr_.num_pages)
        {
            throw PageNotExistError(std::string("Record File"), page_no);
        }
    }

    // 使用缓冲池获取指定页面
    Page *page = rm_manager_->buffer_pool_manager_->fetch_page(PageId{fd_, page_no});
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
    Page *page = rm_manager_->buffer_pool_manager_->new_page(&new_page_id);
    if (page == nullptr)
    {
        throw std::runtime_error("Failed to create new page");
    }

    // 2.更新page handle中的相关信息
    RmPageHandle page_handle(&file_hdr_, page);

    // 初始化页头信息
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    page_handle.page_hdr->num_records = 0; // 当前记录数为0

    // 初始化bitmap，将所有位都设置为0（表示所有slot都是空闲的）
    memset(page_handle.bitmap, 0, file_hdr_.bitmap_size);

    // 3.更新file_hdr_
    ++file_hdr_.num_pages;
    file_hdr_.first_free_page_no = new_page_id.page_no;
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
    std::lock_guard lock(lock_);

    // 1. 判断file_hdr_中是否还有空闲页
    if (file_hdr_.first_free_page_no == RM_NO_PAGE)
    {
        // 1.1 没有空闲页：使用缓冲池来创建一个新page
        RmPageHandle new_handle = create_new_page_handle();
        // 将新页面设置为第一个空闲页
        // file_hdr_.first_free_page_no = file_hdr_.num_pages - 1;
        return new_handle;
    }
    // 1.2 有空闲页：直接获取第一个空闲页
    // 使用缓冲池获取指定页面
    Page *page = rm_manager_->buffer_pool_manager_->fetch_page(PageId{fd_, file_hdr_.first_free_page_no});
    if (page == nullptr)
    {
        throw PageNotExistError(std::string("Record File"), file_hdr_.first_free_page_no);
    }

    // 生成并返回page handle
    return RmPageHandle(&file_hdr_, page);
}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle &page_handle)
{
    // 当page从已满变成未满，考虑如何更新：
    // 1. 将这个新的空闲页面插入到空闲页面链表的头部
    // 2. 让这个页面指向原来的第一个空闲页面
    std::lock_guard lock(lock_);
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    file_hdr_.first_free_page_no = page_handle.page->get_page_id().page_no;
}

void RmFileHandle::abort_insert_record(const Rid &rid)
{
    // 1. 获取指定记录所在的page handle
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    std::lock_guard lock(page_handle.page->latch_);

    // 2. 检查记录是否存在
    // if (!is_record(page_handle, rid)) {
    //     rm_manager_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
    //     throw RecordNotFoundError(rid.page_no, rid.slot_no);
    // }
    bool is_occupied = is_record(page_handle, rid);

    if (!is_occupied)
    {
        rm_manager_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        return; // 如果没有记录，直接返回
    }
    // 3. 更新bitmap，标记slot为空闲
    Bitmap::reset(page_handle.bitmap, rid.slot_no);
    page_handle.page_hdr->num_records--;

    // 4. 如果页面从满状态变为未满状态，需要更新空闲页面链表
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page - 1)
        release_page_handle(page_handle);

    rm_manager_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

void RmFileHandle::abort_delete_record(const Rid &rid, char *buf)
{
    // 获取指定记录所在的page handle
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    std::lock_guard lock(page_handle.page->latch_);

    // 复制数据到指定slot
    memcpy(page_handle.get_slot(rid.slot_no), buf, file_hdr_.record_size);

    // 更新bitmap和记录数
    if (!is_record(page_handle, rid))
    {
        ++page_handle.page_hdr->num_records;
        Bitmap::set(page_handle.bitmap, rid.slot_no);
        // std::lock_guard lock(lock_);
        // if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page)
        //     file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
    }

    rm_manager_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

void RmFileHandle::abort_update_record(const Rid &rid, char *buf)
{
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    std::lock_guard lock(page_handle.page->latch_);

    // 检查记录是否存在
    // if (!is_record(page_handle, rid)) {
    //     rm_manager_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
    //     throw RecordNotFoundError(rid.page_no, rid.slot_no);
    // }

    if (!is_record(page_handle, rid))
    {
        ++page_handle.page_hdr->num_records;
        Bitmap::set(page_handle.bitmap, rid.slot_no);
        // std::lock_guard lock(lock_);
        // if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page)
        //     file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
    }
    memcpy(page_handle.get_slot(rid.slot_no), buf, file_hdr_.record_size);
    rm_manager_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

void RmFileHandle::clean_page(int page_no, TransactionManager *txn_mgr, timestamp_t watermark)
{
    // 清理当前页面
    RmPageHandle page_handle = fetch_page_handle(page_no);
    std::vector<std::pair<Transaction *, int>> to_delete;
    to_delete.reserve(file_hdr_.num_records_per_page);

    // 获取页面版本信息
    auto version = GetPageVersionInfo(page_no);
    if (version == nullptr)
    {
        rm_manager_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        return;
    }

    // 扫描页面中的所有槽位
    Rid rid{.page_no = page_no};
    int slot_no = -1;
    {
        std::shared_lock read_lock(page_handle.page->latch_);
        if (page_handle.page_hdr->num_records == 0)
        {
            // 如果页面没有记录，直接解锁并返回
            rm_manager_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
            return;
        }

        while (true)
        {
            slot_no = Bitmap::next_bit(true, page_handle.bitmap, file_hdr_.num_records_per_page, slot_no);

            if (slot_no >= file_hdr_.num_records_per_page)
            {
                break;
            }

            char *data = page_handle.get_slot(slot_no);
            txn_id_t txn_id = txn_mgr->get_record_txn_id(data);
            Transaction *record_txn = txn_mgr->get_or_create_transaction(txn_id);
            rid.slot_no = slot_no;

            if (txn_mgr->need_clean(record_txn, watermark))
            {
                DeleteVersionChain(rid);
                if (txn_mgr->is_deleted(txn_id))
                {
                    to_delete.emplace_back(std::make_pair(record_txn, slot_no));
                }
            }
            else
            {
                TruncateVersionChain(rid, watermark);
            }
        }
    }

    // 处理需要删除的记录
    bool change = false;
    if (to_delete.size())
    {
        change = true;
        bool was_full = (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page);
        std::lock_guard write_lock(page_handle.page->latch_);
        for (auto &[txn, slot_no] : to_delete)
        {
            Bitmap::reset(page_handle.bitmap, slot_no);
            txn->release();
        }
        page_handle.page_hdr->num_records -= to_delete.size();
        // 如果页面从满状态变为未满状态，需要更新空闲页面链表
        if (was_full)
        {
            release_page_handle(page_handle);
        }
    }

    rm_manager_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), change);
}

bool RmFileHandle::clean_pages(TransactionManager *txn_mgr, timestamp_t watermark)
{
    int num_pages = get_page_num();

    while (cleaning_progress_.current_page < num_pages &&
           cleaning_progress_.pages_scanned < CleaningProgress::MAX_PAGES_PER_SCAN)
    {

        clean_page(cleaning_progress_.current_page, txn_mgr, watermark);

        cleaning_progress_.current_page++;
        cleaning_progress_.pages_scanned++;
    }

    // 如果完成了所有页面的扫描,重置进度
    if (cleaning_progress_.current_page >= num_pages)
    {
        std::shared_lock file_lock(lock_); // 再次检查表头,因为可能已经发生变化
        if (cleaning_progress_.current_page > file_hdr_.num_pages)
        {
            cleaning_progress_.current_page = RM_FIRST_RECORD_PAGE;
            cleaning_progress_.pages_scanned = 0;
            return true; // 表示完成了一轮完整扫描
        }
    }

    // 如果达到了本轮最大页面扫描限制
    if (cleaning_progress_.pages_scanned >= CleaningProgress::MAX_PAGES_PER_SCAN)
    {
        cleaning_progress_.pages_scanned = 0;
        return false; // 表示本轮扫描未完成
    }

    return true;
}

void RmFileHandle::ensure_file_size()
{
    DiskManager *disk_manager = rm_manager_->disk_manager_;
    disk_manager->ensure_file_size(fd_, file_hdr_.num_pages);
}

// 批量插入记录方法
std::vector<Rid> RmFileHandle::batch_insert_records(const std::vector<std::unique_ptr<char[]>> &records, Context *context)
{
    std::vector<Rid> rids;
    rids.reserve(records.size());

    if (records.empty())
    {
        return rids;
    }

    RmPageHandle page_handle = create_page_handle();
    std::unique_lock lock(page_handle.page->latch_);

    for (const auto &record : records)
    {
        // 检查当前页面是否还有空间
        if (page_handle.page_hdr->num_records >= file_hdr_.num_records_per_page)
        {
            // 当前页面已满，释放并创建新页面
            rm_manager_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
            page_handle = create_page_handle();
            lock = std::unique_lock(page_handle.page->latch_);
            // 注意：这里需要重新获取锁，但由于是新的page_handle，会自动获取新的锁
        }

        // 找到空闲slot
        int slot_no = Bitmap::first_bit(false, page_handle.bitmap, file_hdr_.num_records_per_page);

        if (slot_no < file_hdr_.num_records_per_page)
        {
            // 设置bitmap和复制数据
            Bitmap::set(page_handle.bitmap, slot_no);
            memcpy(page_handle.get_slot(slot_no), record.get(), file_hdr_.record_size);
            page_handle.page_hdr->num_records++;

            // 记录RID
            rids.emplace_back(Rid{page_handle.page->get_page_id().page_no, slot_no});

            // 如果页面已满，更新空闲页面链表
            if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page)
            {
                std::lock_guard file_lock(lock_);
                if (file_hdr_.first_free_page_no == page_handle.page->get_page_id().page_no)
                {
                    file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
                }
            }
        }
    }

    rm_manager_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
    return rids;
}

void RmFileHandle::TruncateVersionChain(std::shared_ptr<PageVersionInfo> page_info,
                                              int slot_no, timestamp_t watermark)
{
    // 获取页面级读锁保护版本链遍历
    std::shared_lock page_lock(page_info->mutex_);
    auto version_it = page_info->prev_version_.find(slot_no);
    if (version_it == page_info->prev_version_.end())
    {
        return;
    }
    auto *current = version_it->second;
    page_lock.unlock(); // 释放读锁以避免长时间持有，后续操作需要写锁

    // 遍历版本链找到截断点 - 保持页面级读锁
    while (current)
    {
        int ts = current->txn_->get_commit_ts();
        if (ts != INVALID_TIMESTAMP && ts < watermark)
        {
            // 设置截断点的下一个版本为空
            auto next = current->prev_version_;
            current->prev_version_ = nullptr; // 清除当前版本的前一个版本引用

            // 清理被截断部分的引用计数
            while (next)
            {
                auto tmp = next;
                next = next->prev_version_;
                delete tmp;
            }
            return;
        }

        current = current->prev_version_;
    }
}

void RmFileHandle::TruncateVersionChain(const Rid &rid, timestamp_t watermark)
{
    // 先用读锁获取page_info
    std::shared_lock global_read_lock(version_info_mutex_);
    auto page_it = version_info_.find(rid.page_no);
    if (page_it == version_info_.end())
    {
        return;
    }
    auto page_info = page_it->second;
    global_read_lock.unlock(); // 获取到版本链头后释放全局读锁

    TruncateVersionChain(page_info, rid.slot_no, watermark);
}

void RmFileHandle::DeleteVersionChain(const Rid &rid)
{
    // 先用读锁获取page_info
    std::shared_lock global_read_lock(version_info_mutex_);
    auto page_it = version_info_.find(rid.page_no);
    if (page_it == version_info_.end())
    {
        return;
    }
    auto page_info = page_it->second;
    global_read_lock.unlock();

    DeleteVersionChain(page_info, rid);
}

void RmFileHandle::DeleteVersionChain(std::shared_ptr<PageVersionInfo> page_info,
                                            const Rid &rid)
{
    // 获取page级别的写锁来保护空检查和删除操作
    std::unique_lock page_lock(page_info->mutex_);

    auto version_it = page_info->prev_version_.find(rid.slot_no);
    if (version_it != page_info->prev_version_.end())
    {
        auto current = version_it->second;
        page_info->prev_version_.erase(version_it);

        // 检查是否删除后变为空
        bool is_empty = page_info->prev_version_.empty();
        page_lock.unlock(); // 释放页面锁以避免长时间持有
        if (is_empty)
        {
            std::lock_guard global_write_lock(version_info_mutex_);
            // 二次检查,因为可能在释放页面锁后其他线程添加了记录
            if (page_info->prev_version_.empty())
            {
                version_info_.erase(rid.page_no);
            }
        }

        // 清理链上所有版本
        while (current)
        {
            auto tmp = current;
            current = current->prev_version_;
            delete tmp;
        }
    }
}

bool RmFileHandle::UpdateUndoLink(const Rid &rid, UndoLog *prev_link)
{
    // 1. 快速路径：如果不需要更新版本且没有check函数，直接返回
    std::shared_ptr<PageVersionInfo> page_info;
    {
        // 2. 获取页面版本信息 - 使用局部作用域减少全局锁持有时间
        std::shared_lock global_lock(version_info_mutex_);
        auto it = version_info_.find(rid.page_no);
        if (it != version_info_.end())
        {
            page_info = it->second;
        }
    }

    // 3. 如果需要创建新页面，使用写锁重新检查和创建
    if (!page_info)
    {
        std::lock_guard global_lock(version_info_mutex_);
        page_info = version_info_.try_emplace(rid.page_no, std::make_shared<PageVersionInfo>()).first->second;
    }

    // 4. 更新版本链 - 使用页面级锁
    std::lock_guard page_lock(page_info->mutex_);

    // 5. 获取当前版本用于check
    UndoLog *current_version = nullptr;
    auto version_it = page_info->prev_version_.find(rid.slot_no);
    if (version_it != page_info->prev_version_.end())
    {
        current_version = version_it->second;
    }

    // 7. 更新版本信息
    prev_link->prev_version_ = current_version;

    // 更新或插入新版本
    page_info->prev_version_.insert_or_assign(rid.slot_no, prev_link);
    return true;
}

std::optional<RmRecord> RmFileHandle::GetVisibleRecord(const Rid &rid, Transaction *current_txn)
{
    if (!current_txn)
    {
        return std::nullopt;
    }

    // 获取版本链头
    std::shared_lock global_lock(version_info_mutex_);
    auto page_info = version_info_.find(rid.page_no);
    if (page_info == version_info_.end())
    {
        return std::nullopt;
    }
    auto page_info_ptr = page_info->second;
    global_lock.unlock(); // 释放全局锁以避免长时间持有

    return GetVisibleRecord(page_info_ptr, rid, current_txn);
}

std::optional<RmRecord> RmFileHandle::GetVisibleRecord(std::shared_ptr<PageVersionInfo> page_info_ptr,
                                                             const Rid &rid, Transaction *current_txn)
{
    // 获取记录版本链
    std::shared_lock lock(page_info_ptr->mutex_);
    auto it = page_info_ptr->prev_version_.find(rid.slot_no);
    if (it == page_info_ptr->prev_version_.end())
    {
        return std::nullopt;
    }
    auto current = it->second;
    lock.unlock(); // 释放锁以避免长时间持有

    // 遍历版本链找到可见版本
    while (current)
    {
        // 已提交事务的版本根据时间戳判断可见性
        int ts = current->txn_->get_commit_ts();
        if (ts != INVALID_TIMESTAMP && ts <= current_txn->get_start_ts())
        {
            return RmRecord(current->tuple_.data, current->tuple_.size, false);
        }

        // 获取前一个版本
        current = current->prev_version_;
    }

    return std::nullopt;
}

std::shared_ptr<PageVersionInfo> RmFileHandle::GetPageVersionInfo(int page_no)
{
    // 获取页面版本信息
    std::shared_lock global_read_lock(version_info_mutex_);
    auto page_it = version_info_.find(page_no);
    if (page_it == version_info_.end())
    {
        return nullptr;
    }
    return page_it->second;
}