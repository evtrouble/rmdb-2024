
#include "rm_scan.h"

#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle)
{
    // 初始化file_handle和rid（指向第一个存放了记录的位置）
    rid_.page_no = RM_FILE_HDR_PAGE + 1; // 从第一个数据页开始
    rid_.slot_no = -1;
    next(); // 找到第一个有效的记录位置
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next()
{
    while (rid_.page_no < file_handle_->file_hdr_.num_pages)
    {
        RmPageHandle page_handle = file_handle_->fetch_page_handle(rid_.page_no);
        int next_slot = Bitmap::next_bit(true, page_handle.bitmap, file_handle_->file_hdr_.num_records_per_page, rid_.slot_no);
        file_handle_->buffer_pool_manager_->unpin_page({file_handle_->fd_, rid_.page_no}, false);
        if (next_slot < file_handle_->file_hdr_.num_records_per_page)
        {
            rid_.slot_no = next_slot;
            return;
        }
        // 移动到下一页
        ++rid_.page_no;
        rid_.slot_no = -1;
    }
    // 如果没有找到有效的记录，设置为文件结束
    rid_.page_no = RM_NO_PAGE;
    // rid_.slot_no = RM_NO_SLOT;
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const { return rid_.page_no == RM_NO_PAGE; }

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const { return rid_; }