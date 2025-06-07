#include "rm_manager.h"
#include "rm_file_handle.h"

std::shared_ptr<RmFileHandle> RmManager::open_file(const std::string &filename)
{
    int fd = disk_manager_->open_file(filename);
    return std::make_shared<RmFileHandle>(this, fd);
}

/**
 * @description: 关闭表的数据文件
 * @param {RmFileHandle*} file_handle 要关闭文件的句柄
 */
void RmManager::close_file(const RmFileHandle *file_handle, bool flush)
{
    if(flush)
        disk_manager_->write_page(file_handle->fd_, RM_FILE_HDR_PAGE, (char *)&file_handle->file_hdr_,
                                sizeof(file_handle->file_hdr_));
    // 缓冲区的所有页刷到磁盘，注意这句话必须写在close_file前面
    buffer_pool_manager_->flush_all_pages(file_handle->fd_, flush);
    disk_manager_->close_file(file_handle->fd_);
}