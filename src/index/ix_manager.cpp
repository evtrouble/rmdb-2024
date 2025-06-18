#include "ix_index_handle.h"
#include "ix_manager.h"

void IxManager::create_index(const std::string &filename, const std::vector<ColMeta> &index_cols)
{
    std::string ix_name = get_index_name(filename, index_cols);
    // Create index file
    disk_manager_->create_file(ix_name);
    // Open index file
    int fd = disk_manager_->open_file(ix_name);

    // Create file header and write to file
    // Theoretically we have: |page_hdr| + (|attr| + |rid|) * n <= PAGE_SIZE
    // but we reserve one slot for convenient inserting and deleting, i.e.
    // |page_hdr| + (|attr| + |rid|) * (n + 1) <= PAGE_SIZE
    int col_tot_len = 0;
    int col_num = index_cols.size();
    for (auto &col : index_cols)
    {
        col_tot_len += col.len;
    }
    if (col_tot_len > IX_MAX_COL_LEN)
    {
        throw InvalidColLengthError(col_tot_len);
    }
    // 根据 |page_hdr| + (|attr| + |rid|) * (n + 1) <= PAGE_SIZE 求得n的最大值btree_order
    // 即 n <= btree_order，那么btree_order就是每个结点最多可插入的键值对数量（实际还多留了一个空位，但其不可插入）
    int btree_order = static_cast<int>((PAGE_SIZE - sizeof(IxPageHdr)) / (col_tot_len + sizeof(Rid)) - 1);
    assert(btree_order > 2);

    // Create file header and write to file
    IxFileHdr *fhdr = new IxFileHdr(IX_INIT_ROOT_PAGE, col_num, col_tot_len, btree_order,
                                    (btree_order + 1) * col_tot_len);
    fhdr->col_types_.reserve(col_num);
    fhdr->col_lens_.reserve(col_num);
    for (int i = 0; i < col_num; ++i)
    {
        fhdr->col_types_.emplace_back(index_cols[i].type);
        fhdr->col_lens_.emplace_back(index_cols[i].len);
    }
    fhdr->update_tot_len();

    char *data = new char[fhdr->tot_len_];
    fhdr->serialize(data);

    disk_manager_->write_page(fd, IX_FILE_HDR_PAGE, data, fhdr->tot_len_);
    delete[] data;
    delete fhdr;

    char page_buf[PAGE_SIZE]; // 在内存中初始化page_buf中的内容，然后将其写入磁盘
    // 注意leaf header页号为1，也标记为叶子结点，其后一个叶子指向root node
    // Create leaf list header page and write to file
    {
        memset(page_buf, 0, PAGE_SIZE);
        auto phdr = reinterpret_cast<IxPageHdr *>(page_buf);
        *phdr = {
            .parent = IX_NO_PAGE,
            .num_key = 0,
            .is_leaf = true,
            .next_leaf = IX_INIT_ROOT_PAGE,
        };
        disk_manager_->write_page(fd, IX_LEAF_HEADER_PAGE, page_buf, PAGE_SIZE);
    }
    // 注意root node页号为2，也标记为叶子结点，其后一个叶子指向leaf header
    // Create root node and write to file
    {
        memset(page_buf, 0, PAGE_SIZE);
        auto phdr = reinterpret_cast<IxPageHdr *>(page_buf);
        *phdr = {
            .parent = IX_NO_PAGE,
            .num_key = 0,
            .is_leaf = true,
            .next_leaf = IX_LEAF_HEADER_PAGE};
        // Must write PAGE_SIZE here in case of future fetch_node()
        disk_manager_->write_page(fd, IX_INIT_ROOT_PAGE, page_buf, PAGE_SIZE);
    }

    disk_manager_->set_fd2pageno(fd, IX_INIT_NUM_PAGES - 1); // DEBUG

    // Close index file
    disk_manager_->close_file(fd);
}

void IxManager::destroy_index(const std::string &filename, const std::vector<ColMeta> &index_cols)
{
    std::string ix_name = get_index_name(filename, index_cols);
    disk_manager_->destroy_file(ix_name);
}

void IxManager::destroy_index(const std::string &filename, const std::vector<std::string> &index_cols)
{
    std::string ix_name = get_index_name(filename, index_cols);
    disk_manager_->destroy_file(ix_name);
}

// 注意这里打开文件，创建并返回了index file handle的指针
std::shared_ptr<IxIndexHandle> IxManager::open_index(const std::string &filename, const std::vector<ColMeta> &index_cols)
{
    std::string ix_name = get_index_name(filename, index_cols);
    int fd = disk_manager_->open_file(ix_name);
    return std::make_shared<IxIndexHandle>(this, fd);
}

std::shared_ptr<IxIndexHandle> IxManager::open_index(const std::string &filename, const std::vector<std::string> &index_cols)
{
    std::string ix_name = get_index_name(filename, index_cols);
    int fd = disk_manager_->open_file(ix_name);
    return std::make_shared<IxIndexHandle>(this, fd);
}

void IxManager::close_index(const IxIndexHandle *ih, bool flush)
{
    if(flush)
    {
        char *data = new char[ih->file_hdr_->tot_len_];
        ih->file_hdr_->serialize(data);
        disk_manager_->write_page(ih->fd_, IX_FILE_HDR_PAGE, data, ih->file_hdr_->tot_len_);
        delete[] data;
    }
    // 缓冲区的所有页刷到磁盘，注意这句话必须写在close_file前面
    buffer_pool_manager_->remove_all_pages(ih->fd_, flush);
    disk_manager_->close_file(ih->fd_);
}