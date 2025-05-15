set(SOURCES ix_index_handle.cpp 
    ix_scan.cpp
    block_cache.cpp
    block.cpp
    iterator.cpp
    lsmtree.cpp
    memtable.cpp
    skiplist.cpp
    sstable.cpp
)
add_library(index STATIC ${SOURCES})
target_link_libraries(index storage)