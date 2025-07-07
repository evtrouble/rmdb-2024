set(SOURCES
        disk_manager_final.cpp
        disk_manager.cpp
        buffer_pool_manager_final.cpp
        buffer_pool_manager.cpp
        ../replacer/replacer.h
        ../replacer/lru_replacer_final.cpp
        ../replacer/lru_replacer.cpp
        ../replacer/clock_replacer_final.cpp
)
add_library(storage STATIC ${SOURCES})
