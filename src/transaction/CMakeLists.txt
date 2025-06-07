set(SOURCES concurrency/lock_manager.cpp transaction_manager.cpp watermark.cpp transaction.cpp)
add_library(transaction STATIC ${SOURCES})
target_link_libraries(transaction system recovery pthread)
