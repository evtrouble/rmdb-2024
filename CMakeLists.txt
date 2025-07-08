cmake_minimum_required(VERSION 3.16)
project(RMDB)

# 设置默认构建类型为Release
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif()

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(CMAKE_CXX_STANDARD 17)

# 基础编译标志
set(CMAKE_CXX_FLAGS "-Wall")

# Debug模式标志
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -ggdb3")
set(CMAKE_C_FLAGS_DEBUG "-O0 -g")

# Release模式标志
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
set(CMAKE_C_FLAGS_RELEASE "-O3 -DNDEBUG")

enable_testing()
add_subdirectory(src)
add_subdirectory(deps)
