cmake_minimum_required(VERSION 3.20)
project(kvstore CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)   # 给编辑器的代码提示用

add_compile_options(-Wall -Wextra -g)

# 可选：-DKV_SANITIZE=address 或 -DKV_SANITIZE=thread（两者不能同时开）
set(KV_SANITIZE "" CACHE STRING "Sanitizer to enable: address | thread | (empty)")
if(KV_SANITIZE STREQUAL "address")
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
elseif(KV_SANITIZE STREQUAL "thread")
  add_compile_options(-fsanitize=thread)
  add_link_options(-fsanitize=thread)
endif()

find_package(Threads REQUIRED)

# 存储引擎本体做成一个静态库，主程序和测试都链接它
add_library(kvcore STATIC
  src/bloom.cpp
  src/file_util.cpp
  src/kv_store.cpp
  src/sstable.cpp
)
target_include_directories(kvcore PUBLIC src)
target_link_libraries(kvcore PUBLIC Threads::Threads)

option(KV_FULLFSYNC "macOS: use F_FULLFSYNC instead of fsync (real durability, much slower)" OFF)
if(KV_FULLFSYNC)
  target_compile_definitions(kvcore PRIVATE KV_FULLFSYNC)
endif()

add_executable(kvstore src/main.cpp)
target_link_libraries(kvstore PRIVATE kvcore)

add_executable(lsm_test tests/lsm_test.cpp)
target_link_libraries(lsm_test PRIVATE kvcore)

enable_testing()
add_test(NAME lsm_test COMMAND lsm_test)
