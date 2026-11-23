#pragma once

// 文件 I/O 小工具：都是对系统调用的薄封装，出错一律抛 std::system_error。

#include <cstddef>
#include <cstdint>
#include <string>

[[noreturn]] void throw_errno(const std::string& what);

// 写满 len 字节（处理部分写入和 EINTR）
void write_all(int fd, const char* buf, size_t len);

// 从当前偏移读，尽量读满；返回实际读到的字节数（< len 表示 EOF）
size_t read_full(int fd, char* buf, size_t len);

// 从指定偏移读满 len 字节，读不满就抛异常（说明文件被截断了）。
// pread 不改变 fd 的偏移，所以多个线程可以同时对同一个 fd 调用。
void pread_full(int fd, char* buf, size_t len, uint64_t offset);

// 把 fd 的数据真正落盘（macOS 上用 -DKV_FULLFSYNC=ON 切换到 F_FULLFSYNC）
void sync_fd(int fd);

// fsync 目录本身：新建 / rename / 删除文件后，目录项的变化也要落盘，
// 否则断电后文件内容在、文件名却可能不见了。
void sync_dir(const std::string& dir);

uint64_t file_size(int fd);

// 读整个文件到 string
std::string read_file(const std::string& path);

// 原子地把 data 写成 path：写 path.tmp -> fsync -> rename -> fsync 目录。
// rename 是原子的，所以任何时刻 path 要么是旧内容，要么是完整的新内容。
void write_file_atomic(const std::string& dir, const std::string& path, const std::string& data);

// ---- 文件命名 ----
// <dir>/000005.wal   <dir>/000006.sst   <dir>/MANIFEST
enum class FileType { kWal, kTable, kManifest, kTemp, kUnknown };

std::string wal_file_name(const std::string& dir, uint64_t number);
std::string table_file_name(const std::string& dir, uint64_t number);
std::string manifest_file_name(const std::string& dir);

// 解析文件名（不含目录），数字类文件会把编号写进 *number
FileType parse_file_name(const std::string& name, uint64_t* number);
