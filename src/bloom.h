#pragma once

// Bloom filter：回答“这个 key 可能在表里吗？”
//   返回 false -> 一定不在，不用读磁盘
//   返回 true  -> 可能在（有约 1% 的误判），需要真的去查
//
// 存储格式：[位数组 ...][k: 1 字节]

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

// 稳定的 64 位哈希。不能用 std::hash：它的结果不保证跨编译器 / 跨版本一致，
// 而 filter 是写进文件的，今天写的明天必须还能读。
uint64_t bloom_hash(std::string_view key);

// 用一组 key 的哈希值构造 filter
std::string build_bloom_filter(const std::vector<uint64_t>& key_hashes, int bits_per_key);

bool bloom_may_contain(std::string_view filter, std::string_view key);
