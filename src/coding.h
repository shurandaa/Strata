#pragma once

// 字节编码工具。所有整数一律按小端序写入，不依赖机器的字节序，
// 这样文件拿到别的机器上也能读。

#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>

inline void put_fixed32(std::string& dst, uint32_t v) {
    char buf[4];
    for (int i = 0; i < 4; ++i) buf[i] = static_cast<char>((v >> (8 * i)) & 0xff);
    dst.append(buf, 4);
}

inline void put_fixed64(std::string& dst, uint64_t v) {
    char buf[8];
    for (int i = 0; i < 8; ++i) buf[i] = static_cast<char>((v >> (8 * i)) & 0xff);
    dst.append(buf, 8);
}

inline uint32_t decode_fixed32(const char* p) {
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i) v |= static_cast<uint32_t>(static_cast<uint8_t>(p[i])) << (8 * i);
    return v;
}

inline uint64_t decode_fixed64(const char* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(static_cast<uint8_t>(p[i])) << (8 * i);
    return v;
}

// 一条 kv 记录：[klen 4B][vlen 4B][key][value]。WAL 和 SSTable 数据块共用这个格式。
inline void put_entry(std::string& dst, std::string_view key, std::string_view value) {
    if (key.size() > std::numeric_limits<uint32_t>::max() ||
        value.size() > std::numeric_limits<uint32_t>::max()) {
        throw std::length_error("key or value too large for 4-byte length");
    }
    put_fixed32(dst, static_cast<uint32_t>(key.size()));
    put_fixed32(dst, static_cast<uint32_t>(value.size()));
    dst.append(key.data(), key.size());
    dst.append(value.data(), value.size());
}

inline constexpr size_t entry_size(size_t klen, size_t vlen) { return 8 + klen + vlen; }

// 从 in 的开头解析一条记录，成功则把 in 往后推进。
// 返回 false 表示剩下的字节不够一条完整记录（截断）。
inline bool get_entry(std::string_view& in, std::string_view* key, std::string_view* value) {
    if (in.size() < 8) return false;
    uint32_t klen = decode_fixed32(in.data());
    uint32_t vlen = decode_fixed32(in.data() + 4);
    // 分开比较，避免 klen + vlen 在 32 位上溢出
    if (in.size() - 8 < klen || in.size() - 8 - klen < vlen) return false;
    *key = in.substr(8, klen);
    *value = in.substr(8 + klen, vlen);
    in.remove_prefix(8 + static_cast<size_t>(klen) + vlen);
    return true;
}

// 带长度前缀的字符串：[len 4B][bytes]。用于索引块和 MANIFEST。
inline void put_length_prefixed(std::string& dst, std::string_view s) {
    put_fixed32(dst, static_cast<uint32_t>(s.size()));
    dst.append(s.data(), s.size());
}

inline bool get_length_prefixed(std::string_view& in, std::string_view* out) {
    if (in.size() < 4) return false;
    uint32_t len = decode_fixed32(in.data());
    if (in.size() - 4 < len) return false;
    *out = in.substr(4, len);
    in.remove_prefix(4 + static_cast<size_t>(len));
    return true;
}

inline bool get_fixed64(std::string_view& in, uint64_t* out) {
    if (in.size() < 8) return false;
    *out = decode_fixed64(in.data());
    in.remove_prefix(8);
    return true;
}

inline bool get_fixed32(std::string_view& in, uint32_t* out) {
    if (in.size() < 4) return false;
    *out = decode_fixed32(in.data());
    in.remove_prefix(4);
    return true;
}
