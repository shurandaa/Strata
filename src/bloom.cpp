#include "bloom.h"

#include <algorithm>

uint64_t bloom_hash(std::string_view key) {
    // FNV-1a：简单、稳定
    uint64_t h = 14695981039346656037ull;
    for (char c : key) {
        h ^= static_cast<uint8_t>(c);
        h *= 1099511628211ull;
    }
    // FNV 的低位混合得不够好，再过一遍 splitmix64 的 finalizer 把 bit 打散
    h ^= h >> 30;
    h *= 0xbf58476d1ce4e5b9ull;
    h ^= h >> 27;
    h *= 0x94d049bb133111ebull;
    h ^= h >> 31;
    return h;
}

namespace {

// 最优的哈希函数个数 k = bits_per_key * ln2
int num_probes(int bits_per_key) {
    int k = static_cast<int>(bits_per_key * 0.69);
    return std::clamp(k, 1, 30);
}

// double hashing（Kirsch & Mitzenmacher）：只算一次哈希 h，
// 第 i 个探测位置 = h + i * delta。效果和 k 个独立哈希几乎一样，但快得多。
uint64_t rotate_delta(uint64_t h) { return (h >> 33) | (h << 31); }

}  // namespace

std::string build_bloom_filter(const std::vector<uint64_t>& key_hashes, int bits_per_key) {
    int k = num_probes(bits_per_key);

    // key 很少时位数组太小会导致误判率很高，至少给 64 bit
    uint64_t bits = std::max<uint64_t>(64, key_hashes.size() * static_cast<uint64_t>(bits_per_key));
    uint64_t bytes = (bits + 7) / 8;
    bits = bytes * 8;

    std::string filter(bytes, '\0');
    for (uint64_t h : key_hashes) {
        uint64_t delta = rotate_delta(h);
        for (int i = 0; i < k; ++i) {
            uint64_t pos = h % bits;
            filter[pos / 8] = static_cast<char>(filter[pos / 8] | (1 << (pos % 8)));
            h += delta;
        }
    }
    filter.push_back(static_cast<char>(k));  // 把 k 存进去，读的时候不依赖当前配置
    return filter;
}

bool bloom_may_contain(std::string_view filter, std::string_view key) {
    if (filter.size() < 2) return true;  // 没有 filter：不能排除，只能说“可能在”
    int k = static_cast<uint8_t>(filter.back());
    uint64_t bits = (filter.size() - 1) * 8;

    uint64_t h = bloom_hash(key);
    uint64_t delta = rotate_delta(h);
    for (int i = 0; i < k; ++i) {
        uint64_t pos = h % bits;
        if ((static_cast<uint8_t>(filter[pos / 8]) & (1 << (pos % 8))) == 0) return false;
        h += delta;
    }
    return true;
}
