#pragma once

#include <cstddef>
#include <cstdint>

constexpr int kNumLevels = 7;  // L0 ~ L6

struct Options {
    // memtable 写到这么大（按编码后的字节数估算）就切换成 immutable，交给后台刷盘。
    // 越大：写放大越小、L0 文件越少；代价是内存占用和崩溃后回放 WAL 的时间。
    size_t memtable_size = 4 << 20;  // 4 MB

    // SSTable 数据块大小。查一个 key 最多读一个块，所以它决定了点查的 I/O 粒度。
    size_t block_size = 4 << 10;  // 4 KB

    // bloom filter 每个 key 用多少 bit。10 bit -> 误判率约 1%。
    int bloom_bits_per_key = 10;

    // compaction 输出文件的目标大小
    size_t target_file_size = 2 << 20;  // 2 MB

    // L1 的容量上限，往下每层乘以 level_size_multiplier
    uint64_t l1_max_bytes = 10 << 20;  // 10 MB
    int level_size_multiplier = 10;

    // L0 文件数到这个值就触发 L0 -> L1 compaction
    int l0_compaction_trigger = 4;
    // L0 文件数到这个值就暂停写入，等 compaction 追上来（背压）
    int l0_stop_writes_trigger = 12;
};
