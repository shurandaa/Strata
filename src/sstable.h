#pragma once

// SSTable：一个不可变的、按 key 排好序的文件。
//
// 文件布局：
//   [数据块 0][数据块 1]...[数据块 N-1][filter][索引][footer 40B]
//
//   数据块：若干条 [klen 4B][vlen 4B][key][value]，按 key 升序，每块约 block_size
//   filter：整个表所有 key 的 bloom filter
//   索引  ：每个数据块一条 [len 4B][该块最后一个 key][offset 8B][size 8B]
//   footer：[index_off 8B][index_size 8B][filter_off 8B][filter_size 8B][magic 8B]
//
// 读一个 key：bloom filter 说不在 -> 直接返回；否则在索引里二分找到
// 唯一可能包含它的块，pread 读这一个块，在块里线性扫描。

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "options.h"

struct TableInfo {
    uint64_t file_size = 0;
    uint64_t num_entries = 0;
    std::string smallest;  // 表里最小的 key
    std::string largest;   // 表里最大的 key
};

class SSTableBuilder {
public:
    SSTableBuilder(const std::string& path, const Options& opts);
    ~SSTableBuilder();

    SSTableBuilder(const SSTableBuilder&) = delete;
    SSTableBuilder& operator=(const SSTableBuilder&) = delete;

    // key 必须严格递增
    void add(std::string_view key, std::string_view value);

    // 写 filter、索引、footer，fsync，关闭文件
    TableInfo finish();

    uint64_t estimated_size() const { return offset_ + block_.size(); }
    uint64_t num_entries() const { return num_entries_; }

private:
    void flush_block();

    int fd_ = -1;
    size_t block_size_;
    int bits_per_key_;

    std::string block_;     // 正在攒的数据块
    std::string index_;     // 编码好的索引
    std::vector<uint64_t> key_hashes_;  // 给 bloom filter 用；只存哈希，省内存
    std::string last_key_;
    std::string smallest_;
    uint64_t offset_ = 0;   // 已经写进文件的字节数
    uint64_t num_entries_ = 0;
    bool finished_ = false;
};

class SSTable {
public:
    static std::shared_ptr<SSTable> open(const std::string& path);
    ~SSTable();

    SSTable(const SSTable&) = delete;
    SSTable& operator=(const SSTable&) = delete;

    bool may_contain(std::string_view key) const;

    // 在数据块里查找（不看 bloom filter，调用方自己先问 may_contain）
    std::optional<std::string> get(std::string_view key) const;

    // 按 key 顺序遍历整张表，给 compaction 用
    class Iterator {
    public:
        explicit Iterator(std::shared_ptr<const SSTable> table);
        bool valid() const { return valid_; }
        const std::string& key() const { return key_; }
        const std::string& value() const { return value_; }
        void next();

    private:
        void advance();

        std::shared_ptr<const SSTable> table_;
        size_t block_idx_ = 0;
        std::string block_;
        // block_ 里下一条记录的偏移。不用 string_view 指向 block_：Iterator 被 move 时，
        // 短字符串（SSO）的内容会跟着搬家，view 就悬空了。
        size_t pos_ = 0;
        std::string key_, value_;
        bool valid_ = false;
    };

private:
    struct IndexEntry {
        std::string last_key;
        uint64_t offset;
        uint64_t size;
    };

    SSTable() = default;
    std::string read_block(const IndexEntry& e) const;

    int fd_ = -1;
    std::string path_;
    std::vector<IndexEntry> index_;
    std::string filter_;
};
