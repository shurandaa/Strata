#include "sstable.h"

#include <algorithm>
#include <stdexcept>

#include <fcntl.h>
#include <unistd.h>

#include "bloom.h"
#include "coding.h"
#include "file_util.h"

namespace {
constexpr uint64_t kTableMagic = 0x4b5653544142ull;  // "KVSTAB"
constexpr size_t kFooterSize = 40;
}  // namespace

// ======================= Builder =======================

SSTableBuilder::SSTableBuilder(const std::string& path, const Options& opts)
    : block_size_(opts.block_size), bits_per_key_(opts.bloom_bits_per_key) {
    fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd_ < 0) throw_errno("open " + path);
}

SSTableBuilder::~SSTableBuilder() {
    if (fd_ >= 0) ::close(fd_);
}

void SSTableBuilder::add(std::string_view key, std::string_view value) {
    if (num_entries_ > 0 && key <= last_key_) {
        throw std::logic_error("SSTableBuilder::add: keys must be strictly increasing");
    }
    if (num_entries_ == 0) smallest_.assign(key.data(), key.size());

    put_entry(block_, key, value);
    key_hashes_.push_back(bloom_hash(key));
    last_key_.assign(key.data(), key.size());
    ++num_entries_;

    if (block_.size() >= block_size_) flush_block();
}

void SSTableBuilder::flush_block() {
    if (block_.empty()) return;
    write_all(fd_, block_.data(), block_.size());
    // 索引里记“这个块的最后一个 key”：查 k 时找第一个 last_key >= k 的块就行
    put_length_prefixed(index_, last_key_);
    put_fixed64(index_, offset_);
    put_fixed64(index_, block_.size());
    offset_ += block_.size();
    block_.clear();
}

TableInfo SSTableBuilder::finish() {
    if (finished_) throw std::logic_error("SSTableBuilder::finish called twice");
    finished_ = true;
    flush_block();

    std::string filter = build_bloom_filter(key_hashes_, bits_per_key_);
    uint64_t filter_off = offset_;
    write_all(fd_, filter.data(), filter.size());
    offset_ += filter.size();

    uint64_t index_off = offset_;
    write_all(fd_, index_.data(), index_.size());
    offset_ += index_.size();

    std::string footer;
    put_fixed64(footer, index_off);
    put_fixed64(footer, index_.size());
    put_fixed64(footer, filter_off);
    put_fixed64(footer, filter.size());
    put_fixed64(footer, kTableMagic);
    write_all(fd_, footer.data(), footer.size());
    offset_ += footer.size();

    sync_fd(fd_);
    ::close(fd_);
    fd_ = -1;

    TableInfo info;
    info.file_size = offset_;
    info.num_entries = num_entries_;
    info.smallest = std::move(smallest_);
    info.largest = last_key_;
    return info;
}

// ======================= Reader =======================

std::shared_ptr<SSTable> SSTable::open(const std::string& path) {
    // 构造函数是 private 的，所以不能用 make_shared
    std::shared_ptr<SSTable> t(new SSTable());
    t->path_ = path;
    t->fd_ = ::open(path.c_str(), O_RDONLY);
    if (t->fd_ < 0) throw_errno("open " + path);

    uint64_t size = file_size(t->fd_);
    if (size < kFooterSize) throw std::runtime_error("table too small: " + path);

    char footer[kFooterSize];
    pread_full(t->fd_, footer, kFooterSize, size - kFooterSize);
    uint64_t index_off = decode_fixed64(footer);
    uint64_t index_size = decode_fixed64(footer + 8);
    uint64_t filter_off = decode_fixed64(footer + 16);
    uint64_t filter_size = decode_fixed64(footer + 24);
    if (decode_fixed64(footer + 32) != kTableMagic) throw std::runtime_error("bad table magic: " + path);
    if (index_off + index_size > size - kFooterSize || filter_off + filter_size > index_off) {
        throw std::runtime_error("bad table footer: " + path);
    }

    t->filter_.resize(filter_size);
    pread_full(t->fd_, t->filter_.data(), filter_size, filter_off);

    std::string index(index_size, '\0');
    pread_full(t->fd_, index.data(), index_size, index_off);
    std::string_view in = index;
    while (!in.empty()) {
        IndexEntry e;
        std::string_view last_key;
        if (!get_length_prefixed(in, &last_key) || !get_fixed64(in, &e.offset) ||
            !get_fixed64(in, &e.size)) {
            throw std::runtime_error("corrupted table index: " + path);
        }
        e.last_key.assign(last_key.data(), last_key.size());
        t->index_.push_back(std::move(e));
    }
    return t;
}

SSTable::~SSTable() {
    if (fd_ >= 0) ::close(fd_);
}

bool SSTable::may_contain(std::string_view key) const { return bloom_may_contain(filter_, key); }

std::string SSTable::read_block(const IndexEntry& e) const {
    std::string block(e.size, '\0');
    pread_full(fd_, block.data(), e.size, e.offset);
    return block;
}

std::optional<std::string> SSTable::get(std::string_view key) const {
    // 第一个 last_key >= key 的块：key 如果存在，只可能在这个块里
    auto it = std::lower_bound(index_.begin(), index_.end(), key,
                               [](const IndexEntry& e, std::string_view k) { return e.last_key < k; });
    if (it == index_.end()) return std::nullopt;

    std::string block = read_block(*it);
    std::string_view in = block;
    std::string_view k, v;
    while (get_entry(in, &k, &v)) {
        if (k == key) return std::string(v);
        if (k > key) break;  // 块内有序，已经越过去了
    }
    return std::nullopt;
}

// ======================= Iterator =======================

SSTable::Iterator::Iterator(std::shared_ptr<const SSTable> table) : table_(std::move(table)) {
    advance();
}

void SSTable::Iterator::next() { advance(); }

void SSTable::Iterator::advance() {
    while (true) {
        std::string_view rest = std::string_view(block_).substr(pos_);
        std::string_view k, v;
        if (get_entry(rest, &k, &v)) {
            key_.assign(k.data(), k.size());
            value_.assign(v.data(), v.size());
            pos_ = block_.size() - rest.size();
            valid_ = true;
            return;
        }
        if (!rest.empty()) throw std::runtime_error("corrupted data block: " + table_->path_);
        if (block_idx_ >= table_->index_.size()) {
            valid_ = false;
            return;
        }
        block_ = table_->read_block(table_->index_[block_idx_++]);
        pos_ = 0;
    }
}
