#pragma once

// LSM-tree KV store。
//
// 写路径： put -> group commit 写 WAL + fsync -> 进 memtable
//          memtable 满了 -> 变成 immutable，换新 WAL -> 后台线程刷成 L0 SSTable
//          L0 文件多了 / 某层太大 -> 后台 compaction 合并到下一层
// 读路径： memtable -> immutable memtable -> L0（新到旧，可能重叠）-> L1..L6（每层不重叠，二分）
//
// 线程：
//   调用方线程：put / get
//   wal_writer_：group commit，写 WAL + fsync，放进 memtable，满了就切换 memtable
//               （注意：它不做 flush。“flush” 专指把 imm_ 写成 SSTable，那是 bg_ 的事）
//   bg_       ：把 immutable memtable 刷成 SSTable；做 compaction

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "options.h"
#include "sstable.h"

class KVStore {
public:
    // dir 不存在会创建。打开时读 MANIFEST、回放 WAL。
    explicit KVStore(const std::string& dir, Options opts = Options());
    ~KVStore();

    KVStore(const KVStore&) = delete;
    KVStore& operator=(const KVStore&) = delete;

    // 返回时数据已 fsync 到 WAL
    void put(const std::string& key, const std::string& value);
    std::optional<std::string> get(const std::string& key);

    struct Stats {
        uint64_t wal_syncs = 0;      // WAL fsync 次数（group commit 批次数）
        uint64_t flushes = 0;        // memtable -> L0 次数
        uint64_t compactions = 0;    // 真正合并的 compaction 次数
        uint64_t trivial_moves = 0;  // 没有重叠、直接挪到下一层的次数
        uint64_t flush_bytes_written = 0;
        uint64_t compaction_bytes_read = 0;
        uint64_t compaction_bytes_written = 0;
        uint64_t write_stalls = 0;   // 因背压暂停写入的次数
        uint64_t stall_micros_imm = 0;  // 停顿总时长：等 imm_ 槽空出来
        uint64_t stall_micros_l0 = 0;   // 停顿总时长：等 L0 文件数降下来
        uint64_t bloom_skips = 0;    // bloom filter 帮忙省掉的读表次数
        uint64_t table_reads = 0;    // 真正去读数据块的次数
    };
    Stats stats();
    uint64_t sync_count() { return stats().wal_syncs; }

    // 例如 "L0: 2 files 7.9KB | L1: 5 files 300KB"
    std::string level_summary();

    // 等后台把 immutable 刷完、该做的 compaction 做完（测试和压测用）
    void wait_for_background_idle();

private:
    using MemTable = std::map<std::string, std::string>;

    struct FileMeta {
        uint64_t number;
        uint64_t file_size;
        std::string smallest, largest;
        std::shared_ptr<SSTable> table;
    };
    using FilePtr = std::shared_ptr<const FileMeta>;

    // 某一时刻“每层有哪些文件”。不可变：每次变化都创建新的 Version 再替换，
    // 读线程拿着旧 Version 的 shared_ptr 继续读也是安全的。
    struct Version {
        // L0：新的在前（可能互相重叠）；L1+：按 smallest 排序（互不重叠）
        std::array<std::vector<FilePtr>, kNumLevels> levels;
    };

    struct Compaction {
        int level = -1;              // 从 level 合并到 level + 1
        std::vector<FilePtr> inputs[2];  // [0] 来自 level，[1] 来自 level + 1
    };

    // ---- 打开 / 恢复 ----
    void recover();
    void load_manifest(Version& v);
    void replay_wal(const std::string& path);
    void delete_obsolete_files(const Version& v);

    // ---- 写路径 ----
    void wal_loop();
    void apply_to_mem(std::string&& key, std::string&& value);
    void maybe_switch_memtable(std::unique_lock<std::mutex>& lock);
    int open_wal(uint64_t number);

    // ---- 后台 ----
    void bg_loop();
    void flush_imm(std::unique_lock<std::mutex>& lock);
    void run_compaction(std::unique_lock<std::mutex>& lock);
    bool pick_compaction(Compaction* c);  // 需持有 mu_
    std::vector<FilePtr> merge_tables(const Compaction& c);
    FilePtr build_table_from_mem(const MemTable& mem, uint64_t number);
    FilePtr finish_table(uint64_t number, SSTableBuilder& builder);
    void install_version(std::shared_ptr<Version> v);  // 写 MANIFEST，需持有 mu_
    uint64_t new_file_number();                         // 内部加锁

    // ---- 工具 ----
    uint64_t max_bytes_for_level(int level) const;
    static uint64_t level_bytes(const std::vector<FilePtr>& files);
    static std::vector<FilePtr> overlapping(const std::vector<FilePtr>& files, const std::string& lo,
                                            const std::string& hi);
    void set_failed(const std::string& msg);  // 需持有 mu_

    const std::string dir_;
    const Options opts_;

    int wal_fd_ = -1;          // 只有 wal_writer_ 线程（和构造/析构）使用
    uint64_t wal_number_ = 0;  // 当前 WAL 的编号

    std::mutex mu_;                     // 保护下面所有成员
    std::condition_variable work_cv_;   // put -> WAL 线程：有新的待写记录
    std::condition_variable done_cv_;   // WAL 线程 -> put：某批已落盘
    std::condition_variable bg_cv_;     // -> bg：有 immutable 要刷 / 可能要 compaction
    std::condition_variable stall_cv_;  // bg -> WAL 线程：背压解除
    std::condition_variable idle_cv_;   // bg -> wait_for_background_idle

    MemTable mem_;
    size_t mem_bytes_ = 0;
    std::shared_ptr<const MemTable> imm_;  // 正在等着刷盘的 memtable，可能为空
    uint64_t imm_wal_number_ = 0;          // imm_ 对应的 WAL，刷完就可以删了

    std::shared_ptr<const Version> current_;
    uint64_t next_file_number_ = 1;
    uint64_t log_number_ = 0;  // 编号 < 它的 WAL 都已经刷进 SSTable，不需要回放
    std::array<std::string, kNumLevels> compact_pointer_;  // 每层下次从哪个 key 之后挑文件

    // group commit
    std::string pending_buf_;
    std::vector<std::pair<std::string, std::string>> pending_kv_;
    uint64_t next_seq_ = 0;
    uint64_t durable_seq_ = 0;

    Stats stats_;
    std::atomic<uint64_t> bloom_skips_{0};  // get 不持锁时更新，所以单独用 atomic
    std::atomic<uint64_t> table_reads_{0};

    bool failed_ = false;
    std::string error_;
    bool stop_ = false;     // 让 wal_writer_ 退出
    bool bg_stop_ = false;  // 让 bg_ 退出
    bool bg_busy_ = false;

    std::thread wal_writer_;
    std::thread bg_;
};
