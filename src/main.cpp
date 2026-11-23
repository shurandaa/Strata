// 用法：
//   kvstore write   <dir> [n=1000] [sleep_ms=5] [threads=1] [memtable_kb=4096]
//       多线程写 n 个 key，每个 put 返回后打印 key（= 已确认）
//   kvstore recover <dir> <acked.txt> [memtable_kb=4096]
//       只恢复，检查 acked.txt 里每个 key 都在、值也对
//   kvstore bench   <dir> [threads=8] [puts_per_thread=2000] [memtable_kb=4096]
//       写压测 + 读压测（存在的 key / 不存在的 key），打印 LSM 各层情况

#include "kv_store.h"

#include <chrono>
#include <fstream>
#include <iostream>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace {

std::string key_for(int i) { return "key_" + std::to_string(i); }
std::string value_for(int i) { return "value_" + std::to_string(i); }

// memtable 调小时，其它尺寸按比例缩小，这样少量数据也能触发多层 compaction
Options scaled_options(size_t memtable_kb) {
    Options o;
    o.memtable_size = memtable_kb << 10;
    o.target_file_size = o.memtable_size;
    o.l1_max_bytes = 4 * o.memtable_size;
    if (o.memtable_size < (64u << 10)) o.block_size = 1 << 10;
    return o;
}

size_t arg_or(int argc, char** argv, int idx, size_t def) {
    return argc > idx ? static_cast<size_t>(std::stoul(argv[idx])) : def;
}

double seconds_since(std::chrono::steady_clock::time_point t) {
    return std::chrono::duration<double>(std::chrono::steady_clock::now() - t).count();
}

int do_write(const std::string& dir, int n, int sleep_ms, int threads, size_t memtable_kb) {
    KVStore store(dir, scaled_options(memtable_kb));
    std::mutex print_mu;  // 多线程往 cout 打印，不加锁行会混在一起

    std::vector<std::thread> workers;
    for (int t = 0; t < threads; ++t) {
        workers.emplace_back([&, t] {
            for (int i = t; i < n; i += threads) {
                store.put(key_for(i), value_for(i));
                {
                    std::lock_guard<std::mutex> lock(print_mu);
                    std::cout << key_for(i) << std::endl;  // endl 立即 flush
                }
                if (sleep_ms > 0) std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
            }
        });
    }
    for (auto& w : workers) w.join();
    return 0;
}

int do_recover(const std::string& dir, const std::string& acked_path, size_t memtable_kb) {
    KVStore store(dir, scaled_options(memtable_kb));

    std::ifstream acked(acked_path);
    if (!acked) {
        std::cerr << "cannot open " << acked_path << "\n";
        return 2;
    }
    int checked = 0, missing = 0;
    std::string key;
    while (std::getline(acked, key)) {
        if (key.empty()) continue;
        ++checked;
        int i = std::stoi(key.substr(4));  // "key_123" -> 123
        auto v = store.get(key);
        if (!v || *v != value_for(i)) {
            ++missing;
            std::cerr << "MISSING or WRONG: " << key << "\n";
        }
    }
    std::cout << "checked " << checked << " acked keys, missing " << missing << "\n"
              << "levels: " << store.level_summary() << "\n";
    return missing == 0 ? 0 : 1;
}

int do_bench(const std::string& dir, int threads, int per_thread, size_t memtable_kb) {
    KVStore store(dir, scaled_options(memtable_kb));
    int total = threads * per_thread;

    // ---- 写 ----
    auto start = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
    for (int t = 0; t < threads; ++t) {
        workers.emplace_back([&, t] {
            for (int i = 0; i < per_thread; ++i) {
                int id = t * per_thread + i;
                store.put(key_for(id), value_for(id));
            }
        });
    }
    for (auto& w : workers) w.join();
    double wsecs = seconds_since(start);
    store.wait_for_background_idle();
    auto ws = store.stats();
    std::cout << "write: " << total << " puts in " << wsecs << "s, "
              << static_cast<uint64_t>(total / wsecs) << " ops/s, avg batch "
              << static_cast<double>(total) / ws.wal_syncs << "\n"
              << "  flushes=" << ws.flushes << " compactions=" << ws.compactions
              << " trivial_moves=" << ws.trivial_moves << " stalls=" << ws.write_stalls << "\n"
              // 写放大（不算 WAL）：SSTable 总共写了多少字节 / 刷盘的用户数据字节
              << "  write amplification "
              << (ws.flush_bytes_written > 0
                      ? static_cast<double>(ws.flush_bytes_written + ws.compaction_bytes_written) /
                            static_cast<double>(ws.flush_bytes_written)
                      : 0.0)
              << "\n"
              << "  levels: " << store.level_summary() << "\n";

    // ---- 读：存在的 key 和不存在的 key 各一半 ----
    std::mt19937 rng(42);
    std::uniform_int_distribution<int> dist(0, total - 1);
    int reads = 20000, found = 0;
    auto before = store.stats();
    start = std::chrono::steady_clock::now();
    for (int i = 0; i < reads; ++i) {
        int id = dist(rng);
        // 不存在的 key 要夹在已有 key 中间，否则靠每个文件的 key 范围就直接排除了，测不到 bloom
        std::string k = (i % 2 == 0) ? key_for(id) : key_for(id) + "_missing";
        if (store.get(k)) ++found;
    }
    double rsecs = seconds_since(start);
    auto after = store.stats();
    std::cout << "read: " << reads << " gets in " << rsecs << "s, "
              << static_cast<uint64_t>(reads / rsecs) << " ops/s, found " << found << "\n"
              << "  bloom skipped " << (after.bloom_skips - before.bloom_skips) << " table probes, "
              << "actually read " << (after.table_reads - before.table_reads) << " blocks\n";
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "usage: " << argv[0] << " write   <dir> [n] [sleep_ms] [threads] [memtable_kb]\n"
                  << "       " << argv[0] << " recover <dir> <acked.txt> [memtable_kb]\n"
                  << "       " << argv[0] << " bench   <dir> [threads] [puts_per_thread] [memtable_kb]\n";
        return 2;
    }
    std::string mode = argv[1];
    std::string dir = argv[2];
    try {
        if (mode == "write") {
            return do_write(dir, static_cast<int>(arg_or(argc, argv, 3, 1000)),
                            static_cast<int>(arg_or(argc, argv, 4, 5)),
                            static_cast<int>(arg_or(argc, argv, 5, 1)), arg_or(argc, argv, 6, 4096));
        }
        if (mode == "recover" && argc > 3) return do_recover(dir, argv[3], arg_or(argc, argv, 4, 4096));
        if (mode == "bench") {
            return do_bench(dir, static_cast<int>(arg_or(argc, argv, 3, 8)),
                            static_cast<int>(arg_or(argc, argv, 4, 2000)), arg_or(argc, argv, 5, 4096));
        }
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        return 1;
    }
    std::cerr << "bad arguments\n";
    return 2;
}
