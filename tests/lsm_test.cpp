// LSM 的自动化测试。每个测试用一个临时目录，跑完删掉。
// 用 CHECK 而不是 assert：assert 在 Release（定义了 NDEBUG）下会被整个删掉。

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "bloom.h"
#include "kv_store.h"
#include "sstable.h"

namespace fs = std::filesystem;

#define CHECK(cond)                                                                       \
    do {                                                                                  \
        if (!(cond)) {                                                                    \
            std::fprintf(stderr, "CHECK failed: %s  (%s:%d)\n", #cond, __FILE__, __LINE__); \
            std::abort();                                                                 \
        }                                                                                 \
    } while (0)

namespace {

std::string temp_dir(const std::string& name) {
    fs::path p = fs::temp_directory_path() / ("kvstore_test_" + name);
    fs::remove_all(p);
    return p.string();
}

// 非常小的尺寸，让少量数据就触发 flush 和多层 compaction
Options tiny_options() {
    Options o;
    o.memtable_size = 16 << 10;
    o.block_size = 512;
    o.target_file_size = 16 << 10;
    o.l1_max_bytes = 64 << 10;
    o.level_size_multiplier = 4;
    o.l0_compaction_trigger = 4;
    o.l0_stop_writes_trigger = 8;
    return o;
}

std::string random_value(std::mt19937& rng) {
    std::uniform_int_distribution<int> len(0, 100), byte(0, 255);
    std::string v(static_cast<size_t>(len(rng)), '\0');
    for (char& c : v) c = static_cast<char>(byte(rng));  // 任意字节，包括 \0
    return v;
}

std::string key_of(int i) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "k%08d", i);
    return buf;
}

void verify_all(KVStore& store, const std::map<std::string, std::string>& ref) {
    for (const auto& [k, v] : ref) {
        auto got = store.get(k);
        CHECK(got.has_value());
        CHECK(*got == v);
    }
}

// ---------------------------------------------------------------

void test_bloom() {
    std::vector<uint64_t> hashes;
    for (int i = 0; i < 10000; ++i) hashes.push_back(bloom_hash("in_" + std::to_string(i)));
    std::string filter = build_bloom_filter(hashes, 10);

    for (int i = 0; i < 10000; ++i) CHECK(bloom_may_contain(filter, "in_" + std::to_string(i)));  // 不能漏

    int false_pos = 0;
    for (int i = 0; i < 10000; ++i)
        if (bloom_may_contain(filter, "out_" + std::to_string(i))) ++false_pos;
    double fpr = false_pos / 10000.0;
    std::cout << "  bloom: 10 bits/key, false positive rate = " << fpr * 100 << "%\n";
    CHECK(fpr < 0.02);
}

void test_sstable() {
    std::string dir = temp_dir("sstable");
    fs::create_directories(dir);
    std::string path = dir + "/t.sst";

    Options o;
    o.block_size = 256;
    std::map<std::string, std::string> ref;
    for (int i = 0; i < 3000; i += 2) ref[key_of(i)] = "v" + std::to_string(i);
    ref[std::string("\0bin\0", 5)] = std::string("\0\1\2", 3);  // 二进制 key 和 value

    {
        SSTableBuilder b(path, o);
        for (const auto& [k, v] : ref) b.add(k, v);
        TableInfo info = b.finish();
        CHECK(info.num_entries == ref.size());
        CHECK(info.smallest == ref.begin()->first);
        CHECK(info.largest == ref.rbegin()->first);
    }

    auto t = SSTable::open(path);
    for (const auto& [k, v] : ref) {
        CHECK(t->may_contain(k));
        auto got = t->get(k);
        CHECK(got && *got == v);
    }
    for (int i = 1; i < 3000; i += 2) CHECK(!t->get(key_of(i)));  // 夹在中间的不存在的 key
    CHECK(!t->get("zzzz"));                                       // 比所有 key 都大

    size_t n = 0;
    auto expect = ref.begin();
    for (SSTable::Iterator it(t); it.valid(); it.next(), ++expect, ++n) {
        CHECK(it.key() == expect->first);
        CHECK(it.value() == expect->second);
    }
    CHECK(n == ref.size());
    fs::remove_all(dir);
}

// 随机覆盖写，和 std::map 对照；中途和重启后都校验
void test_random_against_map() {
    std::string dir = temp_dir("random");
    std::mt19937 rng(123);
    std::uniform_int_distribution<int> pick(0, 4999);
    std::map<std::string, std::string> ref;

    {
        KVStore store(dir, tiny_options());
        for (int op = 1; op <= 40000; ++op) {
            std::string k = key_of(pick(rng));
            std::string v = random_value(rng);
            store.put(k, v);
            ref[k] = v;
            if (op % 10000 == 0) verify_all(store, ref);  // 后台还在 flush / compaction 的时候也要对
        }
        store.wait_for_background_idle();
        verify_all(store, ref);
        for (int i = 5000; i < 6000; ++i) CHECK(!store.get(key_of(i)));

        auto s = store.stats();
        std::cout << "  random: flushes=" << s.flushes << " compactions=" << s.compactions
                  << " trivial_moves=" << s.trivial_moves << " stalls=" << s.write_stalls << "\n"
                  << "  levels: " << store.level_summary() << "\n";
        CHECK(s.flushes > 0);
        CHECK(s.compactions > 0);
    }

    // 重启：数据来自 SSTable + 最后一个 WAL
    {
        KVStore store(dir, tiny_options());
        verify_all(store, ref);
        std::cout << "  reopened: levels: " << store.level_summary() << "\n";
    }
    fs::remove_all(dir);
}

// 多个写线程 + 读线程同时跑：读线程只读“已经确认写入”的 key，必须能读到
void test_concurrent() {
    std::string dir = temp_dir("concurrent");
    const int kWriters = 8, kPerWriter = 3000;
    {
        KVStore store(dir, tiny_options());
        std::vector<std::atomic<int>> progress(kWriters);
        for (auto& p : progress) p = 0;
        std::atomic<bool> done{false};

        std::vector<std::thread> threads;
        for (int t = 0; t < kWriters; ++t) {
            threads.emplace_back([&, t] {
                for (int i = 0; i < kPerWriter; ++i) {
                    store.put(key_of(t * kPerWriter + i), "v" + std::to_string(i));
                    progress[t].store(i + 1);
                }
            });
        }
        for (int r = 0; r < 2; ++r) {
            threads.emplace_back([&, r] {
                std::mt19937 rng(r);
                while (!done.load()) {
                    int t = static_cast<int>(rng() % kWriters);
                    int n = progress[t].load();
                    if (n == 0) continue;
                    int i = static_cast<int>(rng() % n);
                    auto got = store.get(key_of(t * kPerWriter + i));
                    CHECK(got && *got == "v" + std::to_string(i));
                }
            });
        }
        for (int t = 0; t < kWriters; ++t) threads[t].join();
        done = true;
        for (size_t t = kWriters; t < threads.size(); ++t) threads[t].join();

        store.wait_for_background_idle();
        for (int t = 0; t < kWriters; ++t)
            for (int i = 0; i < kPerWriter; ++i) CHECK(*store.get(key_of(t * kPerWriter + i)) == "v" + std::to_string(i));
        std::cout << "  concurrent: levels: " << store.level_summary() << "\n";
    }
    fs::remove_all(dir);
}

// 后台正忙的时候直接关掉，再打开，数据不能丢
void test_close_while_busy() {
    std::string dir = temp_dir("close_busy");
    std::map<std::string, std::string> ref;
    std::mt19937 rng(7);
    for (int round = 0; round < 3; ++round) {
        KVStore store(dir, tiny_options());
        verify_all(store, ref);
        for (int i = 0; i < 5000; ++i) {
            std::string k = key_of(static_cast<int>(rng() % 3000));
            std::string v = random_value(rng);
            store.put(k, v);
            ref[k] = v;
        }
        // 不等后台，直接析构
    }
    KVStore store(dir, tiny_options());
    verify_all(store, ref);
    fs::remove_all(dir);
}

}  // namespace

int main() {
    std::cout << "test_bloom\n";
    test_bloom();
    std::cout << "test_sstable\n";
    test_sstable();
    std::cout << "test_random_against_map\n";
    test_random_against_map();
    std::cout << "test_concurrent\n";
    test_concurrent();
    std::cout << "test_close_while_busy\n";
    test_close_while_busy();
    std::cout << "ALL TESTS PASSED\n";
    return 0;
}
