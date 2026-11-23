#include "file_util.h"

#include <cerrno>
#include <cstdio>
#include <system_error>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

void throw_errno(const std::string& what) {
    throw std::system_error(errno, std::generic_category(), what);
}

void write_all(int fd, const char* buf, size_t len) {
    while (len > 0) {
        ssize_t n = ::write(fd, buf, len);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw_errno("write");
        }
        buf += n;
        len -= static_cast<size_t>(n);
    }
}

size_t read_full(int fd, char* buf, size_t len) {
    size_t got = 0;
    while (got < len) {
        ssize_t n = ::read(fd, buf + got, len - got);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw_errno("read");
        }
        if (n == 0) break;  // EOF
        got += static_cast<size_t>(n);
    }
    return got;
}

void pread_full(int fd, char* buf, size_t len, uint64_t offset) {
    size_t got = 0;
    while (got < len) {
        ssize_t n = ::pread(fd, buf + got, len - got, static_cast<off_t>(offset + got));
        if (n < 0) {
            if (errno == EINTR) continue;
            throw_errno("pread");
        }
        if (n == 0) throw std::runtime_error("pread: unexpected end of file (corrupted table?)");
        got += static_cast<size_t>(n);
    }
}

void sync_fd(int fd) {
#if defined(__APPLE__) && defined(KV_FULLFSYNC)
    if (::fcntl(fd, F_FULLFSYNC) < 0) throw_errno("fcntl(F_FULLFSYNC)");
#else
    if (::fsync(fd) < 0) throw_errno("fsync");
#endif
}

void sync_dir(const std::string& dir) {
    int fd = ::open(dir.c_str(), O_RDONLY);
    if (fd < 0) throw_errno("open dir " + dir);
    try {
        sync_fd(fd);
    } catch (...) {
        ::close(fd);
        throw;
    }
    ::close(fd);
}

uint64_t file_size(int fd) {
    struct stat st;
    if (::fstat(fd, &st) < 0) throw_errno("fstat");
    return static_cast<uint64_t>(st.st_size);
}

std::string read_file(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) throw_errno("open " + path);
    std::string data;
    try {
        data.resize(file_size(fd));
        data.resize(read_full(fd, data.data(), data.size()));
    } catch (...) {
        ::close(fd);
        throw;
    }
    ::close(fd);
    return data;
}

void write_file_atomic(const std::string& dir, const std::string& path, const std::string& data) {
    std::string tmp = path + ".tmp";
    int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) throw_errno("open " + tmp);
    try {
        write_all(fd, data.data(), data.size());
        sync_fd(fd);
    } catch (...) {
        ::close(fd);
        throw;
    }
    ::close(fd);
    if (::rename(tmp.c_str(), path.c_str()) < 0) throw_errno("rename " + tmp);
    sync_dir(dir);
}

namespace {
std::string numbered_name(const std::string& dir, uint64_t number, const char* suffix) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "/%06llu.%s", static_cast<unsigned long long>(number), suffix);
    return dir + buf;
}
}  // namespace

std::string wal_file_name(const std::string& dir, uint64_t number) {
    return numbered_name(dir, number, "wal");
}
std::string table_file_name(const std::string& dir, uint64_t number) {
    return numbered_name(dir, number, "sst");
}
std::string manifest_file_name(const std::string& dir) { return dir + "/MANIFEST"; }

FileType parse_file_name(const std::string& name, uint64_t* number) {
    if (name == "MANIFEST") return FileType::kManifest;
    if (name.size() > 4 && name.compare(name.size() - 4, 4, ".tmp") == 0) return FileType::kTemp;

    size_t dot = name.find('.');
    if (dot == std::string::npos || dot == 0) return FileType::kUnknown;
    uint64_t n = 0;
    for (size_t i = 0; i < dot; ++i) {
        if (name[i] < '0' || name[i] > '9') return FileType::kUnknown;
        n = n * 10 + static_cast<uint64_t>(name[i] - '0');
    }
    std::string suffix = name.substr(dot + 1);
    *number = n;
    if (suffix == "wal") return FileType::kWal;
    if (suffix == "sst") return FileType::kTable;
    return FileType::kUnknown;
}
