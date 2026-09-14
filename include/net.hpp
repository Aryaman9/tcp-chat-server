#pragma once
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <poll.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

class Socket {
    const int fd_;
public:
    explicit Socket(int fd) : fd_(fd) {}
    ~Socket() { if (fd_ >= 0) ::close(fd_); }
    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;
    int get() const { return fd_; }
};

// Nonblocking writes share one total deadline, including partial writes.
inline bool send_all(int fd, const std::string& bytes, const std::atomic<bool>& stop,
                     std::chrono::milliseconds timeout = std::chrono::seconds(1)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    std::size_t offset = 0;
    while (offset < bytes.size() && !stop) {
        if (std::chrono::steady_clock::now() >= deadline) return false;
        pollfd p{fd, POLLOUT, 0};
        int ready = ::poll(&p, 1, 50);
        if (ready < 0) { if (errno == EINTR) continue; return false; }
        if (!ready) continue;
        auto count = ::send(fd, bytes.data() + offset, bytes.size() - offset,
                            MSG_NOSIGNAL | MSG_DONTWAIT);
        if (count < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) continue;
        if (count <= 0) return false;
        offset += static_cast<std::size_t>(count);
    }
    return offset == bytes.size();
}

inline int number(const char* text, int min, int max) {
    std::string raw(text);
    int value = 0;
    auto result = std::from_chars(raw.data(), raw.data() + raw.size(), value);
    if (result.ec != std::errc{} || result.ptr != raw.data() + raw.size() || value < min || value > max)
        throw std::invalid_argument("invalid numeric argument");
    return value;
}
