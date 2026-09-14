#pragma once
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

// close() wakes producers and consumers. Consumers can drain accepted work.
template <class T> class BoundedQueue {
    std::queue<T> items_;
    std::mutex mutex_;
    std::condition_variable ready_, space_;
    const std::size_t capacity_;
    bool closed_ = false;
public:
    explicit BoundedQueue(std::size_t capacity) : capacity_(capacity) {
        if (!capacity) throw std::invalid_argument("queue capacity must be positive");
    }
    bool try_push(T item) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (closed_ || items_.size() == capacity_) return false;
        items_.push(std::move(item));
        ready_.notify_one();
        return true;
    }
    bool push(T item) {
        std::unique_lock<std::mutex> lock(mutex_);
        space_.wait(lock, [this] { return closed_ || items_.size() < capacity_; });
        if (closed_) return false;
        items_.push(std::move(item));
        ready_.notify_one();
        return true;
    }
    std::optional<T> pop() {
        std::unique_lock<std::mutex> lock(mutex_);
        ready_.wait(lock, [this] { return closed_ || !items_.empty(); });
        if (items_.empty()) return std::nullopt;
        T item = std::move(items_.front());
        items_.pop();
        space_.notify_one();
        return item;
    }
    void close() {
        std::lock_guard<std::mutex> lock(mutex_);
        closed_ = true;
        ready_.notify_all();
        space_.notify_all();
    }
};

inline bool valid_nickname(const std::string& name) {
    if (name.empty() || name.size() > 16) return false;
    for (unsigned char c : name)
        if (!(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') &&
            !(c >= '0' && c <= '9') && c != '_' && c != '-') return false;
    return true;
}

enum class Operation { nick, message, users, quit, error };
struct Command { Operation op; std::string value; };
inline Command parse_command(const std::string& line) {
    if (line == "USERS") return {Operation::users, {}};
    if (line == "QUIT") return {Operation::quit, {}};
    if (line == "NICK" || line.compare(0, 5, "NICK ") == 0) {
        auto name = line.size() > 5 ? line.substr(5) : "";
        return valid_nickname(name) ? Command{Operation::nick, name}
                                    : Command{Operation::error, "ERR nickname"};
    }
    if (line == "MSG" || line.compare(0, 4, "MSG ") == 0) {
        auto value = line.size() > 4 ? line.substr(4) : "";
        if (value.empty() || value.size() > 1024) return {Operation::error, "ERR message"};
        return {Operation::message, value};
    }
    return {Operation::error, "ERR command"};
}

// Snapshots own their sessions; no registry lock survives into socket I/O.
// Identity-based removal cannot remove a newer owner of a reused nickname.
template <class T> class Room {
    std::map<std::string, std::shared_ptr<T>> members_;
    mutable std::mutex mutex_;
public:
    bool join(const std::string& name, std::shared_ptr<T> session) {
        std::lock_guard<std::mutex> lock(mutex_);
        return members_.emplace(name, std::move(session)).second;
    }
    std::optional<std::string> leave(const std::shared_ptr<T>& session) {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto it = members_.begin(); it != members_.end(); ++it) {
            if (it->second == session) {
                auto name = it->first;
                members_.erase(it);
                return name;
            }
        }
        return std::nullopt;
    }
    std::vector<std::pair<std::string, std::shared_ptr<T>>> snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return {members_.begin(), members_.end()};
    }
};
