#include "core.hpp"
#include "net.hpp"
#include <fcntl.h>
#include <future>
#include <iostream>
#include <thread>

void check(bool ok, const char* name) { if (!ok) throw std::runtime_error(name); }

void queue_checks() {
    bool rejected = false;
    try { BoundedQueue<int> invalid(0); } catch (const std::invalid_argument&) { rejected = true; }
    check(rejected, "zero capacity");
    BoundedQueue<int> queue(2);
    check(queue.try_push(1) && queue.try_push(2) && !queue.try_push(3), "bounded queue");
    queue.close();
    check(queue.pop() == 1 && queue.pop() == 2 && !queue.pop(), "drain closed queue");
    check(!queue.try_push(4) && !queue.push(4), "reject after close");

    BoundedQueue<int> blocked(1);
    blocked.push(10);
    std::promise<void> entered;
    auto producer = std::async(std::launch::async, [&] { entered.set_value(); return blocked.push(20); });
    entered.get_future().wait();
    check(producer.wait_for(std::chrono::milliseconds(30)) == std::future_status::timeout,
          "full queue applies backpressure");
    check(blocked.pop() == 10 && producer.get() && blocked.pop() == 20, "space wakes producer");

    blocked.push(1);
    auto closing_producer = std::async(std::launch::async, [&] { return blocked.push(2); });
    blocked.close();
    check(!closing_producer.get(), "close wakes blocked producer");
    BoundedQueue<int> empty(1);
    std::vector<std::future<std::optional<int>>> consumers;
    for (int i = 0; i < 4; ++i)
        consumers.push_back(std::async(std::launch::async, [&] { return empty.pop(); }));
    empty.close();
    for (auto& consumer : consumers) check(!consumer.get(), "close wakes all consumers");

    BoundedQueue<int> concurrent(3);
    auto reader = std::async(std::launch::async, [&] {
        std::vector<int> seen(400, 0);
        while (auto value = concurrent.pop()) ++seen.at(*value);
        return seen;
    });
    std::vector<std::thread> writers;
    for (int i = 0; i < 4; ++i) writers.emplace_back([&, i] {
        for (int j = 0; j < 100; ++j) concurrent.push(i * 100 + j);
    });
    for (auto& writer : writers) writer.join();
    concurrent.close();
    for (auto count : reader.get()) check(count == 1, "no accepted queue work lost/duplicated");
}

void room_checks() {
    Room<int> room;
    std::atomic<int> winners{0};
    std::vector<std::thread> racers;
    for (int i = 0; i < 16; ++i) racers.emplace_back([&, i] {
        if (room.join("same", std::make_shared<int>(i))) ++winners;
    });
    for (auto& racer : racers) racer.join();
    check(winners == 1 && room.snapshot().size() == 1, "unique concurrent nickname");
    auto snapshot = room.snapshot();
    auto old = snapshot.front().second;
    check(room.leave(old) == "same", "remove by identity");
    check(room.join("same", std::make_shared<int>(99)), "reuse nickname");
    check(!room.leave(old) && room.snapshot().size() == 1, "stale departure preserves new owner");
    check(snapshot.front().second == old, "snapshot retains session ownership");
    room.join("aaa", std::make_shared<int>(1));
    check(room.snapshot().front().first == "aaa", "sorted user listing");
}

void socket_checks() {
    int pair[2];
    check(::socketpair(AF_UNIX, SOCK_STREAM, 0, pair) == 0, "socketpair");
    Socket sender(pair[0]), receiver(pair[1]);
    int small = 1024;
    check(::setsockopt(sender.get(), SOL_SOCKET, SO_SNDBUF, &small, sizeof(small)) == 0, "small send buffer");
    std::atomic<bool> stop{false};
    std::string payload(256 * 1024, 'x');
    auto read = std::async(std::launch::async, [&] {
        std::string received;
        char buffer[333];
        while (received.size() < payload.size()) {
            pollfd p{receiver.get(), POLLIN, 0};
            if (::poll(&p, 1, 3000) <= 0) break;
            auto count = ::recv(receiver.get(), buffer, sizeof(buffer), 0);
            if (count <= 0) break;
            received.append(buffer, static_cast<std::size_t>(count));
        }
        return received;
    });
    check(send_all(sender.get(), payload, stop, std::chrono::seconds(3)), "partial write loop");
    check(read.get() == payload, "partial write bytes intact");
    const auto start = std::chrono::steady_clock::now();
    check(!send_all(sender.get(), payload, stop, std::chrono::milliseconds(100)), "slow receiver deadline");
    check(std::chrono::steady_clock::now() - start < std::chrono::seconds(1), "bounded send time");
    ::shutdown(receiver.get(), SHUT_RDWR);
    check(!send_all(sender.get(), "test", stop), "disconnected receiver without SIGPIPE");
    stop = true;
    check(!send_all(sender.get(), "test", stop), "send observes shutdown");
    int fd;
    {
        auto owned = std::make_shared<Socket>(::dup(receiver.get()));
        fd = owned->get();
        { auto retained = owned; owned.reset(); check(::fcntl(fd, F_GETFD) >= 0, "shared lifetime"); }
    }
    check(::fcntl(fd, F_GETFD) == -1 && errno == EBADF, "RAII closes descriptor");
}

int main() {
    check(valid_nickname("Alice_2-") && valid_nickname(std::string(16, 'a')), "valid nicknames");
    check(!valid_nickname("") && !valid_nickname("bad name") &&
          !valid_nickname(std::string(17, 'a')) && !valid_nickname("é"), "invalid nicknames");
    check(parse_command("NICK alice").op == Operation::nick, "register parser");
    check(parse_command("NICK a b").value == "ERR nickname", "nickname syntax");
    check(parse_command("MSG hello world").value == "hello world", "preserve message spaces");
    check(parse_command("MSG " + std::string(1024, 'x')).op == Operation::message, "message boundary");
    check(parse_command("MSG " + std::string(1025, 'x')).value == "ERR message", "oversized message");
    check(parse_command("MSG").value == "ERR message", "empty message");
    check(parse_command("").value == "ERR command", "empty command");
    check(parse_command("QUIT extra").op == Operation::error, "strict quit");
    check(parse_command("USERS").op == Operation::users && parse_command("QUIT").op == Operation::quit,
          "users and quit parser");
    queue_checks();
    room_checks();
    socket_checks();
    std::cout << "Core checks passed: parser, queue backpressure/wakeups, registry races, socket lifetime/deadlines.\n";
}
