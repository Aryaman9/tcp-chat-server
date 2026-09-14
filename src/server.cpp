#include "core.hpp"
#include "net.hpp"
#include <arpa/inet.h>
#include <csignal>
#include <cstring>
#include <deque>
#include <future>
#include <iostream>
#include <thread>

namespace {
static_assert(std::atomic<bool>::is_always_lock_free, "signal flag must be lock-free");
std::atomic<bool> stopping{false};
void stop_handler(int) { stopping.store(true); }

struct Session {
    Socket socket;
    std::atomic<bool> active{true}, registered{false};
    std::string nickname; // Dispatcher-owned; workers only read registered.
    const std::chrono::steady_clock::time_point admitted = std::chrono::steady_clock::now();
    explicit Session(int fd) : socket(fd) {}
    void disconnect() {
        if (active.exchange(false)) ::shutdown(socket.get(), SHUT_RDWR);
        // Do not close here: queued events may still own this exact descriptor.
    }
};
using Client = std::shared_ptr<Session>;
enum class EventKind { command, error, departure };
struct Event {
    Client client;
    EventKind kind;
    std::string line;
    std::promise<void> done;
};

class ChatServer {
    BoundedQueue<Client> connections_;
    BoundedQueue<std::shared_ptr<Event>> events_;
    Room<Session> room_;
    const std::size_t max_users_;
    std::atomic<std::size_t> admitted_{0};
    std::thread dispatcher_;
    std::vector<std::thread> workers_;

    // Only the dispatcher sends to admitted sockets. Rejections belong solely
    // to the accept loop, so a per-session send mutex is unnecessary.
    bool write(const Client& client, const std::string& line) {
        return client->active && send_all(client->socket.get(), line + "\n", stopping);
    }
    void broadcast(std::string line) {
        std::deque<std::string> pending{std::move(line)};
        while (!pending.empty() && !stopping) {
            auto next = std::move(pending.front());
            pending.pop_front();
            for (const auto& member : room_.snapshot()) {
                if (!write(member.second, next)) {
                    member.second->disconnect();
                    if (auto name = room_.leave(member.second)) pending.push_back("LEAVE " + *name);
                }
            }
        }
    }
    void depart(const Client& client) {
        client->disconnect();
        if (auto name = room_.leave(client)) broadcast("LEAVE " + *name);
    }
    void reply(const Client& client, const std::string& line) {
        if (!write(client, line)) depart(client);
    }
    void dispatch(const Event& event) {
        const auto& client = event.client;
        if (event.kind == EventKind::departure) { depart(client); return; }
        if (!client->active) return;
        if (event.kind == EventKind::error) {
            reply(client, event.line);
            depart(client);
            return;
        }
        const auto command = parse_command(event.line);
        if (command.op == Operation::error) { reply(client, command.value); return; }
        if (command.op == Operation::quit) {
            reply(client, "BYE");
            depart(client);
            return;
        }
        if (command.op == Operation::nick) {
            if (client->registered) { reply(client, "ERR already_registered"); return; }
            if (!room_.join(command.value, client)) { reply(client, "ERR nickname_in_use"); return; }
            client->nickname = command.value;
            client->registered = true;
            if (!write(client, "OK " + client->nickname)) {
                client->disconnect();
                room_.leave(client); // No JOIN was announced, so no LEAVE is needed.
                return;
            }
            broadcast("JOIN " + client->nickname);
            return;
        }
        if (!client->registered) { reply(client, "ERR register_first"); return; }
        if (command.op == Operation::message) {
            broadcast("MSG " + client->nickname + " " + command.value);
        } else {
            std::string users = "USERS";
            for (const auto& member : room_.snapshot()) users += " " + member.first;
            reply(client, users);
        }
    }
    bool submit(const Client& client, EventKind kind, std::string line = {}) {
        auto event = std::make_shared<Event>();
        event->client = client;
        event->kind = kind;
        event->line = std::move(line);
        auto done = event->done.get_future();
        if (!events_.push(event)) return false;
        // One outstanding event per worker bounds work outside the queue too.
        done.wait();
        return client->active && !stopping;
    }
    void serve(const Client& client) {
        std::string pending;
        char buffer[1024];
        while (!stopping && client->active) {
            if (!client->registered && std::chrono::steady_clock::now() - client->admitted >=
                    std::chrono::seconds(10)) {
                submit(client, EventKind::error, "ERR registration_timeout");
                return;
            }
            pollfd p{client->socket.get(), POLLIN, 0};
            int ready = ::poll(&p, 1, 50);
            if (ready < 0) { if (errno == EINTR) continue; return; }
            if (!ready) continue;
            auto count = ::recv(client->socket.get(), buffer, sizeof(buffer), MSG_DONTWAIT);
            if (count < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) continue;
            if (count <= 0) return;
            pending.append(buffer, static_cast<std::size_t>(count));
            std::size_t end;
            while ((end = pending.find('\n')) != std::string::npos) {
                if (end > 1100) { submit(client, EventKind::error, "ERR line_too_long"); return; }
                auto line = pending.substr(0, end);
                pending.erase(0, end + 1);
                if (!line.empty() && line.back() == '\r') line.pop_back();
                for (unsigned char c : line) {
                    if (c < 32 || c == 127) {
                        submit(client, EventKind::error, c == 0 ? "ERR binary_input" : "ERR control_input");
                        return;
                    }
                }
                if (!submit(client, EventKind::command, std::move(line))) return;
            }
            if (pending.size() > 1100) { submit(client, EventKind::error, "ERR line_too_long"); return; }
        }
    }
public:
    ChatServer(std::size_t workers, std::size_t capacity)
        : connections_(workers), events_(capacity), max_users_(workers) {
        try {
            dispatcher_ = std::thread([this] {
                while (auto event = events_.pop()) {
                    try { if (!stopping) dispatch(**event); }
                    catch (...) { stopping = true; }
                    (*event)->done.set_value();
                }
            });
            for (std::size_t i = 0; i < workers; ++i) workers_.emplace_back([this] {
                while (auto client = connections_.pop()) {
                    try {
                        if (!stopping) serve(*client);
                        if (!stopping) submit(*client, EventKind::departure);
                    } catch (...) { stopping = true; }
                    (*client)->disconnect();
                    --admitted_;
                }
            });
        } catch (...) { stop(); throw; }
    }
    ~ChatServer() { stop(); }
    void stop() {
        stopping = true;
        connections_.close();
        events_.close();
        for (auto& thread : workers_) if (thread.joinable()) thread.join();
        if (dispatcher_.joinable()) dispatcher_.join();
        for (const auto& member : room_.snapshot()) member.second->disconnect();
    }
    bool admit(const Client& client) {
        if (admitted_.load() >= max_users_) return false;
        ++admitted_;
        if (connections_.try_push(client)) return true;
        --admitted_;
        return false;
    }
};
}

int main(int argc, char** argv) {
    try {
        if (argc > 4) throw std::invalid_argument("usage: chat_server [port=9090] [workers=4] [event_queue=64]");
        int port = argc > 1 ? number(argv[1], 0, 65535) : 9090;
        int workers = argc > 2 ? number(argv[2], 1, 64) : 4;
        int capacity = argc > 3 ? number(argv[3], 1, 1024) : 64;
        std::signal(SIGINT, stop_handler);
        std::signal(SIGTERM, stop_handler);
        Socket listener(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0));
        if (listener.get() < 0) throw std::runtime_error("socket failed");
        int reuse = 1;
        if (::setsockopt(listener.get(), SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0)
            throw std::runtime_error("setsockopt failed");
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = htons(static_cast<std::uint16_t>(port));
        if (::bind(listener.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0)
            throw std::runtime_error(std::string("bind failed: ") + std::strerror(errno));
        if (::listen(listener.get(), 64) < 0) throw std::runtime_error("listen failed");
        socklen_t length = sizeof(address);
        if (::getsockname(listener.get(), reinterpret_cast<sockaddr*>(&address), &length) < 0)
            throw std::runtime_error("getsockname failed");
        ChatServer server(static_cast<std::size_t>(workers), static_cast<std::size_t>(capacity));
        std::cout << "LISTENING 127.0.0.1:" << ntohs(address.sin_port) << std::endl;
        while (!stopping) {
            pollfd p{listener.get(), POLLIN, 0};
            int ready = ::poll(&p, 1, 50);
            if (ready < 0) { if (errno == EINTR) continue; throw std::runtime_error("poll failed"); }
            if (!ready) continue;
            int fd = ::accept(listener.get(), nullptr, nullptr);
            if (fd < 0) {
                if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) continue;
                throw std::runtime_error("accept failed");
            }
            auto client = std::make_shared<Session>(fd);
            // Keep per-connection kernel output buffering small for this room.
            int send_buffer = 16 * 1024;
            if (::setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &send_buffer, sizeof(send_buffer)) < 0)
                throw std::runtime_error("send buffer configuration failed");
            if (!server.admit(client)) send_all(fd, "ERR capacity\n", stopping);
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
