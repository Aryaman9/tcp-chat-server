#include "core.hpp"
#include "net.hpp"
#include <arpa/inet.h>
#include <csignal>
#include <cstring>
#include <iostream>

int main(int argc, char** argv) {
    try {
        if (argc < 2 || argc > 3 || !valid_nickname(argv[1]))
            throw std::invalid_argument("usage: chat_client NICKNAME [port=9090] (1-16 letters/digits/_/-)");
        const int port = argc == 3 ? number(argv[2], 1, 65535) : 9090;
        const std::atomic<bool> stop{false};
        std::signal(SIGPIPE, SIG_IGN);
        Socket socket(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0));
        if (socket.get() < 0) throw std::runtime_error("socket failed");
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = htons(static_cast<std::uint16_t>(port));
        if (::connect(socket.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
            if (errno != EINPROGRESS) throw std::runtime_error(std::strerror(errno));
            pollfd p{socket.get(), POLLOUT, 0};
            int error = 0;
            socklen_t size = sizeof(error);
            if (::poll(&p, 1, 3000) <= 0 ||
                ::getsockopt(socket.get(), SOL_SOCKET, SO_ERROR, &error, &size) < 0 || error)
                throw std::runtime_error("connection failed; check that the server is running on this port");
        }
        auto send = [&](const std::string& line) {
            if (!send_all(socket.get(), line + "\n", stop)) throw std::runtime_error("send failed or timed out");
        };
        send(std::string("NICK ") + argv[1]);
        std::cerr << "Type a message, /users, /nick NAME (retry registration), or /quit.\n";
        std::string input, incoming;
        bool quitting = false, discard_input = false;
        auto quit_deadline = std::chrono::steady_clock::time_point::max();
        auto quit = [&] {
            send("QUIT");
            quitting = true;
            quit_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        };
        auto submit = [&](std::string line) {
            if (!line.empty() && line.back() == '\r') line.pop_back();
            if (line.empty()) return;
            for (unsigned char c : line) {
                if (c < 32 || c == 127) { std::cerr << "Control characters are not allowed.\n"; return; }
            }
            if (line == "/quit") quit();
            else if (line == "/users") send("USERS");
            else if (line.compare(0, 6, "/nick ") == 0) send("NICK " + line.substr(6));
            else if (line.front() == '/') std::cerr << "Unknown command. Use /users, /nick NAME, or /quit.\n";
            else send("MSG " + line);
        };
        while (true) {
            if (quitting && std::chrono::steady_clock::now() >= quit_deadline)
                throw std::runtime_error("timed out waiting for BYE");
            pollfd fds[2]{{socket.get(), POLLIN, 0}, {quitting ? -1 : STDIN_FILENO, POLLIN, 0}};
            int ready = ::poll(fds, 2, 100);
            if (ready < 0) { if (errno == EINTR) continue; throw std::runtime_error("poll failed"); }
            char buffer[1024];
            if (fds[0].revents) {
                auto count = ::recv(socket.get(), buffer, sizeof(buffer), MSG_DONTWAIT);
                if (count == 0) { std::cerr << "Server disconnected.\n"; return 0; }
                if (count < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
                    throw std::runtime_error("server connection lost");
                if (count > 0) {
                    incoming.append(buffer, static_cast<std::size_t>(count));
                    std::size_t end;
                    while ((end = incoming.find('\n')) != std::string::npos) {
                        if (end > 1100) throw std::runtime_error("oversized server response");
                        auto line = incoming.substr(0, end);
                        incoming.erase(0, end + 1);
                        std::cout << line << std::endl;
                        if (line == "BYE") return 0;
                    }
                    if (incoming.size() > 1100) throw std::runtime_error("oversized server response");
                }
            }
            if (fds[1].revents) {
                auto count = ::read(STDIN_FILENO, buffer, sizeof(buffer));
                if (count < 0) { if (errno == EINTR || errno == EAGAIN) continue; throw std::runtime_error("stdin failed"); }
                if (count == 0) {
                    if (!discard_input && !input.empty()) submit(input);
                    if (!quitting) quit();
                }
                for (ssize_t i = 0; i < count && !quitting; ++i) {
                    if (buffer[i] == '\n') {
                        if (!discard_input) submit(input);
                        input.clear();
                        discard_input = false;
                    } else if (!discard_input) {
                        input += buffer[i];
                        if (input.size() > 1024) {
                            std::cerr << "Input exceeds 1024 bytes; line discarded.\n";
                            input.clear();
                            discard_input = true;
                        }
                    }
                }
            }
        }
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
