# Multithreaded TCP/IP Chat Server

A small Linux C++17 chat application using POSIX TCP sockets. Several terminal clients choose unique nicknames and exchange text in one shared room. Both the server and interactive client are C++. Python is used only for tests and the scripted demonstration.

## Features

- Unique nicknames, message broadcasting, a user list, and join/leave notifications.
- Fixed worker pool and bounded producer–consumer queues with backpressure.
- RAII socket ownership and synchronized registry access.
- An interactive terminal client that receives messages while input is idle.
- Tests for concurrent clients, TCP framing, slow receivers, and graceful shutdown.

## Build and run in Linux / WSL Ubuntu

Prerequisites: Linux, a C++17 compiler, CMake 3.20+, and Python 3 for the tests. On Windows, run these commands inside WSL. This application does not build against native Windows sockets.

```sh
git clone https://github.com/Aryaman9/tcp-chat-server.git
cd tcp-chat-server
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j 2
ctest --test-dir build --output-on-failure
./build/chat_server
```

With the server running, open two more WSL terminals in the same directory:

**Client terminal 1:**
```sh
./build/chat_client alice
```

**Client terminal 2:**
```sh
./build/chat_client bob
```

Type `hello Bob` in Alice's terminal and `hello Alice` in Bob's. Both terminals display `MSG alice hello Bob` and `MSG bob hello Alice`. Use `/users` to list names and `/quit` to leave. Ctrl+D on empty input also quits. If registration reports a duplicate name, use `/nick another_name` within the registration deadline. Names cannot be changed after successful registration.

The client continues receiving while stdin is idle or holds an unfinished line. It prints protocol lines directly; incoming messages may interrupt the line being typed visually. There is no prompt redraw or terminal UI library.

The server takes `[port=9090] [workers=4] [event_queue=64]`:
```sh
./build/chat_server 9091 8 16
./build/chat_client alice 9091
```

Ports range from 0–65535 for the server, workers from 1–64, and event queue capacity from 1–1024. Port zero selects a free port, printed as `LISTENING 127.0.0.1:PORT`; pass that actual port to the client. Stop the server with Ctrl+C or SIGTERM.

A reproducible two-client demonstration using the actual C++ binaries:
```sh
python3 tests/demo_chat.py ./build/chat_server ./build/chat_client
```

Example conversation:

```text
OK alice
JOIN alice
JOIN bob
MSG alice hello Bob
MSG bob hello Alice
USERS alice bob
LEAVE bob
```

## Wire protocol

Commands are case-sensitive and newline-delimited. CRLF is accepted. There is no initial server greeting; send `NICK name`. Arguments use one literal space after the command, with no extra whitespace around nicknames or argument-free commands.

| Client command | Server output |
|---|---|
| `NICK alice` | `OK alice`, then `JOIN alice` to every registered user, including Alice |
| `MSG hello world` | `MSG alice hello world` to every registered user, **including the sender** |
| `USERS` | `USERS alice bob`, names sorted lexicographically |
| `QUIT` | `BYE`, then connection shutdown; peers receive `LEAVE alice` |

The C++ client turns ordinary text into `MSG text`, `/users` into `USERS`, `/nick NAME` into `NICK NAME`, and `/quit` or stdin EOF into `QUIT`. Blank terminal lines are ignored; a blank line sent directly over TCP returns `ERR command`.

- Nicknames contain 1–16 ASCII letters, digits, underscores, or hyphens and are case-sensitive.
- A message contains 1–1024 bytes. Spaces are preserved, including leading/trailing spaces in its payload. UTF-8 text passes through as bytes; Unicode validity is not checked.
- Incoming command lines are limited to 1100 bytes before LF (including an optional CR). NUL, ASCII control bytes, and DEL are forbidden inside a line. A trailing CR is allowed only as part of CRLF.
- `ERR nickname`, `ERR nickname_in_use`, `ERR already_registered`, `ERR message`, and `ERR command` leave the connection open. Correct the command and retry. `MSG` and `USERS` before registration return `ERR register_first`; `QUIT` works before registration.
- `ERR line_too_long`, `ERR binary_input`, `ERR control_input`, `ERR registration_timeout`, and `ERR capacity` are followed by disconnection. A broken/slow socket may close without a deliverable error.
- Registration has a ten-second deadline from admission, checked by its worker while reading. Dispatcher/queue waits can extend when the timeout response is delivered. Sending invalid commands does not reset the deadline. Registered users have no idle timeout or session lifetime cap.
- Complete commands received before TCP EOF are dispatched in order; an unfinished trailing command is discarded. Clean quits, abrupt EOF/resets, and failed writes remove the nickname. It can be reclaimed after registry removal; receiving `LEAVE name` confirms that removal. No duplicate leave is emitted for stale work.

## Threads, ownership, and ordering

```text
accept loop -> bounded connection queue -> fixed connection workers
                                                   |
                                           bounded event queue
                                                   |
                                            one dispatcher
                                                   |
                                     registered users' sockets
```

Each persistent connection occupies one worker. The admitted connection limit equals the worker count, including clients that have not registered yet. The connection queue has that same capacity and only transfers admitted clients to workers; there is no extra waiting room of clients that cannot get a worker. Extra connections receive `ERR capacity` and close. An OS listen backlog is separate from application admission.

Workers frame TCP input, enqueue command/error/departure events, and wait for dispatcher completion before reading the next command. The event queue blocks producers when full, applying backpressure instead of silently dropping messages. At most one event per worker is outstanding, in addition to bounded input buffers and OS socket buffers. A queue capacity above the worker count therefore adds no practical backlog capacity in this implementation. Queue waits use condition variables with predicates; closing wakes both producers and consumers.

The dispatcher is the **only writer to admitted sockets**, including command replies and broadcasts, so frames cannot interleave. The accept loop writes only to rejected sockets. The nickname registry has a mutex and returns snapshots of shared session references; no registry mutex is held during network I/O. Registry changes and room events are serialized by the dispatcher.

Per-sender command order is preserved. Healthy users present for the same broadcasts observe the dispatcher queue's order. There is no promise about which of two concurrent senders wins or their wall-clock send order. The sender's echoed message confirms dispatch; there is no separate acknowledgement, durable delivery, replay, or delivery guarantee across disconnect/shutdown.

`Socket` owns one descriptor and closes it through RAII. Session references in workers, the registry, and queued events keep that wrapper alive. Disconnect uses `shutdown`; it does not close a descriptor still referenced by pending work. Registry removal uses session identity so a stale departure cannot remove a new owner of the same name.

Sending handles partial writes with nonblocking sends and a one-second total deadline per output line. Accepted sockets request a 16 KiB send buffer (Linux may adjust the actual size). Failed recipients are removed and peers receive a leave event. Sequential broadcasting means a slow recipient can delay the room until that deadline; several slow users add delays. This is deliberately a small-room architecture.

SIGINT/SIGTERM stops admission, closes both queues, wakes blocked workers, interrupts socket waits through a checked stop flag, disconnects sessions, and joins all threads. Outstanding work may be discarded during shutdown; users should not expect a final `BYE` from a stopping server.

## Tests

Ordinary tests:
```sh
ctest --test-dir build --output-on-failure
```

AddressSanitizer and UndefinedBehaviorSanitizer:
```sh
cmake -S . -B build-sanitized -DCMAKE_BUILD_TYPE=Debug -DENABLE_SANITIZERS=ON
cmake --build build-sanitized -j 2
ctest --test-dir build-sanitized --output-on-failure
```

Optional ThreadSanitizer (separate build; runtime support depends on the host):
```sh
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DENABLE_TSAN=ON
cmake --build build-tsan -j 2
ctest --test-dir build-tsan --output-on-failure
```

The core suite checks parsing, queue capacity/backpressure/close wakeups, concurrent registry operations, descriptor lifetime, partial writes, and bounded send failure. The 14 network tests exercise real TCP clients and the C++ client, concurrent messages, nickname conflicts, framing, limits, idle users, slow recipients, capacity, and shutdown.

GitHub Actions runs Debug, Release, and ASan/UBSan builds on Ubuntu. ThreadSanitizer can be run separately with the commands above; its runtime support depends on the host. Tests are registered through CTest and require only the Python standard library. To build just the application without Python or tests, configure with `-DBUILD_TESTING=OFF`.

## Project layout

```text
include/core.hpp          Bounded queue, command parser, and room registry
include/net.hpp           Socket ownership and bounded partial-write handling
src/server.cpp            Admission, connection workers, dispatcher, and shutdown
src/client.cpp            Interactive terminal client
tests/core_tests.cpp      Unit and socket-pair checks
tests/test_network.py     TCP and C++ client integration tests
tests/demo_chat.py        Scripted two-client demonstration
```

## Limits and troubleshooting

- Localhost only: no authentication, TLS, history, persistence, private messaging, or multiple rooms.
- `bind failed: Address already in use`: select another port or stop the server you started. Use port zero for tests.
- Connection failed: run server and clients in the same WSL/Linux environment and use the printed port.
- `ERR capacity`: an admitted connection occupies every worker. Close a client or start the server with a larger worker count.
- A duplicate nickname may briefly remain until its disconnect event is processed. Retry after its leave notification.
- No throughput or scalability benchmark is claimed. Socket tests and sanitizers are evidence for the exercised cases, not proof of race freedom.
