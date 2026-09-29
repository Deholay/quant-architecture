#include "quant/ipc/shm_manager.hpp"

#include <array>
#include <chrono>
#include <csignal>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include <fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

using namespace quant::ipc;
using Clock = std::chrono::steady_clock;

void require(bool ok, const char* message) {
    if (!ok) throw std::runtime_error(message);
}

template <class F>
void throws(F&& action) {
    bool caught = false;
    try { action(); } catch (const std::exception&) { caught = true; }
    require(caught, "expected exception");
}

struct Name {
    std::string value;
    explicit Name(const char* suffix)
        : value("/quant_ipc_test." + std::to_string(::getpid()) + "." + suffix) {}
    ~Name() { try { ShmManager::remove(value); } catch (...) {} }
};

struct Packet {
    std::uint64_t sequence;
    std::array<std::uint64_t, 7> payload;
};

Packet packet(std::uint64_t sequence) {
    Packet p{sequence, {}};
    for (std::size_t j = 0; j < p.payload.size(); ++j)
        p.payload[j] = sequence * 13 + j;
    return p;
}

void check_packet(const Packet& p, std::uint64_t sequence) {
    require(p.sequence == sequence && p.payload == packet(sequence).payload,
            "FIFO order or payload integrity failure");
}

void ring_boundaries() {
    // Exercise unsigned counter wrap as well as repeated physical slot wrap.
    SpscRing<Packet, 4> ring(std::numeric_limits<std::uint64_t>::max() - 2);
    Packet output = packet(999);
    require(!ring.try_pop(output), "new ring must be empty");
    check_packet(output, 999); // failed pop leaves output unchanged
    for (std::uint64_t round = 0; round < 100; ++round) {
        for (std::uint64_t i = 0; i < 4; ++i)
            require(ring.try_push(packet(round * 4 + i)), "all capacity slots usable");
        require(!ring.try_push(packet(999)), "full ring must reject push");
        for (std::uint64_t i = 0; i < 4; ++i) {
            require(ring.try_pop(output), "expected message");
            check_packet(output, round * 4 + i);
        }
        require(!ring.try_pop(output), "drained ring must be empty");
    }
}

void manager_lifecycle() {
    Name name("lifecycle");
    auto owner = ShmManager::create<Packet, 8>(name.value, 42, 7);
    throws([&] { auto duplicate = ShmManager::create<Packet, 8>(name.value, 42, 7); });
    // Failed duplicate creation must not remove the existing object.
    auto peer = ShmManager::open<Packet, 8>(name.value, 42, 7);
    throws([&] { auto bad = ShmManager::open<Packet, 8>(name.value, 43, 7); });
    throws([&] { auto bad = ShmManager::open<Packet, 8>(name.value, 42, 8); });
    throws([&] { auto bad = ShmManager::open<Packet, 4>(name.value, 42, 7); });
    throws([&] { auto bad = ShmManager::open<std::uint64_t, 8>(name.value, 42, 7); });
    auto moved = std::move(peer);
    throws([&] { (void)peer.ring(); });
    require(owner.ring().try_push(packet(123)), "push");
    Packet output{};
    require(moved.ring().try_pop(output), "peer pop");
    check_packet(output, 123);
    // unlink removes name; existing mappings are still usable.
    ShmManager::remove(name.value);
    throws([&] { auto missing = ShmManager::open<Packet, 8>(name.value, 42, 7); });
    require(owner.ring().try_push(packet(456)), "push after unlink");
    require(moved.ring().try_pop(output), "pop after unlink");
    check_packet(output, 456);
    ShmManager::remove(name.value); // idempotent cleanup
}

void region_lifecycle_and_failures() {
    Name name("region");
    throws([&] { auto bad = ShmRegion::create(name.value, 64, [](void*) {
        throw std::runtime_error("initialization failed");
    }); });
    // Failed initialization must release mapping, fd and name.
    {
        auto first = ShmRegion::create(name.value, 64, [](void* p) {
            *static_cast<std::uint64_t*>(p) = 123;
        });
        auto second = ShmRegion::open(name.value, 64, [](void*) {});
        second = std::move(first);
        require(first.data() == nullptr && first.size() == 0, "move clears source");
        require(*static_cast<std::uint64_t*>(second.data()) == 123, "move preserves mapping");
    }
    auto reopened = ShmRegion::open(name.value, 64, [](void* p) {
        require(*static_cast<std::uint64_t*>(p) == 123, "destructor must not unlink");
    });
    throws([&] { auto bad = ShmRegion::open(name.value, 128, [](void*) {}); });
    throws([] { auto bad = ShmRegion::create("bad/name", 64, [](void*) {}); });
    throws([] { auto bad = ShmRegion::create("/zero", 0, [](void*) {}); });
    throws([&] { auto bad = ShmManager::create<Packet, 8>(name.value, 0, 1); });
}

void initialization_and_header_validation() {
    Name name("header");
    {
        auto queue = ShmManager::create<Packet, 8>(name.value, 42, 7);
        const int fd = ::shm_open(name.value.c_str(), O_RDWR, 0);
        require(fd >= 0, "open raw descriptor");
        struct stat info {};
        require(::fstat(fd, &info) == 0 && (info.st_mode & 0777) == 0600,
                "private SHM permissions");
        require(::flock(fd, LOCK_EX) == 0, "hold initialization lock");
        throws([&] { auto busy = ShmManager::open<Packet, 8>(name.value, 42, 7); });
        ::close(fd);
    }
    // Each header field must be checked independently, including same-size ABI errors.
    for (std::size_t field = 0; field < 8; ++field) {
        std::array<std::uint64_t, 8> original{};
        auto raw = ShmRegion::open(name.value, sizeof(detail::Layout<Packet, 8>),
                                  [&](void* p) {
            std::memcpy(original.data(), p, sizeof(original));
            auto words = original;
            words[field] ^= 1;
            std::memcpy(p, words.data(), sizeof(words));
        });
        throws([&] { auto bad = ShmManager::open<Packet, 8>(name.value, 42, 7); });
        std::memcpy(raw.data(), original.data(), sizeof(original));
    }
    auto valid = ShmManager::open<Packet, 8>(name.value, 42, 7);
    Name incomplete("incomplete");
    auto raw = ShmRegion::create(incomplete.value, sizeof(detail::Layout<Packet, 8>),
                                 [](void*) {});
    throws([&] { auto bad = ShmManager::open<Packet, 8>(incomplete.value, 42, 7); });
}

void independent_channels() {
    Name feed("feed"), orders("orders"), executions("executions");
    auto a = ShmManager::create<Packet, 8>(feed.value, 1, 1);
    auto b = ShmManager::create<Packet, 4>(orders.value, 2, 1);
    auto c = ShmManager::create<Packet, 16>(executions.value, 3, 1);
    require(a.ring().try_push(packet(1)), "feed push");
    Packet output{};
    require(!b.ring().try_pop(output) && !c.ring().try_pop(output), "independent queues");
    require(b.ring().try_push(packet(2)) && c.ring().try_push(packet(3)), "other pushes");
    require(a.ring().try_pop(output), "feed pop"); check_packet(output, 1);
    require(b.ring().try_pop(output), "orders pop"); check_packet(output, 2);
    require(c.ring().try_pop(output), "executions pop"); check_packet(output, 3);
}

constexpr std::uint64_t transfer_count = 1000000;

int consume(const char* name) {
    auto queue = ShmManager::open<Packet, 64>(name, 99, 1);
    const auto deadline = Clock::now() + std::chrono::seconds(15);
    Packet output{};
    for (std::uint64_t i = 0; i < transfer_count; ++i) {
        while (!queue.ring().try_pop(output)) {
            require(Clock::now() < deadline, "consumer timed out");
            std::this_thread::yield();
        }
        check_packet(output, i);
        if (i % 10000 == 0) std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
    require(!queue.ring().try_pop(output), "unexpected extra message");
    return 0;
}

struct Child {
    pid_t pid;
    ~Child() {
        if (pid > 0) {
            ::kill(pid, SIGKILL);
            while (::waitpid(pid, nullptr, 0) < 0 && errno == EINTR) {}
        }
    }
    void wait_success() {
        const auto deadline = Clock::now() + std::chrono::seconds(5);
        int status = 0;
        for (;;) {
            const auto result = ::waitpid(pid, &status, WNOHANG);
            if (result == pid) { pid = -1; break; }
            require(result == 0 || errno == EINTR, "waitpid failed");
            require(Clock::now() < deadline, "child exit timed out");
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        require(WIFEXITED(status) && WEXITSTATUS(status) == 0, "consumer failed");
    }
};

void abrupt_exit() {
    Name partial("aborted_init");
    auto pid = ::fork();
    require(pid >= 0, "fork failed");
    if (pid == 0) {
        // Exit without unwinding while holding the initialization lock.
        auto region = ShmRegion::create(partial.value, sizeof(detail::Layout<Packet, 8>),
                                        [](void*) { ::_exit(0); });
        ::_exit(1);
    }
    Child creator{pid};
    creator.wait_success();
    throws([&] { auto bad = ShmManager::open<Packet, 8>(partial.value, 42, 1); });
    // Verify kernel released dead creator's lock, rather than just rejecting a
    // permanently busy object. Manager above rejects its incomplete header.
    auto raw = ShmRegion::open(partial.value, sizeof(detail::Layout<Packet, 8>), [](void*) {});

    Name published("aborted_producer");
    auto queue = ShmManager::create<Packet, 8>(published.value, 42, 1);
    pid = ::fork();
    require(pid >= 0, "fork failed");
    if (pid == 0) {
        const bool pushed = queue.ring().try_push(packet(77));
        ::_exit(pushed ? 0 : 1); // no destructors or cleanup
    }
    Child producer{pid};
    producer.wait_success();
    Packet output{};
    require(queue.ring().try_pop(output), "published data survives producer exit");
    check_packet(output, 77);
    require(!queue.ring().try_pop(output), "dead producer does not imply more data");
}

void cross_process() {
    Name name("process");
    auto queue = ShmManager::create<Packet, 64>(name.value, 99, 1);
    const auto pid = ::fork();
    require(pid >= 0, "fork failed");
    if (pid == 0) {
        // exec discards inherited mappings: consumer must attach independently.
        ::execl("/proc/self/exe", "ipc_tests", "--consume", name.value.c_str(), nullptr);
        ::_exit(127);
    }
    Child child{pid};
    const auto deadline = Clock::now() + std::chrono::seconds(15);
    for (std::uint64_t i = 0; i < transfer_count; ++i) {
        const auto input = packet(i);
        while (!queue.ring().try_push(input)) {
            require(Clock::now() < deadline, "producer timed out");
            std::this_thread::yield();
        }
    }
    child.wait_success();
}

int main(int argc, char** argv) {
    try {
        if (argc == 3 && std::string(argv[1]) == "--consume") return consume(argv[2]);
        ring_boundaries();
        manager_lifecycle();
        region_lifecycle_and_failures();
        initialization_and_header_validation();
        independent_channels();
        abrupt_exit();
        cross_process();
        std::cout << "All IPC tests passed (including 1000000 cross-process messages).\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
