#include "quant/ipc/shm_manager.hpp"

#include <array>
#include <chrono>
#include <csignal>
#include <iostream>
#include <latch>
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

void fill_packet(Packet& p, std::uint64_t sequence) noexcept {
    p.sequence = sequence;
    for (std::size_t j = 0; j < p.payload.size(); ++j)
        p.payload[j] = sequence * 13 + j;
}

Packet packet(std::uint64_t sequence) {
    Packet p{};
    fill_packet(p, sequence);
    return p;
}

bool packet_matches(const Packet& p, std::uint64_t sequence) noexcept {
    if (p.sequence != sequence) return false;
    for (std::size_t j = 0; j < p.payload.size(); ++j)
        if (p.payload[j] != sequence * 13 + j) return false;
    return true;
}

void check_packet(const Packet& p, std::uint64_t sequence) {
    require(packet_matches(p, sequence), "FIFO order or payload integrity failure");
}

void direct_slot_access() {
    SpscRing<Packet, 2> ring;
    std::size_t read_calls = 0;
    auto empty_reader = [&read_calls](const Packet&) noexcept { ++read_calls; };
    require(!ring.try_read(empty_reader), "empty direct read must fail");
    require(read_calls == 0, "empty direct read must not invoke callback");

    const Packet expected = packet(7);
    const Packet* written_slot = nullptr;
    std::size_t write_calls = 0;
    require(ring.try_write([&](Packet& slot) noexcept {
        ++write_calls;
        written_slot = &slot;
        slot.sequence = expected.sequence;
        for (std::size_t i = 0; i < slot.payload.size(); ++i)
            slot.payload[i] = expected.payload[i];
    }), "direct write");
    require(write_calls == 1, "direct write callback count");

    const Packet* read_slot = nullptr;
    bool payload_matches = false;
    require(ring.try_read([&](const Packet& slot) noexcept {
        ++read_calls;
        read_slot = &slot;
        payload_matches = slot.sequence == expected.sequence &&
                          slot.payload == expected.payload;
    }), "direct read");
    require(read_calls == 1, "direct read callback count");
    require(written_slot == read_slot, "direct APIs must access the ring slot");
    require(payload_matches, "direct read payload integrity");

    const Packet eighth = packet(8);
    const Packet ninth = packet(9);
    require(ring.try_write([&](Packet& slot) noexcept { slot = eighth; }),
            "first direct write into capacity");
    require(ring.try_write([&](Packet& slot) noexcept { slot = ninth; }),
            "second direct write into capacity");
    bool full_writer_called = false;
    require(!ring.try_write([&](Packet&) noexcept { full_writer_called = true; }),
            "full direct write must fail");
    require(!full_writer_called, "full direct write must not invoke callback");

    Packet output{};
    require(ring.try_pop(output), "direct write must interoperate with pop");
    check_packet(output, 8);
    require(ring.try_pop(output), "second direct write must interoperate with pop");
    check_packet(output, 9);

    const Packet tenth = packet(10);
    require(ring.try_push(tenth), "push must interoperate with direct read");
    payload_matches = false;
    require(ring.try_read([&](const Packet& slot) noexcept {
        payload_matches = slot.sequence == tenth.sequence && slot.payload == tenth.payload;
    }), "direct read after push");
    require(payload_matches, "push/direct read payload integrity");
}

void reader_gives_up_before_publish() {
    SpscRing<Packet, 2> ring;
    std::latch writer_entered{1};
    std::latch allow_publish{1};
    bool write_succeeded = false;

    std::thread producer([&] {
        write_succeeded = ring.try_write([&](Packet& slot) noexcept {
            slot.sequence = 100;
            slot.payload[0] = 100 * 13;
            writer_entered.count_down();
            allow_publish.wait();
            for (std::size_t j = 1; j < slot.payload.size(); ++j)
                slot.payload[j] = 100 * 13 + j;
        });
    });

    writer_entered.wait();
    bool reader_called = false;
    const bool read_before_publish = ring.try_read(
        [&](const Packet&) noexcept { reader_called = true; });
    allow_publish.count_down();
    producer.join();

    require(write_succeeded, "paused direct write must eventually publish");
    require(!read_before_publish, "reader must give up before producer publishes");
    require(!reader_called, "reader callback must not see an unpublished slot");

    bool payload_matches = false;
    require(ring.try_read([&](const Packet& slot) noexcept {
        payload_matches = packet_matches(slot, 100);
    }), "reader must see slot after publish");
    require(payload_matches, "reader saw a partially written slot");
}

void writer_uses_next_free_slot() {
    SpscRing<Packet, 2> ring;
    require(ring.try_write([](Packet& slot) noexcept { fill_packet(slot, 200); }),
            "publish first slot");

    std::latch consumer_entered{1};
    std::latch allow_release{1};
    std::latch first_released{1};
    std::latch third_published{1};
    bool first_read = false;
    bool second_read = false;
    bool third_read = false;
    bool first_matches = false;
    bool second_matches = false;
    bool third_matches = false;

    std::thread consumer([&] {
        first_read = ring.try_read([&](const Packet& slot) noexcept {
            first_matches = packet_matches(slot, 200);
            consumer_entered.count_down();
            allow_release.wait();
        });
        first_released.count_down();
        third_published.wait();
        second_read = ring.try_read([&](const Packet& slot) noexcept {
            second_matches = packet_matches(slot, 201);
        });
        third_read = ring.try_read([&](const Packet& slot) noexcept {
            third_matches = packet_matches(slot, 202);
        });
    });

    consumer_entered.wait();
    const bool wrote_next = ring.try_write(
        [](Packet& slot) noexcept { fill_packet(slot, 201); });
    bool full_writer_called = false;
    const bool overwrote_held_slot = ring.try_write([&](Packet&) noexcept {
        full_writer_called = true;
    });

    allow_release.count_down();
    first_released.wait();
    const bool reused_released_slot = ring.try_write(
        [](Packet& slot) noexcept { fill_packet(slot, 202); });
    third_published.count_down();
    consumer.join();

    require(first_read && first_matches, "consumer must read the held first slot");
    require(wrote_next, "producer must be able to write the next free slot");
    require(!overwrote_held_slot && !full_writer_called,
            "producer must not overwrite a slot still held by consumer");
    require(reused_released_slot, "producer must reuse slot after consumer release");
    require(second_read && second_matches && third_read && third_matches,
            "writer-next path must preserve FIFO and payload integrity");
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
    for (std::uint64_t i = 0; i < transfer_count; ++i) {
        bool payload_matches = false;
        while (!queue.ring().try_read([&](const Packet& slot) noexcept {
            payload_matches = slot.sequence == i;
            for (std::size_t j = 0; j < slot.payload.size(); ++j)
                payload_matches = payload_matches && slot.payload[j] == i * 13 + j;
        })) {
            require(Clock::now() < deadline, "consumer timed out");
            std::this_thread::yield();
        }
        require(payload_matches, "cross-process payload integrity failure");
        if (i % 10000 == 0) std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
    require(!queue.ring().try_read([](const Packet&) noexcept {}),
            "unexpected extra message");
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
        while (!queue.ring().try_write([i](Packet& slot) noexcept {
            slot.sequence = i;
            for (std::size_t j = 0; j < slot.payload.size(); ++j)
                slot.payload[j] = i * 13 + j;
        })) {
            require(Clock::now() < deadline, "producer timed out");
            std::this_thread::yield();
        }
    }
    child.wait_success();
}

int main(int argc, char** argv) {
    try {
        if (argc == 3 && std::string(argv[1]) == "--consume") return consume(argv[2]);
        direct_slot_access();
        reader_gives_up_before_publish();
        writer_uses_next_free_slot();
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
