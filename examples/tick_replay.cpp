#include "quant/ipc/shm_manager.hpp"
#include "tick_replay_support.hpp"

#include <charconv>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string_view>
#include <thread>

using quant::ipc::ShmManager;
using Queue = quant::ipc::ShmQueue<replay::Record, replay::capacity>;
using Clock = std::chrono::steady_clock;

std::uint64_t number(std::string_view text) {
    std::uint64_t value{};
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (error != std::errc{} || end != text.data() + text.size() || text.empty())
        throw std::invalid_argument("invalid unsigned integer: " + std::string(text));
    return value;
}

std::chrono::milliseconds timeout_arg(const char* text) {
    const auto n = number(text);
    if (!n || n > 600000) throw std::invalid_argument("stall timeout must be 1..600000 ms");
    return std::chrono::milliseconds(n);
}

void retry(Clock::time_point deadline) {
    if (Clock::now() >= deadline) throw std::runtime_error("peer stalled or missing (queue timeout)");
    std::this_thread::yield();
}

void send(Queue& queue, const char* path, std::chrono::milliseconds timeout) {
    if (!std::filesystem::is_regular_file(path)) throw std::runtime_error("replay input must be a regular file");
    std::ifstream input(path, std::ios::binary);
    replay::FileHeader header{};
    if (!input.read(reinterpret_cast<char*>(&header), sizeof(header)))
        throw std::runtime_error("cannot read replay file header");
    const auto bytes = std::filesystem::file_size(path);
    if (header.magic != replay::magic || header.schema != replay::schema ||
        header.record_bytes != sizeof(replay::Tick) ||
        (bytes - sizeof(header)) % sizeof(replay::Tick) != 0 ||
        header.count != (bytes - sizeof(header)) / sizeof(replay::Tick))
        throw std::runtime_error("replay file format/size/count mismatch");

    replay::Histogram wait_latency;
    std::uint64_t full_retries = 0;
    std::uint64_t state = replay::initial_checksum;
    const auto start = Clock::now();
    for (std::uint64_t sequence = 0; sequence < header.count; ++sequence) {
        const auto attempt_ns = replay::monotonic_ns();
        if (!attempt_ns) throw std::runtime_error("CLOCK_MONOTONIC failed");
        const auto deadline = Clock::now() + timeout;
        std::uint64_t entered_ns = 0;
        bool valid = false;
        while (!queue.ring().try_write([&](replay::Record& slot) noexcept {
            entered_ns = replay::monotonic_ns();
            slot.kind = replay::Kind::error;
            slot.replay_sequence = sequence;
            slot.enqueue_ns = 0;
            slot.checksum = 0;
            // ifstream has exceptions disabled; read directly into the SHM slot.
            // File I/O belongs to this example's callback, not the ring implementation.
            input.read(reinterpret_cast<char*>(&slot.tick), sizeof(slot.tick));
            if (!input || !entered_ns || entered_ns < attempt_ns) return;
            state = replay::checksum(state, slot.tick);
            slot.checksum = state;
            slot.kind = replay::Kind::tick;
            slot.enqueue_ns = replay::monotonic_ns(); // last operation before publish
            valid = slot.enqueue_ns != 0 && slot.enqueue_ns >= entered_ns;
            if (!valid) slot.kind = replay::Kind::error;
        })) {
            ++full_retries;
            retry(deadline);
        }
        if (!valid) throw std::runtime_error("file read/clock failed at row " + std::to_string(sequence + 1));
        wait_latency.add(entered_ns - attempt_ns);
    }
    const auto deadline = Clock::now() + timeout;
    while (!queue.ring().try_write([&](replay::Record& slot) noexcept {
        slot.kind = replay::Kind::end;
        slot.replay_sequence = header.count;
        slot.checksum = state;
        slot.enqueue_ns = 0;
    })) {
        ++full_retries;
        retry(deadline);
    }
    const auto seconds = std::chrono::duration<double>(Clock::now() - start).count();
    std::ostringstream report;
    report << "producer rows=" << header.count << " checksum=" << state
           << " full_retries=" << full_retries << " seconds=" << seconds
           << " replay_ticks_per_sec=" << (seconds > 0 ? header.count / seconds : 0) << '\n';
    wait_latency.report(report, "enqueue_wait");
    std::cout << report.str();
}

void receive(Queue& queue, std::chrono::milliseconds timeout, std::uint64_t delay_us) {
    replay::Histogram queue_latency;
    auto state = replay::initial_checksum;
    std::uint64_t count = 0;
    std::uint64_t empty_retries = 0;
    const auto start = Clock::now();
    bool finished = false;
    while (!finished) {
        const auto deadline = Clock::now() + timeout;
        bool valid = false;
        bool clock_valid = true;
        std::uint64_t latency = 0;
        while (!queue.ring().try_read([&](const replay::Record& slot) noexcept {
            const auto received_ns = replay::monotonic_ns(); // before checksum/processing
            if (slot.replay_sequence != count) return;
            if (slot.kind == replay::Kind::end) {
                finished = true;
                valid = slot.checksum == state;
                return;
            }
            if (slot.kind != replay::Kind::tick) return;
            clock_valid = received_ns != 0 && slot.enqueue_ns != 0 && received_ns >= slot.enqueue_ns;
            if (!clock_valid) return;
            latency = received_ns - slot.enqueue_ns;
            const auto next = replay::checksum(state, slot.tick);
            valid = next == slot.checksum;
            state = next;
        })) {
            ++empty_retries;
            retry(deadline);
        }
        if (!clock_valid) throw std::runtime_error("invalid monotonic timestamp");
        if (!valid) throw std::runtime_error("sequence/checksum/producer error at row " + std::to_string(count + 1));
        if (!finished) {
            ++count;
            queue_latency.add(latency);
            if (delay_us) std::this_thread::sleep_for(std::chrono::microseconds(delay_us));
        }
    }
    const auto seconds = std::chrono::duration<double>(Clock::now() - start).count();
    std::ostringstream report;
    report << "consumer rows=" << count << " checksum=" << state << " verified=true"
           << " empty_retries=" << empty_retries << " seconds=" << seconds
           << " receive_wall_ticks_per_sec=" << (seconds > 0 ? count / seconds : 0) << '\n';
    queue_latency.report(report, "queue_delivery");
    std::cout << report.str();
}

int main(int argc, char** argv) {
    try {
        if (argc < 3) {
            std::cerr << "Usage:\n"
                         "  tick_replay create /name generation\n"
                         "  tick_replay send /name generation ticks.bin [stall_timeout_ms]\n"
                         "  tick_replay receive /name generation [stall_timeout_ms] [delay_us]\n"
                         "  tick_replay remove /name\n";
            return 2;
        }
        const std::string command = argv[1];
        if (command == "remove" && argc == 3) { ShmManager::remove(argv[2]); return 0; }
        if (argc < 4) throw std::invalid_argument("generation required");
        const auto generation = number(argv[3]);
        if (command == "create" && argc == 4) {
            auto owner = ShmManager::create<replay::Record, replay::capacity>(argv[2], replay::schema, generation);
            return 0;
        }
        if (command == "send" && (argc == 5 || argc == 6)) {
            const auto timeout = argc == 6 ? timeout_arg(argv[5]) : std::chrono::milliseconds(10000);
            auto queue = ShmManager::open<replay::Record, replay::capacity>(argv[2], replay::schema, generation);
            send(queue, argv[4], timeout);
            return 0;
        }
        if (command == "receive" && argc >= 4 && argc <= 6) {
            const auto timeout = argc >= 5 ? timeout_arg(argv[4]) : std::chrono::milliseconds(10000);
            const auto delay = argc == 6 ? number(argv[5]) : 0;
            if (delay > 1000000) throw std::invalid_argument("delay_us must be <= 1000000");
            auto queue = ShmManager::open<replay::Record, replay::capacity>(argv[2], replay::schema, generation);
            receive(queue, timeout, delay);
            return 0;
        }
        throw std::invalid_argument("unknown command or wrong argument count");
    } catch (const std::exception& error) {
        std::cerr << "tick_replay: " << error.what() << '\n';
        return 1;
    }
}
