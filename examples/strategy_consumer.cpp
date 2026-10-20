#include "quant/ipc/shm_manager.hpp"
#include "tick_replay_support.hpp"

#include <charconv>
#include <chrono>
#include <cmath>
#include <numeric>
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

// Demonstration only: compare last price with the best bid/ask midpoint.
// No position, order routing, or interpretation of feed status/time units.
enum class Signal { buy, sell, hold, ignored };

Signal on_tick(const replay::Tick& tick) noexcept {
    if (!std::isfinite(tick.last_price) || !std::isfinite(tick.bid_price1) ||
        !std::isfinite(tick.ask_price1) || tick.last_price <= 0 ||
        tick.bid_price1 <= 0 || tick.ask_price1 < tick.bid_price1 ||
        tick.bid_volume1 <= 0 || tick.ask_volume1 <= 0)
        return Signal::ignored;
    const auto mid = std::midpoint(tick.bid_price1, tick.ask_price1);
    if (tick.last_price > mid) return Signal::buy;
    if (tick.last_price < mid) return Signal::sell;
    return Signal::hold;
}

void receive(Queue& queue, std::chrono::milliseconds timeout, std::uint64_t delay_us) {
    auto state = replay::initial_checksum;
    std::uint64_t count = 0;
    std::array<std::uint64_t, 4> signals{};
    constexpr std::array labels{"buy", "sell", "hold", "ignored"};
    std::ostringstream samples;
    bool finished = false;
    while (!finished) {
        const auto deadline = Clock::now() + timeout;
        bool valid = false;
        Signal signal = Signal::ignored;
        // Only these display fields are copied; the strategy reads the SHM tick in place.
        std::array<char, 16> symbol{};
        double last = 0, bid = 0, ask = 0;
        std::int64_t source_sequence = 0;
        while (!queue.ring().try_read([&](const replay::Record& slot) noexcept {
            if (slot.replay_sequence != count) return;
            if (slot.kind == replay::Kind::end) {
                finished = true;
                valid = slot.checksum == state;
                return;
            }
            if (slot.kind != replay::Kind::tick) return;
            const auto next = replay::checksum(state, slot.tick);
            if (next != slot.checksum) return;
            state = next;
            signal = on_tick(slot.tick);
            if (count < 5) {
                symbol = slot.tick.symbol;
                last = slot.tick.last_price;
                bid = slot.tick.bid_price1;
                ask = slot.tick.ask_price1;
                source_sequence = slot.tick.sequence;
            }
            valid = true;
        })) {
            retry(deadline);
        }
        if (!valid) throw std::runtime_error("sequence/checksum/producer error at row " + std::to_string(count + 1));
        if (!finished) {
            ++signals[static_cast<std::size_t>(signal)];
            if (count < 5) {
                const auto end = std::find(symbol.begin(), symbol.end(), '\0');
                samples << "tick replay_sequence=" << count << " symbol="
                        << std::string_view(symbol.data(), static_cast<std::size_t>(end - symbol.begin()))
                        << " source_sequence=" << source_sequence << " last=" << last
                        << " bid=" << bid << " ask=" << ask
                        << " signal=" << labels[static_cast<std::size_t>(signal)] << '\n';
            }
            ++count;
            if (delay_us) std::this_thread::sleep_for(std::chrono::microseconds(delay_us));
        }
    }
    std::cout << samples.str() << "strategy rows=" << count << " checksum=" << state
              << " verified=true buy=" << signals[0] << " sell=" << signals[1]
              << " hold=" << signals[2] << " ignored=" << signals[3] << '\n';
}

int main(int argc, char** argv) {
    try {
        if (argc < 4 || argc > 6 || std::string_view(argv[1]) != "receive") {
            std::cerr << "Usage: strategy_consumer receive /name generation [stall_timeout_ms] [delay_us]\n";
            return 2;
        }
        const auto generation = number(argv[3]);
        const auto timeout = argc >= 5 ? timeout_arg(argv[4]) : std::chrono::milliseconds(10000);
        const auto delay = argc == 6 ? number(argv[5]) : 0;
        if (delay > 1000000) throw std::invalid_argument("delay_us must be <= 1000000");
        auto queue = ShmManager::open<replay::Record, replay::capacity>(argv[2], replay::schema, generation);
        receive(queue, timeout, delay);
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "strategy_consumer: " << error.what() << '\n';
        return 1;
    }
}
