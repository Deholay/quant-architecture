#include "../examples/tick_replay_support.hpp"

#include <iostream>
#include <sstream>

void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

int main() {
    try {
        replay::Histogram empty;
        require(empty.count() == 0 && empty.percentile(99) == 0, "empty histogram");
        std::ostringstream output;
        empty.report(output, "empty");
        require(output.str().find("n/a") != std::string::npos, "empty latency must be n/a");
        // Exercise all exponent transitions, including saturation at UINT64_MAX.
        for (unsigned exponent = 0; exponent < 64; ++exponent) {
            const auto n = std::uint64_t{1} << exponent;
            for (auto value : {n - 1, n, n + (n - 1)}) {
                replay::Histogram histogram;
                histogram.add(value);
                histogram.add(UINT64_MAX);
                const auto upper = histogram.percentile(50);
                require(upper >= value, "percentile must not underestimate sample");
                require(upper - value <= value / 16, "histogram bucket width exceeds 6.25%");
                require(histogram.maximum() == UINT64_MAX, "maximum must be exact");
                require(histogram.percentile(100) == UINT64_MAX, "top bucket must not overflow");
            }
        }
        replay::Histogram known;
        for (std::uint64_t n = 1; n <= 100; ++n) known.add(n);
        require(known.percentile(50) == 51, "p50 nearest-rank bucket upper");
        require(known.percentile(95) == 95, "p95 nearest-rank bucket upper");
        require(known.percentile(99) == 99, "p99 nearest-rank bucket upper");
        replay::Tick a{};
        a.symbol[0] = 'A';
        a.last_price = 35.55;
        const auto original = replay::checksum(replay::initial_checksum, a);
        a.bid_volume5 = 123;
        require(original != replay::checksum(replay::initial_checksum, a), "checksum includes depth fields");
        require(replay::monotonic_ns() != 0, "monotonic clock unavailable");
        std::cout << "Replay histogram and checksum tests passed\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
