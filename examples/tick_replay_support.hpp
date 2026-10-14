#pragma once

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <cstring>
#include <limits>
#include <ostream>
#include <stdexcept>
#include <time.h>
#include <type_traits>

namespace replay {
inline constexpr std::uint64_t schema = 0x52504C4159000001ULL;
inline constexpr std::size_t capacity = 1024;
inline constexpr std::array<char, 8> magic{'Q', 'T', 'I', 'C', 'K', '0', '0', '1'};
static_assert(std::endian::native == std::endian::little,
              "Replay file v1 uses little-endian fields");
static_assert(sizeof(double) == 8 && std::numeric_limits<double>::is_iec559);

// Replay-only schema: preserve every source field and the original file order.
struct Tick {
    std::array<char, 16> symbol{};
#define TICK_INT(name) std::int64_t name{};
#define TICK_DOUBLE(name) double name{};
#include "replay_fields.def"
#undef TICK_INT
#undef TICK_DOUBLE
};
static_assert(std::is_trivially_copyable_v<Tick> && std::is_standard_layout_v<Tick>);
static_assert(sizeof(Tick) == 312);

struct FileHeader {
    std::array<char, 8> magic;
    std::uint64_t schema;
    std::uint64_t record_bytes;
    std::uint64_t count;
};
static_assert(sizeof(FileHeader) == 32);

enum class Kind : std::uint64_t { tick, end, error };
struct Record {
    Tick tick{};
    std::uint64_t replay_sequence{};
    std::uint64_t enqueue_ns{};
    std::uint64_t checksum{};
    Kind kind{};
};

inline constexpr std::uint64_t initial_checksum = 14695981039346656037ULL;
// Non-cryptographic rolling checksum over all field bytes, excluding timing metadata.
inline std::uint64_t checksum(std::uint64_t state, const Tick& tick) noexcept {
    const auto* bytes = reinterpret_cast<const unsigned char*>(&tick);
    for (std::size_t i = 0; i < sizeof(Tick); i += sizeof(std::uint64_t)) {
        std::uint64_t word;
        std::memcpy(&word, bytes + i, sizeof(word));
        state = (state ^ word) * 1099511628211ULL;
    }
    return state;
}

// A clock failure is reported after leaving the noexcept ring callback.
inline std::uint64_t monotonic_ns() noexcept {
    timespec now{};
    if (::clock_gettime(CLOCK_MONOTONIC, &now) != 0) return 0;
    return static_cast<std::uint64_t>(now.tv_sec) * 1000000000ULL + now.tv_nsec;
}

// Fixed memory: 16 sub-buckets per power of two; exact below 16 ns.
// Percentiles are bucket upper bounds (at most ~6.25% bucket width).
class Histogram {
    std::array<std::uint64_t, 64 * 16> bins_{};
    std::uint64_t count_ = 0;
    std::uint64_t max_ = 0;
    long double sum_ = 0;

    static std::size_t index(std::uint64_t n) noexcept {
        if (n < 16) return n;
        const auto exponent = std::bit_width(n) - 1;
        const auto shift = exponent - 4;
        return exponent * 16 + ((n >> shift) - 16);
    }
    static std::uint64_t upper(std::size_t i) noexcept {
        if (i < 16) return i;
        const auto exponent = i / 16;
        const auto sub = i % 16;
        const auto shift = exponent - 4;
        if (exponent == 63 && sub == 15) return UINT64_MAX;
        return ((17ULL + sub) << shift) - 1;
    }

public:
    void add(std::uint64_t ns) noexcept {
        ++bins_[index(ns)];
        ++count_;
        max_ = std::max(max_, ns);
        sum_ += ns;
    }
    std::uint64_t count() const noexcept { return count_; }
    std::uint64_t maximum() const noexcept { return max_; }
    std::uint64_t percentile(unsigned percent) const {
        if (percent == 0 || percent > 100) throw std::invalid_argument("percentile range");
        if (!count_) return 0;
        const auto target = (count_ / 100) * percent + ((count_ % 100) * percent + 99) / 100;
        std::uint64_t seen = 0;
        for (std::size_t i = 0; i < bins_.size(); ++i) {
            seen += bins_[i];
            if (seen >= target) return std::min(max_, upper(i));
        }
        return max_;
    }
    void report(std::ostream& out, const char* label) const {
        out << label << " samples=" << count_;
        if (!count_) { out << " latency_ns=n/a\n"; return; }
        out << " mean_ns=" << static_cast<double>(sum_ / count_)
            << " p50_upper_ns=" << percentile(50)
            << " p95_upper_ns=" << percentile(95)
            << " p99_upper_ns=" << percentile(99)
            << " max_ns=" << max_ << '\n';
    }
};
} // namespace replay
