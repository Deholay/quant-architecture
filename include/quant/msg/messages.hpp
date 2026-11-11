#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <type_traits>

// Fixed-layout business messages carried by the three SHM channels:
//   Feed Queue             MD  -> ST   MarketTick
//   Order Queue            ST  -> OMS  OrderRequest
//   Execution Report Queue OMS -> ST   ExecutionReport
// No process-local pointers or dynamic containers. Changing a field's layout or
// meaning requires bumping that message's schema ID below.
namespace quant::msg {

inline constexpr std::size_t message_alignment = 64;

// Fixed-point price: real price = Price / price_scale (e.g. 35.55 -> 355500).
using Price = std::int64_t;
inline constexpr Price price_scale = 10'000;
using Quantity = std::int64_t;
using SymbolId = std::uint32_t;
using OrderId = std::uint64_t; // client order ID, assigned by ST, unique per generation

// Timestamps:
//   *_exchange_ns / transact_time_ns  venue time, UTC nanoseconds since epoch
//   *_local_ns                        host CLOCK_MONOTONIC nanoseconds (shared by
//                                     all three processes on the same host)

enum class Side : std::uint8_t { buy = 1, sell = 2 };
enum class OrderType : std::uint8_t { limit = 1, market = 2 };
enum class TimeInForce : std::uint8_t { day = 1, ioc = 2, fok = 3 };
enum class OrderAction : std::uint8_t { new_order = 1, cancel = 2, replace = 3 };

enum class ExecType : std::uint8_t {
    accepted = 1,        // order acknowledged by venue
    rejected = 2,        // new order rejected (see RejectReason)
    partial_fill = 3,
    fill = 4,            // order fully filled, leaves_quantity == 0
    canceled = 5,
    replaced = 6,
    cancel_rejected = 7, // cancel/replace rejected; original order still live
    expired = 8,         // IOC/FOK/day remainder removed by venue
};

enum class RejectReason : std::uint8_t {
    none = 0,
    invalid_order = 1, // malformed fields
    risk_limit = 2,    // blocked by OMS pre-trade checks
    unknown_order = 3, // cancel/replace target not found
    venue = 4,         // rejected by exchange/broker
};

// Bit flags for MarketTick::flags.
namespace tick_flag {
inline constexpr std::uint32_t trade = 1u << 0;    // last_price/last_quantity: new trade
inline constexpr std::uint32_t snapshot = 1u << 1; // book is a full snapshot
inline constexpr std::uint32_t halted = 1u << 2;   // instrument trading halted
} // namespace tick_flag

inline constexpr std::size_t book_depth = 5;

struct PriceLevel {
    Price price{};
    Quantity quantity{}; // 0 means empty level
};

struct alignas(message_alignment) MarketTick {
    std::uint64_t sequence{};     // MD feed sequence, strictly increasing per generation
    std::int64_t exchange_ns{};
    std::int64_t receive_local_ns{};
    SymbolId symbol_id{};
    std::uint32_t flags{};        // bitwise OR of tick_flag values
    Price last_price{};
    Quantity last_quantity{};
    Quantity total_volume{};
    std::array<PriceLevel, book_depth> bids{}; // best first, descending price
    std::array<PriceLevel, book_depth> asks{}; // best first, ascending price
};

struct alignas(message_alignment) OrderRequest {
    std::uint64_t sequence{};      // ST order-queue sequence, strictly increasing
    OrderId client_order_id{};     // new ID for every request, including cancel/replace
    OrderId target_order_id{};     // cancel/replace: client_order_id of live order; else 0
    std::int64_t create_local_ns{};
    SymbolId symbol_id{};
    OrderAction action{};
    Side side{};
    OrderType type{};
    TimeInForce time_in_force{};
    Price price{};                 // ignored for market orders and cancel
    Quantity quantity{};           // replace: new total quantity; ignored for cancel
};

struct alignas(message_alignment) ExecutionReport {
    std::uint64_t sequence{};       // OMS report sequence, strictly increasing
    OrderId client_order_id{};      // request this report answers
    OrderId target_order_id{};      // cancel/replace: order affected; else 0
    std::uint64_t venue_order_id{}; // venue-assigned ID, 0 if not yet known
    std::int64_t transact_time_ns{};
    std::int64_t report_local_ns{};
    SymbolId symbol_id{};
    ExecType exec_type{};
    Side side{};
    RejectReason reject_reason{};
    Price last_price{};             // fill price; 0 unless partial_fill/fill
    Quantity last_quantity{};       // fill quantity; 0 unless partial_fill/fill
    Quantity cumulative_quantity{};
    Quantity leaves_quantity{};
};

// Application schema IDs passed to ShmManager::create/open. ASCII tag + version.
inline constexpr std::uint64_t market_tick_schema = 0x4D4B544943'000001ULL;      // "MKTIC" v1
inline constexpr std::uint64_t order_request_schema = 0x4F52445251'000001ULL;    // "ORDRQ" v1
inline constexpr std::uint64_t execution_report_schema = 0x4558455250'000001ULL; // "EXERP" v1

template <class T>
inline constexpr bool is_shm_message_v =
    std::is_trivially_copyable_v<T> && std::is_standard_layout_v<T> &&
    std::is_nothrow_default_constructible_v<T> && alignof(T) == message_alignment &&
    sizeof(T) % message_alignment == 0;

static_assert(is_shm_message_v<MarketTick>);
static_assert(is_shm_message_v<OrderRequest>);
static_assert(is_shm_message_v<ExecutionReport>);
static_assert(sizeof(PriceLevel) == 16);
static_assert(sizeof(MarketTick) == 256);
static_assert(sizeof(OrderRequest) == 64);
static_assert(sizeof(ExecutionReport) == 128);
static_assert(offsetof(MarketTick, bids) == 56 && offsetof(MarketTick, asks) == 136);
static_assert(offsetof(OrderRequest, price) == 40 && offsetof(OrderRequest, quantity) == 48);
static_assert(offsetof(ExecutionReport, last_price) == 56 &&
              offsetof(ExecutionReport, leaves_quantity) == 80);

} // namespace quant::msg
