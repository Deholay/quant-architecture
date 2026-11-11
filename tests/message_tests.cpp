#include "quant/ipc/shm_manager.hpp"
#include "quant/msg/messages.hpp"

#include <iostream>
#include <stdexcept>
#include <string>

#include <unistd.h>

using namespace quant::ipc;
using namespace quant::msg;

void require(bool ok, const char* message) {
    if (!ok) throw std::runtime_error(message);
}

struct Name {
    std::string value;
    explicit Name(const char* suffix)
        : value("/qm." + std::to_string(::getpid()) + "." + suffix) {}
    ~Name() { try { ShmManager::remove(value); } catch (...) {} }
};

MarketTick sample_tick() {
    MarketTick t{};
    t.sequence = 42;
    t.exchange_ns = 1'700'000'000'123'456'789;
    t.receive_local_ns = 987'654'321;
    t.symbol_id = 2330;
    t.flags = tick_flag::trade | tick_flag::snapshot;
    t.last_price = 355'500;
    t.last_quantity = 3;
    t.total_volume = 1'000'000;
    for (std::size_t i = 0; i < book_depth; ++i) {
        t.bids[i] = {355'000 - static_cast<Price>(i) * 500, 10 + static_cast<Quantity>(i)};
        t.asks[i] = {355'500 + static_cast<Price>(i) * 500, 20 + static_cast<Quantity>(i)};
    }
    return t;
}

OrderRequest sample_order() {
    OrderRequest o{};
    o.sequence = 7;
    o.client_order_id = 1001;
    o.target_order_id = 1000;
    o.create_local_ns = 123;
    o.symbol_id = 2330;
    o.action = OrderAction::replace;
    o.side = Side::sell;
    o.type = OrderType::limit;
    o.time_in_force = TimeInForce::ioc;
    o.price = 356'000;
    o.quantity = 5;
    return o;
}

ExecutionReport sample_report() {
    ExecutionReport r{};
    r.sequence = 9;
    r.client_order_id = 1001;
    r.target_order_id = 1000;
    r.venue_order_id = 0xABCDEF;
    r.transact_time_ns = 1'700'000'000'000'000'001;
    r.report_local_ns = 456;
    r.symbol_id = 2330;
    r.exec_type = ExecType::partial_fill;
    r.side = Side::sell;
    r.reject_reason = RejectReason::none;
    r.last_price = 356'000;
    r.last_quantity = 2;
    r.cumulative_quantity = 2;
    r.leaves_quantity = 3;
    return r;
}

bool same(const MarketTick& a, const MarketTick& b) {
    for (std::size_t i = 0; i < book_depth; ++i)
        if (a.bids[i].price != b.bids[i].price || a.bids[i].quantity != b.bids[i].quantity ||
            a.asks[i].price != b.asks[i].price || a.asks[i].quantity != b.asks[i].quantity)
            return false;
    return a.sequence == b.sequence && a.exchange_ns == b.exchange_ns &&
           a.receive_local_ns == b.receive_local_ns && a.symbol_id == b.symbol_id &&
           a.flags == b.flags && a.last_price == b.last_price &&
           a.last_quantity == b.last_quantity && a.total_volume == b.total_volume;
}

bool same(const OrderRequest& a, const OrderRequest& b) {
    return a.sequence == b.sequence && a.client_order_id == b.client_order_id &&
           a.target_order_id == b.target_order_id && a.create_local_ns == b.create_local_ns &&
           a.symbol_id == b.symbol_id && a.action == b.action && a.side == b.side &&
           a.type == b.type && a.time_in_force == b.time_in_force && a.price == b.price &&
           a.quantity == b.quantity;
}

bool same(const ExecutionReport& a, const ExecutionReport& b) {
    return a.sequence == b.sequence && a.client_order_id == b.client_order_id &&
           a.target_order_id == b.target_order_id && a.venue_order_id == b.venue_order_id &&
           a.transact_time_ns == b.transact_time_ns && a.report_local_ns == b.report_local_ns &&
           a.symbol_id == b.symbol_id && a.exec_type == b.exec_type && a.side == b.side &&
           a.reject_reason == b.reject_reason && a.last_price == b.last_price &&
           a.last_quantity == b.last_quantity &&
           a.cumulative_quantity == b.cumulative_quantity &&
           a.leaves_quantity == b.leaves_quantity;
}

// Producer writes through the creator mapping; consumer reads through an
// independently opened mapping that validated the schema ID.
template <class T, std::size_t N>
void roundtrip(const char* suffix, std::uint64_t schema, const T& sample) {
    Name name(suffix);
    auto producer = ShmManager::create<T, N>(name.value, schema, 1);
    auto consumer = ShmManager::open<T, N>(name.value, schema, 1);
    require(producer.ring().try_push(sample), "push message");
    T output{};
    require(consumer.ring().try_pop(output), "pop message");
    require(same(output, sample), "message field integrity");
    bool rejected = false;
    try {
        auto wrong = ShmManager::open<T, N>(name.value, schema + 1, 1);
    } catch (const std::exception&) {
        rejected = true;
    }
    require(rejected, "schema mismatch must be rejected");
}

int main() {
    try {
        static_assert(market_tick_schema != order_request_schema &&
                      order_request_schema != execution_report_schema &&
                      market_tick_schema != execution_report_schema);
        roundtrip<MarketTick, 1024>("feed", market_tick_schema, sample_tick());
        roundtrip<OrderRequest, 256>("orders", order_request_schema, sample_order());
        roundtrip<ExecutionReport, 256>("executions", execution_report_schema, sample_report());
        std::cout << "Message layout and SHM roundtrip tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
