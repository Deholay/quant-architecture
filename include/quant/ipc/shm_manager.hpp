#pragma once

#include "quant/ipc/shm_region.hpp"
#include "quant/ipc/spsc_ring.hpp"

#include <new>
#include <stdexcept>
#include <utility>

namespace quant::ipc {
namespace detail {
inline constexpr std::uint64_t magic = 0x5149504353505343ULL;
inline constexpr std::uint64_t abi_version = 1;

struct alignas(cache_line_size) Header {
    std::uint64_t magic;
    std::uint64_t abi;
    std::uint64_t schema;
    std::uint64_t generation;
    std::uint64_t message_size;
    std::uint64_t message_alignment;
    std::uint64_t capacity;
    std::uint64_t region_size;
};
static_assert(sizeof(Header) == cache_line_size);

template <class T, std::size_t N>
struct Layout {
    Header header{};
    SpscRing<T, N> ring{};
};
} // namespace detail

class ShmManager;

template <class T, std::size_t N>
class ShmQueue {
public:
    ShmQueue(ShmQueue&&) noexcept = default;
    ShmQueue& operator=(ShmQueue&&) noexcept = default;
    ShmQueue(const ShmQueue&) = delete;
    ShmQueue& operator=(const ShmQueue&) = delete;

    // References are valid only while this mapping remains alive and unmoved.
    [[nodiscard]] SpscRing<T, N>& ring() {
        if (!region_.data()) throw std::logic_error("moved-from SHM queue");
        return static_cast<detail::Layout<T, N>*>(region_.data())->ring;
    }

private:
    friend class ShmManager;
    explicit ShmQueue(ShmRegion region) noexcept : region_(std::move(region)) {}
    ShmRegion region_;
};

// Stateless lifecycle utility, not a daemon and not on the message hot path.
class ShmManager {
public:
    template <class T, std::size_t N>
    static ShmQueue<T, N> create(const std::string& name, std::uint64_t schema,
                                 std::uint64_t generation) {
        check_ids(schema, generation);
        using Layout = detail::Layout<T, N>;
        static_assert(std::is_standard_layout_v<Layout>);
        static_assert(offsetof(Layout, ring) == sizeof(detail::Header));
        return ShmQueue<T, N>(ShmRegion::create(name, sizeof(Layout), [=](void* p) {
            auto* layout = ::new (p) Layout{};
            layout->header = {detail::magic, detail::abi_version, schema, generation,
                              sizeof(T), alignof(T), N, sizeof(Layout)};
        }));
    }

    template <class T, std::size_t N>
    static ShmQueue<T, N> open(const std::string& name, std::uint64_t schema,
                               std::uint64_t generation) {
        check_ids(schema, generation);
        using Layout = detail::Layout<T, N>;
        return ShmQueue<T, N>(ShmRegion::open(name, sizeof(Layout), [=](void* p) {
            // Validate fixed header before treating the mapping as a typed ring.
            detail::Header h{};
            std::memcpy(&h, p, sizeof(h));
            if (h.magic != detail::magic || h.abi != detail::abi_version ||
                h.schema != schema || h.generation != generation ||
                h.message_size != sizeof(T) || h.message_alignment != alignof(T) ||
                h.capacity != N || h.region_size != sizeof(Layout))
                throw std::runtime_error("SHM header/schema/generation mismatch");
        }));
    }

    // Call only as lifecycle owner after coordinated shutdown, not on restart
    // of a single participant. This removes the name, not outstanding mappings.
    static void remove(const std::string& name) { ShmRegion::unlink(name); }

private:
    static void check_ids(std::uint64_t schema, std::uint64_t generation) {
        if (schema == 0 || generation == 0)
            throw std::invalid_argument("schema and generation must be nonzero");
    }
};
} // namespace quant::ipc
