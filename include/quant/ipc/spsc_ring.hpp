#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <type_traits>
#include <utility>

namespace quant::ipc {

// Platform contract: Linux, GCC/Clang, coherent shared mappings, native lock-free
// 64-bit atomics. This is intentionally not a portable ISO C++ IPC abstraction.
inline constexpr std::size_t cache_line_size = 64;

template <class T, std::size_t Capacity>
class SpscRing {
    using AtomicCursorRef = std::atomic_ref<std::uint64_t>;

    static_assert(AtomicCursorRef::is_always_lock_free,
                  "IPC requires native lock-free 64-bit atomics");
    static_assert(Capacity >= 2 && (Capacity & (Capacity - 1)) == 0,
                  "Capacity must be a power of two >= 2");
    static_assert(Capacity < (std::uint64_t{1} << 63));
    static_assert(std::is_trivially_copyable_v<T> && std::is_standard_layout_v<T>);
    static_assert(std::is_trivially_copy_assignable_v<T>);
    static_assert(std::is_nothrow_default_constructible_v<T>);
    static_assert(alignof(T) <= cache_line_size);

    struct alignas(cache_line_size) Cursor {
        std::uint64_t value = 0;
    };
    static_assert(sizeof(Cursor) == cache_line_size);
    static_assert(alignof(Cursor) >= AtomicCursorRef::required_alignment);
    static_assert(offsetof(Cursor, value) % AtomicCursorRef::required_alignment == 0);

    Cursor write_;
    Cursor read_;
    alignas(cache_line_size) T slots_[Capacity]{};

public:
    using value_type = T;
    static constexpr std::size_t capacity = Capacity;

    // Construct once, in the creator, before making the region available.
    // initial_sequence is useful for testing counter rollover; both cursors
    // always start equal, so the queue starts empty.
    explicit SpscRing(std::uint64_t initial_sequence = 0) noexcept {
        write_.value = initial_sequence;
        read_.value = initial_sequence;
    }
    SpscRing(const SpscRing&) = delete;
    SpscRing& operator=(const SpscRing&) = delete;

    // Exactly one producer thread may call this. The callback receives the
    // unpublished slot and must not retain its reference or re-enter this ring.
    // The slot is published only after the noexcept callback returns.
    template <class Fill>
        requires std::is_nothrow_invocable_v<Fill&&, T&>
    [[nodiscard]] bool try_write(Fill&& fill) noexcept {
        AtomicCursorRef write_cursor(write_.value);
        AtomicCursorRef read_cursor(read_.value);
        const auto w = write_cursor.load(std::memory_order_relaxed);
        const auto r = read_cursor.load(std::memory_order_acquire);
        if (w - r == Capacity) return false;
        std::invoke(std::forward<Fill>(fill), slots_[w & (Capacity - 1)]);
        write_cursor.store(w + 1, std::memory_order_release);
        return true;
    }

    // Exactly one consumer thread may call this. The callback receives the
    // published slot and must not retain its reference or re-enter this ring.
    // The slot remains owned by the consumer until the callback returns.
    template <class Consume>
        requires std::is_nothrow_invocable_v<Consume&&, const T&>
    [[nodiscard]] bool try_read(Consume&& consume) noexcept {
        AtomicCursorRef read_cursor(read_.value);
        AtomicCursorRef write_cursor(write_.value);
        const auto r = read_cursor.load(std::memory_order_relaxed);
        const auto w = write_cursor.load(std::memory_order_acquire);
        if (r == w) return false;
        std::invoke(std::forward<Consume>(consume),
                    static_cast<const T&>(slots_[r & (Capacity - 1)]));
        read_cursor.store(r + 1, std::memory_order_release);
        return true;
    }

    // Copying compatibility APIs. No allocation or system calls.
    [[nodiscard]] bool try_push(const T& message) noexcept {
        return try_write([&message](T& slot) noexcept { slot = message; });
    }

    [[nodiscard]] bool try_pop(T& output) noexcept {
        return try_read([&output](const T& slot) noexcept { output = slot; });
    }
};

} // namespace quant::ipc
