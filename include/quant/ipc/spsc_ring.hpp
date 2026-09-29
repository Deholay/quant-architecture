#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

namespace quant::ipc {

// Platform contract: Linux, GCC/Clang, coherent shared mappings, native lock-free
// 64-bit atomics. This is intentionally not a portable ISO C++ IPC abstraction.
static_assert(__atomic_always_lock_free(sizeof(std::uint64_t), nullptr),
              "IPC requires native lock-free 64-bit atomics");
inline constexpr std::size_t cache_line_size = 64;

template <class T, std::size_t Capacity>
class SpscRing {
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

    // Exactly one producer thread may call this. No allocation or system calls.
    [[nodiscard]] bool try_push(const T& message) noexcept {
        const auto w = __atomic_load_n(&write_.value, __ATOMIC_RELAXED);
        const auto r = __atomic_load_n(&read_.value, __ATOMIC_ACQUIRE);
        if (w - r == Capacity) return false;
        slots_[w & (Capacity - 1)] = message;
        __atomic_store_n(&write_.value, w + 1, __ATOMIC_RELEASE);
        return true;
    }

    // Exactly one consumer thread may call this. Copies before releasing slot.
    [[nodiscard]] bool try_pop(T& output) noexcept {
        const auto r = __atomic_load_n(&read_.value, __ATOMIC_RELAXED);
        const auto w = __atomic_load_n(&write_.value, __ATOMIC_ACQUIRE);
        if (r == w) return false;
        output = slots_[r & (Capacity - 1)];
        __atomic_store_n(&read_.value, r + 1, __ATOMIC_RELEASE);
        return true;
    }
};

} // namespace quant::ipc
