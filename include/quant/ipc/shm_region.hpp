#pragma once

#include <cstddef>
#include <functional>
#include <string>

namespace quant::ipc {

// Move-only local mapping. Destruction unmaps; it NEVER unlinks the name.
class ShmRegion {
public:
    using MappingAction = std::function<void(void*)>;

    static ShmRegion create(const std::string& name, std::size_t bytes,
                            const MappingAction& initialize);
    static ShmRegion open(const std::string& name, std::size_t expected_bytes,
                          const MappingAction& validate);
    // Explicit lifecycle-owner operation. Existing mappings survive unlink.
    static void unlink(const std::string& name);

    ~ShmRegion();
    ShmRegion(ShmRegion&& other) noexcept;
    ShmRegion& operator=(ShmRegion&& other) noexcept;
    ShmRegion(const ShmRegion&) = delete;
    ShmRegion& operator=(const ShmRegion&) = delete;

    [[nodiscard]] void* data() const noexcept { return address_; }
    [[nodiscard]] std::size_t size() const noexcept { return bytes_; }

private:
    ShmRegion(void* address, std::size_t bytes) noexcept
        : address_(address), bytes_(bytes) {}
    void reset() noexcept;
    void* address_ = nullptr;
    std::size_t bytes_ = 0;
};

} // namespace quant::ipc
