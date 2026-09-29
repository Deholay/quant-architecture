#include "quant/ipc/shm_region.hpp"

#include <cerrno>
#include <limits>
#include <stdexcept>
#include <system_error>
#include <utility>

#include <fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace quant::ipc {
namespace {
void validate_name(const std::string& name) {
    if (name.size() < 2 || name.size() > 255 || name.front() != '/' ||
        name.find('/', 1) != std::string::npos ||
        name.find('\0') != std::string::npos)
        throw std::invalid_argument("SHM name must be /name, at most 255 bytes");
}

void validate_size(std::size_t bytes) {
    if (bytes == 0 || bytes > static_cast<std::size_t>(std::numeric_limits<off_t>::max()))
        throw std::invalid_argument("invalid SHM size");
}

[[noreturn]] void fail(const char* operation) {
    throw std::system_error(errno, std::generic_category(), operation);
}

struct Fd {
    int value;
    ~Fd() {
        if (value >= 0) {
            // mmap can retain the open file description after close(fd).
            // Release the startup lock explicitly before closing the descriptor.
            while (::flock(value, LOCK_UN) != 0 && errno == EINTR) {}
            ::close(value);
        }
    }
};

void lock(int fd, int operation) {
    while (::flock(fd, operation) != 0) {
        if (errno != EINTR) fail("flock");
    }
}
} // namespace

ShmRegion ShmRegion::create(const std::string& name, std::size_t bytes,
                            const MappingAction& initialize) {
    validate_name(name);
    validate_size(bytes);
    Fd fd{::shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600)};
    if (fd.value < 0) fail("shm_open(create)");
    try {
        lock(fd.value, LOCK_EX);
        if (::ftruncate(fd.value, static_cast<off_t>(bytes)) != 0) fail("ftruncate");
        void* address = ::mmap(nullptr, bytes, PROT_READ | PROT_WRITE,
                               MAP_SHARED, fd.value, 0);
        if (address == MAP_FAILED) fail("mmap(create)");
        ShmRegion region(address, bytes);
        initialize(address);
        return region; // fd guard explicitly releases initialization lock
    } catch (...) {
        ::shm_unlink(name.c_str()); // only this successful O_EXCL creator cleans up
        throw;
    }
}

ShmRegion ShmRegion::open(const std::string& name, std::size_t expected_bytes,
                          const MappingAction& validate) {
    validate_name(name);
    validate_size(expected_bytes);
    Fd fd{::shm_open(name.c_str(), O_RDWR | O_CLOEXEC, 0)};
    if (fd.value < 0) fail("shm_open(open)");
    // Startup only; fail promptly if creation is still in progress. Opening in
    // the tiny gap before creator flock is safe: size/header validation fails.
    lock(fd.value, LOCK_SH | LOCK_NB);
    struct stat info {};
    if (::fstat(fd.value, &info) != 0) fail("fstat");
    if (info.st_size != static_cast<off_t>(expected_bytes))
        throw std::runtime_error("SHM size mismatch or initialization incomplete");
    void* address = ::mmap(nullptr, expected_bytes, PROT_READ | PROT_WRITE,
                           MAP_SHARED, fd.value, 0);
    if (address == MAP_FAILED) fail("mmap(open)");
    ShmRegion region(address, expected_bytes);
    validate(address);
    return region;
}

void ShmRegion::unlink(const std::string& name) {
    validate_name(name);
    if (::shm_unlink(name.c_str()) != 0 && errno != ENOENT) fail("shm_unlink");
}

ShmRegion::~ShmRegion() { reset(); }
ShmRegion::ShmRegion(ShmRegion&& other) noexcept
    : address_(std::exchange(other.address_, nullptr)),
      bytes_(std::exchange(other.bytes_, 0)) {}

ShmRegion& ShmRegion::operator=(ShmRegion&& other) noexcept {
    if (this != &other) {
        reset();
        address_ = std::exchange(other.address_, nullptr);
        bytes_ = std::exchange(other.bytes_, 0);
    }
    return *this;
}

void ShmRegion::reset() noexcept {
    if (address_ != nullptr) ::munmap(address_, bytes_);
    address_ = nullptr;
    bytes_ = 0;
}
} // namespace quant::ipc
