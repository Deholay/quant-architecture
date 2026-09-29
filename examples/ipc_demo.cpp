#include "quant/ipc/shm_manager.hpp"

#include <chrono>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

// Transport-only sample, deliberately not a trading message or business module.
struct Sample {
    std::uint64_t sequence;
    std::uint64_t value;
};
constexpr std::size_t capacity = 1024;
constexpr std::uint64_t schema = 1;

int main(int argc, char** argv) {
    try {
        if (argc < 3) {
            std::cerr << "Usage: ipc_demo create|send|receive|remove /name [generation]\n"
                         "send and receive exchange 100000 messages; start within 20 seconds.\n";
            return 2;
        }
        const std::string command = argv[1];
        const std::string name = argv[2];
        using quant::ipc::ShmManager;
        if (command == "remove") {
            ShmManager::remove(name);
            return 0;
        }
        if (argc != 4) throw std::invalid_argument("generation required");
        const auto generation = std::stoull(argv[3]);
        if (command == "create") {
            auto queue = ShmManager::create<Sample, capacity>(name, schema, generation);
            std::cout << "Created " << name << '\n';
            return 0; // unmaps locally; name remains for the endpoints
        }
        if (command != "send" && command != "receive")
            throw std::invalid_argument("unknown command");
        auto queue = ShmManager::open<Sample, capacity>(name, schema, generation);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
        for (std::uint64_t i = 0; i < 100000; ++i) {
            Sample sample{i, i * 7};
            while (!(command == "send" ? queue.ring().try_push(sample)
                                        : queue.ring().try_pop(sample))) {
                if (std::chrono::steady_clock::now() > deadline)
                    throw std::runtime_error("peer stalled or missing");
                std::this_thread::yield();
            }
            if (command == "receive" && (sample.sequence != i || sample.value != i * 7))
                throw std::runtime_error("message integrity/order failure");
        }
        std::cout << command << ": 100000 messages complete\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
