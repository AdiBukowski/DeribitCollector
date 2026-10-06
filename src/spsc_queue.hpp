#pragma once

// Bounded single-producer/single-consumer ring. One per WebSocket connection:
// the connection's network thread pushes, the writer thread pops.

#include <atomic>
#include <cstddef>
#include <stdexcept>
#include <vector>

namespace collector {

constexpr size_t CACHE_LINE = 64;

template <class T>
class SpscQueue {
public:
	explicit SpscQueue(size_t capacity) : buf_(capacity), mask_(capacity - 1) {
		if (capacity < 2 || (capacity & (capacity - 1)) != 0)
			throw std::invalid_argument("SpscQueue capacity must be a power of two");
	}

	SpscQueue(const SpscQueue&) = delete;
	SpscQueue& operator=(const SpscQueue&) = delete;

	// Producer only. Returns false when full; the item is left untouched.
	bool push(T&& v) {
		const size_t head = head_.load(std::memory_order_relaxed);
		if (head - tail_.load(std::memory_order_acquire) == buf_.size()) return false;
		buf_[head & mask_] = std::move(v);
		head_.store(head + 1, std::memory_order_release);
		return true;
	}

	// Consumer only.
	bool pop(T& out) {
		const size_t tail = tail_.load(std::memory_order_relaxed);
		if (tail == head_.load(std::memory_order_acquire)) return false;
		out = std::move(buf_[tail & mask_]);
		tail_.store(tail + 1, std::memory_order_release);
		return true;
	}

	size_t size() const {
		return head_.load(std::memory_order_acquire) - tail_.load(std::memory_order_acquire);
	}
	size_t capacity() const { return buf_.size(); }

private:
	std::vector<T> buf_;
	size_t mask_;
	alignas(CACHE_LINE) std::atomic<size_t> head_{0};
	alignas(CACHE_LINE) std::atomic<size_t> tail_{0};
};

}  // namespace collector
