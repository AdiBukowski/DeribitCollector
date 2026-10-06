#pragma once

// One Deribit WebSocket session. The ixwebsocket network thread only stamps
// the receive time and pushes the raw text into this connection's SPSC queue;
// all parsing happens on the writer thread. Each (re)connect bumps the epoch
// so the writer knows which messages belong to which session.

#include <array>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include <ixwebsocket/IXWebSocket.h>
#include <nlohmann/json.hpp>

#include "spsc_queue.hpp"
#include "util.hpp"

namespace collector {

struct RawMessage {
	int64_t recv_ns = 0;
	uint32_t epoch = 0;
	std::string text;
};

class Connection {
public:
	static constexpr int PING_SECONDS = 20;
	static constexpr int HEARTBEAT_SECONDS = 30;
	static constexpr uint32_t BACKOFF_MAX_MS = 30'000;

	Connection(uint32_t id, std::string group, std::string url, size_t queue_capacity, ix::SocketTLSOptions tls = {})
		: id_(id), group_(std::move(group)), url_(std::move(url)), tls_(std::move(tls)), queue_(queue_capacity) {}

	~Connection() { stop(); }
	Connection(const Connection&) = delete;
	Connection& operator=(const Connection&) = delete;

	void start() {
		ws_.setUrl(url_);
		ws_.setTLSOptions(tls_);
		ws_.setPingInterval(PING_SECONDS);
		ws_.enableAutomaticReconnection();
		ws_.setMaxWaitBetweenReconnectionRetries(BACKOFF_MAX_MS);
		ws_.setOnMessageCallback([this](const ix::WebSocketMessagePtr& m) { on_message(m); });
		ws_.start();
		started_ = true;
	}

	void stop() {
		if (!started_) return;
		ws_.stop();
		started_ = false;
		connected_ = false;
	}

	// Fresh session: every channel is resubscribed and snapshots are resent.
	void restart() {
		stop();
		start();
	}

	bool subscribe(const std::vector<std::string>& channels) {
		return rpc("public/subscribe", {{"channels", channels}}) >= 0;
	}
	bool unsubscribe(const std::vector<std::string>& channels) {
		return rpc("public/unsubscribe", {{"channels", channels}}) >= 0;
	}

	// Returns the request id, or -1 if the send failed.
	int64_t rpc(const std::string& method, nlohmann::json params) {
		const int64_t id = next_id_.fetch_add(1);
		const nlohmann::json req{{"jsonrpc", "2.0"}, {"id", id}, {"method", method}, {"params", std::move(params)}};
		return ws_.sendText(req.dump()).success ? id : -1;
	}

	SpscQueue<RawMessage>& queue() { return queue_; }
	uint32_t id() const { return id_; }
	const std::string& group() const { return group_; }
	bool connected() const { return connected_.load(std::memory_order_acquire); }
	bool take_needs_subscribe() { return needs_subscribe_.exchange(false); }
	bool take_overflow() { return overflow_.exchange(false); }
	uint64_t received() const { return received_.load(std::memory_order_relaxed); }
	uint64_t dropped() const { return dropped_.load(std::memory_order_relaxed); }
	size_t max_depth() const { return max_depth_.load(std::memory_order_relaxed); }
	uint32_t epoch() const { return epoch_.load(std::memory_order_relaxed); }

private:
	void on_message(const ix::WebSocketMessagePtr& m) {
		switch (m->type) {
			case ix::WebSocketMessageType::Open:
				epoch_.fetch_add(1, std::memory_order_relaxed);
				connected_.store(true, std::memory_order_release);
				rpc("public/set_heartbeat", {{"interval", HEARTBEAT_SECONDS}});
				needs_subscribe_ = true;
				std::fprintf(stderr, "[conn %u %s] connected (epoch %u)\n", id_, group_.c_str(), epoch());
				break;
			case ix::WebSocketMessageType::Close:
				connected_.store(false, std::memory_order_release);
				std::fprintf(stderr, "[conn %u %s] closed: %d %s\n", id_, group_.c_str(), m->closeInfo.code,
							 m->closeInfo.reason.c_str());
				break;
			case ix::WebSocketMessageType::Error:
				connected_.store(false, std::memory_order_release);
				std::fprintf(stderr, "[conn %u %s] error: %s (retry %u)\n", id_, group_.c_str(),
							 m->errorInfo.reason.c_str(), m->errorInfo.retries);
				break;
			case ix::WebSocketMessageType::Message: {
				const std::string& s = m->str;
				if (s.find("\"test_request\"") != std::string::npos) {
					rpc("public/test", nlohmann::json::object());
					return;
				}
				if (!queue_.push(RawMessage{wall_ns(), epoch(), s})) {
					dropped_.fetch_add(1, std::memory_order_relaxed);
					overflow_ = true;
					return;
				}
				received_.fetch_add(1, std::memory_order_relaxed);
				const size_t depth = queue_.size();
				if (depth > max_depth_.load(std::memory_order_relaxed))
					max_depth_.store(depth, std::memory_order_relaxed);
				break;
			}
			default:
				break;
		}
	}

	uint32_t id_;
	std::string group_;
	std::string url_;
	ix::SocketTLSOptions tls_;
	ix::WebSocket ws_;
	SpscQueue<RawMessage> queue_;
	bool started_ = false;
	std::atomic<int64_t> next_id_{1};
	std::atomic<uint32_t> epoch_{0};
	std::atomic<bool> connected_{false};
	std::atomic<bool> needs_subscribe_{false};
	std::atomic<bool> overflow_{false};
	std::atomic<uint64_t> received_{0};
	std::atomic<uint64_t> dropped_{0};
	std::atomic<size_t> max_depth_{0};
};

// Append-only table shared by the control thread (adds) and the writer (reads).
class ConnectionTable {
public:
	static constexpr size_t MAX_CONNECTIONS = 64;

	// Control thread only. Returns MAX_CONNECTIONS when full.
	size_t add(std::unique_ptr<Connection> c) {
		const size_t n = count_.load(std::memory_order_relaxed);
		if (n == MAX_CONNECTIONS) return MAX_CONNECTIONS;
		items_[n] = std::move(c);
		count_.store(n + 1, std::memory_order_release);
		return n;
	}

	size_t size() const { return count_.load(std::memory_order_acquire); }
	Connection& at(size_t i) { return *items_[i]; }

	void stop_all() {
		for (size_t i = 0; i < size(); ++i) items_[i]->stop();
	}

private:
	std::array<std::unique_ptr<Connection>, MAX_CONNECTIONS> items_;
	std::atomic<size_t> count_{0};
};

}  // namespace collector
