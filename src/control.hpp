#pragma once

// Control thread: discovers instruments (REST get_instruments, refreshed
// periodically and immediately on instrument.state notifications), assigns
// them to connections per group, (re)subscribes after every connect, resyncs
// books after sequence gaps and restarts connections whose queue overflowed.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <ixwebsocket/IXHttpClient.h>
#include <ixwebsocket/IXWebSocket.h>
#include <nlohmann/json.hpp>

#include "connection.hpp"
#include "instrument.hpp"
#include "util.hpp"
#include "writer.hpp"

namespace collector {

struct ControlConfig {
	std::string host = "www.deribit.com";
	std::vector<std::string> bases{"BTC", "ETH", "SOL"};
	std::vector<std::string> currencies{"BTC", "ETH", "USDC"};  // get_instruments currencies
	std::vector<std::string> kinds{"option", "future"};
	std::string interval = "100ms";
	bool trades = true;
	size_t instruments_per_conn = 400;
	size_t queue_capacity = 1 << 13;  // ~100 s of backlog per connection, bounded RAM on small boards
	int refresh_seconds = 600;
	ix::SocketTLSOptions tls;
};

class Control {
public:
	static constexpr size_t SUBSCRIBE_BATCH = 100;
	static constexpr int SUBSCRIBE_PAUSE_MS = 50;
	static constexpr int TICK_MS = 100;
	static constexpr int MIN_REFRESH_GAP_S = 5;
	static constexpr int RETRY_REFRESH_S = 30;
	static constexpr int HTTP_TIMEOUT_S = 30;

	Control(ControlConfig cfg, ConnectionTable& conns, MutexQueue<WriterCommand>& commands,
			MutexQueue<std::string>& resync)
		: cfg_(std::move(cfg)), conns_(conns), commands_(commands), resync_(resync) {}

	void run(const std::atomic<bool>& running) {
		start_state_listener();
		if (cfg_.trades) start_trades_connection();

		auto next_refresh = std::chrono::steady_clock::now();
		auto last_refresh = std::chrono::steady_clock::now() - std::chrono::hours(1);
		while (running.load(std::memory_order_acquire)) {
			const auto now = std::chrono::steady_clock::now();
			const bool triggered = state_changed_.exchange(false);
			if (now >= next_refresh || (triggered && now - last_refresh >= std::chrono::seconds(MIN_REFRESH_GAP_S))) {
				const bool ok = refresh();
				last_refresh = now;
				next_refresh = now + std::chrono::seconds(ok ? cfg_.refresh_seconds : RETRY_REFRESH_S);
			} else if (triggered) {
				state_changed_ = true;  // keep it pending until the gap has passed
			}
			service_connections();
			resync_books();
			std::this_thread::sleep_for(std::chrono::milliseconds(TICK_MS));
		}
		state_ws_.stop();
		conns_.stop_all();
	}

private:
	struct Assigned {
		InstrumentInfo info;
		size_t conn = 0;
	};

	std::string url(const std::string& path) const { return "https://" + cfg_.host + path; }
	std::string ws_url() const { return "wss://" + cfg_.host + "/ws/api/v2"; }

	// --- instrument discovery ------------------------------------------------

	bool refresh() {
		std::map<std::string, InstrumentInfo> live;
		for (const auto& currency : cfg_.currencies) {
			for (const auto& kind : cfg_.kinds) {
				if (!fetch(currency, kind, live)) return false;
			}
		}
		std::vector<std::string> removed;
		for (const auto& [name, a] : assigned_)
			if (!live.contains(name)) removed.push_back(name);
		size_t added = 0;
		for (auto& [name, info] : live) {
			if (assigned_.contains(name)) continue;
			add_instrument(std::move(info));
			++added;
		}
		for (const auto& name : removed) remove_instrument(name, "delisted");
		if (added || !removed.empty())
			std::fprintf(stderr, "[control] instruments: +%zu -%zu, total %zu on %zu connections\n", added,
						 removed.size(), assigned_.size(), conns_.size());
		return true;
	}

	bool fetch(const std::string& currency, const std::string& kind, std::map<std::string, InstrumentInfo>& out) {
		ix::HttpClient http;
		http.setTLSOptions(cfg_.tls);
		auto args = http.createRequest();
		args->connectTimeout = HTTP_TIMEOUT_S;
		args->transferTimeout = HTTP_TIMEOUT_S;
		const auto resp = http.get(
			url("/api/v2/public/get_instruments?currency=" + currency + "&kind=" + kind + "&expired=false"), args);
		if (resp->statusCode != 200) {
			std::fprintf(stderr, "[control] get_instruments %s/%s failed: %d %s\n", currency.c_str(), kind.c_str(),
						 resp->statusCode, resp->errorMsg.c_str());
			return false;
		}
		const auto j = nlohmann::json::parse(resp->body, nullptr, false);
		if (j.is_discarded() || !j.contains("result") || !j["result"].is_array()) return false;
		for (const auto& item : j["result"]) {
			auto info = parse_instrument(item, cfg_.bases, cfg_.kinds);
			if (!info) continue;
			info->currency = currency;
			out.emplace(info->name, std::move(*info));
		}
		return true;
	}

	// Event-driven trigger; the authoritative list still comes from REST.
	void start_state_listener() {
		state_ws_.setUrl(ws_url());
		state_ws_.setTLSOptions(cfg_.tls);
		state_ws_.setPingInterval(Connection::PING_SECONDS);
		state_ws_.setOnMessageCallback([this](const ix::WebSocketMessagePtr& m) {
			if (m->type == ix::WebSocketMessageType::Open) {
				std::vector<std::string> channels;
				for (const auto& c : cfg_.currencies)
					for (const auto& k : cfg_.kinds) channels.push_back("instrument.state." + k + "." + c);
				const nlohmann::json req{{"jsonrpc", "2.0"},
										 {"id", 1},
										 {"method", "public/subscribe"},
										 {"params", {{"channels", channels}}}};
				state_ws_.sendText(req.dump());
			} else if (m->type == ix::WebSocketMessageType::Message &&
					   m->str.find("\"instrument.state.") != std::string::npos) {
				state_changed_ = true;
			}
		});
		state_ws_.start();
	}

	void add_instrument(InstrumentInfo info) {
		const size_t conn = connection_for(info.group);
		const std::string name = info.name;
		commands_.push({WriterCommand::Kind::ADD, info, static_cast<uint32_t>(conn), {}});  // copies info
		channels_[conn].insert(name);
		assigned_.emplace(name, Assigned{std::move(info), conn});
		auto& c = conns_.at(conn);
		if (c.connected()) pending_[conn].push_back(book_channel(name, cfg_.interval));
	}

	void remove_instrument(const std::string& name, const std::string& reason) {
		const auto it = assigned_.find(name);
		if (it == assigned_.end()) return;
		const size_t conn = it->second.conn;
		auto& c = conns_.at(conn);
		if (c.connected()) c.unsubscribe({book_channel(name, cfg_.interval)});
		channels_[conn].erase(name);
		commands_.push({WriterCommand::Kind::REMOVE, it->second.info, static_cast<uint32_t>(conn), reason});
		assigned_.erase(it);
	}

	// Fills the group's connections in order; opens a new one when all are full.
	size_t connection_for(const std::string& group) {
		for (const size_t i : group_conns_[group])
			if (channels_[i].size() < cfg_.instruments_per_conn) return i;
		const size_t id = conns_.size();
		auto c = std::make_unique<Connection>(static_cast<uint32_t>(id), group, ws_url(), cfg_.queue_capacity,
											  cfg_.tls);
		Connection* raw = c.get();
		if (conns_.add(std::move(c)) == ConnectionTable::MAX_CONNECTIONS) {
			std::fprintf(stderr, "[control] connection limit reached, overloading %s\n", group.c_str());
			return group_conns_[group].empty() ? 0 : group_conns_[group].back();
		}
		group_conns_[group].push_back(id);
		raw->start();
		return id;
	}

	void start_trades_connection() {
		const size_t id = conns_.size();
		auto c = std::make_unique<Connection>(static_cast<uint32_t>(id), "trades", ws_url(), cfg_.queue_capacity,
											  cfg_.tls);
		Connection* raw = c.get();
		conns_.add(std::move(c));
		trades_conn_ = id;
		raw->start();
	}

	std::vector<std::string> trade_channels() const {
		std::vector<std::string> out;
		for (const auto& c : cfg_.currencies)
			for (const auto& k : cfg_.kinds) out.push_back("trades." + k + "." + c + "." + cfg_.interval);
		return out;
	}

	// --- subscriptions -------------------------------------------------------

	void service_connections() {
		for (size_t i = 0; i < conns_.size(); ++i) {
			auto& c = conns_.at(i);
			if (c.take_overflow()) {
				std::fprintf(stderr, "[control] conn %zu queue overflow, restarting for fresh snapshots\n", i);
				pending_.erase(i);
				c.restart();
				continue;
			}
			if (c.take_needs_subscribe()) {
				auto& p = pending_[i];
				p.clear();
				if (i == trades_conn_) {
					p = trade_channels();
				} else {
					for (const auto& name : channels_[i]) p.push_back(book_channel(name, cfg_.interval));
				}
			}
		}
		// One batch per connection per tick keeps request rates modest.
		for (auto it = pending_.begin(); it != pending_.end();) {
			auto& c = conns_.at(it->first);
			auto& p = it->second;
			if (!c.connected() || p.empty()) {
				it = p.empty() ? pending_.erase(it) : std::next(it);
				continue;
			}
			const size_t n = std::min(SUBSCRIBE_BATCH, p.size());
			std::vector<std::string> batch(p.begin(), p.begin() + static_cast<std::ptrdiff_t>(n));
			if (c.subscribe(batch)) p.erase(p.begin(), p.begin() + static_cast<std::ptrdiff_t>(n));
			std::this_thread::sleep_for(std::chrono::milliseconds(SUBSCRIBE_PAUSE_MS));
			++it;
		}
	}

	// Unsubscribe + subscribe makes Deribit send a fresh snapshot.
	void resync_books() {
		resync_.drain(resync_buf_);
		std::map<size_t, std::vector<std::string>> by_conn;
		for (const auto& name : resync_buf_) {
			const auto it = assigned_.find(name);
			if (it != assigned_.end()) by_conn[it->second.conn].push_back(book_channel(name, cfg_.interval));
		}
		for (auto& [conn, channels] : by_conn) {
			auto& c = conns_.at(conn);
			if (!c.connected()) continue;
			for (size_t i = 0; i < channels.size(); i += SUBSCRIBE_BATCH) {
				const std::vector<std::string> batch(
					channels.begin() + static_cast<std::ptrdiff_t>(i),
					channels.begin() + static_cast<std::ptrdiff_t>(std::min(channels.size(), i + SUBSCRIBE_BATCH)));
				c.unsubscribe(batch);
				c.subscribe(batch);
			}
		}
	}

	ControlConfig cfg_;
	ConnectionTable& conns_;
	MutexQueue<WriterCommand>& commands_;
	MutexQueue<std::string>& resync_;
	std::map<std::string, Assigned> assigned_;
	std::map<size_t, std::set<std::string>> channels_;
	std::map<std::string, std::vector<size_t>> group_conns_;
	std::map<size_t, std::vector<std::string>> pending_;
	std::vector<std::string> resync_buf_;
	size_t trades_conn_ = static_cast<size_t>(-1);
	ix::WebSocket state_ws_;
	std::atomic<bool> state_changed_{false};
};

}  // namespace collector
