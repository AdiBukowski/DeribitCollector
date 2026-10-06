#pragma once

// Writer thread: drains every connection queue, validates book sequences,
// routes messages to per-group hourly zstd files and writes checkpoints at
// each file start so any hour can be replayed on its own.
//
// Record types (one JSON object per line):
//   header      first line of every file
//   instrument  event=checkpoint|added|removed, full get_instruments record
//   snapshot    rebuilt book at file start (bids/asks as [price, amount])
//   session     a connection (re)connected; its books restart from snapshots
//   gap         change_id discontinuity; a resubscribe was requested
//   book/trades raw Deribit notification in "msg", with local receive time

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <functional>
#include <fstream>
#include <map>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include <nlohmann/json.hpp>

#include "book.hpp"
#include "connection.hpp"
#include "instrument.hpp"
#include "util.hpp"
#include "zstd_file.hpp"

namespace collector {

constexpr int COLLECTOR_FORMAT_VERSION = 1;

struct WriterCommand {
	enum class Kind { ADD, REMOVE };
	Kind kind = Kind::ADD;
	InstrumentInfo info;
	uint32_t conn = 0;
	std::string reason;
};

struct WriterConfig {
	std::filesystem::path out_dir;
	std::string interval = "100ms";
	int zstd_level = 3;
	int flush_seconds = 1;
	std::filesystem::path stats_path;  // empty: <out_dir>/stats.json
	std::function<int64_t()> clock = wall_ns;  // injectable for tests
	bool console = true;
};

class Writer {
public:
	static constexpr size_t DRAIN_BATCH = 1'024;
	static constexpr int64_t STATS_NS = 10 * NS_PER_SEC;
	static constexpr int64_t CONSOLE_NS = 30 * NS_PER_SEC;

	Writer(WriterConfig cfg, ConnectionTable& conns, MutexQueue<WriterCommand>& commands,
		   MutexQueue<std::string>& resync)
		: cfg_(std::move(cfg)), conns_(conns), commands_(commands), resync_(resync) {}

	// Runs until running is false and all queues are empty, then closes files.
	void run(const std::atomic<bool>& running) {
		const int64_t start = now_ns();
		hour_start_ = start - start % NS_PER_HOUR;
		next_flush_ = start + flush_ns();
		next_stats_ = start + STATS_NS;
		next_console_ = start + CONSOLE_NS;
		while (true) {
			const bool stopping = !running.load(std::memory_order_acquire);
			apply_commands();
			const size_t n = drain();
			const int64_t now = now_ns();
			rotate_if_needed(now);
			if (now >= next_flush_) {
				for (auto& [g, f] : files_) f.flush();
				next_flush_ = now + flush_ns();
			}
			if (now >= next_stats_) {
				write_stats(now);
				next_stats_ = now + STATS_NS;
			}
			if (cfg_.console && now >= next_console_) {
				print_status();
				next_console_ = now + CONSOLE_NS;
			}
			if (stopping && n == 0) break;
			if (n == 0) std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		apply_commands();
		for (auto& [g, f] : files_) f.close();
		write_stats(now_ns());
	}

private:
	int64_t now_ns() const { return cfg_.clock(); }
	int64_t flush_ns() const { return std::max(1, cfg_.flush_seconds) * NS_PER_SEC; }

	struct Tracked {
		InstrumentInfo info;
		uint32_t conn = 0;
		L2Book book;
		uint64_t messages = 0;
	};

	void apply_commands() {
		commands_.drain(cmd_buf_);
		const int64_t now = now_ns();
		for (auto& c : cmd_buf_) {
			if (c.kind == WriterCommand::Kind::ADD) {
				const std::string group = c.info.group;
				ensure_open(group, now);
				auto& t = tracked_[c.info.name];
				t.info = std::move(c.info);
				t.conn = c.conn;
				write_record(group, {{"type", "instrument"}, {"event", "added"}, {"ts_ns", now}, {"conn", c.conn},
									 {"info", t.info.meta}});
			} else {
				const auto it = tracked_.find(c.info.name);
				if (it == tracked_.end()) continue;
				write_record(it->second.info.group, {{"type", "instrument"},
													 {"event", "removed"},
													 {"ts_ns", now},
													 {"reason", c.reason},
													 {"info", it->second.info.meta}});
				tracked_.erase(it);
			}
		}
	}

	size_t drain() {
		size_t total = 0;
		RawMessage m;
		for (size_t i = 0; i < conns_.size(); ++i) {
			auto& q = conns_.at(i).queue();
			for (size_t k = 0; k < DRAIN_BATCH && q.pop(m); ++k) {
				try {
					handle(static_cast<uint32_t>(i), m);
				} catch (const std::exception& e) {
					++parse_errors_;
					std::fprintf(stderr, "[writer] conn %zu bad message: %s\n", i, e.what());
				}
				++total;
			}
		}
		return total;
	}

	void handle(uint32_t conn, const RawMessage& m) {
		if (conn >= epochs_.size()) epochs_.resize(conn + 1, 0);
		if (epochs_[conn] != m.epoch) {
			epochs_[conn] = m.epoch;
			new_session(conn, m.epoch, m.recv_ns);
		}
		++messages_in_;
		const auto j = nlohmann::json::parse(m.text, nullptr, false);
		if (j.is_discarded()) {
			++parse_errors_;
			return;
		}
		const auto method = j.find("method");
		if (method == j.end() || *method != "subscription") {
			const auto err = j.find("error");
			if (err != j.end()) {
				++rpc_errors_;
				std::fprintf(stderr, "[writer] conn %u rpc error: %s\n", conn, err->dump().c_str());
			}
			return;
		}
		const auto pit = j.find("params");
		if (pit == j.end() || !pit->is_object()) return;
		const auto& params = *pit;
		const auto ch = params.find("channel");
		if (ch == params.end() || !ch->is_string()) return;
		const auto& channel = ch->get_ref<const std::string&>();

		if (channel.starts_with("book.")) {
			const auto name = instrument_of_book_channel(channel);
			auto it = tracked_.find(name);
			if (it == tracked_.end()) {
				// The ADD command may have been queued after this drain started.
				apply_commands();
				it = tracked_.find(name);
			}
			if (it == tracked_.end()) {
				++unknown_;
				return;
			}
			auto& t = it->second;
			++t.messages;
			const auto dit = params.find("data");
			if (dit == params.end()) return;
			const auto& data = *dit;
			const int64_t expected = t.book.change_id();
			if (t.book.apply(data) == L2Book::Result::GAP) {
				++gaps_;
				write_record(t.info.group, {{"type", "gap"},
											{"recv_ns", m.recv_ns},
											{"instrument", t.info.name},
											{"expected_prev", expected},
											{"got_prev", data.contains("prev_change_id") ? data["prev_change_id"] : nlohmann::json()}});
				resync_.push(t.info.name);
			}
			write_raw(t.info.group, "book", conn, m);
		} else if (channel.starts_with("trades.")) {
			const auto parts = split(channel, '.');
			write_raw(parts.size() > 2 ? "trades_" + lower(parts[2]) : "trades", "trades", conn, m);
		}
	}

	// Every book on the connection is stale until its new snapshot arrives.
	void new_session(uint32_t conn, uint32_t epoch, int64_t ts) {
		std::map<std::string, bool> groups;
		for (auto& [name, t] : tracked_) {
			if (t.conn != conn) continue;
			t.book.invalidate();
			groups[t.info.group] = true;
		}
		for (const auto& [g, _] : groups)
			write_record(g, {{"type", "session"}, {"ts_ns", ts}, {"conn", conn}, {"epoch", epoch}});
	}

	void write_raw(const std::string& group, std::string_view type, uint32_t conn, const RawMessage& m) {
		line_.clear();
		line_ += "{\"type\":\"";
		line_ += type;
		line_ += "\",\"recv_ns\":";
		line_ += std::to_string(m.recv_ns);
		line_ += ",\"conn\":";
		line_ += std::to_string(conn);
		line_ += ",\"msg\":";
		line_ += m.text;
		line_ += "}\n";
		write_line(group, line_);
	}

	void write_record(const std::string& group, const nlohmann::json& j) { write_line(group, j.dump() + "\n"); }

	void write_line(const std::string& group, std::string_view line) {
		auto& f = ensure_open(group, now_ns());
		if (!f.write(line)) ++write_errors_;
		++lines_out_;
	}

	ZstdFile& ensure_open(const std::string& group, int64_t now) {
		auto& f = files_[group];
		if (!f.is_open()) open_file(group, f, now);
		return f;
	}

	void open_file(const std::string& group, ZstdFile& f, int64_t now) {
		const int64_t hour = now - now % NS_PER_HOUR;
		const auto dir = cfg_.out_dir / "deribit" / group / utc_date(hour);
		std::error_code ec;
		std::filesystem::create_directories(dir, ec);
		// A restart within the same hour gets a suffixed file instead of overwriting.
		std::filesystem::path final_path = dir / (utc_hour(hour) + ".jsonl.zst");
		for (int n = 1; std::filesystem::exists(final_path) || std::filesystem::exists(final_path.string() + ".part");
			 ++n)
			final_path = dir / (utc_hour(hour) + "_" + std::to_string(n) + ".jsonl.zst");
		if (!f.open(final_path.string() + ".part", final_path, cfg_.zstd_level)) {
			++write_errors_;
			std::fprintf(stderr, "[writer] cannot open %s\n", final_path.string().c_str());
			return;
		}
		f.write(nlohmann::json{{"type", "header"},
							   {"version", COLLECTOR_FORMAT_VERSION},
							   {"venue", "deribit"},
							   {"group", group},
							   {"interval", cfg_.interval},
							   {"ts_ns", now},
							   {"hour_start_ns", hour}}
					.dump() +
				"\n");
		write_checkpoint(group, f, now);
	}

	void write_checkpoint(const std::string& group, ZstdFile& f, int64_t now) {
		for (const auto& [name, t] : tracked_) {
			if (t.info.group != group) continue;
			f.write(nlohmann::json{{"type", "instrument"},
								   {"event", "checkpoint"},
								   {"ts_ns", now},
								   {"conn", t.conn},
								   {"info", t.info.meta}}
						.dump() +
					"\n");
			if (!t.book.valid()) continue;
			nlohmann::json bids = nlohmann::json::array();
			nlohmann::json asks = nlohmann::json::array();
			for (const auto& [p, a] : t.book.bids()) bids.push_back({p, a});
			for (const auto& [p, a] : t.book.asks()) asks.push_back({p, a});
			f.write(nlohmann::json{{"type", "snapshot"},
								   {"ts_ns", now},
								   {"instrument", name},
								   {"change_id", t.book.change_id()},
								   {"bids", std::move(bids)},
								   {"asks", std::move(asks)}}
						.dump() +
					"\n");
		}
	}

	void rotate_if_needed(int64_t now) {
		const int64_t hour = now - now % NS_PER_HOUR;
		if (hour == hour_start_) return;
		hour_start_ = hour;
		for (auto& [g, f] : files_) {
			if (!f.is_open()) continue;
			const auto closed = f.final_path();
			if (!f.close()) ++write_errors_;
			std::fprintf(stderr, "[writer] closed %s\n", closed.string().c_str());
			open_file(g, f, now);
		}
	}

	void write_stats(int64_t now) {
		size_t valid = 0;
		for (const auto& [n, t] : tracked_) valid += t.book.valid();
		nlohmann::json conns = nlohmann::json::array();
		for (size_t i = 0; i < conns_.size(); ++i) {
			auto& c = conns_.at(i);
			conns.push_back({{"id", i},
							 {"group", c.group()},
							 {"connected", c.connected()},
							 {"epoch", c.epoch()},
							 {"received", c.received()},
							 {"dropped", c.dropped()},
							 {"queue_max_depth", c.max_depth()}});
		}
		uint64_t in = 0, out = 0;
		for (const auto& [g, f] : files_) {
			in += f.bytes_in();
			out += f.bytes_out();
		}
		const nlohmann::json s{{"ts", utc_iso(now)},
							   {"instruments", tracked_.size()},
							   {"books_valid", valid},
							   {"messages", messages_in_},
							   {"lines_written", lines_out_},
							   {"gaps", gaps_},
							   {"parse_errors", parse_errors_},
							   {"rpc_errors", rpc_errors_},
							   {"write_errors", write_errors_},
							   {"unknown_channel_messages", unknown_},
							   {"current_hour_bytes_raw", in},
							   {"current_hour_bytes_compressed", out},
							   {"connections", std::move(conns)}};
		const auto path = cfg_.stats_path.empty() ? cfg_.out_dir / "stats.json" : cfg_.stats_path;
		auto tmp = path;
		tmp += ".tmp";
		{
			std::ofstream f(tmp, std::ios::trunc);
			f << s.dump(2) << "\n";
		}
		std::error_code ec;
		std::filesystem::rename(tmp, path, ec);
	}

	void print_status() {
		size_t valid = 0;
		for (const auto& [n, t] : tracked_) valid += t.book.valid();
		uint64_t dropped = 0, received = 0;
		size_t connected = 0;
		for (size_t i = 0; i < conns_.size(); ++i) {
			dropped += conns_.at(i).dropped();
			received += conns_.at(i).received();
			connected += conns_.at(i).connected();
		}
		uint64_t in = 0, out = 0;
		for (const auto& [g, f] : files_) {
			in += f.bytes_in();
			out += f.bytes_out();
		}
		const uint64_t delta = messages_in_ - last_messages_;
		last_messages_ = messages_in_;
		std::printf("%s instruments=%zu valid=%zu conns=%zu/%zu msg/s=%.0f gaps=%llu dropped=%llu hour=%.1fMB->%.1fMB\n",
					utc_iso(now_ns()).c_str(), tracked_.size(), valid, connected, conns_.size(),
					static_cast<double>(delta) / (CONSOLE_NS / NS_PER_SEC), static_cast<unsigned long long>(gaps_),
					static_cast<unsigned long long>(dropped), in / 1e6, out / 1e6);
		std::fflush(stdout);
		(void)received;
	}

	WriterConfig cfg_;
	ConnectionTable& conns_;
	MutexQueue<WriterCommand>& commands_;
	MutexQueue<std::string>& resync_;
	std::vector<WriterCommand> cmd_buf_;
	std::unordered_map<std::string, Tracked> tracked_;
	std::map<std::string, ZstdFile> files_;
	std::vector<uint32_t> epochs_;
	std::string line_;
	int64_t hour_start_ = 0;
	int64_t next_flush_ = 0;
	int64_t next_stats_ = 0;
	int64_t next_console_ = 0;
	uint64_t messages_in_ = 0;
	uint64_t last_messages_ = 0;
	uint64_t lines_out_ = 0;
	uint64_t gaps_ = 0;
	uint64_t parse_errors_ = 0;
	uint64_t rpc_errors_ = 0;
	uint64_t write_errors_ = 0;
	uint64_t unknown_ = 0;
};

}  // namespace collector
