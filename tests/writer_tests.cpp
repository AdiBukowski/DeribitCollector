// Writer thread tests: drive a Writer with messages pushed straight into
// connection queues (no network) and a controllable clock, then decode the
// produced hourly files.

#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <ixwebsocket/IXNetSystem.h>
#include <nlohmann/json.hpp>

#include "writer.hpp"
#include "zstd_reader.hpp"

using namespace collector;
using collector::testing::read_records;
using nlohmann::json;

namespace {

constexpr int64_t HOUR_0 = 1'767'225'600 * NS_PER_SEC;  // 2026-01-01T00:00:00Z

json instrument_meta(const std::string& name, const std::string& base, const std::string& settle) {
	return {{"instrument_name", name}, {"kind", "option"}, {"base_currency", base}, {"settlement_currency", settle}};
}

InstrumentInfo instrument(const std::string& name, const std::string& base = "BTC",
						  const std::string& settle = "BTC") {
	return *parse_instrument(instrument_meta(name, base, settle), {"BTC", "ETH", "SOL"}, {"option", "future"});
}

std::string book_msg(const std::string& name, const std::string& type, int64_t id, int64_t prev,
					 const std::string& levels = R"("bids":[["new",100.0,1.0]],"asks":[["new",101.0,2.0]])") {
	std::string data = R"({"type":")" + type + R"(","change_id":)" + std::to_string(id);
	if (prev >= 0) data += R"(,"prev_change_id":)" + std::to_string(prev);
	data += "," + levels + "}";
	return R"({"jsonrpc":"2.0","method":"subscription","params":{"channel":"book.)" + name + R"(.100ms","data":)" +
		   data + "}}";
}

class WriterTest : public ::testing::Test {
protected:
	void SetUp() override {
		ix::initNetSystem();
		dir_ = std::filesystem::temp_directory_path() / ("dc_writer_" + std::to_string(::testing::UnitTest::GetInstance()->random_seed()) + "_" +
														 ::testing::UnitTest::GetInstance()->current_test_info()->name());
		std::filesystem::remove_all(dir_);
		std::filesystem::create_directories(dir_);
		clock_ = HOUR_0 + 10 * NS_PER_SEC;
		for (uint32_t i = 0; i < 2; ++i)
			conns_.add(std::make_unique<Connection>(i, "test", "wss://invalid", 64));
	}

	void TearDown() override {
		stop();
		std::filesystem::remove_all(dir_);
	}

	void start() {
		WriterConfig cfg;
		cfg.out_dir = dir_;
		cfg.stats_path = stats_path_;
		cfg.console = false;
		cfg.clock = [this] { return clock_.load(); };
		writer_ = std::make_unique<Writer>(cfg, conns_, commands_, resync_);
		thread_ = std::thread([this] { writer_->run(running_); });
	}

	void stop() {
		if (!thread_.joinable()) return;
		running_ = false;
		thread_.join();
	}

	void add(const InstrumentInfo& info, uint32_t conn = 0) {
		commands_.push({WriterCommand::Kind::ADD, info, conn, {}});
	}

	void push(uint32_t conn, const std::string& text, uint32_t epoch = 1) {
		ASSERT_TRUE(conns_.at(conn).queue().push(RawMessage{clock_.load(), epoch, text}));
	}

	// Waits until the writer drained every queue (and a little more for the record writes).
	void settle() {
		for (int i = 0; i < 500; ++i) {
			bool empty = true;
			for (size_t c = 0; c < conns_.size(); ++c) empty &= conns_.at(c).queue().size() == 0;
			if (empty) break;
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(30));
	}

	std::filesystem::path file(const std::string& group, const std::string& hour = "00") const {
		return dir_ / "deribit" / group / "2026-01-01" / (hour + ".jsonl.zst");
	}

	static std::vector<json> of_type(const std::vector<json>& recs, const std::string& type) {
		std::vector<json> out;
		for (const auto& r : recs)
			if (r.value("type", "") == type) out.push_back(r);
		return out;
	}

	std::filesystem::path dir_;
	std::filesystem::path stats_path_;
	std::atomic<int64_t> clock_{0};
	std::atomic<bool> running_{true};
	ConnectionTable conns_;
	MutexQueue<WriterCommand> commands_;
	MutexQueue<std::string> resync_;
	std::unique_ptr<Writer> writer_;
	std::thread thread_;
};

}  // namespace

TEST_F(WriterTest, WritesHeaderInstrumentAndRawBookRecords) {
	const std::string name = "BTC-27MAR26-100000-C";
	start();
	add(instrument(name));
	push(0, book_msg(name, "snapshot", 10, -1));
	push(0, book_msg(name, "change", 11, 10));
	settle();
	stop();

	ASSERT_TRUE(std::filesystem::exists(file("btc")));
	EXPECT_FALSE(std::filesystem::exists(file("btc").string() + ".part"));
	const auto recs = read_records(file("btc"));
	ASSERT_FALSE(recs.empty());
	EXPECT_EQ(recs[0]["type"], "header");
	EXPECT_EQ(recs[0]["group"], "btc");
	EXPECT_EQ(recs[0]["hour_start_ns"], HOUR_0);

	const auto added = of_type(recs, "instrument");
	ASSERT_EQ(added.size(), 1u);
	EXPECT_EQ(added[0]["event"], "added");
	EXPECT_EQ(added[0]["info"]["instrument_name"], name);

	const auto books = of_type(recs, "book");
	ASSERT_EQ(books.size(), 2u);
	EXPECT_EQ(books[0]["conn"], 0);
	EXPECT_EQ(books[0]["msg"]["params"]["data"]["change_id"], 10);
	EXPECT_EQ(books[1]["msg"]["params"]["data"]["prev_change_id"], 10);
	EXPECT_EQ(of_type(recs, "session").size(), 1u);
	EXPECT_TRUE(of_type(recs, "gap").empty());
}

TEST_F(WriterTest, RoutesGroupsAndTradesToSeparateFiles) {
	start();
	add(instrument("BTC-27MAR26-100000-C"));
	add(instrument("SOL_USDC-27MAR26-150-C", "SOL", "USDC"), 1);
	push(0, book_msg("BTC-27MAR26-100000-C", "snapshot", 1, -1));
	push(1, book_msg("SOL_USDC-27MAR26-150-C", "snapshot", 1, -1));
	push(1, R"({"jsonrpc":"2.0","method":"subscription","params":{"channel":"trades.option.USDC.100ms","data":[{"trade_id":"1"}]}})");
	settle();
	stop();

	EXPECT_EQ(of_type(read_records(file("btc")), "book").size(), 1u);
	EXPECT_EQ(of_type(read_records(file("sol_usdc")), "book").size(), 1u);
	const auto trades = of_type(read_records(file("trades_usdc")), "trades");
	ASSERT_EQ(trades.size(), 1u);
	EXPECT_EQ(trades[0]["msg"]["params"]["data"][0]["trade_id"], "1");
}

TEST_F(WriterTest, SequenceGapIsRecordedAndResyncRequested) {
	const std::string name = "BTC-27MAR26-100000-C";
	start();
	add(instrument(name));
	push(0, book_msg(name, "snapshot", 10, -1));
	push(0, book_msg(name, "change", 13, 12));
	settle();
	stop();

	const auto gaps = of_type(read_records(file("btc")), "gap");
	ASSERT_EQ(gaps.size(), 1u);
	EXPECT_EQ(gaps[0]["instrument"], name);
	EXPECT_EQ(gaps[0]["expected_prev"], 10);
	EXPECT_EQ(gaps[0]["got_prev"], 12);

	std::vector<std::string> resync;
	resync_.drain(resync);
	ASSERT_EQ(resync.size(), 1u);
	EXPECT_EQ(resync[0], name);
}

TEST_F(WriterTest, NewEpochInvalidatesBooksOfThatConnection) {
	const std::string name = "BTC-27MAR26-100000-C";
	start();
	add(instrument(name));
	push(0, book_msg(name, "snapshot", 10, -1), 1);
	// Reconnect: an update continuing the old sequence must not be applied or flagged as a gap.
	push(0, book_msg(name, "change", 11, 10), 2);
	push(0, book_msg(name, "snapshot", 50, -1), 2);
	push(0, book_msg(name, "change", 51, 50), 2);
	settle();
	stop();

	const auto recs = read_records(file("btc"));
	const auto sessions = of_type(recs, "session");
	ASSERT_EQ(sessions.size(), 2u);
	EXPECT_EQ(sessions[1]["epoch"], 2);
	EXPECT_TRUE(of_type(recs, "gap").empty());
	EXPECT_EQ(of_type(recs, "book").size(), 4u);  // raw messages are always preserved
}

TEST_F(WriterTest, BadMessagesAreCountedNotFatal) {
	const std::string name = "BTC-27MAR26-100000-C";
	start();
	add(instrument(name));
	push(0, "not json");
	push(0, R"({"jsonrpc":"2.0","method":"subscription"})");
	push(0, R"({"jsonrpc":"2.0","method":"subscription","params":"oops"})");
	push(0, R"({"jsonrpc":"2.0","id":5,"error":{"code":11050,"message":"bad_request"}})");
	push(0, book_msg("UNKNOWN-1JAN26-1-C", "snapshot", 1, -1));
	push(0, book_msg(name, "snapshot", 10, -1));
	settle();
	stop();

	EXPECT_EQ(of_type(read_records(file("btc")), "book").size(), 1u);
	const auto stats = json::parse(std::ifstream(dir_ / "stats.json"));
	EXPECT_EQ(stats["parse_errors"], 1);
	EXPECT_EQ(stats["rpc_errors"], 1);
	EXPECT_EQ(stats["unknown_channel_messages"], 1);
	EXPECT_EQ(stats["books_valid"], 1);
	EXPECT_EQ(stats["instruments"], 1);
}

TEST_F(WriterTest, RemovedInstrumentIsLoggedAndNoLongerTracked) {
	const std::string name = "BTC-27MAR26-100000-C";
	start();
	add(instrument(name));
	settle();
	commands_.push({WriterCommand::Kind::REMOVE, instrument(name), 0, "delisted"});
	settle();
	push(0, book_msg(name, "snapshot", 10, -1));
	settle();
	stop();

	const auto recs = read_records(file("btc"));
	const auto inst = of_type(recs, "instrument");
	ASSERT_EQ(inst.size(), 2u);
	EXPECT_EQ(inst[1]["event"], "removed");
	EXPECT_EQ(inst[1]["reason"], "delisted");
	EXPECT_TRUE(of_type(recs, "book").empty());
}

TEST_F(WriterTest, HourRotationFinishesFileAndCheckpointsBooks) {
	const std::string name = "BTC-27MAR26-100000-C";
	start();
	add(instrument(name));
	push(0, book_msg(name, "snapshot", 10, -1,
					 R"("bids":[["new",100.0,1.0],["new",99.0,2.0]],"asks":[["new",101.0,3.0]])"));
	push(0, book_msg(name, "change", 11, 10, R"("bids":[["delete",100.0,0.0]],"asks":[])"));
	settle();
	clock_ = HOUR_0 + NS_PER_HOUR + NS_PER_SEC;
	settle();
	push(0, book_msg(name, "change", 12, 11, R"("bids":[],"asks":[["change",101.0,4.0]])"));
	settle();
	stop();

	// Previous hour is complete and renamed.
	ASSERT_TRUE(std::filesystem::exists(file("btc", "00")));
	EXPECT_FALSE(std::filesystem::exists(file("btc", "00").string() + ".part"));

	// New hour starts with header, instrument checkpoint and the rebuilt book.
	const auto recs = read_records(file("btc", "01"));
	ASSERT_GE(recs.size(), 3u);
	EXPECT_EQ(recs[0]["type"], "header");
	EXPECT_EQ(recs[0]["hour_start_ns"], HOUR_0 + NS_PER_HOUR);
	EXPECT_EQ(recs[1]["type"], "instrument");
	EXPECT_EQ(recs[1]["event"], "checkpoint");
	ASSERT_EQ(recs[2]["type"], "snapshot");
	EXPECT_EQ(recs[2]["instrument"], name);
	EXPECT_EQ(recs[2]["change_id"], 11);
	EXPECT_EQ(recs[2]["bids"], json::parse("[[99.0,2.0]]"));
	EXPECT_EQ(recs[2]["asks"], json::parse("[[101.0,3.0]]"));
	EXPECT_EQ(of_type(recs, "book").size(), 1u);
	EXPECT_TRUE(of_type(recs, "gap").empty());
}

TEST_F(WriterTest, RestartWithinHourDoesNotOverwrite) {
	const std::string name = "BTC-27MAR26-100000-C";
	start();
	add(instrument(name));
	settle();
	stop();
	running_ = true;
	start();
	add(instrument(name));
	settle();
	stop();

	EXPECT_TRUE(std::filesystem::exists(file("btc", "00")));
	EXPECT_TRUE(std::filesystem::exists(file("btc", "00_1")));
}

TEST_F(WriterTest, StatsPathOverride) {
	stats_path_ = dir_ / "health.json";
	start();
	stop();
	EXPECT_FALSE(std::filesystem::exists(dir_ / "stats.json"));
	EXPECT_FALSE(std::filesystem::exists(dir_ / "health.json.tmp"));
	const auto stats = json::parse(std::ifstream(stats_path_));
	EXPECT_EQ(stats["connections"].size(), 2u);
	EXPECT_EQ(stats["gaps"], 0);
}
