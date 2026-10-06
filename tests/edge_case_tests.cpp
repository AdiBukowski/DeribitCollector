// Edge cases for the self-contained building blocks (no network, no threads
// except the queue test in collector_tests.cpp).

#include <filesystem>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "book.hpp"
#include "instrument.hpp"
#include "spsc_queue.hpp"
#include "util.hpp"
#include "zstd_file.hpp"

using namespace collector;
using nlohmann::json;

TEST(SpscQueue, WrapsAroundManyTimes) {
	SpscQueue<std::string> q(2);
	std::string v;
	for (int i = 0; i < 1000; ++i) {
		ASSERT_TRUE(q.push(std::to_string(i)));
		ASSERT_TRUE(q.pop(v));
		ASSERT_EQ(v, std::to_string(i));
	}
	EXPECT_EQ(q.size(), 0u);
	EXPECT_EQ(q.capacity(), 2u);
}

TEST(L2Book, SnapshotReplacesExistingLevels) {
	L2Book b;
	b.apply(json::parse(R"({"type":"snapshot","change_id":10,"bids":[["new",100.0,1.0]],"asks":[["new",101.0,1.0]]})"));
	b.apply(json::parse(R"({"type":"snapshot","change_id":30,"bids":[["new",50.0,1.0]],"asks":[]})"));
	ASSERT_EQ(b.bids().size(), 1u);
	EXPECT_EQ(b.bids().begin()->first, 50.0);
	EXPECT_TRUE(b.asks().empty());
	EXPECT_EQ(b.change_id(), 30);
}

TEST(L2Book, BidsDescendingAsksAscending) {
	L2Book b;
	b.apply(json::parse(R"({"type":"snapshot","change_id":1,
		"bids":[["new",98.0,1],["new",100.0,1],["new",99.0,1]],
		"asks":[["new",103.0,1],["new",101.0,1],["new",102.0,1]]})"));
	std::vector<double> bids, asks;
	for (const auto& [p, a] : b.bids()) bids.push_back(p);
	for (const auto& [p, a] : b.asks()) asks.push_back(p);
	EXPECT_EQ(bids, (std::vector<double>{100.0, 99.0, 98.0}));
	EXPECT_EQ(asks, (std::vector<double>{101.0, 102.0, 103.0}));
}

TEST(L2Book, ZeroAmountRemovesLevelAndMalformedLevelsAreSkipped) {
	L2Book b;
	b.apply(json::parse(R"({"type":"snapshot","change_id":10,"bids":[["new",100.0,1.0],["new",99.0,2.0]]})"));
	ASSERT_EQ(b.apply(json::parse(R"({"type":"change","change_id":11,"prev_change_id":10,
		"bids":[["change",99.0,0.0],"junk",["new",1.0],["new","x",1.0]]})")),
			  L2Book::Result::OK);
	ASSERT_EQ(b.bids().size(), 1u);
	EXPECT_EQ(b.bids().begin()->first, 100.0);
}

TEST(L2Book, NonObjectAndMissingPrevAreHandled) {
	L2Book b;
	EXPECT_EQ(b.apply(json::array()), L2Book::Result::IGNORED);
	b.apply(json::parse(R"({"type":"snapshot","change_id":10})"));
	EXPECT_EQ(b.apply(json::parse(R"({"type":"change","change_id":11})")), L2Book::Result::GAP);
}

TEST(Instrument, InverseAndLinearEth) {
	const std::vector<std::string> bases{"BTC", "ETH", "SOL"}, kinds{"option", "future"};
	const json eth{{"instrument_name", "ETH-27MAR26-3000-P"},
				   {"kind", "option"},
				   {"base_currency", "ETH"},
				   {"settlement_currency", "ETH"}};
	const json eth_usdc{{"instrument_name", "ETH_USDC-27MAR26-3000-P"},
						{"kind", "option"},
						{"base_currency", "ETH"},
						{"settlement_currency", "USDC"}};
	EXPECT_EQ(parse_instrument(eth, bases, kinds)->group, "eth");
	EXPECT_EQ(parse_instrument(eth_usdc, bases, kinds)->group, "eth_usdc");
	EXPECT_EQ(parse_instrument(eth, bases, kinds)->expiry_ms, 0);
	EXPECT_FALSE(parse_instrument(json::array(), bases, kinds));
	EXPECT_FALSE(parse_instrument(eth, {"BTC"}, kinds));
}

TEST(Util, LowerAndSplitEdges) {
	EXPECT_EQ(lower("BTC_USDC"), "btc_usdc");
	EXPECT_EQ(split("a..b", '.'), (std::vector<std::string>{"a", "", "b"}));
	EXPECT_EQ(split("a.", '.'), (std::vector<std::string>{"a", ""}));
}

TEST(Util, UtcDayBoundary) {
	const int64_t ns = 1'767'225'599 * NS_PER_SEC + 999'999'999;  // 2025-12-31T23:59:59.999999999Z
	EXPECT_EQ(utc_date(ns), "2025-12-31");
	EXPECT_EQ(utc_hour(ns), "23");
	EXPECT_EQ(utc_date(ns + 1), "2026-01-01");
	EXPECT_EQ(utc_hour(ns + 1), "00");
}

TEST(MutexQueue, DrainReturnsAllInOrderAndClearsOutput) {
	MutexQueue<int> q;
	std::vector<int> out{42};
	q.drain(out);
	EXPECT_TRUE(out.empty());
	for (int i = 0; i < 5; ++i) q.push(int{i});
	q.drain(out);
	EXPECT_EQ(out, (std::vector<int>{0, 1, 2, 3, 4}));
	q.drain(out);
	EXPECT_TRUE(out.empty());
}

TEST(ZstdFile, OperationsOnClosedFileAreSafe) {
	ZstdFile f;
	EXPECT_FALSE(f.is_open());
	EXPECT_FALSE(f.write("x"));
	EXPECT_FALSE(f.flush());
	EXPECT_TRUE(f.close());
}

TEST(ZstdFile, OpenFailsForMissingDirectory) {
	ZstdFile f;
	const auto bad = std::filesystem::temp_directory_path() / "dc_no_such_dir" / "x" / "00.jsonl.zst";
	EXPECT_FALSE(f.open(bad.string() + ".part", bad, 3));
	EXPECT_FALSE(f.is_open());
}
