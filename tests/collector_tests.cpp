#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <zstd.h>

#include "book.hpp"
#include "instrument.hpp"
#include "spsc_queue.hpp"
#include "util.hpp"
#include "zstd_file.hpp"
#include "zstd_reader.hpp"

using namespace collector;
using collector::testing::decompress;
using nlohmann::json;

// --- SpscQueue ---------------------------------------------------------------

TEST(SpscQueue, RejectsNonPowerOfTwo) {
	EXPECT_THROW(SpscQueue<int>(3), std::invalid_argument);
	EXPECT_THROW(SpscQueue<int>(1), std::invalid_argument);
}

TEST(SpscQueue, FullAndEmpty) {
	SpscQueue<int> q(4);
	int v = 0;
	EXPECT_FALSE(q.pop(v));
	for (int i = 0; i < 4; ++i) EXPECT_TRUE(q.push(int{i}));
	EXPECT_FALSE(q.push(int{9}));
	EXPECT_EQ(q.size(), 4u);
	for (int i = 0; i < 4; ++i) {
		ASSERT_TRUE(q.pop(v));
		EXPECT_EQ(v, i);
	}
	EXPECT_FALSE(q.pop(v));
}

TEST(SpscQueue, ConcurrentPreservesOrder) {
	constexpr int COUNT = 200'000;
	SpscQueue<int> q(1024);
	std::thread producer([&] {
		for (int i = 0; i < COUNT;)
			if (q.push(int{i})) ++i;
	});
	int expected = 0, v = 0;
	while (expected < COUNT)
		if (q.pop(v)) ASSERT_EQ(v, expected++);
	producer.join();
}

// --- L2Book ------------------------------------------------------------------

static json snapshot(int64_t id) {
	return json::parse(R"({"type":"snapshot","change_id":)" + std::to_string(id) +
					   R"(,"bids":[["new",100.0,1.0],["new",99.0,2.0]],"asks":[["new",101.0,3.0]]})");
}

TEST(L2Book, IgnoresUpdatesBeforeSnapshot) {
	L2Book b;
	EXPECT_EQ(b.apply(json::parse(R"({"type":"change","change_id":2,"prev_change_id":1})")), L2Book::Result::IGNORED);
	EXPECT_FALSE(b.valid());
}

TEST(L2Book, SnapshotThenChanges) {
	L2Book b;
	ASSERT_EQ(b.apply(snapshot(10)), L2Book::Result::OK);
	EXPECT_TRUE(b.valid());
	EXPECT_EQ(b.bids().begin()->first, 100.0);
	EXPECT_EQ(b.asks().begin()->first, 101.0);

	const auto upd = json::parse(
		R"({"type":"change","change_id":11,"prev_change_id":10,
			"bids":[["delete",100.0,0.0],["change",99.0,5.0]],"asks":[["new",100.5,1.0]]})");
	ASSERT_EQ(b.apply(upd), L2Book::Result::OK);
	EXPECT_EQ(b.change_id(), 11);
	ASSERT_EQ(b.bids().size(), 1u);
	EXPECT_EQ(b.bids().begin()->first, 99.0);
	EXPECT_EQ(b.bids().begin()->second, 5.0);
	EXPECT_EQ(b.asks().begin()->first, 100.5);
}

TEST(L2Book, GapInvalidatesUntilNextSnapshot) {
	L2Book b;
	b.apply(snapshot(10));
	EXPECT_EQ(b.apply(json::parse(R"({"type":"change","change_id":13,"prev_change_id":12})")), L2Book::Result::GAP);
	EXPECT_FALSE(b.valid());
	EXPECT_EQ(b.apply(json::parse(R"({"type":"change","change_id":14,"prev_change_id":13})")),
			  L2Book::Result::IGNORED);
	EXPECT_EQ(b.apply(snapshot(20)), L2Book::Result::OK);
	EXPECT_TRUE(b.valid());
}

TEST(L2Book, NullPrevChangeIdIsGap) {
	L2Book b;
	b.apply(snapshot(10));
	EXPECT_EQ(b.apply(json::parse(R"({"type":"change","change_id":11,"prev_change_id":null})")), L2Book::Result::GAP);
}

TEST(L2Book, InvalidateRequiresSnapshot) {
	L2Book b;
	b.apply(snapshot(10));
	b.invalidate();
	EXPECT_EQ(b.apply(json::parse(R"({"type":"change","change_id":11,"prev_change_id":10})")),
			  L2Book::Result::IGNORED);
}

// --- Instruments -------------------------------------------------------------

TEST(Instrument, GroupNames) {
	EXPECT_EQ(group_of("BTC", "BTC"), "btc");
	EXPECT_EQ(group_of("ETH", "ETH"), "eth");
	EXPECT_EQ(group_of("BTC", "USDC"), "btc_usdc");
	EXPECT_EQ(group_of("SOL", "USDC"), "sol_usdc");
}

TEST(Instrument, ParseFiltersBaseAndKind) {
	const std::vector<std::string> bases{"BTC", "ETH", "SOL"}, kinds{"option", "future"};
	const auto make = [](const char* name, const char* kind, const char* base, const char* settle) {
		return json{{"instrument_name", name},
					{"kind", kind},
					{"base_currency", base},
					{"settlement_currency", settle},
					{"expiration_timestamp", 1767225600000}};
	};
	const auto sol = parse_instrument(make("SOL_USDC-27MAR26-150-C", "option", "SOL", "USDC"), bases, kinds);
	ASSERT_TRUE(sol);
	EXPECT_EQ(sol->group, "sol_usdc");
	EXPECT_EQ(sol->expiry_ms, 1767225600000);
	EXPECT_EQ(sol->meta["instrument_name"], "SOL_USDC-27MAR26-150-C");
	EXPECT_EQ(parse_instrument(make("BTC-PERPETUAL", "future", "BTC", "BTC"), bases, kinds)->group, "btc");
	EXPECT_FALSE(parse_instrument(make("XRP_USDC-PERPETUAL", "future", "XRP", "USDC"), bases, kinds));
	EXPECT_FALSE(parse_instrument(make("BTC-FS-27MAR26_PERP", "future_combo", "BTC", "BTC"), bases, kinds));
	EXPECT_FALSE(parse_instrument(json{{"kind", "option"}}, bases, kinds));
}

TEST(Instrument, BookChannelRoundTrip) {
	EXPECT_EQ(book_channel("BTC-27DEC24-50000-C", "100ms"), "book.BTC-27DEC24-50000-C.100ms");
	EXPECT_EQ(instrument_of_book_channel("book.BTC-27DEC24-50000-C.100ms"), "BTC-27DEC24-50000-C");
	EXPECT_EQ(instrument_of_book_channel("book.XRP_USDC-27DEC24-0d625-C.100ms"), "XRP_USDC-27DEC24-0d625-C");
	EXPECT_EQ(instrument_of_book_channel("trades.option.BTC.100ms"), "");
}

// --- util --------------------------------------------------------------------

TEST(Util, UtcFormatting) {
	const int64_t ns = 1'767'229'322'000'000'000;  // 2026-01-01T01:02:02Z
	EXPECT_EQ(utc_date(ns), "2026-01-01");
	EXPECT_EQ(utc_hour(ns), "01");
	EXPECT_EQ(utc_iso(ns), "2026-01-01T01:02:02Z");
}

TEST(Util, Split) {
	EXPECT_EQ(split("trades.option.BTC.100ms", '.'), (std::vector<std::string>{"trades", "option", "BTC", "100ms"}));
	EXPECT_EQ(split("", '.'), (std::vector<std::string>{""}));
}

// --- ZstdFile ----------------------------------------------------------------

TEST(ZstdFile, PartRenamedOnCloseAndRoundTrips) {
	const auto dir = std::filesystem::temp_directory_path() / "dc_zstd_test";
	std::filesystem::remove_all(dir);
	std::filesystem::create_directories(dir);
	const auto final_path = dir / "00.jsonl.zst";
	const auto part = dir / "00.jsonl.zst.part";

	ZstdFile f;
	ASSERT_TRUE(f.open(part, final_path, 3));
	std::string expected;
	for (int i = 0; i < 5000; ++i) {
		const std::string line = R"({"i":)" + std::to_string(i) + "}\n";
		ASSERT_TRUE(f.write(line));
		expected += line;
		if (i == 2500) ASSERT_TRUE(f.flush());
	}
	EXPECT_TRUE(std::filesystem::exists(part));
	EXPECT_FALSE(std::filesystem::exists(final_path));
	// Flushed prefix is already decodable while the file is still open.
	EXPECT_EQ(decompress(part).substr(0, 10), expected.substr(0, 10));

	ASSERT_TRUE(f.close());
	EXPECT_FALSE(std::filesystem::exists(part));
	ASSERT_TRUE(std::filesystem::exists(final_path));
	EXPECT_EQ(decompress(final_path), expected);
	EXPECT_EQ(f.bytes_in(), expected.size());
	std::filesystem::remove_all(dir);
}
