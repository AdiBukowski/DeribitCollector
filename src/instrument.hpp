#pragma once

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <nlohmann/json.hpp>

#include "util.hpp"

namespace collector {

struct InstrumentInfo {
	std::string name;
	std::string kind;        // option | future (perpetuals are futures)
	std::string base;        // BTC, ETH, SOL
	std::string settlement;  // BTC/ETH for inverse, USDC for linear
	std::string currency;    // get_instruments currency it was listed under
	std::string group;       // output stream: btc, eth, btc_usdc, eth_usdc, sol_usdc
	int64_t expiry_ms = 0;
	nlohmann::json meta;     // full get_instruments record, written to the data files
};

// Inverse contracts settle in the base asset and get the bare base name.
inline std::string group_of(const std::string& base, const std::string& settlement) {
	return settlement == base ? lower(base) : lower(base) + "_" + lower(settlement);
}

inline std::optional<InstrumentInfo> parse_instrument(const nlohmann::json& j, const std::vector<std::string>& bases,
													  const std::vector<std::string>& kinds) {
	if (!j.is_object()) return std::nullopt;
	const auto str = [&](const char* key) {
		const auto it = j.find(key);
		return it != j.end() && it->is_string() ? it->get<std::string>() : std::string();
	};
	InstrumentInfo i;
	i.name = str("instrument_name");
	i.kind = str("kind");
	i.base = str("base_currency");
	i.settlement = str("settlement_currency");
	if (i.name.empty() || i.base.empty() || i.settlement.empty()) return std::nullopt;
	if (std::find(bases.begin(), bases.end(), i.base) == bases.end()) return std::nullopt;
	if (std::find(kinds.begin(), kinds.end(), i.kind) == kinds.end()) return std::nullopt;
	const auto exp = j.find("expiration_timestamp");
	if (exp != j.end() && exp->is_number()) i.expiry_ms = exp->get<int64_t>();
	i.group = group_of(i.base, i.settlement);
	i.meta = j;
	return i;
}

inline std::string book_channel(const std::string& name, const std::string& interval) {
	return "book." + name + "." + interval;
}

// "book.BTC-27DEC24-50000-C.100ms" -> "BTC-27DEC24-50000-C". Deribit names have
// no dots (fractional strikes use 'd', e.g. 0d625).
inline std::string instrument_of_book_channel(std::string_view channel) {
	constexpr std::string_view PREFIX = "book.";
	if (!channel.starts_with(PREFIX)) return {};
	channel.remove_prefix(PREFIX.size());
	const size_t dot = channel.rfind('.');
	return std::string(dot == std::string_view::npos ? channel : channel.substr(0, dot));
}

}  // namespace collector
