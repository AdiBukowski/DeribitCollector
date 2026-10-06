#pragma once

// L2 book rebuilt from Deribit book.{instrument}.{interval} notifications. The
// writer keeps one per instrument to validate change_id continuity and to emit
// hourly checkpoints, so every hourly file can be replayed on its own.

#include <cstdint>
#include <functional>
#include <map>
#include <string>

#include <nlohmann/json.hpp>

namespace collector {

class L2Book {
public:
	enum class Result { OK, GAP, IGNORED };

	using Bids = std::map<double, double, std::greater<>>;
	using Asks = std::map<double, double>;

	Result apply(const nlohmann::json& data) {
		if (!data.is_object()) return Result::IGNORED;
		const auto type = data.value("type", std::string());
		const auto change_id = int_field(data, "change_id", -1);
		if (type == "snapshot") {
			bids_.clear();
			asks_.clear();
			apply_side(bids_, data, "bids");
			apply_side(asks_, data, "asks");
			change_id_ = change_id;
			valid_ = true;
			return Result::OK;
		}
		if (!valid_) return Result::IGNORED;
		if (int_field(data, "prev_change_id", -2) != change_id_) {
			valid_ = false;
			return Result::GAP;
		}
		apply_side(bids_, data, "bids");
		apply_side(asks_, data, "asks");
		change_id_ = change_id;
		return Result::OK;
	}

	void invalidate() { valid_ = false; }
	bool valid() const { return valid_; }
	int64_t change_id() const { return change_id_; }
	const Bids& bids() const { return bids_; }
	const Asks& asks() const { return asks_; }

private:
	static int64_t int_field(const nlohmann::json& data, const char* key, int64_t fallback) {
		const auto it = data.find(key);
		return it != data.end() && it->is_number_integer() ? it->get<int64_t>() : fallback;
	}

	// Levels are [action, price, amount] with action new|change|delete.
	template <class Side>
	static void apply_side(Side& side, const nlohmann::json& data, const char* key) {
		const auto it = data.find(key);
		if (it == data.end() || !it->is_array()) return;
		for (const auto& level : *it) {
			if (!level.is_array() || level.size() < 3 || !level[1].is_number() || !level[2].is_number()) continue;
			const double price = level[1].get<double>();
			const double amount = level[2].get<double>();
			if ((level[0].is_string() && level[0].get_ref<const std::string&>() == "delete") || amount <= 0.0)
				side.erase(price);
			else
				side[price] = amount;
		}
	}

	Bids bids_;
	Asks asks_;
	int64_t change_id_ = -1;
	bool valid_ = false;
};

}  // namespace collector
