#pragma once

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <deque>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

namespace collector {

constexpr int64_t NS_PER_SEC = 1'000'000'000;
constexpr int64_t NS_PER_HOUR = 3'600 * NS_PER_SEC;
constexpr int64_t NS_PER_DAY = 24 * NS_PER_HOUR;

inline int64_t wall_ns() {
	return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
		.count();
}

inline std::tm utc_tm(int64_t ns) {
	const auto t = static_cast<std::time_t>(ns / NS_PER_SEC);
	std::tm tm{};
#ifdef _WIN32
	gmtime_s(&tm, &t);
#else
	gmtime_r(&t, &tm);
#endif
	return tm;
}

inline std::string utc_date(int64_t ns) {
	const auto tm = utc_tm(ns);
	char buf[48];
	std::snprintf(buf, sizeof buf, "%04d-%02d-%02d", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
	return buf;
}

inline std::string utc_hour(int64_t ns) {
	char buf[16];
	std::snprintf(buf, sizeof buf, "%02d", utc_tm(ns).tm_hour);
	return buf;
}

inline std::string utc_iso(int64_t ns) {
	const auto tm = utc_tm(ns);
	char buf[96];
	std::snprintf(buf, sizeof buf, "%04d-%02d-%02dT%02d:%02d:%02dZ", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
				  tm.tm_hour, tm.tm_min, tm.tm_sec);
	return buf;
}

inline std::string lower(std::string s) {
	std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
	return s;
}

inline std::vector<std::string> split(std::string_view s, char sep) {
	std::vector<std::string> out;
	size_t start = 0;
	while (true) {
		const size_t pos = s.find(sep, start);
		out.emplace_back(s.substr(start, pos == std::string_view::npos ? std::string_view::npos : pos - start));
		if (pos == std::string_view::npos) break;
		start = pos + 1;
	}
	return out;
}

// Low-rate command channel between the control and writer threads.
template <class T>
class MutexQueue {
public:
	void push(T v) {
		std::lock_guard lock(mutex_);
		items_.push_back(std::move(v));
	}

	void drain(std::vector<T>& out) {
		out.clear();
		std::lock_guard lock(mutex_);
		while (!items_.empty()) {
			out.push_back(std::move(items_.front()));
			items_.pop_front();
		}
	}

private:
	std::mutex mutex_;
	std::deque<T> items_;
};

}  // namespace collector
