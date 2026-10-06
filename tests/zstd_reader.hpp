#pragma once

#include <filesystem>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>
#include <zstd.h>

namespace collector::testing {

// Decodes a (possibly still-open, flushed) zstd stream file.
inline std::string decompress(const std::filesystem::path& p) {
	std::ifstream in(p, std::ios::binary);
	const std::string src((std::istreambuf_iterator<char>(in)), {});
	std::string out;
	ZSTD_DStream* ds = ZSTD_createDStream();
	ZSTD_inBuffer zin{src.data(), src.size(), 0};
	std::vector<char> buf(ZSTD_DStreamOutSize());
	while (zin.pos < zin.size) {
		ZSTD_outBuffer zout{buf.data(), buf.size(), 0};
		const size_t r = ZSTD_decompressStream(ds, &zout, &zin);
		if (ZSTD_isError(r)) {
			ZSTD_freeDStream(ds);
			throw std::runtime_error(ZSTD_getErrorName(r));
		}
		out.append(buf.data(), zout.pos);
	}
	ZSTD_freeDStream(ds);
	return out;
}

inline std::vector<nlohmann::json> read_records(const std::filesystem::path& p) {
	std::vector<nlohmann::json> out;
	const std::string text = decompress(p);
	size_t start = 0;
	while (start < text.size()) {
		const size_t nl = text.find('\n', start);
		const size_t end = nl == std::string::npos ? text.size() : nl;
		if (end > start) out.push_back(nlohmann::json::parse(text.substr(start, end - start)));
		start = end + 1;
	}
	return out;
}

}  // namespace collector::testing
