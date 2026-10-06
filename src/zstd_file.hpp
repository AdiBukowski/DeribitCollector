#pragma once

// Streaming zstd writer. Data goes to "<name>.part" and is renamed to the final
// name on close, so uploaders only ever see complete files.

#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <string_view>
#include <system_error>
#include <vector>

#include <zstd.h>

namespace collector {

class ZstdFile {
public:
	ZstdFile() = default;
	~ZstdFile() { close(); }
	ZstdFile(const ZstdFile&) = delete;
	ZstdFile& operator=(const ZstdFile&) = delete;

	bool open(const std::filesystem::path& part, const std::filesystem::path& final_path, int level) {
		close();
		file_ = std::fopen(part.string().c_str(), "wb");
		if (!file_) return false;
		cctx_ = ZSTD_createCCtx();
		ZSTD_CCtx_setParameter(cctx_, ZSTD_c_compressionLevel, level);
		ZSTD_CCtx_setParameter(cctx_, ZSTD_c_checksumFlag, 1);
		out_.resize(ZSTD_CStreamOutSize());
		part_ = part;
		final_ = final_path;
		bytes_in_ = 0;
		bytes_out_ = 0;
		return true;
	}

	bool is_open() const { return file_ != nullptr; }

	bool write(std::string_view s) {
		if (!file_) return false;
		ZSTD_inBuffer in{s.data(), s.size(), 0};
		while (in.pos < in.size) {
			ZSTD_outBuffer out{out_.data(), out_.size(), 0};
			const size_t r = ZSTD_compressStream2(cctx_, &out, &in, ZSTD_e_continue);
			if (ZSTD_isError(r) || !emit(out)) return false;
		}
		bytes_in_ += s.size();
		return true;
	}

	// Pushes buffered data to disk; a crash then loses at most the last interval.
	bool flush() { return file_ && finish(ZSTD_e_flush) && std::fflush(file_) == 0; }

	bool close() {
		if (!file_) return true;
		bool ok = finish(ZSTD_e_end);
		ok = std::fclose(file_) == 0 && ok;
		file_ = nullptr;
		ZSTD_freeCCtx(cctx_);
		cctx_ = nullptr;
		if (ok) {
			std::error_code ec;
			std::filesystem::rename(part_, final_, ec);
			ok = !ec;
		}
		return ok;
	}

	uint64_t bytes_in() const { return bytes_in_; }
	uint64_t bytes_out() const { return bytes_out_; }
	const std::filesystem::path& final_path() const { return final_; }

private:
	bool finish(ZSTD_EndDirective mode) {
		ZSTD_inBuffer in{nullptr, 0, 0};
		size_t remaining = 0;
		do {
			ZSTD_outBuffer out{out_.data(), out_.size(), 0};
			remaining = ZSTD_compressStream2(cctx_, &out, &in, mode);
			if (ZSTD_isError(remaining) || !emit(out)) return false;
		} while (remaining != 0);
		return true;
	}

	bool emit(const ZSTD_outBuffer& out) {
		if (out.pos == 0) return true;
		bytes_out_ += out.pos;
		return std::fwrite(out.dst, 1, out.pos, file_) == out.pos;
	}

	std::FILE* file_ = nullptr;
	ZSTD_CCtx* cctx_ = nullptr;
	std::vector<char> out_;
	std::filesystem::path part_;
	std::filesystem::path final_;
	uint64_t bytes_in_ = 0;
	uint64_t bytes_out_ = 0;
};

}  // namespace collector
