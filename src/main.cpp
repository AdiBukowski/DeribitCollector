// Deribit L2 option-chain recorder.
//
// Records book.{instrument}.{interval} (snapshot + change_id deltas) for every
// option and future of the selected underlyings, both inverse (BTC, ETH) and
// linear USDC (BTC_USDC, ETH_USDC, SOL_USDC), plus trades. Output: hourly
// zstd-compressed JSONL per group under <out>/deribit/<group>/<date>/<HH>.jsonl.zst.
//
// Usage:
//   deribit_collector [--out dir] [--bases BTC,ETH,SOL] [--interval 100ms] [--per-conn 400]
//                     [--zstd 3] [--refresh 600] [--flush-seconds 1] [--stats-path file]
//                     [--no-trades] [--test] [--cafile path]

#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>

#include <ixwebsocket/IXNetSystem.h>

#include "control.hpp"
#include "util.hpp"
#include "writer.hpp"

namespace {

std::atomic<bool> g_running{true};

void on_signal(int) { g_running.store(false, std::memory_order_release); }

void usage() {
	std::fprintf(stderr,
				 "usage: deribit_collector [--out dir] [--bases BTC,ETH,SOL] [--interval 100ms] [--per-conn N]\n"
				 "                         [--zstd level] [--refresh seconds] [--flush-seconds N] [--stats-path file]\n"
								  "                         [--no-trades] [--test] [--cafile path]\n");
}

}  // namespace

int main(int argc, char* argv[]) {
	using namespace collector;

	ControlConfig ctl;
	WriterConfig wcfg;
	wcfg.out_dir = "data";
	for (int i = 1; i < argc; ++i) {
		const std::string a = argv[i];
		const auto next = [&]() -> std::string {
			if (i + 1 >= argc) {
				usage();
				std::exit(2);
			}
			return argv[++i];
		};
		if (a == "--out") wcfg.out_dir = next();
		else if (a == "--bases") ctl.bases = split(next(), ',');
		else if (a == "--interval") ctl.interval = next();
		else if (a == "--per-conn") ctl.instruments_per_conn = std::stoul(next());
		else if (a == "--zstd") wcfg.zstd_level = std::stoi(next());
		else if (a == "--refresh") ctl.refresh_seconds = std::stoi(next());
		else if (a == "--flush-seconds") wcfg.flush_seconds = std::stoi(next());
		else if (a == "--stats-path") wcfg.stats_path = next();
		else if (a == "--no-trades") ctl.trades = false;
		else if (a == "--test") ctl.host = "test.deribit.com";
		else if (a == "--cafile") ctl.tls.caFile = next();
		else {
			usage();
			return 2;
		}
	}
	if (ctl.interval == "raw") {
		std::fprintf(stderr, "raw channels need an authenticated session; this collector records 100ms/agg2 only\n");
		return 2;
	}
	wcfg.interval = ctl.interval;

	ix::initNetSystem();
	std::signal(SIGINT, on_signal);
	std::signal(SIGTERM, on_signal);

	std::string bases;
	for (const auto& b : ctl.bases) bases += b + " ";
	std::printf("deribit_collector host=%s bases=%sinterval=%s out=%s\n", ctl.host.c_str(), bases.c_str(),
				ctl.interval.c_str(), wcfg.out_dir.string().c_str());
	std::fflush(stdout);

	ConnectionTable conns;
	MutexQueue<WriterCommand> commands;
	MutexQueue<std::string> resync;
	Writer writer(wcfg, conns, commands, resync);
	Control control(ctl, conns, commands, resync);

	std::atomic<bool> writer_running{true};
	std::thread writer_thread([&] { writer.run(writer_running); });

	control.run(g_running);  // returns after stopping all connections
	writer_running.store(false, std::memory_order_release);
	writer_thread.join();

	ix::uninitNetSystem();
	std::printf("stopped\n");
	return 0;
}
