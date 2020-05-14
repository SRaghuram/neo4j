#!/usr/bin/python3
#
# neosyscount   Summarize syscall counts and latencies for neo4j.
#
# USAGE: neosyscount [-p PID] [-i INTERVAL] [-T TOP] [-x] [-L] [-m]
# created based on syscount bcc tool created by Sasha Goldshtein
#

import argparse
import signal
from subprocess import check_output, CalledProcessError
from time import sleep, strftime

from bcc import BPF
from bcc.syscall import syscall_name
from bcc.utils import printb


# signal handler
def signal_handler(signal, frame):
    print()


def find_neo_pid():
    try:
        entries = list(map(int, check_output(["pgrep", "-f", ".EntryPoint"]).split()))
        if len(entries) > 1:
            raise ValueError("Detected more then 1 processes which looks like neo4j entry point. "
                         "Please provide specific process PID.")
        return entries[0]
    except CalledProcessError as e:
        print("Neo4j process not found. Please ensure you have neo4j running or pass PID explicitly.")
        exit()


parser = argparse.ArgumentParser(
    description="Summarize syscall performed by neo4j counts and latencies.")
parser.add_argument("-p", "--pid", type=int, help="Provide neo4j PID if default lookup fails")
parser.add_argument("-i", "--interval", type=int,
                    help="print summary at this interval (seconds)")
parser.add_argument("-d", "--duration", type=int,
                    help="total duration of trace, in seconds")
parser.add_argument("-T", "--top", type=int, default=100,
                    help="print only the top syscalls by count or latency")
parser.add_argument("-x", "--failures", action="store_true",
                    help="trace only failed syscalls (return < 0)")
parser.add_argument("-L", "--latency", action="store_true",
                    help="collect syscall latency")
parser.add_argument("-m", "--milliseconds", action="store_true",
                    help="display latency in milliseconds (default: microseconds)")
parser.add_argument("--ebpf", action="store_true",
                    help=argparse.SUPPRESS)
args = parser.parse_args()
if args.duration and not args.interval:
    args.interval = args.duration
if not args.interval:
    args.interval = 99999999
if not args.pid:
    args.pid = find_neo_pid()

text = """
#ifdef LATENCY
struct data_t {
    u64 count;
    u64 total_ns;
};

BPF_HASH(start, u64, u64);
BPF_HASH(data, u32, struct data_t);
#else
BPF_HASH(data, u32, u64);
#endif

#ifdef LATENCY
TRACEPOINT_PROBE(raw_syscalls, sys_enter) {
    u64 pid_tgid = bpf_get_current_pid_tgid();

#ifdef FILTER_PID
    if (pid_tgid >> 32 != FILTER_PID)
        return 0;
#endif

    u64 t = bpf_ktime_get_ns();
    start.update(&pid_tgid, &t);
    return 0;
}
#endif

TRACEPOINT_PROBE(raw_syscalls, sys_exit) {
    u64 pid_tgid = bpf_get_current_pid_tgid();

#ifdef FILTER_PID
    if (pid_tgid >> 32 != FILTER_PID)
        return 0;
#endif

#ifdef FILTER_FAILED
    if (args->ret >= 0)
        return 0;
#endif

u32 key = args->id;

#ifdef LATENCY
    struct data_t *val, zero = {};
    u64 *start_ns = start.lookup(&pid_tgid);
    if (!start_ns)
        return 0;

    val = data.lookup_or_try_init(&key, &zero);
    if (val) {
        val->count++;
        val->total_ns += bpf_ktime_get_ns() - *start_ns;
    }
#else
    u64 *val, zero = 0;
    val = data.lookup_or_try_init(&key, &zero);
    if (val) {
        ++(*val);
    }
#endif
    return 0;
}
"""

text = ("#define FILTER_PID %d\n" % args.pid) + text
if args.failures:
    text = "#define FILTER_FAILED\n" + text
if args.latency:
    text = "#define LATENCY\n" + text
if args.ebpf:
    print(text)
    exit()

bpf = BPF(text=text)

agg_colname = "SYSCALL"
time_colname = "TIME (ms)" if args.milliseconds else "TIME (us)"


def print_stats():
    if args.latency:
        print_latency_stats()
    else:
        print_count_stats()


def print_count_stats():
    data = bpf["data"]
    print("[%s]" % strftime("%H:%M:%S"))
    print("%-22s %8s" % (agg_colname, "COUNT"))
    for k, v in sorted(data.items(), key=lambda kv: -kv[1].value)[:args.top]:
        if k.value == 0xFFFFFFFF:
            continue  # happens occasionally, we don't need it
        printb(b"%-22s %8d" % (agg_colval(k), v.value))
    print("")
    data.clear()


def print_latency_stats():
    data = bpf["data"]
    print("[%s]" % strftime("%H:%M:%S"))
    print("%-22s %8s %16s" % (agg_colname, "COUNT", time_colname))
    for k, v in sorted(data.items(),
                       key=lambda kv: -kv[1].total_ns)[:args.top]:
        if k.value == 0xFFFFFFFF:
            continue  # happens occasionally, we don't need it
        printb((b"%-22s %8d " + (b"%16.6f" if args.milliseconds else b"%16.3f")) %
               (agg_colval(k), v.count,
                v.total_ns / (1e6 if args.milliseconds else 1e3)))
    print("")
    data.clear()


def comm_for_pid(pid):
    try:
        return open("/proc/%d/comm" % pid, "rb").read().strip()
    except Exception:
        return b"[unknown]"


def agg_colval(key):
    return syscall_name(key.value)


print("Attaching to neo4j with PID: %d" % args.pid)
print("Tracing %ssyscalls, printing top %d... Ctrl+C to quit." %
      ("failed " if args.failures else "", args.top))
exiting = 0 if args.interval else 1
seconds = 0
while True:
    try:
        sleep(args.interval)
        seconds += args.interval
    except KeyboardInterrupt:
        exiting = 1
        signal.signal(signal.SIGINT, signal_handler)
    if args.duration and seconds >= args.duration:
        exiting = 1

    print_stats()

    if exiting:
        print("Detaching...")
        exit()
