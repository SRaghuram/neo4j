#!/usr/bin/python3
#
# neoio   Neo4j read write monitor
#
# USAGE: neoio [-p PID] [-m]
#

import argparse
import os
import signal
from re import search
from subprocess import check_output, CalledProcessError, Popen, PIPE
from time import strftime

from bcc import BPF


# signal handler
def signal_handler(signal, frame):
    print()


threadNames = {}
fileDescriptors = {}

def reload_threads():
    jstack = Popen([os.environ['JAVA_HOME'] + '/bin/jstack', str(pid)], stdout=PIPE)

    for line in jstack.stdout:
        match = search('^\"(.*?)\" .*nid=(0x[a-f0-9]+)', line.decode('utf-8'))
        if match:
            thread_name = match.group(1)
            tid = int(match.group(2), 0)
            threadNames[tid] = thread_name

    jstack.wait()


def resolve_descriptor(fd):
    link = Popen(['readlink', '-f', '/proc/' + str(pid) + '/fd/' + str(fd)], stdout=PIPE)
    for line in link.stdout:
        basename = os.path.basename(line.decode('utf-8', 'replace')).strip()
        fileDescriptors[fd] = basename
    link.wait()


def find_neo_pid():
    try:
        entries = list(map(int, check_output(["pgrep", "-f", ".EntryPoint"]).split()))
        if len(entries) > 1:
            raise ValueError("Detected more then 1 processes which looks like neo4j entry point. "
                             "Please provide specific process PID.")
        return entries[0]
    except CalledProcessError:
        raise ValueError("Neo4j process not found please provide PID explicitly with -p.")


if not 'JAVA_HOME' in os.environ:
    print("Please define JAVA_HOME env variable to be able to use this tool")
    exit(-1)

parser = argparse.ArgumentParser(
    description="Neo4j IO monitor.")
parser.add_argument("-p", "--pid", type=int, help="Provide neo4j PID if default lookup fails")
parser.add_argument("-m", "--milliseconds", action="store_true",
                    help="display latency in milliseconds (default: microseconds)")
parser.add_argument("--ebpf", action="store_true",
                    help=argparse.SUPPRESS)
args = parser.parse_args()
pid = find_neo_pid() if not args.pid else args.pid
reload_threads()

text = """
#include <uapi/linux/ptrace.h>
#include <linux/blkdev.h>

struct descriptor_t {
    u64 fd;
};

BPF_HASH(descriptors, u64, struct descriptor_t);

struct info_t {
    u64 ts;
    u32 tid;
    u64 sector;
    u64 len;
    u64 fd;
};

struct data_t {
    u32 tid;
    u64 rwflag;
    u64 delta;
    u64 sector;
    u64 len;
    char disk_name[DISK_NAME_LEN];
    u64 fd;
};

BPF_HASH(request_info, struct request *, struct info_t);
BPF_PERF_OUTPUT(data);

int trace_io_start(struct pt_regs *ctx, struct request *req)
{
    struct descriptor_t *descriptorp;
    u64 pid_tgid = bpf_get_current_pid_tgid();
    if (pid_tgid >> 32 != FILTER_PID) {
        return 0;
    }
    descriptorp = descriptors.lookup(&pid_tgid);

    struct info_t val = {};
    val.ts = bpf_ktime_get_ns();
    val.tid = pid_tgid & 0xffffffff;
    val.sector = req->__sector;
    val.len = req->__data_len;
    if (descriptorp != 0) {
        val.fd = descriptorp->fd;
    }
    request_info.update(&req, &val);
    descriptors.delete(&pid_tgid);
    return 0;
}

int trace_req_completion(struct pt_regs *ctx, struct request *req)
{
   struct info_t *infop;
    
   infop = request_info.lookup(&req);
   if (infop == 0) {
       return 0;
   }

   struct data_t result = {};
   result.tid = infop->tid;
   result.delta = (bpf_ktime_get_ns() - infop->ts) / LATENCY;
   result.len = infop->len;
   result.sector = infop->sector;
   struct gendisk *rq_disk = req->rq_disk;
   bpf_probe_read(&result.disk_name, sizeof(result.disk_name), rq_disk->disk_name);
   result.fd = infop->fd;
   result.rwflag = !!((req->cmd_flags & REQ_OP_MASK) == REQ_OP_WRITE);
   
   request_info.delete(&req);
   data.perf_submit(ctx, &result, sizeof(result));
   return 0;
};

static int trace_read_write(u64 fd) {
    u64 pid_tgid = bpf_get_current_pid_tgid();
    if (pid_tgid >> 32 != FILTER_PID) {
        return 0;
    }
    struct descriptor_t desc = {.fd = fd};
    descriptors.update(&pid_tgid, &desc);
    return 0;
};

TRACEPOINT_PROBE(syscalls, sys_enter_readv) {
    return trace_read_write(args->fd);
};

TRACEPOINT_PROBE(syscalls, sys_enter_read) {
    return trace_read_write(args->fd);
};

TRACEPOINT_PROBE(syscalls, sys_enter_preadv) {
    return trace_read_write(args->fd);
};

TRACEPOINT_PROBE(syscalls, sys_enter_pread64) {
    return trace_read_write(args->fd);
};

TRACEPOINT_PROBE(syscalls, sys_enter_preadv2) {
    return trace_read_write(args->fd);
};

TRACEPOINT_PROBE(syscalls, sys_enter_pwritev) {
    return trace_read_write(args->fd);
};

TRACEPOINT_PROBE(syscalls, sys_enter_pwrite64) {
    return trace_read_write(args->fd);
};

TRACEPOINT_PROBE(syscalls, sys_enter_pwritev2) {
    return trace_read_write(args->fd);
};

TRACEPOINT_PROBE(syscalls, sys_enter_write) {
    return trace_read_write(args->fd);
};

TRACEPOINT_PROBE(syscalls, sys_enter_writev) {
    return trace_read_write(args->fd);
};

"""

text = ("#define FILTER_PID %d\n" % pid) + text
text = ("#define LATENCY %d\n" % (1000000 if args.milliseconds else 1000)) + text
bpf = BPF(text=text)

bpf.attach_kprobe(event="blk_mq_start_request", fn_name="trace_io_start")
bpf.attach_kprobe(event="blk_account_io_completion", fn_name="trace_req_completion")

time_colname = "Latency (ms)" if args.milliseconds else "Latency (us)"
print("Attaching to neo4j with PID: %d" % pid)
print("Tracing io... Ctrl+C to quit." )
print("%-11s %-40s %-50s %-14s %-6s %-10s %-7s %-7s" % ("TIME(s)", "Thread", "File", "Disk", "Type", "Sector", "Bytes", time_colname))


def lookup_thread(tid):
    if tid in threadNames:
        return threadNames[tid]
    else:
        reload_threads()
        return threadNames.get(tid, tid)


def lookup_file(fd):
    if fd in fileDescriptors:
        return fileDescriptors[fd]
    else:
        resolve_descriptor(fd)
        return fileDescriptors.get(fd, fd)


def process_event(cpu, data, size):
    event = bpf["data"].event(data)
    print("%-11s %-40s %-50s %-14s %-6s %-10s %-7s %-7s" % (
    strftime("%H:%M:%S"),
    lookup_thread(event.tid),
    lookup_file(event.fd),
    event.disk_name.decode('utf-8', 'replace'),
    "Write" if event.rwflag == 1 else "Read",
    event.sector,
    event.len,
    event.delta))


bpf["data"].open_perf_buffer(process_event, page_cnt=16)

while True:
    try:
        bpf.perf_buffer_poll()
    except KeyboardInterrupt:
        signal.signal(signal.SIGINT, signal_handler)
        print("Detaching...")
        exit()
