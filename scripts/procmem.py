#!/usr/bin/env python3
"""Print RSS (MiB) and thread count for PIDs without needing `ps`.

macOS: libproc `proc_pidinfo(PROC_PIDTASKINFO)`.
Linux: /proc/<pid>/status.

Usage:
  procmem.py PID [PID ...]
Output:
  "<first_pid_rss_mb> <first_pid_threads> <sum_rss_mb> <successful_pid_count>"
"""

import ctypes
import os
import sys


class _ProcTaskInfo(ctypes.Structure):
    _fields_ = [
        ("virtual_size", ctypes.c_uint64),
        ("resident_size", ctypes.c_uint64),
        ("total_user", ctypes.c_uint64),
        ("total_system", ctypes.c_uint64),
        ("threads_user", ctypes.c_uint64),
        ("threads_system", ctypes.c_uint64),
        ("policy", ctypes.c_int32),
        ("faults", ctypes.c_int32),
        ("pageins", ctypes.c_int32),
        ("cow_faults", ctypes.c_int32),
        ("messages_sent", ctypes.c_int32),
        ("messages_received", ctypes.c_int32),
        ("syscalls_mach", ctypes.c_int32),
        ("syscalls_unix", ctypes.c_int32),
        ("csw", ctypes.c_int32),
        ("threadnum", ctypes.c_int32),
        ("numrunning", ctypes.c_int32),
        ("priority", ctypes.c_int32),
    ]


def _macos_libproc():
    libproc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    return libproc


def read_linux(pid):
    try:
        with open(f"/proc/{pid}/status", encoding="utf-8") as fh:
            rss_kb = threads = None
            for line in fh:
                if line.startswith("VmRSS:"):
                    rss_kb = int(line.split()[1])
                elif line.startswith("Threads:"):
                    threads = int(line.split()[1])
            if rss_kb is None:
                return None
            return rss_kb / 1024.0, threads or 0
    except OSError:
        return None


def read_macos(libproc, pid):
    info = _ProcTaskInfo()
    written = libproc.proc_pidinfo(
        int(pid), 4, 0, ctypes.byref(info), ctypes.sizeof(info)
    )
    if written != ctypes.sizeof(info):
        return None
    return info.resident_size / 1048576.0, info.threadnum


def main():
    pids = [int(value) for value in sys.argv[1:]]
    if not pids:
        print("0 0 0 0")
        return
    if os.path.exists("/proc/self/status"):
        values = [read_linux(pid) for pid in pids]
    else:
        libproc = _macos_libproc()
        values = [read_macos(libproc, pid) for pid in pids]
    values = [value for value in values if value]
    if not values:
        print("0 0 0 0")
        return
    total = sum(value[0] for value in values)
    print(f"{values[0][0]:.1f} {values[0][1]} {total:.1f} {len(values)}")


if __name__ == "__main__":
    main()
