#ifndef GPU_CR_SRC_CTL_PATH_H_
#define GPU_CR_SRC_CTL_PATH_H_

// The control plane (control-<pid>, ctl-ready-<pid>, pid_map_*,
// the id counter) moves off hugetlbfs onto a tmpfs, so no hugetlb page is
// ever faulted from the coordinating process's cgroup. Dump/staging DATA
// files stay in EXPORT_FILE_PATH untouched.
//
// The tmpfs is found without ANY workload-side configuration: the
// coordinator mounts it at <data dir>/ctl on the host, inside the dump
// store the workload already mounts, so the .so discovers it through the
// volume it has anyway. The consumer contract stays exactly LD_PRELOAD +
// EXPORT_FILE_PATH. Resolution order:
//
// GPU_CR_CTL_PATH set, tmpfs       -> ctl mode with that dir (coordinator-
//     side override; kept for compat, never required on the consumer).
// GPU_CR_CTL_PATH set, NOT tmpfs   -> legacy fallback, loud warning.
//     Explicit config never silently redirects: discovery is NOT consulted
//     (the .so must never write an advertisement into a disk-backed dir
//     that a later tmpfs mount would shadow); cr_client refuses instead,
//     so the misconfiguration surfaces on the coordinator side.
// GPU_CR_CTL_PATH unset, <data dir>/ctl tmpfs-backed -> ctl mode
//     (zero-config discovery). The statfs gate reports the CONTAINING
//     filesystem: a plain "ctl" subdir inside a store that is itself
//     tmpfs also enables ctl mode — intended, the control plane lands
//     where legacy mode would have put it anyway.
// otherwise                        -> legacy: ctl dir == data dir.
//
// CtlDir() memoizes the resolution on first use. In the .so that first
// use is the ELF-constructor advertisement block (it runs before init_CR),
// so the snapshot is taken exactly when the advertisement is written and
// one process can never straddle two layouts, even if a tmpfs appears
// mid-life. A tmpfs mounted after that point is simply not seen; the
// coordinator's advertisement gate turns the mixed-order case into a loud
// refusal, same contract as the env-based mount-order rule.

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/vfs.h>
#include <unistd.h>

namespace gpu_cr {

inline constexpr unsigned long kTmpfsMagic = 0x01021994UL;

// Control-channel protocol level advertised by the .so and required by
// cr_client. Proto >= 3 implies readiness advertisements and
// destination-path support.
inline constexpr int kCtlProto = 3;

// Subdirectory of the data dir probed for the zero-config control plane.
// Reserved: nothing else may claim <data dir>/ctl (dumps are files at the
// store root, destination slots live under <data dir>/groups, and the GC
// sweeper skips directories in the store root).
inline constexpr char kCtlSubdir[] = "ctl";

constexpr size_t RoundUp4K(size_t x) { return (x + 4095UL) & ~4095UL; }

// Directory holding dump/staging DATA files (unchanged by the ctl split).
inline const char* DataDir() {
  const char* dir = getenv("EXPORT_FILE_PATH");
  return (dir && dir[0]) ? dir : "/mnt/huge-ckpt";
}

inline bool DirIsTmpfs(const char* dir) {
  struct statfs sfs;
  if (statfs(dir, &sfs) != 0) return false;
  return static_cast<unsigned long>(sfs.f_type) == kTmpfsMagic;
}

// Writes "<DataDir()>/ctl" into buf. Returns false when it does not fit.
inline bool CtlCandidatePath(char* buf, size_t n) {
  int len = snprintf(buf, n, "%s/%s", DataDir(), kCtlSubdir);
  return len > 0 && static_cast<size_t>(len) < n;
}

// A resolved control-plane directory. dir is a copy, not a pointer into
// the environment: the cache below outlives any putenv/setenv churn.
// 480, not 512: every caller formats "<dir>/<name>-<pid>" into a 512-byte
// name buffer, and bounding the dir keeps those snprintfs provably
// truncation-free (-Wformat-truncation) with headroom for the longest
// name pattern. A dir this long could not have produced usable control
// paths before either. A configured GPU_CR_CTL_PATH that does not fit is
// treated as INVALID (warn + legacy, cr_client refuses) — never statfs'd
// unclamped, so a real tmpfs at an unaddressable path can't silently
// enable ctl mode on a truncated dir. The discovery candidate and the
// legacy data dir clamp the same way; a clamped legacy dir then simply
// fails open() exactly like any other wrong path.
inline constexpr size_t kCtlDirMax = 480;

struct CtlResolution {
  bool ctl_mode = false;
  char dir[kCtlDirMax] = "";
};

// Pure resolver (no caching — one statfs per call): applies the header's
// resolution order. Unit tests target this directly; production code goes
// through the memoizing CtlDir() below.
inline void ResolveCtlDir(CtlResolution* out) {
  out->ctl_mode = false;
  const char* env = getenv("GPU_CR_CTL_PATH");
  if (env && env[0]) {
    // Copy/clamp BEFORE the statfs: validating the unclamped path would
    // enable ctl mode on a dir the callers cannot address. A path that
    // does not fit is invalid, same as missing or non-tmpfs.
    int len = snprintf(out->dir, sizeof(out->dir), "%s", env);
    bool fits = len > 0 && static_cast<size_t>(len) < sizeof(out->dir);
    if (fits && DirIsTmpfs(out->dir)) {
      out->ctl_mode = true;
      return;
    }
    fprintf(stderr,
            "[gpu-cr] GPU_CR_CTL_PATH=%s missing, not tmpfs, or too long; "
            "using legacy control dir %s\n",
            env, DataDir());
    snprintf(out->dir, sizeof(out->dir), "%s", DataDir());
    return;
  }
  if (CtlCandidatePath(out->dir, sizeof(out->dir)) && DirIsTmpfs(out->dir)) {
    out->ctl_mode = true;
    return;
  }
  snprintf(out->dir, sizeof(out->dir), "%s", DataDir());
}

namespace internal {
struct CtlCache {
  bool resolved = false;
  CtlResolution res;
};
inline CtlCache& GetCtlCache() {
  static CtlCache cache;
  return cache;
}
}  // namespace internal

// Resolves the control-plane directory, memoized on first call (see the
// header comment for why the snapshot matters). Sets *ctl_mode to true
// when a tmpfs-backed control dir is in effect, false for legacy mode.
// Not locked: the first call happens before any threads exist (the .so's
// ELF constructor; cr_client's main), later calls only read.
inline const char* CtlDir(bool* ctl_mode) {
  internal::CtlCache& cache = internal::GetCtlCache();
  if (!cache.resolved) {
    ResolveCtlDir(&cache.res);
    cache.resolved = true;
  }
  if (ctl_mode) *ctl_mode = cache.res.ctl_mode;
  return cache.res.dir;
}

// Unit-test hook: a gtest binary drives resolution under several layouts
// in one process, which the memoization would otherwise pin to the first.
inline void ResetCtlDirCacheForTests() {
  internal::GetCtlCache().resolved = false;
}

// Extracts starttime (field 22 of /proc/<pid>/stat) from a stat line.
// Parses backwards from the last ')' because the comm field may contain
// spaces and parentheses. Returns -1 if the buffer is not a stat line.
inline long long ParseStarttimeFromStat(const char* stat_line) {
  const char* p = strrchr(stat_line, ')');
  if (!p || p[1] == '\0') return -1;
  p += 2;
  char buf[4096];
  strncpy(buf, p, sizeof(buf) - 1);
  buf[sizeof(buf) - 1] = '\0';
  long long val = -1;
  int field = 3;
  char* save = nullptr;
  for (char* tok = strtok_r(buf, " ", &save); tok;
       tok = strtok_r(nullptr, " ", &save), field++) {
    if (field == 22) {
      char* end = nullptr;
      val = strtoll(tok, &end, 10);
      if (end == tok) val = -1;  // no digits consumed: not a stat line
      break;
    }
  }
  return val;
}

// starttime of a live PID: the PID-reuse guard for the readiness
// advertisement (a recycled PID gets a new starttime, and signaling an
// innocent recycled PID is termination by default). Returns -1 when the
// PID does not exist.
inline long long ProcStarttime(pid_t pid) {
  char path[64];
  char buf[4096];
  snprintf(path, sizeof(path), "/proc/%d/stat", pid);
  FILE* f = fopen(path, "r");
  if (!f) return -1;
  size_t n = fread(buf, 1, sizeof(buf) - 1, f);
  fclose(f);
  if (n == 0) return -1;
  buf[n] = '\0';
  return ParseStarttimeFromStat(buf);
}

// Readiness advertisement written by the .so at load and read back by
// cr_client before it signals; carries the buffer-sizing keys so the
// agent can observe workload sizing.
struct Advertisement {
  int proto = 0;
  long long starttime = -1;
  char ctl[512] = "";
};

// Parses "proto=<n> starttime=<n> ctl=<path> ..." (unknown trailing keys
// ignored). The sscanf format is an ordered prefix match: only a suffix of
// the fields may be absent — a missing middle field stops the scan there
// and every later field keeps its default. %511s also means the ctl path
// must not contain whitespace. Returns false only for an empty/absent
// proto line.
inline bool ParseAdvertisement(const char* line, Advertisement* out) {
  if (!line || !line[0]) return false;
  return sscanf(line, "proto=%d starttime=%lld ctl=%511s", &out->proto,
                &out->starttime, out->ctl) >= 1;
}

// Writes the ctl-ready-<pid> advertisement, including the buffer-sizing
// keys so the agent can observe workload sizing.
// Called from the .so's constructor — failures are reported, never fatal:
// this must not take down the workload. Returns false on write failure.
inline bool WriteAdvertisement(const char* ctl_dir, pid_t pid, size_t shm_mb,
                               size_t staging_mb, bool deferred) {
  char ready_path[512];
  char content[600];
  // Both snprintf results are clamped BEFORE open: a truncated advert must
  // never be written (O_TRUNC would blank a good pre-existing one), and
  // snprintf returns the would-be length, not the written length.
  int path_len = snprintf(ready_path, sizeof(ready_path), "%s/ctl-ready-%d",
                          ctl_dir, static_cast<int>(pid));
  if (path_len < 0 || path_len >= static_cast<int>(sizeof(ready_path))) {
    fprintf(stderr, "[vGPU] WARNING: ctl dir too long for advert path (%s)\n",
            ctl_dir);
    return false;
  }
  int content_len = snprintf(
      content, sizeof(content),
      "proto=%d starttime=%lld ctl=%s shm_mb=%zu staging_mb=%zu deferred=%d\n",
      kCtlProto, ProcStarttime(pid), ctl_dir, shm_mb, staging_mb,
      deferred ? 1 : 0);
  if (content_len < 0 || content_len >= static_cast<int>(sizeof(content))) {
    fprintf(stderr, "[vGPU] WARNING: advert line truncated for %s; not written\n",
            ready_path);
    return false;
  }
  int fd = open(ready_path, O_CREAT | O_TRUNC | O_WRONLY | O_CLOEXEC, 0644);
  if (fd < 0) {
    fprintf(stderr, "[vGPU] WARNING: cannot write %s (%s) — cr_client will "
                    "refuse ops\n",
            ready_path, strerror(errno));
    return false;
  }
  bool ok = write(fd, content, content_len) == content_len;
  if (!ok)
    fprintf(stderr, "[vGPU] WARNING: short write to %s (%s)\n", ready_path,
            strerror(errno));
  close(fd);
  return ok;
}

}  // namespace gpu_cr

#endif  // GPU_CR_SRC_CTL_PATH_H_
