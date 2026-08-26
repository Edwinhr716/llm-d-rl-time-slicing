// Control-plane path resolution (env override, zero-config discovery,
// memoization), /proc/<pid>/stat parsing, and readiness-advertisement
// round trip. Linux-only (statfs, /proc).

#include "ctl_path.h"

#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include <string>

#include "gtest/gtest.h"

namespace gpu_cr {
namespace {

class CtlPathTest : public ::testing::Test {
 protected:
  void SetUp() override {
    unsetenv("GPU_CR_CTL_PATH");
    unsetenv("EXPORT_FILE_PATH");
    ResetCtlDirCacheForTests();
  }

  // A data dir on tmpfs: a plain "ctl" subdir inside it satisfies the
  // containing-filesystem statfs gate, which is exactly the documented
  // discovery predicate — no mount needed in a unit test.
  std::string MakeTmpfsDataDir() {
    char tmpl[] = "/dev/shm/gpu-cr-ctl-path-XXXXXX";
    if (!mkdtemp(tmpl)) return "";
    return tmpl;
  }

  void RemoveDataDir(const std::string& dir) {
    rmdir((dir + "/" + kCtlSubdir).c_str());
    rmdir(dir.c_str());
  }
};

TEST_F(CtlPathTest, RoundUp4K) {
  EXPECT_EQ(RoundUp4K(0), 0u);
  EXPECT_EQ(RoundUp4K(1), 4096u);
  EXPECT_EQ(RoundUp4K(4096), 4096u);
  EXPECT_EQ(RoundUp4K(4097), 8192u);
}

TEST_F(CtlPathTest, DataDirDefaultsToHugeCkpt) {
  EXPECT_STREQ(DataDir(), "/mnt/huge-ckpt");
}

TEST_F(CtlPathTest, DataDirHonorsExportFilePath) {
  setenv("EXPORT_FILE_PATH", "/some/dir", 1);
  EXPECT_STREQ(DataDir(), "/some/dir");
}

TEST_F(CtlPathTest, EmptyExportFilePathTreatedAsUnset) {
  setenv("EXPORT_FILE_PATH", "", 1);
  EXPECT_STREQ(DataDir(), "/mnt/huge-ckpt");
}

TEST_F(CtlPathTest, CtlCandidatePathIsDataDirSlashCtl) {
  setenv("EXPORT_FILE_PATH", "/some/dir", 1);
  char buf[512];
  ASSERT_TRUE(CtlCandidatePath(buf, sizeof(buf)));
  EXPECT_STREQ(buf, "/some/dir/ctl");
}

TEST_F(CtlPathTest, CtlCandidatePathRejectsTruncation) {
  setenv("EXPORT_FILE_PATH", "/some/dir", 1);
  char buf[8];
  EXPECT_FALSE(CtlCandidatePath(buf, sizeof(buf)));
}

TEST_F(CtlPathTest, CtlDirUnsetIsLegacyMode) {
  bool ctl_mode = true;
  EXPECT_STREQ(CtlDir(&ctl_mode), "/mnt/huge-ckpt");
  EXPECT_FALSE(ctl_mode);
}

TEST_F(CtlPathTest, CtlDirTmpfsEnablesCtlMode) {
  if (!DirIsTmpfs("/dev/shm")) GTEST_SKIP() << "/dev/shm is not tmpfs here";
  setenv("GPU_CR_CTL_PATH", "/dev/shm", 1);
  bool ctl_mode = false;
  EXPECT_STREQ(CtlDir(&ctl_mode), "/dev/shm");
  EXPECT_TRUE(ctl_mode);
}

TEST_F(CtlPathTest, CtlDirNonTmpfsFallsBackToLegacy) {
  if (DirIsTmpfs("/")) GTEST_SKIP() << "/ is tmpfs here";
  setenv("GPU_CR_CTL_PATH", "/", 1);
  setenv("EXPORT_FILE_PATH", "/data/dir", 1);
  bool ctl_mode = true;
  EXPECT_STREQ(CtlDir(&ctl_mode), "/data/dir");
  EXPECT_FALSE(ctl_mode);
}

// Empty means "unset", same as EXPORT_FILE_PATH — silent legacy, no
// misconfiguration warning (discovery still probes, finds nothing here).
TEST_F(CtlPathTest, EmptyCtlPathTreatedAsUnset) {
  setenv("GPU_CR_CTL_PATH", "", 1);
  bool ctl_mode = true;
  EXPECT_STREQ(CtlDir(&ctl_mode), "/mnt/huge-ckpt");
  EXPECT_FALSE(ctl_mode);
}

TEST_F(CtlPathTest, CtlDirMissingPathFallsBackToLegacy) {
  setenv("GPU_CR_CTL_PATH", "/nonexistent-gpu-cr-test-dir", 1);
  bool ctl_mode = true;
  EXPECT_STREQ(CtlDir(&ctl_mode), "/mnt/huge-ckpt");
  EXPECT_FALSE(ctl_mode);
}

TEST_F(CtlPathTest, DirIsTmpfsFalseForMissingDir) {
  EXPECT_FALSE(DirIsTmpfs("/nonexistent-gpu-cr-test-dir"));
}

// Zero-config discovery: env unset, <data>/ctl tmpfs-backed -> ctl mode.
// This is the whole feature: the consumer sets nothing beyond
// EXPORT_FILE_PATH.
TEST_F(CtlPathTest, DiscoveryFindsTmpfsCtlSubdir) {
  if (!DirIsTmpfs("/dev/shm")) GTEST_SKIP() << "/dev/shm is not tmpfs here";
  std::string data = MakeTmpfsDataDir();
  ASSERT_FALSE(data.empty());
  setenv("EXPORT_FILE_PATH", data.c_str(), 1);
  ASSERT_EQ(mkdir((data + "/" + kCtlSubdir).c_str(), 0777), 0);

  CtlResolution res;
  ResolveCtlDir(&res);
  EXPECT_TRUE(res.ctl_mode);
  EXPECT_EQ(std::string(res.dir), data + "/" + kCtlSubdir);

  RemoveDataDir(data);
}

// No "ctl" entry in the store -> legacy, even on a tmpfs store.
TEST_F(CtlPathTest, DiscoveryNeedsCtlSubdir) {
  if (!DirIsTmpfs("/dev/shm")) GTEST_SKIP() << "/dev/shm is not tmpfs here";
  std::string data = MakeTmpfsDataDir();
  ASSERT_FALSE(data.empty());
  setenv("EXPORT_FILE_PATH", data.c_str(), 1);

  CtlResolution res;
  ResolveCtlDir(&res);
  EXPECT_FALSE(res.ctl_mode);
  EXPECT_EQ(std::string(res.dir), data);

  RemoveDataDir(data);
}

// A "ctl" subdir on a NON-tmpfs store (stray mkdir on hugetlbfs/disk)
// must not enable ctl mode.
TEST_F(CtlPathTest, DiscoveryIgnoresNonTmpfsCtlSubdir) {
  char tmpl[] = "/tmp/gpu-cr-ctl-path-XXXXXX";
  ASSERT_NE(mkdtemp(tmpl), nullptr);
  std::string data = tmpl;
  if (DirIsTmpfs(data.c_str())) {
    RemoveDataDir(data);
    GTEST_SKIP() << "/tmp is tmpfs here";
  }
  setenv("EXPORT_FILE_PATH", data.c_str(), 1);
  ASSERT_EQ(mkdir((data + "/" + kCtlSubdir).c_str(), 0777), 0);

  CtlResolution res;
  ResolveCtlDir(&res);
  EXPECT_FALSE(res.ctl_mode);
  EXPECT_EQ(std::string(res.dir), data);

  RemoveDataDir(data);
}

// A valid env override beats discovery: the coordinator's explicit
// configuration is authoritative.
TEST_F(CtlPathTest, ValidEnvWinsOverDiscovery) {
  if (!DirIsTmpfs("/dev/shm")) GTEST_SKIP() << "/dev/shm is not tmpfs here";
  std::string data = MakeTmpfsDataDir();
  ASSERT_FALSE(data.empty());
  setenv("EXPORT_FILE_PATH", data.c_str(), 1);
  ASSERT_EQ(mkdir((data + "/" + kCtlSubdir).c_str(), 0777), 0);
  setenv("GPU_CR_CTL_PATH", "/dev/shm", 1);

  CtlResolution res;
  ResolveCtlDir(&res);
  EXPECT_TRUE(res.ctl_mode);
  EXPECT_STREQ(res.dir, "/dev/shm");

  RemoveDataDir(data);
}

// An INVALID env override never falls through to discovery: explicit
// config must not silently redirect (cr_client refuses in this state, and
// the .so must agree with that refusal by staying legacy).
TEST_F(CtlPathTest, InvalidEnvNeverDiscovers) {
  if (!DirIsTmpfs("/dev/shm")) GTEST_SKIP() << "/dev/shm is not tmpfs here";
  std::string data = MakeTmpfsDataDir();
  ASSERT_FALSE(data.empty());
  setenv("EXPORT_FILE_PATH", data.c_str(), 1);
  ASSERT_EQ(mkdir((data + "/" + kCtlSubdir).c_str(), 0777), 0);
  setenv("GPU_CR_CTL_PATH", "/nonexistent-gpu-cr-test-dir", 1);

  CtlResolution res;
  ResolveCtlDir(&res);
  EXPECT_FALSE(res.ctl_mode);
  EXPECT_EQ(std::string(res.dir), data);

  RemoveDataDir(data);
}

// CtlDir pins the first resolution for the life of the process: a tmpfs
// appearing later must not split one process across two layouts.
TEST_F(CtlPathTest, CtlDirMemoizesFirstResolution) {
  if (!DirIsTmpfs("/dev/shm")) GTEST_SKIP() << "/dev/shm is not tmpfs here";
  std::string data = MakeTmpfsDataDir();
  ASSERT_FALSE(data.empty());
  setenv("EXPORT_FILE_PATH", data.c_str(), 1);

  bool ctl_mode = true;
  EXPECT_EQ(std::string(CtlDir(&ctl_mode)), data);  // resolves: legacy
  EXPECT_FALSE(ctl_mode);

  // The candidate appears AFTER first use: still legacy.
  ASSERT_EQ(mkdir((data + "/" + kCtlSubdir).c_str(), 0777), 0);
  EXPECT_EQ(std::string(CtlDir(&ctl_mode)), data);
  EXPECT_FALSE(ctl_mode);

  // Only a fresh process (here: the test reset) re-resolves.
  ResetCtlDirCacheForTests();
  EXPECT_EQ(std::string(CtlDir(&ctl_mode)), data + "/" + kCtlSubdir);
  EXPECT_TRUE(ctl_mode);

  RemoveDataDir(data);
}

// stat-line fields: pid (comm) state then 18 numbers with the 19th
// post-state field (field 22 overall) being starttime.
TEST_F(CtlPathTest, ParseStarttimeFromStat) {
  const char* line =
      "7 (proc) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 9999 555";
  EXPECT_EQ(ParseStarttimeFromStat(line), 9999);
}

// comm may contain spaces and parentheses; parsing anchors on the LAST ')'.
TEST_F(CtlPathTest, ParseStarttimeHandlesHostileComm) {
  const char* line =
      "7 (we ird) (name) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 "
      "4242 555";
  EXPECT_EQ(ParseStarttimeFromStat(line), 4242);
}

TEST_F(CtlPathTest, ParseStarttimeRejectsGarbage) {
  EXPECT_EQ(ParseStarttimeFromStat("not a stat line"), -1);
  EXPECT_EQ(ParseStarttimeFromStat("7 (proc) S 1 2 3"), -1);  // too few fields
  EXPECT_EQ(ParseStarttimeFromStat(""), -1);
}

// A non-numeric field 22 is -1, not a plausible-looking 0.
TEST_F(CtlPathTest, ParseStarttimeRejectsNonNumericField) {
  const char* line =
      "7 (proc) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 zzz 555";
  EXPECT_EQ(ParseStarttimeFromStat(line), -1);
}

TEST_F(CtlPathTest, ProcStarttimeOfSelfIsPositive) {
  EXPECT_GT(ProcStarttime(getpid()), 0);
}

TEST_F(CtlPathTest, ProcStarttimeOfMissingPidIsNegative) {
  EXPECT_EQ(ProcStarttime(999999999), -1);
}

TEST_F(CtlPathTest, ParseAdvertisement) {
  Advertisement adv;
  ASSERT_TRUE(ParseAdvertisement(
      "proto=3 starttime=12345 ctl=/dev/shm shm_mb=8192 staging_mb=1024 "
      "deferred=0\n",
      &adv));
  EXPECT_EQ(adv.proto, 3);
  EXPECT_EQ(adv.starttime, 12345);
  EXPECT_STREQ(adv.ctl, "/dev/shm");
}

TEST_F(CtlPathTest, ParseAdvertisementRejectsEmpty) {
  Advertisement adv;
  EXPECT_FALSE(ParseAdvertisement("", &adv));
  EXPECT_FALSE(ParseAdvertisement(nullptr, &adv));
}

TEST_F(CtlPathTest, ParseAdvertisementDefaultsWhenFieldsMissing) {
  Advertisement adv;
  ASSERT_TRUE(ParseAdvertisement("proto=2\n", &adv));
  EXPECT_EQ(adv.proto, 2);
  EXPECT_EQ(adv.starttime, -1);
  EXPECT_STREQ(adv.ctl, "");
}

TEST_F(CtlPathTest, WriteAdvertisementFailsOnMissingDir) {
  EXPECT_FALSE(WriteAdvertisement("/nonexistent-gpu-cr-test-dir", getpid(),
                                  8192, 1024, false));
}

TEST_F(CtlPathTest, WriteAdvertisementRoundTrip) {
  char dir_template[] = "/tmp/gpu-cr-ctl-test-XXXXXX";
  ASSERT_NE(mkdtemp(dir_template), nullptr);
  ASSERT_TRUE(WriteAdvertisement(dir_template, getpid(), 8192, 1024, false));

  std::string path =
      std::string(dir_template) + "/ctl-ready-" + std::to_string(getpid());
  FILE* f = fopen(path.c_str(), "r");
  ASSERT_NE(f, nullptr);
  char buf[600] = "";
  ASSERT_NE(fgets(buf, sizeof(buf), f), nullptr);
  fclose(f);

  Advertisement adv;
  ASSERT_TRUE(ParseAdvertisement(buf, &adv));
  EXPECT_EQ(adv.proto, kCtlProto);
  EXPECT_EQ(adv.starttime, ProcStarttime(getpid()));
  EXPECT_STREQ(adv.ctl, dir_template);

  unlink(path.c_str());
  rmdir(dir_template);
}

}  // namespace
}  // namespace gpu_cr
