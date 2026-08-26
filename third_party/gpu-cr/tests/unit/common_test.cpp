// Shared-protocol helpers from common.h: the consume-once FINISH
// bookkeeping and the wire-layout guards for the selective request.

#include "common.h"

#include <errno.h>
#include <stddef.h>
#include <string.h>

#include "gtest/gtest.h"

namespace gpu_cr {
namespace {

// FinishOpControls is the only writer of op_status: op sites hand it a
// positive errno (or 0), and it stores kOpStatusOk or the negated errno,
// so 0 always means "no result reported".
TEST(FinishOpControlsTest, ReportsStatus) {
  signal_controls c;
  memset(&c, 0, sizeof(c));
  FinishOpControls(&c, ENOSPC);
  EXPECT_EQ(c.op_status, -ENOSPC);

  FinishOpControls(&c, 0);
  EXPECT_EQ(c.op_status, kOpStatusOk);
  EXPECT_GT(c.op_status, 0);
}

// A stale dest_path must never redirect a later op: a non-empty path is
// what selects destination-file routing, so FINISH zeroes it.
TEST(FinishOpControlsTest, ConsumesDestPath) {
  signal_controls c;
  memset(&c, 0, sizeof(c));
  strncpy(c.selective_req.dest_path, "/store/dump.bin",
          kSelectiveCrMaxPath - 1);
  c.selective_req.num_regions = 2;  // region-list prefix is NOT consumed

  FinishOpControls(&c, 0);
  for (size_t i = 0; i < kSelectiveCrMaxPath; i++)
    ASSERT_EQ(c.selective_req.dest_path[i], '\0');
  EXPECT_EQ(c.selective_req.num_regions, 2u);
}

// The published ready word must survive the consume-once zeroing and be
// re-asserted at every FINISH, so a client that attaches after init_CR
// (word still 0 from its point of view) sees it once any op completes.
TEST(FinishOpControlsTest, ReassertsSelectiveReady) {
  signal_controls c;
  memset(&c, 0, sizeof(c));  // as if the client raced init_CR
  FinishOpControls(&c, EIO);
  EXPECT_EQ(c.selective_ready, kSelectiveReady);
}

// Layout guards for the shared-memory wire structs: fields are only ever
// appended, so the region-list prefix keeps its exact offsets.
TEST(WireLayoutTest, RegionListPrefixOffsets) {
  EXPECT_EQ(offsetof(SelectiveCrRequest, num_regions), 0u);
  EXPECT_EQ(offsetof(SelectiveCrRequest, regions), 8u);
  EXPECT_EQ(offsetof(signal_controls, selective_req), 8u);
}

// The appended fields are what a separately-built cr_client and .so agree
// on through the shared mapping: pin the exact x86-64 offsets and sizes
// (style of common_baseline_test.cpp) so an added/reordered field — or a
// changed kMaxSelectiveRegions/kSelectiveCrMaxPath, which would silently
// shift every later offset — fails here instead of on the wire.
TEST(WireLayoutTest, AppendedFieldsOffsetsPinned) {
  EXPECT_EQ(kMaxSelectiveRegions, 4096u);
  EXPECT_EQ(kSelectiveCrMaxPath, 256u);

  EXPECT_EQ(offsetof(SelectiveCrRegion, ptr), 0u);
  EXPECT_EQ(offsetof(SelectiveCrRegion, size), 8u);
  EXPECT_EQ(sizeof(SelectiveCrRegion), 16u);

  EXPECT_EQ(offsetof(SelectiveCrRequest, dest_path), 65544u);
  EXPECT_EQ(sizeof(SelectiveCrRequest), 65800u);

  EXPECT_EQ(offsetof(signal_controls, selective_ready), 65808u);
  EXPECT_EQ(offsetof(signal_controls, op_status), 65812u);
  EXPECT_EQ(sizeof(signal_controls), 65816u);
}

}  // namespace
}  // namespace gpu_cr
