// Shared-protocol helpers from common.h: the consume-once FINISH
// bookkeeping and the wire-layout guards for the v2 extension.

#include "common.h"

#include <errno.h>
#include <stddef.h>
#include <string.h>

#include "gtest/gtest.h"

namespace gpu_cr {
namespace {

TEST(FinishOpControlsTest, ReportsStatusAndAck) {
  signal_controls c;
  memset(&c, 0, sizeof(c));
  FinishOpControls(&c, ENOSPC);
  EXPECT_EQ(c.op_status, ENOSPC);
  EXPECT_EQ(c.proto_ack, kSelectiveCrProtoV2);
  EXPECT_EQ(c.capability & kCrCapDestPath, kCrCapDestPath);
}

// A stale dest_path must never redirect a later op: the v2 extension is
// consumed at FINISH.
TEST(FinishOpControlsTest, ConsumesRequestExtension) {
  signal_controls c;
  memset(&c, 0, sizeof(c));
  c.selective_req.proto_version = kSelectiveCrProtoV2;
  strncpy(c.selective_req.dest_path, "/store/dump.bin",
          kSelectiveCrMaxPath - 1);
  c.selective_req.num_regions = 2;  // v1 prefix is NOT consumed

  FinishOpControls(&c, 0);
  EXPECT_EQ(c.selective_req.proto_version, 0u);
  for (size_t i = 0; i < kSelectiveCrMaxPath; i++)
    ASSERT_EQ(c.selective_req.dest_path[i], '\0');
  EXPECT_EQ(c.selective_req.num_regions, 2u);
}

// Bits FINISH does not own must survive it: pre-set a foreign bit with
// kCrCapDestPath clear and check FINISH keeps the former while OR-ing in
// the latter.
TEST(FinishOpControlsTest, CapabilityPersistsAcrossOps) {
  constexpr uint32_t kForeignCap = 1u << 1;
  signal_controls c;
  memset(&c, 0, sizeof(c));
  c.capability = kForeignCap;
  FinishOpControls(&c, EIO);
  EXPECT_EQ(c.capability & kForeignCap, kForeignCap);
  EXPECT_EQ(c.capability & kCrCapDestPath, kCrCapDestPath);
}

// Layout guards for the shared-memory wire structs: the v2 fields are
// appended so the v1 prefix keeps its exact offsets.
TEST(WireLayoutTest, V1PrefixOffsetsUnchanged) {
  EXPECT_EQ(offsetof(SelectiveCrRequest, num_regions), 0u);
  EXPECT_EQ(offsetof(SelectiveCrRequest, regions), 8u);
  EXPECT_EQ(offsetof(signal_controls, selective_req), 8u);
}

// The appended v2 fields are what a separately-built cr_client and .so
// agree on through the shared mapping: pin the exact x86-64 offsets and
// sizes (style of common_baseline_test.cpp) so an added/reordered field —
// or a changed kMaxSelectiveRegions/kSelectiveCrMaxPath, which would
// silently shift every v2 offset — fails here instead of on the wire.
TEST(WireLayoutTest, V2ExtensionOffsetsPinned) {
  EXPECT_EQ(kMaxSelectiveRegions, 4096u);
  EXPECT_EQ(kSelectiveCrMaxPath, 256u);

  EXPECT_EQ(offsetof(SelectiveCrRegion, ptr), 0u);
  EXPECT_EQ(offsetof(SelectiveCrRegion, size), 8u);
  EXPECT_EQ(sizeof(SelectiveCrRegion), 16u);

  EXPECT_EQ(offsetof(SelectiveCrRequest, proto_version), 65544u);
  EXPECT_EQ(offsetof(SelectiveCrRequest, dest_path), 65548u);
  EXPECT_EQ(sizeof(SelectiveCrRequest), 65808u);

  EXPECT_EQ(offsetof(signal_controls, capability), 65816u);
  EXPECT_EQ(offsetof(signal_controls, proto_ack), 65820u);
  EXPECT_EQ(offsetof(signal_controls, op_status), 65824u);
  EXPECT_EQ(sizeof(signal_controls), 65832u);
}

}  // namespace
}  // namespace gpu_cr
