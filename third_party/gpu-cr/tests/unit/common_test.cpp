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

TEST(FinishOpControlsTest, CapabilityPersistsAcrossOps) {
  signal_controls c;
  memset(&c, 0, sizeof(c));
  c.capability = kCrCapDestPath;
  FinishOpControls(&c, EIO);
  EXPECT_EQ(c.capability & kCrCapDestPath, kCrCapDestPath);
}

// Layout guards for the shared-memory wire structs: the v2 fields are
// appended so the v1 prefix keeps its exact offsets.
TEST(WireLayoutTest, V1PrefixOffsetsUnchanged) {
  EXPECT_EQ(offsetof(SelectiveCrRequest, num_regions), 0u);
  EXPECT_EQ(offsetof(SelectiveCrRequest, regions), 8u);
  EXPECT_EQ(offsetof(signal_controls, selective_req), 8u);
}

}  // namespace
}  // namespace gpu_cr
