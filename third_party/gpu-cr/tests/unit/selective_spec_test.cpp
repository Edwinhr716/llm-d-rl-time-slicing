// cr_client "-s ptr:size,..." region-spec parser.

#include "selective_spec.h"

#include <string>

#include "gtest/gtest.h"

namespace gpu_cr {
namespace {

TEST(ParseSelectiveRegionsTest, SingleHexRegion) {
  SelectiveCrRequest req;
  ASSERT_TRUE(ParseSelectiveRegions("0x7f0000000000:4096", &req));
  ASSERT_EQ(req.num_regions, 1u);
  EXPECT_EQ(req.regions[0].ptr, reinterpret_cast<void*>(0x7f0000000000UL));
  EXPECT_EQ(req.regions[0].size, 4096u);
}

TEST(ParseSelectiveRegionsTest, MultipleRegionsMixedBases) {
  SelectiveCrRequest req;
  ASSERT_TRUE(ParseSelectiveRegions("0x1000:0x2000,4096:65536", &req));
  ASSERT_EQ(req.num_regions, 2u);
  EXPECT_EQ(req.regions[0].ptr, reinterpret_cast<void*>(0x1000UL));
  EXPECT_EQ(req.regions[0].size, 0x2000u);
  EXPECT_EQ(req.regions[1].ptr, reinterpret_cast<void*>(4096UL));
  EXPECT_EQ(req.regions[1].size, 65536u);
}

TEST(ParseSelectiveRegionsTest, RejectsEmptySpec) {
  SelectiveCrRequest req;
  EXPECT_FALSE(ParseSelectiveRegions("", &req));
}

TEST(ParseSelectiveRegionsTest, RejectsMissingColon) {
  SelectiveCrRequest req;
  EXPECT_FALSE(ParseSelectiveRegions("0x1000", &req));
}

TEST(ParseSelectiveRegionsTest, RejectsZeroSize) {
  SelectiveCrRequest req;
  EXPECT_FALSE(ParseSelectiveRegions("0x1000:0", &req));
}

TEST(ParseSelectiveRegionsTest, RejectsMalformedTrailingRegion) {
  SelectiveCrRequest req;
  EXPECT_FALSE(ParseSelectiveRegions("0x1000:4096,bogus", &req));
}

TEST(ParseSelectiveRegionsTest, RejectsGarbagePtr) {
  SelectiveCrRequest req;
  // strtoull would quietly parse "bogus" as 0 — the parser must not.
  EXPECT_FALSE(ParseSelectiveRegions("bogus:4096", &req));
}

TEST(ParseSelectiveRegionsTest, RejectsTrailingJunkInSize) {
  SelectiveCrRequest req;
  EXPECT_FALSE(ParseSelectiveRegions("0x1000:12junk", &req));
}

TEST(ParseSelectiveRegionsTest, RejectsEmptyMiddleToken) {
  SelectiveCrRequest req;
  EXPECT_FALSE(ParseSelectiveRegions("0x1000:1,,0x2000:1", &req));
}

TEST(ParseSelectiveRegionsTest, RejectsTrailingComma) {
  SelectiveCrRequest req;
  EXPECT_FALSE(ParseSelectiveRegions("0x1000:1,", &req));
}

TEST(ParseSelectiveRegionsTest, RejectsNegativeSize) {
  SelectiveCrRequest req;
  // strtoull would negate "-1" to ULLONG_MAX — the parser must not.
  EXPECT_FALSE(ParseSelectiveRegions("0x1000:-1", &req));
}

TEST(ParseSelectiveRegionsTest, RejectsOverflowingPtr) {
  SelectiveCrRequest req;
  // strtoull would saturate 2^65-ish to ULLONG_MAX (ERANGE) — the parser
  // must reject, not hand the .so a region at 0xFFFFFFFFFFFFFFFF.
  EXPECT_FALSE(ParseSelectiveRegions("0x1FFFFFFFFFFFFFFFF:4096", &req));
}

TEST(ParseSelectiveRegionsTest, RejectsOverflowingSize) {
  SelectiveCrRequest req;
  EXPECT_FALSE(ParseSelectiveRegions("0x1000:18446744073709551616", &req));
}

TEST(ParseSelectiveRegionsTest, RejectsEmptySize) {
  SelectiveCrRequest req;
  EXPECT_FALSE(ParseSelectiveRegions("0x1000:", &req));
}

// Syntax-only parser: duplicate regions pass through untouched —
// dedup/overlap semantics belong to the .so and the agent above it.
TEST(ParseSelectiveRegionsTest, AcceptsDuplicateRegions) {
  SelectiveCrRequest req;
  ASSERT_TRUE(ParseSelectiveRegions("0x1000:4096,0x1000:4096", &req));
  ASSERT_EQ(req.num_regions, 2u);
  EXPECT_EQ(req.regions[0].ptr, req.regions[1].ptr);
  EXPECT_EQ(req.regions[0].size, req.regions[1].size);
}

// The bound is exclusive: kMaxSelectiveRegions-1 is the largest request
// the dump writer can serve without filling the last extent-table slot.
TEST(ParseSelectiveRegionsTest, AcceptsLargestServableRequest) {
  std::string spec;
  for (uint32_t i = 0; i < kMaxSelectiveRegions - 1; i++) {
    if (i) spec += ",";
    spec += "0x1000:1";
  }
  SelectiveCrRequest req;
  ASSERT_TRUE(ParseSelectiveRegions(spec.c_str(), &req));
  EXPECT_EQ(req.num_regions, kMaxSelectiveRegions - 1);
}

TEST(ParseSelectiveRegionsTest, RejectsRequestAtExclusiveBound) {
  std::string spec;
  for (uint32_t i = 0; i < kMaxSelectiveRegions; i++) {
    if (i) spec += ",";
    spec += "0x1000:1";
  }
  SelectiveCrRequest req;
  EXPECT_FALSE(ParseSelectiveRegions(spec.c_str(), &req));
}

}  // namespace
}  // namespace gpu_cr
