// Copyright 2026 The llm-d Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store_test

import (
	"context"
	"slices"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/store"
)

// Tests for the host ack barrier of PENDING LEAD DECISION D-NS-4, option
// hybrid.

func newTwoHostGroup(t *testing.T) *store.GroupSpec {
	t.Helper()
	group, err := store.NewGroup(context.Background(), "group-1", nil)
	if err != nil {
		t.Fatalf("NewGroup failed: %v", err)
	}
	group.Status().SetNodes([]string{"node-a", "node-b"})
	group.Status().SetState(pb.GroupStatus_STATE_IDLE_YIELDED)
	spec := group.Spec()
	now := time.Now()
	for _, node := range []string{"node-a", "node-b"} {
		spec.RegisterParticipant(node, "vk/"+node, now)
		if !spec.Grant(node) {
			t.Fatalf("Grant(%s) = false", node)
		}
	}
	return spec
}

func TestNS4_Hybrid_StoreBarrier(t *testing.T) {
	spec := newTwoHostGroup(t)
	if got := spec.PendingAcks(); got != nil {
		t.Fatalf("PendingAcks() without a notice = %v, want nil", got)
	}
	if spec.NoticeEpoch() != 0 {
		t.Fatalf("NoticeEpoch() without a notice = %d, want 0", spec.NoticeEpoch())
	}

	spec.EnsureNotice(time.Now())
	epoch := spec.NoticeEpoch()
	if epoch == 0 {
		t.Fatal("NoticeEpoch() = 0 while a notice runs")
	}
	if got, want := spec.PendingAcks(), []string{"node-a", "node-b"}; !slices.Equal(got, want) {
		t.Fatalf("PendingAcks() = %v, want %v", got, want)
	}

	if !spec.AckVacate("node-a", epoch) {
		t.Fatal("AckVacate(node-a, current epoch) = false")
	}
	if spec.Granted("node-a") {
		t.Fatal("node-a still granted after its ack")
	}
	if got, want := spec.PendingAcks(), []string{"node-b"}; !slices.Equal(got, want) {
		t.Fatalf("PendingAcks() after node-a acked = %v, want %v", got, want)
	}

	// node-b yields without acking: its grant ends but the barrier still
	// waits for its ack (a Yield is not proof that the guests are suspended).
	spec.ClearGrant("node-b")
	if spec.BackgroundHeld() {
		t.Fatal("BackgroundHeld() after every grant ended")
	}
	if got, want := spec.PendingAcks(), []string{"node-b"}; !slices.Equal(got, want) {
		t.Fatalf("PendingAcks() after node-b yielded = %v, want %v", got, want)
	}

	if !spec.AckVacate("node-b", epoch) {
		t.Fatal("AckVacate(node-b, current epoch) = false")
	}
	if got := spec.PendingAcks(); len(got) != 0 {
		t.Fatalf("PendingAcks() after every ack = %v, want empty", got)
	}
}

func TestNS4_Hybrid_StoreLateAck(t *testing.T) {
	spec := newTwoHostGroup(t)
	first := time.Now()
	spec.EnsureNotice(first)
	oldEpoch := spec.NoticeEpoch()
	spec.ClearNotice()
	if spec.AckVacate("node-a", oldEpoch) {
		t.Fatal("AckVacate with no notice running = true")
	}

	spec.EnsureNotice(first.Add(time.Second))
	if spec.NoticeEpoch() == oldEpoch {
		t.Fatal("a new notice reused the old epoch")
	}
	// An ack from the previous cycle must not count for this one.
	if spec.AckVacate("node-a", oldEpoch) {
		t.Fatal("AckVacate with the previous notice's epoch = true")
	}
	if !spec.Granted("node-a") {
		t.Fatal("a stale ack cleared node-a's grant")
	}
	if got, want := spec.PendingAcks(), []string{"node-a", "node-b"}; !slices.Equal(got, want) {
		t.Fatalf("PendingAcks() after a stale ack = %v, want %v", got, want)
	}
	if spec.AckVacate("node-a", 0) {
		t.Fatal("AckVacate with epoch 0 = true")
	}
}

func TestNS4_Hybrid_StoreHeartbeatAfterAck(t *testing.T) {
	spec := newTwoHostGroup(t)
	now := time.Now()
	spec.EnsureNotice(now)
	epoch := spec.NoticeEpoch()

	// node-a acks, yields and its Acquire ends: its record is gone.
	spec.AckVacate("node-a", epoch)
	spec.ClearGrant("node-a")
	spec.UnregisterParticipant("node-a")
	if spec.Touch("node-a", "vk/node-a", now) {
		t.Fatal("a heartbeat from a host that acked this notice registered a claim")
	}

	// node-b yields before its ack arrives and its record is gone: its next
	// heartbeat is a claim (fail closed), and the ack clears it.
	spec.ClearGrant("node-b")
	spec.UnregisterParticipant("node-b")
	if spec.BackgroundHeld() {
		t.Fatal("BackgroundHeld() after every grant ended")
	}
	if !spec.Touch("node-b", "vk/node-b", now) {
		t.Fatal("a heartbeat from an unknown host that has not acked did not register a claim")
	}
	if !spec.BackgroundHeld() {
		t.Fatal("the claim does not hold the group")
	}
	spec.AckVacate("node-b", epoch)
	if spec.BackgroundHeld() {
		t.Fatal("the ack did not clear node-b's claim")
	}
}

func TestNS4_Hybrid_StoreLendableParticipants(t *testing.T) {
	group, err := store.NewGroup(context.Background(), "group-1", nil)
	if err != nil {
		t.Fatalf("NewGroup failed: %v", err)
	}
	group.Status().SetNodes([]string{"node-a", "node-b", "node-c"})
	spec := group.Spec()
	now := time.Now()
	spec.RegisterParticipant("node-b", "vk/node-b", now)
	spec.RegisterParticipant("node-a", "vk/node-a", now)
	spec.Touch("node-c", "vk/node-c", now) // a claim
	if got, want := spec.LendableParticipants(), []string{"node-a", "node-b", "node-c"}; !slices.Equal(got, want) {
		t.Fatalf("LendableParticipants() = %v, want %v", got, want)
	}
	spec.Grant("node-a")
	if got, want := spec.LendableParticipants(), []string{"node-b", "node-c"}; !slices.Equal(got, want) {
		t.Fatalf("LendableParticipants() after granting node-a = %v, want %v", got, want)
	}
}

func TestNS4_Hybrid_StoreFirstAcquireClaims(t *testing.T) {
	group, err := store.NewGroup(context.Background(), "group-1", nil)
	if err != nil {
		t.Fatalf("NewGroup failed: %v", err)
	}
	group.Status().SetNodes([]string{"node-a", "node-b"})
	spec := group.Spec()
	now := time.Now()

	// First contact through Acquire: the host may hold a grant from an
	// earlier process.
	if !spec.RegisterParticipantFirstClaim("node-a", "vk/node-a", now) {
		t.Fatal("first Acquire from an unknown host did not register a claim")
	}
	if !spec.BackgroundHeld() || spec.Granted("node-a") {
		t.Fatal("the claim must hold the group and must not be a grant")
	}
	if got, want := spec.LendableParticipants(), []string{"node-a"}; !slices.Equal(got, want) {
		t.Fatalf("LendableParticipants() = %v, want %v", got, want)
	}

	// A host this process already heard from is registered without a claim,
	// even after its record was dropped by a background Yield.
	spec.Touch("node-b", "vk/node-b", now) // a claim (first contact)
	spec.ClearGrant("node-b")              // background Yield: forgotten
	if spec.RegisterParticipantFirstClaim("node-b", "vk/node-b", now) {
		t.Fatal("Acquire from a known host registered a claim")
	}
	spec.ClearGrant("node-a")
	if spec.BackgroundHeld() {
		t.Fatal("background still held after both claims ended")
	}
	if spec.RegisterParticipantFirstClaim("node-a", "vk/node-a", now) {
		t.Fatal("a second Acquire registered a claim")
	}
}
