package server_test

import (
	"context"
	"slices"
	"testing"
	"time"

	pb "github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/api/v1alpha1"
	"github.com/llm-d-incubation/llm-d-rl-time-slicing/pkg/timeslice-orchestrator/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// The tests in this file act as a client built from the orchestrator schema
// as it was before roles existed: no role, node_name, expected_idle,
// participant_id, vram_unconfirmed, background_protocol or vacate_within, and
// group states only up to STATE_SWITCHING. The schema is rebuilt here by hand
// so the test keeps checking the old wire format after the generated code
// moves on.

const oldServicePrefix = "/timeslice_orchestrator.v1alpha1.TimeSliceOrchestratorService/"

func oldField(
	name string,
	number int32,
	typ descriptorpb.FieldDescriptorProto_Type,
	typeName string,
) *descriptorpb.FieldDescriptorProto {
	field := &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(number),
		Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		Type:   typ.Enum(),
	}
	if typeName != "" {
		field.TypeName = proto.String(typeName)
	}
	return field
}

func oldEnum(names ...string) *descriptorpb.EnumDescriptorProto {
	enum := &descriptorpb.EnumDescriptorProto{Name: proto.String("State")}
	var number int32
	for _, name := range names {
		enum.Value = append(enum.Value, &descriptorpb.EnumValueDescriptorProto{
			Name:   proto.String(name),
			Number: proto.Int32(number),
		})
		number++
	}
	return enum
}

// oldSchema returns the pre-roles messages, keyed by message name.
func oldSchema(t *testing.T) map[string]protoreflect.MessageDescriptor {
	t.Helper()
	const (
		str   = descriptorpb.FieldDescriptorProto_TYPE_STRING
		boolT = descriptorpb.FieldDescriptorProto_TYPE_BOOL
		i64   = descriptorpb.FieldDescriptorProto_TYPE_INT64
		enumT = descriptorpb.FieldDescriptorProto_TYPE_ENUM
		msgT  = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
	)
	msg := func(name string, fields ...*descriptorpb.FieldDescriptorProto) *descriptorpb.DescriptorProto {
		return &descriptorpb.DescriptorProto{Name: proto.String(name), Field: fields}
	}
	groupStatus := msg("GroupStatus",
		oldField("group_id", 1, str, ""),
		oldField("group_state", 2, enumT, ".oldclient.v0.GroupStatus.State"),
		oldField("state_timestamp", 3, msgT, ".google.protobuf.Timestamp"),
		oldField("locking_job", 4, str, ""),
		oldField("active_job", 5, str, ""),
		oldField("waiter_queue_depth", 6, i64, ""),
		oldField("loaded_job", 7, str, ""),
	)
	groupStatus.EnumType = []*descriptorpb.EnumDescriptorProto{oldEnum(
		"STATE_UNSPECIFIED", "STATE_UNKNOWN", "STATE_IDLE", "STATE_IDLE_YIELDED", "STATE_LOCKED", "STATE_SWITCHING",
	)}
	agentState := msg("SnapshotAgentJobState",
		oldField("agent", 1, str, ""),
		oldField("job_state", 2, enumT, ".oldclient.v0.SnapshotAgentJobState.State"),
		oldField("job_id", 3, str, ""),
	)
	agentState.EnumType = []*descriptorpb.EnumDescriptorProto{oldEnum(
		"STATE_UNSPECIFIED", "STATE_IDLE", "STATE_RUNNING", "STATE_TRANSITIONING", "STATE_SAVED", "STATE_FAULTED",
	)}
	agentStates := oldField("agent_job_states", 2, msgT, ".oldclient.v0.SnapshotAgentJobState")
	agentStates.Label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()

	file := &descriptorpb.FileDescriptorProto{
		Name:       proto.String("oldclient/timeslice_orchestrator_v0.proto"),
		Package:    proto.String("oldclient.v0"),
		Syntax:     proto.String("proto3"),
		Dependency: []string{"google/protobuf/timestamp.proto"},
		MessageType: []*descriptorpb.DescriptorProto{
			msg("AcquireRequest", oldField("job_id", 1, str, ""), oldField("group_id", 2, str, "")),
			msg("AcquireResponse",
				oldField("success", 1, boolT, ""),
				oldField("waited_ms", 2, i64, ""),
				oldField("context_restored", 3, boolT, "")),
			msg("YieldRequest", oldField("job_id", 1, str, ""), oldField("group_id", 2, str, "")),
			msg("YieldResponse",
				oldField("success", 1, boolT, ""),
				oldField("pending_waiters", 2, i64, ""),
				oldField("snapshot_deferred", 3, boolT, "")),
			msg("GetGroupStatusRequest", oldField("group_id", 1, str, "")),
			msg("GetGroupStatusResponse", oldField("group", 1, msgT, ".oldclient.v0.GroupStatus"), agentStates),
			groupStatus,
			agentState,
		},
	}
	fd, err := protodesc.NewFile(file, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatalf("failed to build the old schema: %v", err)
	}
	out := make(map[string]protoreflect.MessageDescriptor)
	for i := range fd.Messages().Len() {
		md := fd.Messages().Get(i)
		out[string(md.Name())] = md
	}
	return out
}

type oldClient struct {
	t      *testing.T
	conn   *grpc.ClientConn
	schema map[string]protoreflect.MessageDescriptor
}

// call sends fields as an old request message and returns the old response.
func (c *oldClient) call(
	ctx context.Context,
	method, reqName, respName string,
	fields map[string]string,
) (*dynamicpb.Message, error) {
	c.t.Helper()
	req := dynamicpb.NewMessage(c.schema[reqName])
	for name, value := range fields {
		req.Set(req.Descriptor().Fields().ByName(protoreflect.Name(name)), protoreflect.ValueOfString(value))
	}
	resp := dynamicpb.NewMessage(c.schema[respName])
	err := c.conn.Invoke(ctx, oldServicePrefix+method, req, resp)
	return resp, err
}

func get(m protoreflect.Message, name string) protoreflect.Value {
	return m.Get(m.Descriptor().Fields().ByName(protoreflect.Name(name)))
}

// unknownFieldNumbers lists the field numbers an old decoder did not know.
func unknownFieldNumbers(t *testing.T, m protoreflect.Message) []protowire.Number {
	t.Helper()
	var numbers []protowire.Number
	raw := m.GetUnknown()
	for len(raw) > 0 {
		num, _, n := protowire.ConsumeField(raw)
		if n < 0 {
			t.Fatalf("malformed unknown fields: %v", protowire.ParseError(n))
		}
		numbers = append(numbers, num)
		raw = raw[n:]
	}
	return numbers
}

func TestCompat_OldClient(t *testing.T) {
	schema := oldSchema(t)

	t.Run("acquire and yield behave as foreground", func(t *testing.T) {
		gs, group := backgroundGroup(t, "job-1", true)
		// The new server runs with every new feature on.
		oc := &oldClient{t: t, schema: schema, conn: dialServer(t, gs,
			server.WithBackgroundRole(true), server.WithMinBubble(time.Second))}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ids := map[string]string{"job_id": "job-1", "group_id": bgGroup}

		resp, err := oc.call(ctx, "Acquire", "AcquireRequest", "AcquireResponse", ids)
		if err != nil || !get(resp, "success").Bool() {
			t.Fatalf("old Acquire = %v, %v; want success", resp, err)
		}
		if !get(resp, "context_restored").Bool() {
			t.Error("old Acquire lost context_restored")
		}

		resp, err = oc.call(ctx, "Yield", "YieldRequest", "YieldResponse", ids)
		if err != nil || !get(resp, "success").Bool() {
			t.Fatalf("old Yield = %v, %v; want success", resp, err)
		}
		if group.Spec().LockingJob() != "" {
			t.Error("old Yield did not release the lock")
		}
		// No expected_idle means no lend hint, whatever --min-bubble is.
		if group.Spec().Lend() {
			t.Error("old Yield recorded a lend hint")
		}
	})

	t.Run("status with new states and fields decodes", func(t *testing.T) {
		gs, group := backgroundGroup(t, "job-1", true)
		oc := &oldClient{t: t, schema: schema, conn: dialServer(t, gs, server.WithBackgroundRole(true))}
		group.Spec().RegisterParticipant(bgNode, bgParticipant, time.Now())
		group.Spec().Grant(bgNode)
		group.Spec().UnregisterParticipant(bgNode)

		// An old foreground Acquire over a held grant fails closed: it blocks
		// and starts the notice, so the server now reports STATE_VACATING (7)
		// and sets vacate_within (9) next to background_protocol (8).
		shortCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err := oc.call(shortCtx, "Acquire", "AcquireRequest", "AcquireResponse",
			map[string]string{"job_id": "job-1", "group_id": bgGroup})
		assertCode(t, err, codes.DeadlineExceeded)

		ctx, cancelStatus := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelStatus()
		resp, err := oc.call(ctx, "GetGroupStatus", "GetGroupStatusRequest", "GetGroupStatusResponse",
			map[string]string{"group_id": bgGroup})
		if err != nil {
			t.Fatalf("old GetGroupStatus failed: %v", err)
		}
		groupMsg := get(resp, "group").Message()
		if got := get(groupMsg, "group_id").String(); got != bgGroup {
			t.Errorf("group_id = %q, want %q", got, bgGroup)
		}
		if got := get(groupMsg, "locking_job").String(); got != "job-1" {
			t.Errorf("locking_job = %q, want job-1", got)
		}
		// proto3 enums are open: the old decoder keeps the unknown number.
		if got := get(groupMsg, "group_state").Enum(); got != protoreflect.EnumNumber(pb.GroupStatus_STATE_VACATING) {
			t.Errorf("group_state = %d, want %d", got, pb.GroupStatus_STATE_VACATING)
		}
		unknown := unknownFieldNumbers(t, groupMsg)
		for _, want := range []protowire.Number{8, 9} {
			if !slices.Contains(unknown, want) {
				t.Errorf("unknown fields = %v, want field %d carried and skipped", unknown, want)
			}
		}
	})
}
