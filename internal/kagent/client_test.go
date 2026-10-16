package kagent

import (
	"context"
	"errors"
	"net"
	"testing"

	a2atype "github.com/a2aproject/a2a-go/v2/a2a"
	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/orka-agents/orka-runtime-kagent/internal/config"
)

func TestMatchTaskTreatsUnfinishedTasksAsPossiblyHoldingTheMessage(t *testing.T) {
	message := &a2atype.Message{ID: "orka-message"}
	done := &a2atype.Task{ID: "earlier", Status: a2atype.TaskStatus{State: a2atype.TaskStateCompleted}}
	// kagent shows a task whose first save was terminal as submitted, with its
	// input withheld from public history until native cleanup ends.
	withheld := &a2atype.Task{ID: "withheld", Status: a2atype.TaskStatus{State: a2atype.TaskStateSubmitted}}
	found := &a2atype.Task{ID: "found", History: []*a2atype.Message{message}, Status: a2atype.TaskStatus{State: a2atype.TaskStateWorking}}

	if task, unfinished := matchTask([]*a2atype.Task{done, withheld, found}, message.ID); task != found || unfinished {
		t.Fatalf("match = %v %v", task, unfinished)
	}
	if task, unfinished := matchTask([]*a2atype.Task{done, withheld}, message.ID); task != nil || !unfinished {
		t.Fatalf("an unfinished task did not make the lookup ambiguous: %v %v", task, unfinished)
	}
	if task, unfinished := matchTask([]*a2atype.Task{done, nil}, message.ID); task != nil || unfinished {
		t.Fatalf("finished tasks alone = %v %v", task, unfinished)
	}
}

type refusingSessions struct {
	apiv1.UnimplementedSessionServiceServer
	code codes.Code
}

func (s refusingSessions) CreateSession(context.Context, *apiv1.CreateSessionRequest) (*apiv1.CreateSessionResponse, error) {
	return nil, status.Error(s.code, "refused")
}

// Only FailedPrecondition proves that nothing live exists for a request ID.
// Any other refusal can follow a committed attempt whose response was lost.
func TestCreateSessionTreatsOnlyFailedPreconditionAsDefinite(t *testing.T) {
	for _, test := range []struct {
		code     codes.Code
		definite bool
	}{
		{codes.FailedPrecondition, true},
		{codes.PermissionDenied, false},
		{codes.Unauthenticated, false},
		{codes.AlreadyExists, false},
		{codes.InvalidArgument, false},
		{codes.Unavailable, false},
	} {
		t.Run(test.code.String(), func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server := grpc.NewServer()
			apiv1.RegisterSessionServiceServer(server, refusingSessions{code: test.code})
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(server.Stop)
			_, port, _ := net.SplitHostPort(listener.Addr().String())
			client, err := New(config.Kagent{ControlPlaneURL: "http://localhost:" + port, Agent: config.AgentRef{Namespace: "kagent", Name: "probe"}})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close() //nolint:errcheck
			_, err = client.CreateSession(context.Background(), "orka-user", "request-1")
			if err == nil || errors.Is(err, ErrRejected) != test.definite {
				t.Fatalf("CreateSession refused with %s = %v, definite refusal want %v", test.code, err, test.definite)
			}
		})
	}
}
