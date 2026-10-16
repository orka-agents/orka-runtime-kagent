// Package kagent is a thin client for the public kagent v1 APIs the broker
// uses: the Sessions gRPC service and A2A over gRPC. Every call carries the
// caller-selected kagent user so one connection serves many Orka sessions.
package kagent

import (
	"context"
	"crypto/tls"
	"errors"
	"iter"
	"net"
	"time"

	a2atype "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	a2agrpc "github.com/a2aproject/a2a-go/v2/a2agrpc/v1"
	a2apb "github.com/a2aproject/a2a-go/v2/a2apb/v1"
	apiv1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/orka-agents/orka-runtime-kagent/internal/config"
)

const (
	userIDHeader       = "x-user-id"
	maxMessageBytes    = 16 << 20
	defaultCallTimeout = 30 * time.Second
	listPageSize       = 100
	maxListPages       = 50
)

var (
	// ErrNotFound reports that kagent has no such session or task for the user.
	ErrNotFound = errors.New("kagent resource not found")
	// ErrRejected reports a definite refusal: the request had no effect.
	ErrRejected = errors.New("kagent rejected the request")
	// ErrAmbiguous reports that kagent's public view cannot prove whether a
	// message created a task.
	ErrAmbiguous = errors.New("kagent task lookup is ambiguous")
)

// Session is the subset of a kagent Session the broker relies on.
type Session struct {
	ID       string
	Revision string
	// Deleting reports that kagent has begun deleting the session.
	Deleting bool
}

// Client talks to one kagent Agent through the public control plane.
type Client struct {
	conn        *grpc.ClientConn
	sessions    apiv1.SessionServiceClient
	a2a         a2apb.A2AServiceClient
	agent       *apiv1.ResourceReference
	url         string
	callTimeout time.Duration
}

// New connects lazily to the control plane named by cfg.
func New(cfg config.Kagent) (*Client, error) {
	base, err := config.ControlPlaneURL(cfg.ControlPlaneURL)
	if err != nil {
		return nil, err
	}
	host, port := base.Hostname(), base.Port()
	creds := insecure.NewCredentials()
	if base.Scheme == "https" {
		creds = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
		if port == "" {
			port = "443"
		}
	} else if port == "" {
		port = "80"
	}
	conn, err := grpc.NewClient("dns:///"+net.JoinHostPort(host, port),
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageBytes), grpc.MaxCallSendMsgSize(maxMessageBytes)))
	if err != nil {
		return nil, err
	}
	return &Client{
		conn: conn, sessions: apiv1.NewSessionServiceClient(conn), a2a: a2apb.NewA2AServiceClient(conn),
		agent: &apiv1.ResourceReference{Namespace: cfg.Agent.Namespace, Name: cfg.Agent.Name},
		url:   base.String(), callTimeout: defaultCallTimeout,
	}, nil
}

// Close releases the connection.
func (c *Client) Close() error { return c.conn.Close() }

func (c *Client) call(ctx context.Context, userID string) (context.Context, context.CancelFunc) {
	ctx = metadata.AppendToOutgoingContext(ctx, userIDHeader, userID)
	return context.WithTimeout(ctx, c.callTimeout)
}

// CreateSession creates, or on retry recovers, the session identified by
// (userID, requestID). kagent makes that pair idempotent.
func (c *Client) CreateSession(ctx context.Context, userID, requestID string) (Session, error) {
	ctx, cancel := c.call(ctx, userID)
	defer cancel()
	response, err := c.sessions.CreateSession(ctx, &apiv1.CreateSessionRequest{Agent: c.agent, RequestId: requestID})
	if status.Code(err) == codes.FailedPrecondition {
		// kagent refuses with FailedPrecondition only when nothing live exists
		// for the request: its ID belongs to a deleted Session (kagent keeps
		// the tombstone), or the Agent has no ready revision.
		return Session{}, errors.Join(ErrRejected, err)
	}
	if err != nil {
		return Session{}, classifyRPC(err)
	}
	return c.sessionFrom(response.GetSession())
}

// GetSession reads a session owned by userID.
func (c *Client) GetSession(ctx context.Context, userID, id string) (Session, error) {
	ctx, cancel := c.call(ctx, userID)
	defer cancel()
	response, err := c.sessions.GetSession(ctx, &apiv1.GetSessionRequest{SessionId: id})
	if err != nil {
		return Session{}, classifyRPC(err)
	}
	return c.sessionFrom(response.GetSession())
}

// DeleteSession deletes a session owned by userID. NotFound is reported so
// callers can distinguish completed deletion from other failures.
func (c *Client) DeleteSession(ctx context.Context, userID, id string) error {
	ctx, cancel := c.call(ctx, userID)
	defer cancel()
	_, err := c.sessions.DeleteSession(ctx, &apiv1.DeleteSessionRequest{SessionId: id})
	return classifyRPC(err)
}

func (c *Client) sessionFrom(session *apiv1.Session) (Session, error) {
	if session == nil || session.GetId() == "" {
		return Session{}, errors.New("kagent returned an empty session")
	}
	// A session for another Agent must never be adopted.
	if agent := session.GetAgent(); agent.GetNamespace() != c.agent.GetNamespace() || agent.GetName() != c.agent.GetName() {
		return Session{}, errors.New("kagent session belongs to a different Agent")
	}
	deleting := session.GetState() == apiv1.RuntimeState_RUNTIME_STATE_DELETING ||
		session.GetState() == apiv1.RuntimeState_RUNTIME_STATE_DELETED ||
		session.GetOperation() == apiv1.RuntimeOperation_RUNTIME_OPERATION_DELETE
	return Session{ID: session.GetId(), Revision: session.GetPreparedRevision(), Deleting: deleting}, nil
}

func classifyRPC(err error) error {
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.NotFound:
		return errors.Join(ErrNotFound, err)
	case codes.InvalidArgument, codes.PermissionDenied, codes.Unauthenticated, codes.AlreadyExists, codes.OutOfRange, codes.Unimplemented:
		return errors.Join(ErrRejected, err)
	default:
		return err
	}
}

// a2aClient builds an A2A client bound to the Agent and the caller's user.
func (c *Client) a2aClient(ctx context.Context, userID string) (*a2aclient.Client, error) {
	transport := a2agrpc.NewGRPCTransportFromClient(c.a2a)
	return a2aclient.NewFromEndpoints(ctx, []*a2atype.AgentInterface{{
		URL:             c.url,
		ProtocolBinding: a2atype.TransportProtocolGRPC,
		ProtocolVersion: a2atype.Version,
		Tenant:          c.agent.GetNamespace() + "/" + c.agent.GetName(),
	}},
		a2aclient.WithDefaultsDisabled(),
		a2aclient.WithTransport(a2atype.TransportProtocolGRPC, a2aclient.TransportFactoryFn(
			func(context.Context, *a2atype.AgentCard, *a2atype.AgentInterface) (a2aclient.Transport, error) {
				return transport, nil
			},
		)),
		a2aclient.WithCallInterceptors(&userInterceptor{userID: userID}),
	)
}

type userInterceptor struct {
	a2aclient.PassthroughInterceptor
	userID string
}

func (i *userInterceptor) Before(ctx context.Context, request *a2aclient.Request) (context.Context, any, error) {
	request.ServiceParams.Append(userIDHeader, i.userID)
	return ctx, nil, nil
}

// Stream sends one user message into an existing conversation and yields its
// events. The message ID is the caller's durable dispatch identity.
func (c *Client) Stream(ctx context.Context, userID, contextID, messageID, text string) iter.Seq2[a2atype.Event, error] {
	return func(yield func(a2atype.Event, error) bool) {
		client, err := c.a2aClient(ctx, userID)
		if err != nil {
			yield(nil, err)
			return
		}
		defer client.Destroy() //nolint:errcheck
		message := a2atype.NewMessage(a2atype.MessageRoleUser, a2atype.NewTextPart(text))
		message.ID, message.ContextID = messageID, contextID
		for event, err := range client.SendStreamingMessage(ctx, &a2atype.SendMessageRequest{Message: message}) {
			if !yield(event, err) || err != nil {
				return
			}
		}
	}
}

// GetTask reads committed task state, including artifacts.
func (c *Client) GetTask(ctx context.Context, userID string, taskID a2atype.TaskID) (*a2atype.Task, error) {
	ctx, cancel := context.WithTimeout(ctx, c.callTimeout)
	defer cancel()
	client, err := c.a2aClient(ctx, userID)
	if err != nil {
		return nil, err
	}
	defer client.Destroy() //nolint:errcheck
	task, err := client.GetTask(ctx, &a2atype.GetTaskRequest{ID: taskID})
	return task, classifyA2A(err)
}

// FindTaskByMessage returns the task whose history contains messageID. It
// reports ErrNotFound only when the context has no unfinished task: kagent
// withholds a task's input from public history until native cleanup ends, and
// shows such a task as submitted until then.
func (c *Client) FindTaskByMessage(ctx context.Context, userID, contextID, messageID string) (*a2atype.Task, error) {
	ctx, cancel := context.WithTimeout(ctx, c.callTimeout)
	defer cancel()
	client, err := c.a2aClient(ctx, userID)
	if err != nil {
		return nil, err
	}
	defer client.Destroy() //nolint:errcheck
	pageToken, unfinished := "", false
	for range maxListPages {
		response, err := client.ListTasks(ctx, &a2atype.ListTasksRequest{
			ContextID: contextID, PageSize: listPageSize, PageToken: pageToken, IncludeArtifacts: true,
		})
		if err != nil {
			return nil, classifyA2A(err)
		}
		task, pending := matchTask(response.Tasks, messageID)
		if task != nil {
			return task, nil
		}
		unfinished = unfinished || pending
		if response.NextPageToken == "" {
			if unfinished {
				return nil, ErrAmbiguous
			}
			return nil, ErrNotFound
		}
		pageToken = response.NextPageToken
	}
	return nil, errors.New("kagent task listing did not finish")
}

// matchTask finds the task holding messageID, and otherwise reports whether
// any listed task is unfinished and so may still hold it unpublished.
func matchTask(tasks []*a2atype.Task, messageID string) (*a2atype.Task, bool) {
	unfinished := false
	for _, task := range tasks {
		if task == nil {
			continue
		}
		if taskHasMessage(task, messageID) {
			return task, false
		}
		unfinished = unfinished || !task.Status.State.Terminal()
	}
	return nil, unfinished
}

func taskHasMessage(task *a2atype.Task, messageID string) bool {
	if task == nil {
		return false
	}
	for _, message := range task.History {
		if message != nil && message.ID == messageID {
			return true
		}
	}
	return task.Status.Message != nil && task.Status.Message.ID == messageID
}

// CancelTask asks kagent to cancel a task and returns its resulting state.
func (c *Client) CancelTask(ctx context.Context, userID string, taskID a2atype.TaskID) (*a2atype.Task, error) {
	ctx, cancel := context.WithTimeout(ctx, c.callTimeout)
	defer cancel()
	client, err := c.a2aClient(ctx, userID)
	if err != nil {
		return nil, err
	}
	defer client.Destroy() //nolint:errcheck
	task, err := client.CancelTask(ctx, &a2atype.CancelTaskRequest{ID: taskID})
	return task, classifyA2A(err)
}

func classifyA2A(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, a2atype.ErrTaskNotFound) {
		return errors.Join(ErrNotFound, err)
	}
	return classifyRPC(err)
}

// NotAccepted reports kagent's explicit proof that a send was never accepted
// and its input never persisted, so the same message may be retried safely.
func NotAccepted(err error) bool {
	var protocolError *a2atype.Error
	if !errors.Is(err, a2atype.ErrUnsupportedOperation) || !errors.As(err, &protocolError) {
		return false
	}
	info := protocolError.ErrorInfo().Value
	meta, _ := info["metadata"].(map[string]string)
	return info["domain"] == a2atype.ProtocolDomain && meta["reason"] == "KAGENT_SEND_NOT_ACCEPTED"
}
