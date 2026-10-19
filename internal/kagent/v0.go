package kagent

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	a2atype "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2acompat/a2av0"

	"github.com/orka-agents/orka-runtime-kagent/internal/config"
)

// kagent v0.x runs each Agent as a Deployment behind its controller. The
// controller serves sessions over REST and A2A over JSON-RPC, and answers task
// reads from its database. It forwards sends and cancels to the Agent's
// Service, which may route them to any of the Agent's pods; the broker sends
// them to the one pod it pinned instead. The pod persists its sessions and
// tasks to the controller itself, so reads still go through the controller.
const (
	v0ConfigHashAnnotation = "kagent.dev/config-hash"
	maxV0ResponseBytes     = 4 << 20
	serviceAccountDir      = "/var/run/secrets/kubernetes.io/serviceaccount"
	// v0PodPort names the A2A port of the container kagent runs the Agent in.
	v0PodPort = "http"
	// v0PodProtocol is the A2A version kagent v0.10's Go runtime serves.
	v0PodProtocol = a2atype.ProtocolVersion("0.3")
	// v0AgentCardPath serves the Agent's card, whose URL names the Agent:
	// http://<name>.<namespace>:8080.
	v0AgentCardPath   = "/.well-known/agent-card.json"
	maxAgentCardBytes = 64 << 10
)

// v0SessionOnly reads a session without its events. kagent answers a session
// read with the session's events too, and one event can be larger than the
// whole reply the client accepts; it lists only events created after "after".
const v0SessionOnly = "?limit=1&after=9999-12-31T23:59:59Z"

var (
	v0ConfigHash  = regexp.MustCompile(`^(0|[1-9][0-9]{0,19})$`)
	v0ImageDigest = regexp.MustCompile(`(?:^|@)(sha256:[0-9a-f]{64})$`)
)

// ErrRollout reports that the Agent does not run as exactly one ready pod.
// Its config cannot be pinned, and a cancel may reach a pod that is not
// running the task.
var ErrRollout = errors.New("kagent Agent is not exactly one ready pod")

// ErrUnpinned reports that kagent holds the session but the Agent's config
// could not be pinned. The returned Session carries the ID so the session can
// still be owned and deleted; nothing may be sent in it until a read pins it.
var ErrUnpinned = errors.New("kagent session exists but its Agent config is not pinned")

// ErrPinLost reports that the pod a send was addressed to was no longer the
// Agent's only pod once the send was accepted, so the turn may not have run
// on the pinned config.
var ErrPinLost = errors.New("kagent Agent pod changed during the send")

// PinLostError is ErrPinLost for a send that may have reached a pod: which
// pod runs it is unknown. TaskID is the task kagent admitted, if it said.
type PinLostError struct {
	TaskID a2atype.TaskID
}

func (e *PinLostError) Error() string { return ErrPinLost.Error() }

// Is matches ErrPinLost.
func (e *PinLostError) Is(target error) bool { return target == ErrPinLost }

// Pod is the Agent's only ready pod: what it runs and where it listens.
type Pod struct {
	UID        string
	ConfigHash string
	// ImageDigest is the "sha256:<hex>" digest of the Agent container's image.
	ImageDigest string
	// Address is the host:port of the pod's A2A listener.
	Address string
}

// ConfigPin reports the pods kagent runs an Agent in.
type ConfigPin interface {
	// Pod returns the Agent's only pod, or ErrRollout unless exactly one
	// ready pod exists.
	Pod(ctx context.Context) (Pod, error)
	// PodUIDs lists every pod object of the Agent, terminating ones included.
	PodUIDs(ctx context.Context) ([]string, error)
}

// V0Client talks to one kagent v0.x Agent through its controller, and sends
// to the Agent's pinned pod.
type V0Client struct {
	a2aAccess
	http     *http.Client
	dialer   *net.Dialer
	base     string
	agentRef string
	pin      ConfigPin
	hash     string
	image    string
	accepted []string
}

// NewV0 builds a client for the Agent named by cfg. pin reads the Agent's
// pods, which carry the config hash v0 has in place of a revision.
func NewV0(cfg config.Kagent, pin ConfigPin) (*V0Client, error) {
	base, err := config.ControlPlaneURL(cfg.ControlPlaneURL)
	if err != nil || pin == nil {
		return nil, config.ErrConfig
	}
	c := &V0Client{
		// The client has no overall timeout: a streamed turn lasts as long as
		// the turn. Calls that must be short are bounded by callTimeout.
		http:     &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()},
		dialer:   &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second},
		base:     strings.TrimSuffix(base.String(), "/"),
		agentRef: cfg.Agent.Namespace + "/" + cfg.Agent.Name,
		pin:      pin,
		hash:     cfg.ExpectedRevision,
		image:    cfg.ExpectedImageDigest,
		accepted: cfg.AcceptedCommits,
	}
	a2aURL := c.base + "/api/a2a/" + url.PathEscape(cfg.Agent.Namespace) + "/" + url.PathEscape(cfg.Agent.Name) + "/"
	transport := a2aclient.NewJSONRPCTransport(a2aURL, c.http)
	c.a2aAccess = a2aAccess{callTimeout: defaultCallTimeout, historyPublished: true, open: func(ctx context.Context, userID, authorization string) (*a2aclient.Client, error) {
		return a2aclient.NewFromEndpoints(ctx, []*a2atype.AgentInterface{{
			URL: a2aURL, ProtocolBinding: a2atype.TransportProtocolJSONRPC, ProtocolVersion: a2atype.Version,
		}},
			a2aclient.WithDefaultsDisabled(),
			a2aclient.WithTransport(a2atype.TransportProtocolJSONRPC, a2aclient.TransportFactoryFn(
				func(context.Context, *a2atype.AgentCard, *a2atype.AgentInterface) (a2aclient.Transport, error) {
					return transport, nil
				},
			)),
			a2aclient.WithCallInterceptors(&userInterceptor{userID: userID, authorization: authorization}),
		)
	}}
	return c, nil
}

// Close releases idle connections.
func (c *V0Client) Close() error {
	c.http.CloseIdleConnections()
	return nil
}

// CheckVersion refuses a controller build this release was not verified
// against and the configuration does not accept.
func (c *V0Client) CheckVersion(ctx context.Context) error {
	var version struct {
		GitCommit string `json:"git_commit"`
	}
	status, err := c.do(ctx, http.MethodGet, "/version", "", nil, &version)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("kagent version read answered %d", status)
	}
	return verifiedBuild(config.KagentV0, version.GitCommit, c.accepted)
}

// SessionIDForRequest returns the session ID a create with requestID makes:
// v0 creates the session under the ID the caller chooses.
func (c *V0Client) SessionIDForRequest(requestID string) (string, bool) { return requestID, true }

type v0Session struct {
	ID      string `json:"id"`
	UserID  string `json:"user_id"`
	AgentID string `json:"agent_id"`
}

// CreateSession creates the session whose ID is requestID. kagent upserts a
// session on (ID, user), so a retry returns the same session.
func (c *V0Client) CreateSession(ctx context.Context, userID, requestID string) (Session, error) {
	body, _ := json.Marshal(map[string]string{"agent_ref": c.agentRef, "id": requestID})
	var envelope struct {
		Data *v0Session `json:"data"`
	}
	status, err := c.do(ctx, http.MethodPost, "/api/sessions", userID, body, &envelope)
	switch {
	case err != nil:
		return Session{}, err
	case status != http.StatusCreated && status != http.StatusOK:
		// No refusal proves nothing was created: v0 answers 400 for any
		// failure to look up the Agent, transient ones included, before it
		// checks for an existing session, and 500 when the ID was deleted. A
		// retry may follow an attempt that committed and lost its response.
		return Session{}, fmt.Errorf("kagent session create answered %d", status)
	}
	return c.session(ctx, envelope.Data, requestID, userID)
}

// GetSession reads a live session. A deleted session reads as ErrNotFound.
func (c *V0Client) GetSession(ctx context.Context, userID, id string) (Session, error) {
	var envelope struct {
		Data *struct {
			Session *v0Session `json:"session"`
		} `json:"data"`
	}
	status, err := c.do(ctx, http.MethodGet, "/api/sessions/"+url.PathEscape(id)+v0SessionOnly, userID, nil, &envelope)
	switch {
	case err != nil:
		return Session{}, err
	case status == http.StatusNotFound:
		return Session{}, ErrNotFound
	case status != http.StatusOK || envelope.Data == nil:
		return Session{}, fmt.Errorf("kagent session read answered %d", status)
	}
	return c.session(ctx, envelope.Data.Session, id, userID)
}

// AgentPodUIDs lists every pod kagent runs the Agent in. An execution lives in
// its pod's process, so once a pod is gone so is everything it ran.
func (c *V0Client) AgentPodUIDs(ctx context.Context) ([]string, error) { return c.pin.PodUIDs(ctx) }

// Stream sends one user message to the Agent's pinned pod and yields its
// events. The pod must be the Agent's only pod and run the pinned config and
// image, and the connection must serve this Agent's card (see bindPod). A pod
// keeps its address for its whole life, so once the send may have arrived,
// finding the same pod still at that address proves the turn runs there.
// That check runs before the first event or error is yielded, so a caller
// cannot stop reading in between; if it fails, the stream yields only a
// PinLostError. A failure before anything reached a pod is ErrNotSent.
func (c *V0Client) Stream(ctx context.Context, userID, contextID, messageID, text, authorization string) iter.Seq2[a2atype.Event, error] {
	return func(yield func(a2atype.Event, error) bool) {
		pod, err := c.pinnedPod(ctx)
		if err != nil {
			yield(nil, errors.Join(ErrNotSent, err))
			return
		}
		client, done, err := c.podClient(ctx, pod, userID, authorization)
		if err != nil {
			yield(nil, err)
			return
		}
		defer done()
		message := a2atype.NewMessage(a2atype.MessageRoleUser, a2atype.NewTextPart(text))
		message.ID, message.ContextID = messageID, contextID
		verified := false
		verify := func(event a2atype.Event, err error) bool {
			if verified || errors.Is(err, ErrNotSent) {
				return true
			}
			// A cancelled send is checked too, so the check outlives it.
			checkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.callTimeout)
			again, perr := c.pinnedPod(checkCtx)
			cancel()
			if perr != nil || again != pod {
				lost := &PinLostError{}
				if err == nil && event != nil {
					lost.TaskID = event.TaskInfo().TaskID
				}
				yield(nil, lost)
				return false
			}
			verified = true
			return true
		}
		for event, err := range client.SendStreamingMessage(ctx, &a2atype.SendMessageRequest{Message: message}) {
			if !verify(event, err) {
				return
			}
			if err != nil && !errors.Is(err, ErrNotSent) {
				err = errors.Join(ErrPinHeld, err)
			}
			if !yield(event, err) || err != nil {
				return
			}
		}
		verify(nil, nil)
	}
}

// CancelTask cancels an unfinished or canceled task and returns it only once
// its executor is done. kagent v0 runs tasks in the Agent pod's in-process
// execution manager, which records a cancellation before the executor has
// unwound but makes a cancel of an unfinished or canceled task wait for a
// running execution to end. The cancel goes to the Agent's only pod.
//
// The broker never needs to cancel a completed or failed task: kagent's
// executor writes those statuses only as its final statement (return
// queue.Write in go/adk/pkg/a2a/executor.go). A refusal to cancel one is
// answered with the task itself.
func (c *V0Client) CancelTask(ctx context.Context, userID string, taskID a2atype.TaskID) (*a2atype.Task, error) {
	// The proof holds only if the cancel reached the pod that could be
	// running the task, so the same single pod must exist before and after
	// the call. A pod that replaced it in between has nothing to join while
	// the old one may still run the task.
	pod, err := c.onlyPod(ctx)
	if err != nil {
		return nil, err
	}
	task, err := c.cancelOnPod(ctx, pod, userID, taskID)
	if errors.Is(err, a2atype.ErrTaskNotCancelable) {
		task, err = c.GetTask(ctx, userID, taskID)
	}
	if err != nil {
		return task, err
	}
	if after, err := c.onlyPod(ctx); err != nil || after != pod {
		return nil, ErrRollout
	}
	return task, nil
}

func (c *V0Client) cancelOnPod(ctx context.Context, pod Pod, userID string, taskID a2atype.TaskID) (*a2atype.Task, error) {
	ctx, cancel := context.WithTimeout(ctx, c.callTimeout)
	defer cancel()
	client, done, err := c.podClient(ctx, pod, userID, "")
	if err != nil {
		return nil, err
	}
	defer done()
	task, err := client.CancelTask(ctx, &a2atype.CancelTaskRequest{ID: taskID})
	return task, classifyA2A(err)
}

// podClient opens an A2A client to one pod as userID, bound to a connection
// that serves this Agent's card. The headers are the ones kagent's controller
// forwards: the user, and the caller's bearer. done releases the connection.
func (c *V0Client) podClient(ctx context.Context, pod Pod, userID, authorization string) (*a2aclient.Client, func(), error) {
	httpClient, err := c.bindPod(ctx, pod)
	if err != nil {
		return nil, nil, errors.Join(ErrNotSent, err)
	}
	release := func() { httpClient.CloseIdleConnections() }
	podURL := "http://" + pod.Address + "/"
	client, err := a2aclient.NewFromEndpoints(ctx, []*a2atype.AgentInterface{{
		URL: podURL, ProtocolBinding: a2atype.TransportProtocolJSONRPC, ProtocolVersion: v0PodProtocol,
	}},
		a2aclient.WithDefaultsDisabled(),
		a2aclient.WithCompatTransport(v0PodProtocol, a2atype.TransportProtocolJSONRPC,
			a2av0.NewJSONRPCTransportFactory(a2av0.JSONRPCTransportConfig{Client: httpClient})),
		a2aclient.WithCallInterceptors(&userInterceptor{userID: userID, authorization: authorization}),
	)
	if err != nil {
		release()
		return nil, nil, errors.Join(ErrNotSent, err)
	}
	return client, func() { _ = client.Destroy(); release() }, nil
}

// bindPod returns an HTTP client whose one connection serves this Agent's
// card. The pod's address was read from the Kubernetes API a moment before;
// the card proves the listener at that address is this Agent's runtime, not
// another workload that took the address. The client may dial only once, so
// every later request on it, the send included, reaches that same process,
// and a send never starts on a new connection.
func (c *V0Client) bindPod(ctx context.Context, pod Pod) (*http.Client, error) {
	var dialed atomic.Bool
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			if !dialed.CompareAndSwap(false, true) {
				// The connection that served the card is gone, so nothing was
				// written on a new one.
				return nil, errors.Join(ErrNotSent, errors.New("the pod connection closed before the request"))
			}
			return c.dialer.DialContext(ctx, network, address)
		},
		MaxConnsPerHost: 1, MaxIdleConnsPerHost: 1, IdleConnTimeout: time.Minute,
	}
	client := &http.Client{Transport: transport}
	cardCtx, cancel := context.WithTimeout(ctx, c.callTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(cardCtx, http.MethodGet, "http://"+pod.Address+v0AgentCardPath, nil)
	if err != nil {
		return nil, err
	}
	response, err := client.Do(request)
	if err != nil {
		transport.CloseIdleConnections()
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxAgentCardBytes+1))
	_ = response.Body.Close()
	var card struct {
		URL string `json:"url"`
	}
	if err != nil || response.StatusCode != http.StatusOK || len(data) > maxAgentCardBytes || json.Unmarshal(data, &card) != nil {
		transport.CloseIdleConnections()
		return nil, errors.New("the Agent pod's card is unreadable")
	}
	namespace, name, _ := strings.Cut(c.agentRef, "/")
	if u, err := url.Parse(card.URL); err != nil || u.Hostname() != name+"."+namespace {
		transport.CloseIdleConnections()
		return nil, errors.New("the listener at the Agent pod's address serves another Agent")
	}
	return client, nil
}

// pinnedPod returns the Agent's only pod if it runs the pinned config and
// image. The session read before a dispatch checks them too, but a rollout
// can finish in between, so the pod actually addressed is checked again.
func (c *V0Client) pinnedPod(ctx context.Context) (Pod, error) {
	pod, err := c.pin.Pod(ctx)
	if err != nil {
		return Pod{}, err
	}
	if pod.ConfigHash != c.hash || pod.ImageDigest != c.image {
		return Pod{}, errors.New("the Agent pod runs another config or image")
	}
	return pod, nil
}

// onlyPod returns the Agent's only pod, which must be ready, and requires
// that no other pod object exists.
func (c *V0Client) onlyPod(ctx context.Context) (Pod, error) {
	pod, err := c.pin.Pod(ctx)
	if err != nil {
		return Pod{}, err
	}
	uids, err := c.pin.PodUIDs(ctx)
	if err != nil {
		return Pod{}, err
	}
	if len(uids) != 1 || uids[0] != pod.UID {
		return Pod{}, ErrRollout
	}
	return pod, nil
}

// DeleteSession marks the session deleted. v0 only marks the row: the Agent
// is not told, and a turn already running continues. A later send in the
// session fails when the Agent loads it.
func (c *V0Client) DeleteSession(ctx context.Context, userID, id string) error {
	status, err := c.do(ctx, http.MethodDelete, "/api/sessions/"+url.PathEscape(id), userID, nil, nil)
	switch {
	case err != nil:
		return err
	case status == http.StatusNotFound:
		return ErrNotFound
	case status != http.StatusOK && status != http.StatusNoContent:
		return fmt.Errorf("kagent session delete answered %d", status)
	}
	return nil
}

func (c *V0Client) session(ctx context.Context, s *v0Session, id, userID string) (Session, error) {
	if s == nil || s.ID != id || s.UserID != userID {
		return Session{}, errors.New("kagent returned a different session")
	}
	// A session for another Agent must never be adopted. v0 names the
	// Agent in its Python-identifier form.
	if s.AgentID != v0AgentID(c.agentRef) {
		return Session{}, errors.New("kagent session belongs to a different Agent")
	}
	pod, err := c.pin.Pod(ctx)
	if err != nil {
		return Session{ID: s.ID}, fmt.Errorf("%w: %w", ErrUnpinned, err)
	}
	if pod.ImageDigest != c.image {
		// The config hash does not cover the image, so a pod running another
		// image never matches the pinned revision.
		return Session{ID: s.ID}, nil
	}
	return Session{ID: s.ID, Revision: pod.ConfigHash}, nil
}

// v0AgentID is the controller's database ID for an Agent: namespace and name
// joined by __NS__, with dashes as underscores.
func v0AgentID(ref string) string {
	namespace, name, _ := strings.Cut(ref, "/")
	return strings.ReplaceAll(namespace+"__NS__"+name, "-", "_")
}

// do sends one controller REST call as userID and decodes a JSON reply.
func (c *V0Client) do(ctx context.Context, method, path, userID string, body []byte, out any) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, c.callTimeout)
	defer cancel()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		return 0, err
	}
	if userID != "" {
		request.Header.Set(userIDHeader, userID)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.http.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close() //nolint:errcheck
	data, err := io.ReadAll(io.LimitReader(response.Body, maxV0ResponseBytes+1))
	if err != nil || len(data) > maxV0ResponseBytes {
		return 0, errors.New("kagent reply was unreadable or too large")
	}
	if out != nil && (response.StatusCode == http.StatusOK || response.StatusCode == http.StatusCreated) {
		if err := json.Unmarshal(data, out); err != nil {
			return 0, errors.New("kagent reply was not valid JSON")
		}
	}
	return response.StatusCode, nil
}

// AgentPods reads an Agent's pods through the Kubernetes API with the pod's
// service account. It needs list on pods in the Agent's namespace.
type AgentPods struct {
	http      *http.Client
	url       string
	tokenFile string
}

// NewAgentPods returns a reader for the pods kagent runs ref in, selected by
// the labels kagent puts on every Agent pod. It uses the in-cluster API
// address and service account files.
func NewAgentPods(ref config.AgentRef, getenv func(string) string) (*AgentPods, error) {
	host, port := getenv("KUBERNETES_SERVICE_HOST"), getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, errors.New("not running in a Kubernetes pod")
	}
	ca, err := os.ReadFile(serviceAccountDir + "/ca.crt")
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("service account CA is invalid")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	selector := url.QueryEscape("app=kagent,kagent=" + ref.Name)
	return &AgentPods{
		http:      &http.Client{Transport: transport, Timeout: 10 * time.Second},
		url:       "https://" + net.JoinHostPort(host, port) + "/api/v1/namespaces/" + url.PathEscape(ref.Namespace) + "/pods?labelSelector=" + selector,
		tokenFile: serviceAccountDir + "/token",
	}, nil
}

// Pod returns the Agent's only pod. Terminating pods still exist as objects,
// so a rollout or restart in progress shows as more than one pod.
func (p *AgentPods) Pod(ctx context.Context) (Pod, error) {
	pods, err := p.list(ctx)
	if err != nil {
		return Pod{}, err
	}
	return pods.only()
}

// PodUIDs lists the UID of every pod object of the Agent.
func (p *AgentPods) PodUIDs(ctx context.Context) ([]string, error) {
	pods, err := p.list(ctx)
	if err != nil {
		return nil, err
	}
	uids := make([]string, 0, len(pods.Items))
	for _, pod := range pods.Items {
		if pod.Metadata.UID == "" {
			return nil, errors.New("an Agent pod has no UID")
		}
		uids = append(uids, pod.Metadata.UID)
	}
	return uids, nil
}

func (p *AgentPods) list(ctx context.Context) (podList, error) {
	// Projected tokens rotate, so read the token on every call.
	token, err := os.ReadFile(p.tokenFile)
	if err != nil {
		return podList{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, p.url, nil)
	if err != nil {
		return podList{}, err
	}
	request.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	request.Header.Set("Accept", "application/json")
	response, err := p.http.Do(request)
	if err != nil {
		return podList{}, err
	}
	defer response.Body.Close() //nolint:errcheck
	if response.StatusCode != http.StatusOK {
		return podList{}, fmt.Errorf("listing the Agent pods answered %d", response.StatusCode)
	}
	var pods podList
	if err := json.NewDecoder(io.LimitReader(response.Body, maxV0ResponseBytes)).Decode(&pods); err != nil {
		return podList{}, err
	}
	return pods, nil
}

type podList struct {
	Items []struct {
		Metadata struct {
			UID               string            `json:"uid"`
			DeletionTimestamp *string           `json:"deletionTimestamp"`
			Annotations       map[string]string `json:"annotations"`
		} `json:"metadata"`
		Spec struct {
			Containers []struct {
				Name  string `json:"name"`
				Ports []struct {
					Name          string `json:"name"`
					ContainerPort int    `json:"containerPort"`
				} `json:"ports"`
			} `json:"containers"`
		} `json:"spec"`
		Status struct {
			Phase      string `json:"phase"`
			PodIP      string `json:"podIP"`
			Conditions []struct {
				Type   string `json:"type"`
				Status string `json:"status"`
			} `json:"conditions"`
			ContainerStatuses []struct {
				Name    string `json:"name"`
				ImageID string `json:"imageID"`
			} `json:"containerStatuses"`
		} `json:"status"`
	} `json:"items"`
}

// only requires exactly one pod object, running, ready, and not being
// deleted. kagent v0 cancels a task only in the pod that runs it, so a
// wrapped Agent must run a single replica.
func (l podList) only() (Pod, error) {
	if len(l.Items) != 1 {
		return Pod{}, ErrRollout
	}
	pod := l.Items[0]
	ready := false
	for _, condition := range pod.Status.Conditions {
		ready = ready || (condition.Type == "Ready" && condition.Status == "True")
	}
	if pod.Metadata.DeletionTimestamp != nil || pod.Status.Phase != "Running" || !ready {
		return Pod{}, ErrRollout
	}
	hash := pod.Metadata.Annotations[v0ConfigHashAnnotation]
	if !v0ConfigHash.MatchString(hash) {
		return Pod{}, errors.New("the Agent pod has no kagent config hash")
	}
	if pod.Metadata.UID == "" || net.ParseIP(pod.Status.PodIP) == nil {
		return Pod{}, errors.New("the Agent pod has no UID or address")
	}
	// The Agent runs in the one container that serves the A2A port.
	container, port := "", 0
	for _, c := range pod.Spec.Containers {
		for _, p := range c.Ports {
			if p.Name != v0PodPort {
				continue
			}
			if container != "" || p.ContainerPort < 1 || p.ContainerPort > 65535 {
				return Pod{}, errors.New("the Agent pod has no single A2A port")
			}
			container, port = c.Name, p.ContainerPort
		}
	}
	if container == "" {
		return Pod{}, errors.New("the Agent pod has no single A2A port")
	}
	digest := ""
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name != container {
			continue
		}
		match := v0ImageDigest.FindStringSubmatch(status.ImageID)
		if match == nil || digest != "" {
			return Pod{}, errors.New("the Agent container reports no image digest")
		}
		digest = match[1]
	}
	if digest == "" {
		return Pod{}, errors.New("the Agent container reports no image digest")
	}
	return Pod{
		UID: pod.Metadata.UID, ConfigHash: hash, ImageDigest: digest,
		Address: net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(port)),
	}, nil
}
