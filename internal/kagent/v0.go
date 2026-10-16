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
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	a2atype "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"

	"github.com/orka-agents/orka-runtime-kagent/internal/config"
)

// kagent v0.x runs each Agent as a Deployment behind its controller. The
// controller serves sessions over REST and A2A over JSON-RPC; it answers task
// reads from its database and forwards sends and cancels to the Agent.
const (
	v0ConfigHashAnnotation = "kagent.dev/config-hash"
	maxV0ResponseBytes     = 4 << 20
	serviceAccountDir      = "/var/run/secrets/kubernetes.io/serviceaccount"
)

var v0ConfigHash = regexp.MustCompile(`^(0|[1-9][0-9]{0,19})$`)

// ErrRollout reports that the Agent does not run as exactly one ready pod.
// Its config cannot be pinned, and a cancel may reach a pod that is not
// running the task.
var ErrRollout = errors.New("kagent Agent is not exactly one ready pod")

// ConfigPin reports the pods kagent runs an Agent in.
type ConfigPin interface {
	// ConfigHash returns the config hash of the Agent's only pod, or
	// ErrRollout unless exactly one ready pod exists.
	ConfigHash(ctx context.Context) (string, error)
	// PodUIDs lists every pod object of the Agent, terminating ones included.
	PodUIDs(ctx context.Context) ([]string, error)
}

// V0Client talks to one kagent v0.x Agent through its controller.
type V0Client struct {
	a2aAccess
	http     *http.Client
	base     string
	agentRef string
	pin      ConfigPin
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
		base:     strings.TrimSuffix(base.String(), "/"),
		agentRef: cfg.Agent.Namespace + "/" + cfg.Agent.Name,
		pin:      pin,
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
	case status == http.StatusBadRequest || status == http.StatusNotFound:
		// The controller refuses these before it writes anything: a malformed
		// request or an Agent it does not know.
		return Session{}, fmt.Errorf("%w: kagent answered %d", ErrRejected, status)
	case status != http.StatusCreated && status != http.StatusOK:
		// Anything else, including the 500 a deleted ID gets, proves nothing.
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
	status, err := c.do(ctx, http.MethodGet, "/api/sessions/"+url.PathEscape(id)+"?limit=1", userID, nil, &envelope)
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

// CancelTask cancels an unfinished or canceled task and returns it only once
// its executor is done. kagent v0 runs tasks in the Agent pod's in-process
// execution manager, which records a cancellation before the executor has
// unwound but makes a cancel of an unfinished or canceled task wait for a
// running execution to end. A cancel reaches only one pod, so it is sent only
// while the Agent runs exactly one.
//
// The broker never needs to cancel a completed or failed task: kagent's
// executor writes those statuses only as its final statement (return
// queue.Write in go/adk/pkg/a2a/executor.go). kagent v0.10's controller also
// cannot relay the Agent's refusal to cancel one; it answers an internal
// error. A refusal that does arrive is answered with the task itself.
func (c *V0Client) CancelTask(ctx context.Context, userID string, taskID a2atype.TaskID) (*a2atype.Task, error) {
	if _, err := c.pin.ConfigHash(ctx); err != nil {
		return nil, err
	}
	task, err := c.a2aAccess.CancelTask(ctx, userID, taskID)
	if errors.Is(err, a2atype.ErrTaskNotCancelable) {
		return c.GetTask(ctx, userID, taskID)
	}
	return task, err
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
	hash, err := c.pin.ConfigHash(ctx)
	if err != nil {
		return Session{}, err
	}
	return Session{ID: s.ID, Revision: hash}, nil
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
	request.Header.Set(userIDHeader, userID)
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

// ConfigHash returns the config hash of the Agent's only pod. Terminating
// pods still exist as objects, so a rollout or restart in progress shows as
// more than one pod.
func (p *AgentPods) ConfigHash(ctx context.Context) (string, error) {
	pods, err := p.list(ctx)
	if err != nil {
		return "", err
	}
	return pods.configHash()
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
		Status struct {
			Phase      string `json:"phase"`
			Conditions []struct {
				Type   string `json:"type"`
				Status string `json:"status"`
			} `json:"conditions"`
		} `json:"status"`
	} `json:"items"`
}

// configHash requires exactly one pod object, running, ready, and not being
// deleted. kagent v0 cancels a task only in the pod that runs it, so a
// wrapped Agent must run a single replica.
func (l podList) configHash() (string, error) {
	if len(l.Items) != 1 {
		return "", ErrRollout
	}
	pod := l.Items[0]
	ready := false
	for _, condition := range pod.Status.Conditions {
		ready = ready || (condition.Type == "Ready" && condition.Status == "True")
	}
	if pod.Metadata.DeletionTimestamp != nil || pod.Status.Phase != "Running" || !ready {
		return "", ErrRollout
	}
	hash := pod.Metadata.Annotations[v0ConfigHashAnnotation]
	if !v0ConfigHash.MatchString(hash) {
		return "", errors.New("the Agent pod has no kagent config hash")
	}
	return hash, nil
}
