package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/orka-agents/orka-runtime-kagent/internal/kagent"
)

const (
	// admissionRecheck is how long a passing admission check is reused.
	admissionRecheck = 5 * time.Minute
	// unreachableDialTimeout bounds each probe of an address the broker must
	// not reach. A NetworkPolicy that drops packets shows as a timeout.
	unreachableDialTimeout = 3 * time.Second
)

var (
	errUnverifiedBuild = errors.New("kagent controller build is not verified")
	errIsolation       = errors.New("broker can reach an address kagent should keep from it")
)

// admission caches the last check of the kagent install.
type admission struct {
	mu        sync.Mutex
	checkedAt time.Time
	err       error
}

// admit refuses new turns unless the kagent controller build is one this
// release, or the configuration, accepts, and the broker cannot connect to
// any address the install should admit only kagent's controller to. The
// broker depends on behavior read from verified builds, and on the router and
// database isolation it cannot otherwise observe. A passing check is reused
// for a while; a failing one is repeated on the next turn, so recovery is
// noticed at once. Cleanup never waits on it.
func (b *lifecycleBroker) admit(ctx context.Context) error {
	b.admission.mu.Lock()
	defer b.admission.mu.Unlock()
	now := b.cfg.now()
	if b.admission.err == nil && !b.admission.checkedAt.IsZero() && now.Sub(b.admission.checkedAt) < admissionRecheck {
		return nil
	}
	err := b.checkAdmission(ctx)
	if ctx.Err() != nil {
		// An interrupted check proves nothing: every probe after the
		// interruption fails without testing its address.
		return ctx.Err()
	}
	if err != nil && (b.admission.err == nil || b.admission.err.Error() != err.Error()) {
		b.cfg.log.Error("kagent broker is refusing new turns", "reason", err.Error())
	}
	if err == nil && b.admission.err != nil {
		b.cfg.log.Info("kagent broker is admitting turns again")
	}
	b.admission.checkedAt, b.admission.err = now, err
	return err
}

func (b *lifecycleBroker) checkAdmission(ctx context.Context) error {
	if err := b.remote.CheckVersion(ctx); err != nil {
		if errors.Is(err, kagent.ErrUnverifiedBuild) {
			return fmt.Errorf("%w: %w", errUnverifiedBuild, err)
		}
		return fmt.Errorf("%w: the kagent controller version could not be read", errRemote)
	}
	for _, address := range b.cfg.unreachable {
		dialCtx, cancel := context.WithTimeout(ctx, unreachableDialTimeout)
		conn, err := b.cfg.dial(dialCtx, "tcp", address)
		cancel()
		if err == nil {
			_ = conn.Close()
			return fmt.Errorf("%w: %s", errIsolation, address)
		}
	}
	return nil
}
