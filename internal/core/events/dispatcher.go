package events

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

type Binding struct {
	ID          string
	Class       Class
	Principal   model.PrincipalRef
	Filter      Filter
	Handler     Handler
	Projector   Projector
	MaxAttempts uint64
	Timeout     time.Duration
}

// Administration is supplied by the authenticated admin service. A caller
// cannot turn an arbitrary reason or principal string into skip permission.
type Administration interface {
	Authorize(context.Context, string, string, model.PrincipalRef) error
}

type Dispatcher struct {
	Reader      *Reader
	Checkpoints Checkpoints
	Active      Active
	Observer    Observer
	Admin       Administration
	Now         func() time.Time
	mu          sync.Mutex
	locks       map[string]*sync.Mutex
}

type Progress struct {
	Position  Position
	Delivered int
	Advanced  bool
}

func (d *Dispatcher) lock(id string) func() {
	d.mu.Lock()
	if d.locks == nil {
		d.locks = make(map[string]*sync.Mutex)
	}
	lock := d.locks[id]
	if lock == nil {
		lock = &sync.Mutex{}
		d.locks[id] = lock
	}
	d.mu.Unlock()
	lock.Lock()
	return lock.Unlock
}
func (d *Dispatcher) now() time.Time {
	if d.Now != nil {
		return d.Now().UTC()
	}
	return time.Now().UTC()
}
func (d *Dispatcher) active(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.Reader == nil || d.Checkpoints == nil || d.Active == nil {
		return fmt.Errorf("dispatcher requires reader, persisted checkpoints and active lease")
	}
	select {
	case <-d.Active.Lost():
		return errors.New("event dispatcher lease lost")
	default:
		return nil
	}
}
func validateBinding(b Binding) error {
	if b.ID == "" || strings.TrimSpace(b.ID) != b.ID || strings.ContainsAny(b.ID, "\x00\r\n") {
		return fmt.Errorf("invalid event binding ID")
	}
	if b.Class != Post && b.Class != Listener && b.Class != Projection {
		return fmt.Errorf("invalid event delivery class")
	}
	if (b.Class == Projection && b.Projector == nil) || (b.Class != Projection && b.Handler == nil) {
		return fmt.Errorf("binding requires matching handler")
	}
	if b.Timeout < 0 || b.Timeout > 30*time.Second || (b.Class == Post && b.Timeout > time.Second) {
		return fmt.Errorf("binding deadline exceeds host ceiling")
	}
	return nil
}
func (d *Dispatcher) load(ctx context.Context, b Binding) (State, uint64, error) {
	state, generation, err := d.Checkpoints.Load(ctx, b.ID)
	if err != nil {
		return state, generation, err
	}
	if generation == 0 {
		head, err := d.Reader.Log.Head(ctx)
		if err != nil {
			return state, generation, err
		}
		state = State{Position: Position{Incarnation: head.Incarnation}, Class: b.Class}
	}
	if state.Class != b.Class {
		return state, generation, fmt.Errorf("checkpoint delivery class changed")
	}
	return state, generation, nil
}
func (d *Dispatcher) notify(ctx context.Context, b Binding, reason string, failure *Failure) {
	if d.Observer != nil {
		d.Observer.Notify(ctx, Notice{Binding: b.ID, Class: b.Class, Reason: reason, Failure: failure})
	}
}

// Step processes one complete changeset for a projection, or its remaining
// events in order for a listener/post binding. Listeners expose resource IDs
// as partition keys; this initial dispatcher serializes partitions, preserving
// their order. Every successful event is checkpointed after the side effect.
// A crash between the two replays the same DeliveryKey (at least once).
func (d *Dispatcher) Step(ctx context.Context, b Binding) (Progress, error) {
	return d.step(ctx, b, nil)
}
func (d *Dispatcher) step(ctx context.Context, b Binding, liveRevision *model.Revision) (Progress, error) {
	var progress Progress
	if err := validateBinding(b); err != nil {
		return progress, err
	}
	unlock := d.lock(b.ID)
	defer unlock()
	if err := d.active(ctx); err != nil {
		return progress, err
	}
	state, generation, err := d.load(ctx, b)
	if err != nil {
		return progress, err
	}
	progress.Position = state.Position
	if state.Halted {
		d.notify(ctx, b, "halted", state.Failure)
		return progress, ErrHalted
	}
	if state.Stalled {
		d.notify(ctx, b, "stalled", state.Failure)
		return progress, ErrStalled
	}
	page, err := d.Reader.Read(ctx, ReadRequest{After: state.Position, Principal: b.Principal, Filter: b.Filter, Limit: 1000, WholeChangesets: true, Changesets: 2, OneChangeset: true})
	if err != nil {
		// A poison materialization or policy failure must halt a projection at
		// its previous checkpoint. It must never be swallowed as an empty page.
		var boundary *BoundaryError
		if errors.As(err, &boundary) {
			d.notify(ctx, b, "event.gap", state.Failure)
		}
		if b.Class == Projection && !errors.As(err, &boundary) {
			state.Halted = true
			if _, saveErr := d.Checkpoints.Save(ctx, b.ID, generation, state); saveErr != nil {
				return progress, saveErr
			}
			d.notify(ctx, b, "projection_failed", nil)
		}
		return progress, err
	}
	var next *Group
	for i := range page.Groups {
		group := &page.Groups[i]
		if !samePosition(group.End, state.Position) {
			next = group
			break
		}
	}
	if next == nil {
		return progress, nil
	}
	if liveRevision != nil {
		cmp, err := CompareNatural(next.Revision, *liveRevision)
		if err != nil {
			return progress, err
		}
		if cmp > 0 {
			return progress, nil
		}
	}
	stall := time.Duration(0)
	if b.Class == Listener {
		stall = 7 * 24 * time.Hour
	}
	if b.Class == Projection {
		stall = 30 * 24 * time.Hour
	}
	accepted, _ := CompareNatural(next.Revision, state.LagAcceptedThrough)
	if stall > 0 && accepted > 0 && !next.Time.IsZero() && !d.now().Before(next.Time.Add(stall)) {
		state.Stalled = true
		if _, err = d.Checkpoints.Save(ctx, b.ID, generation, state); err != nil {
			return progress, err
		}
		d.notify(ctx, b, "stalled", state.Failure)
		return progress, ErrStalled
	}
	late := liveRevision == nil || !equalNatural(next.Revision, *liveRevision) || state.Failure != nil
	if b.Class == Projection {
		deliveries := make([]Delivery, 0, len(next.Events))
		for _, event := range next.Events {
			deliveries = append(deliveries, delivery(b, event, late))
		}
		if err = invoke(ctx, b, func(call context.Context) error {
			return b.Projector.Apply(call, ChangeDelivery{Incarnation: next.End.Incarnation, Revision: next.Revision, Events: deliveries, Late: late})
		}); err != nil {
			key := DeliveryKey{Position: next.End, Binding: b.ID}
			if len(deliveries) > 0 {
				key = deliveries[0].Key
			}
			return progress, d.failed(ctx, b, &state, generation, key, true)
		}
		if err = d.active(ctx); err != nil {
			return progress, err
		}
		state.Position = next.End
		state.Failure = nil
		if _, err = d.Checkpoints.Save(ctx, b.ID, generation, state); err != nil {
			return progress, err
		}
		return Progress{Position: state.Position, Delivered: len(deliveries), Advanced: true}, nil
	}
	for _, event := range next.Events {
		if err = d.active(ctx); err != nil {
			return progress, err
		}
		item := delivery(b, event, late)
		if err = invoke(ctx, b, func(call context.Context) error { return b.Handler.Deliver(call, item) }); err != nil {
			return progress, d.failed(ctx, b, &state, generation, item.Key, false)
		}
		if err = d.active(ctx); err != nil {
			return progress, err
		}
		state.Position = item.Key.Position
		state.Failure = nil
		generation, err = d.Checkpoints.Save(ctx, b.ID, generation, state)
		if err != nil {
			return progress, err
		}
		progress = Progress{Position: state.Position, Delivered: progress.Delivered + 1, Advanced: true}
	}
	// Filtered events still advance the scan checkpoint. They never create
	// callbacks with data the principal cannot read.
	if !samePosition(state.Position, next.End) {
		state.Position = next.End
		state.Failure = nil
		if _, err = d.Checkpoints.Save(ctx, b.ID, generation, state); err != nil {
			return progress, err
		}
		progress.Position = state.Position
		progress.Advanced = true
	}
	return progress, nil
}

func samePosition(a, b Position) bool {
	return a.Incarnation == b.Incarnation && equalNatural(a.Revision, b.Revision) && equalNatural(a.Index, b.Index)
}
func delivery(b Binding, event model.Event, late bool) Delivery {
	partition := ""
	if event.Durable != nil {
		partition = event.Durable.Resource
	} else if event.Control != nil {
		partition = event.Control.Resource
	}
	return Delivery{Key: DeliveryKey{Position: PositionOf(event), Binding: b.ID}, Event: event, Partition: partition, Late: late}
}

func invoke(ctx context.Context, b Binding, call func(context.Context) error) error {
	budget := b.Timeout
	if budget == 0 {
		budget = 30 * time.Second
		if b.Class == Post {
			budget = 500 * time.Millisecond
		}
	}
	callCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		defer func() {
			if recover() != nil {
				finished <- errors.New("event handler panicked")
			}
		}()
		finished <- call(callCtx)
	}()
	select {
	case err := <-finished:
		return err
	case <-callCtx.Done():
		return callCtx.Err()
	}
}

func (d *Dispatcher) failed(ctx context.Context, b Binding, state *State, generation uint64, key DeliveryKey, projection bool) error {
	// A post timeout must still dead-letter the committed delivery. The
	// independent storage deadline cannot revive a lost lease; Save fences it.
	if ctx.Err() != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()
	}
	failure := &Failure{Key: key, FirstAt: d.now(), Reason: "delivery_failed"}
	if state.Failure != nil && state.Failure.Key.String() == key.String() {
		*failure = *state.Failure
	}
	if failure.Attempts == math.MaxUint64 {
		return fmt.Errorf("event attempt count overflow")
	}
	failure.Attempts++
	maximum := b.MaxAttempts
	if maximum == 0 {
		maximum = 3
	}
	failure.DeadLetter = b.Class == Post || failure.Attempts >= maximum
	state.Failure = failure
	state.Halted = projection || failure.Attempts >= maximum
	if _, err := d.Checkpoints.Save(ctx, b.ID, generation, *state); err != nil {
		return err
	}
	d.notify(ctx, b, "delivery_failed", failure)
	return errors.New("event delivery failed")
}

// RunPost is called only for a fresh committed result, never for an
// idempotency replay. Bindings run in parallel, within the one-second ceiling.
// All failures become warnings on that already committed result.
func (d *Dispatcher) RunPost(ctx context.Context, bindings []Binding, revision model.Revision) []model.Warning {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	warnings := make(chan model.Warning, len(bindings))
	var group sync.WaitGroup
	for _, binding := range bindings {
		group.Add(1)
		go func(b Binding) {
			defer group.Done()
			failed := b.Class != Post
			for !failed {
				progress, err := d.step(ctx, b, &revision)
				if err != nil {
					failed = true
					break
				}
				cmp, err := CompareNatural(progress.Position.Revision, revision)
				if err != nil {
					failed = true
					break
				}
				if cmp >= 0 {
					break
				}
				if !progress.Advanced {
					failed = true
					break
				}
			}
			if failed {
				warnings <- model.Warning{Code: "post_subscriber_failed", Message: "Committed; post subscriber delivery failed", Details: map[string]any{"binding": b.ID}}
			}
		}(binding)
	}
	group.Wait()
	close(warnings)
	result := []model.Warning{}
	for warning := range warnings {
		result = append(result, warning)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Details["binding"].(string) < result[j].Details["binding"].(string)
	})
	return result
}

func (d *Dispatcher) Skip(ctx context.Context, b Binding, by model.PrincipalRef, reason string) error {
	if b.Class == Projection || (b.Class != Listener && b.Class != Post) || strings.TrimSpace(reason) == "" || d.Admin == nil {
		return ErrSkipForbidden
	}
	if err := d.Admin.Authorize(ctx, "event.subscription.skip", b.ID, by); err != nil {
		return err
	}
	unlock := d.lock(b.ID)
	defer unlock()
	if err := d.active(ctx); err != nil {
		return err
	}
	state, generation, err := d.load(ctx, b)
	if err != nil {
		return err
	}
	if state.Failure == nil {
		return ErrSkipForbidden
	}
	state.Skips = append(state.Skips, SkipAudit{Key: state.Failure.Key, Principal: by, Reason: reason, At: d.now()})
	state.Position = state.Failure.Key.Position
	state.Failure = nil
	state.Halted = false
	state.Stalled = false
	_, err = d.Checkpoints.Save(ctx, b.ID, generation, state)
	return err
}

// Resume retries the SAME checkpoint after an operator repairs a projection
// or accepts a stalled binding's lag. It never advances or skips an event.
func (d *Dispatcher) Resume(ctx context.Context, b Binding, by model.PrincipalRef) error {
	if d.Admin == nil {
		return ErrSkipForbidden
	}
	if err := d.Admin.Authorize(ctx, "event.subscription.resume", b.ID, by); err != nil {
		return err
	}
	unlock := d.lock(b.ID)
	defer unlock()
	if err := d.active(ctx); err != nil {
		return err
	}
	state, generation, err := d.load(ctx, b)
	if err != nil {
		return err
	}
	state.Halted = false
	state.Stalled = false
	head, err := d.Reader.Log.Head(ctx)
	if err != nil {
		return err
	}
	state.LagAcceptedThrough = head.Revision
	_, err = d.Checkpoints.Save(ctx, b.ID, generation, state)
	return err
}
