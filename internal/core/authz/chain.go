package authz

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Chain is assembled by the trusted host. Links run in order, followed by all
// rule sets. No plugin is an allow; installed plugins without a permit deny.
// Callers must not mutate its installation configuration during a request.
type Chain struct {
	Namespace Namespace
	Links     []Link
	Rules     []RuleSet
	Budget    time.Duration
	Alert     func(PolicyError)
}

func (c Chain) installed() bool { return len(c.Links)+len(c.Rules) > 0 }
func (c Chain) budget() (time.Duration, error) {
	if c.Budget < 0 || c.Budget > 100*time.Millisecond {
		return 0, errors.New("authorization budget exceeds 100 ms ceiling")
	}
	if c.Budget == 0 {
		return 50 * time.Millisecond, nil
	}
	return c.Budget, nil
}

func bounded[T any](ctx context.Context, b time.Duration, fn func(context.Context) (T, error)) (T, error) {
	ctx, cancel := context.WithTimeout(ctx, b)
	defer cancel()
	// Timer delivery can lag the deadline under load. Do not start or accept
	// component work after the elapsed budget, even before Done is closed.
	budgetError := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
		return nil
	}
	if err := budgetError(); err != nil {
		var zero T
		return zero, err
	}
	type result struct {
		v   T
		err error
	}
	ch := make(chan result, 1)
	go func() {
		var r result
		defer func() {
			if p := recover(); p != nil {
				r.err = fmt.Errorf("authorization component panic: %v", p)
			}
			ch <- r
		}()
		r.v, r.err = fn(ctx)
	}()
	select {
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	case r := <-ch:
		if err := budgetError(); err != nil {
			var zero T
			return zero, err
		}
		return r.v, r.err
	}
}

func scoped(r CheckRequest) bool {
	p := &r.Caller.Principal
	depth := 0
	for p != nil {
		if depth > 1 {
			return true
		}
		depth++
		if len(p.Caveats) > 0 {
			return true
		}
		p = p.Sponsor
	}
	return false
}

func (c Chain) Check(ctx context.Context, s Slice, reqs []CheckRequest) ([]Decision, error) {
	out := make([]Decision, len(reqs))
	for i := range out {
		out[i] = Decision{Effect: NoDecision}
	}
	if !c.installed() {
		for i, r := range reqs {
			if scoped(r) {
				return nil, &Error{Code: "authz_not_installed"}
			}
			out[i] = Decision{Effect: Allow}
		}
		return out, nil
	}
	b, err := c.budget()
	if err != nil {
		return nil, unavailable(err)
	}
	batch := PrepareChecks(reqs)
	reqs, err = batch.frozen()
	if err != nil {
		return nil, unavailable(err)
	}
	valid := make([]bool, len(reqs))
	restrictions := make([][]FilterExpr, len(reqs))
	state := batch.state(s)
	for i, r := range reqs {
		valid[i], err = batch.bindings[i].prepare(ctx, s, c.Namespace, r.Resource, state)
		if err != nil {
			return nil, err
		}
		if valid[i] {
			valid[i], err = batch.bindings[i].caveatsAllow(ctx, s, r.Resource)
			if err != nil {
				return nil, err
			}
		}
		if !valid[i] {
			out[i] = Decision{Effect: Deny, Reasons: []string{"invalid_context_or_caveat"}}
		}
	}
	for j, l := range c.Links {
		input := batch.isolated()
		steps, e := bounded(ctx, b, func(call context.Context) ([]Step, error) { return l.Check(call, s, input) })
		if e != nil {
			return nil, unavailable(e)
		}
		if len(steps) != len(reqs) {
			return nil, unavailable(errors.New("link result cardinality"))
		}
		for i, step := range steps {
			if step.Restriction != nil {
				if e := step.Restriction.Validate(); e != nil {
					return nil, unavailable(e)
				}
				restrictions[i] = append(restrictions[i], *step.Restriction)
			}
			switch step.Effect {
			case Deny:
				out[i] = Decision{Effect: Deny, Reasons: []string{step.Reason}}
			case NoDecision:
			case Allow:
				if j != len(c.Links)-1 || len(c.Rules) > 0 {
					return nil, &Error{Code: "authz_link_allow_not_last"}
				}
				if out[i].Effect != Deny {
					out[i] = Decision{Effect: Allow, Reasons: []string{step.Reason}}
				}
			default:
				return nil, unavailable(errors.New("invalid link effect"))
			}
		}
	}
	for _, rules := range c.Rules {
		prepared, finite := rules.(PreparedRuleSet)
		var input []CheckRequest
		if !finite {
			input = batch.isolated()
		}
		ds, e := bounded(ctx, b, func(call context.Context) ([]Decision, error) {
			if finite {
				return prepared.EvalPrepared(call, s, batch)
			}
			return rules.Eval(call, s, input)
		})
		if e != nil {
			return nil, unavailable(e)
		}
		if len(ds) != len(reqs) {
			return nil, unavailable(errors.New("ruleset result cardinality"))
		}
		for i, d := range ds {
			d, e = c.normalize(d)
			if e != nil {
				return nil, unavailable(e)
			}
			out[i].Errors = append(out[i].Errors, d.Errors...)
			if d.Effect == Deny || (d.Effect == Allow && out[i].Effect != Deny) {
				out[i].Effect = d.Effect
				out[i].Reasons = append(out[i].Reasons, d.Reasons...)
			}
		}
	}
	for i, r := range reqs {
		if out[i].Effect != Allow || !valid[i] {
			out[i].Effect = Deny
			continue
		}
		for _, f := range restrictions[i] {
			ok, e := evalExpr(ctx, s, r, f)
			if e != nil {
				return nil, unavailable(e)
			}
			if !ok {
				out[i].Effect = Deny
				out[i].Reasons = append(out[i].Reasons, "link_restriction")
				break
			}
		}
	}
	return out, nil
}

func (c Chain) normalize(d Decision) (Decision, error) {
	if d.Effect != Allow && d.Effect != Deny && d.Effect != NoDecision {
		return d, errors.New("invalid ruleset effect")
	}
	errored := map[string]bool{}
	for _, e := range d.Errors {
		if e.PolicyID == "" || (e.Effect != Allow && e.Effect != Deny) {
			return d, errors.New("invalid policy error")
		}
		if c.Alert != nil {
			c.Alert(e)
		}
		if e.Effect == Deny {
			d.Effect = Deny
		}
		errored[e.PolicyID] = true
	}
	if d.Effect == Allow {
		reasons := make([]string, 0, len(d.Reasons))
		for _, p := range d.Reasons {
			if !errored[p] {
				reasons = append(reasons, p)
			}
		}
		if len(d.Errors) > 0 && len(reasons) == 0 {
			d.Effect = NoDecision
		}
		d.Reasons = reasons
	}
	return d, nil
}

func (c Chain) Compile(r ListRequest) (FilterTemplate, error) {
	if !c.installed() {
		if scoped(CheckRequest{Caller: r.Caller}) {
			return FilterTemplate{}, &Error{Code: "authz_not_installed"}
		}
		return FilterTemplate{NoPlugins: true, Request: r, Permit: FilterExpr{Op: True}, Forbid: FilterExpr{Op: False}}, nil
	}
	b, err := c.budget()
	if err != nil {
		return FilterTemplate{}, unavailable(err)
	}
	prepared, err := PrepareRequest(r)
	if err != nil {
		return FilterTemplate{}, unavailable(err)
	}
	r = prepared.Request()
	t := FilterTemplate{Namespace: c.Namespace, Request: r, Permit: FilterExpr{Op: False}, Forbid: FilterExpr{Op: False}}
	for j, l := range c.Links {
		input := prepared.Request()
		f, err := bounded(context.Background(), b, func(context.Context) (FilterExpr, error) { return l.Filter(input) })
		if err != nil {
			return FilterTemplate{}, compileError(err)
		}
		if err := f.Validate(); err != nil {
			return FilterTemplate{}, err
		}
		t.Restrictions = append(t.Restrictions, f)
		terminal, ok := l.(TerminalLink)
		if !ok {
			return FilterTemplate{}, ErrCheckOnly
		}
		input = prepared.Request()
		allow, err := bounded(context.Background(), b, func(context.Context) (FilterExpr, error) { return terminal.AllowFilter(input) })
		if err != nil {
			return FilterTemplate{}, compileError(err)
		}
		if err := allow.Validate(); err != nil {
			return FilterTemplate{}, err
		}
		if j == len(c.Links)-1 && len(c.Rules) == 0 {
			t.Permit = allow
		} else if allow.Op != False {
			// A passing restriction may include an illegal Allow subset. The
			// finite filter has no error-valued predicate, so do not turn the
			// per-item P83 error into membership or a successful denial.
			return FilterTemplate{}, ErrCheckOnly
		}
	}
	for _, rules := range c.Rules {
		input := prepared.Request()
		f, err := bounded(context.Background(), b, func(context.Context) (FilterTemplate, error) {
			if compiler, ok := rules.(PreparedCompiler); ok {
				return compiler.CompilePrepared(prepared)
			}
			return rules.Compile(input)
		})
		if err != nil {
			return FilterTemplate{}, compileError(err)
		}
		if !sameTemplateBinding(t, f) {
			return FilterTemplate{}, errors.New("ruleset changed filter binding")
		}
		if err := f.Validate(); err != nil {
			return FilterTemplate{}, err
		}
		t.Permit = Or(t.Permit, f.Permit)
		t.Forbid = Or(t.Forbid, f.Forbid)
		t.Restrictions = append(t.Restrictions, f.Restrictions...)
	}
	return prepared.Filter(t)
}

func compileError(err error) error {
	if errors.Is(err, ErrCheckOnly) {
		return ErrCheckOnly
	}
	return unavailable(err)
}

func freezeChecks(reqs []CheckRequest) ([]CheckRequest, error) {
	out := make([]CheckRequest, len(reqs))
	for i, q := range reqs {
		r, err := FreezeList(ListRequest{Caller: q.Caller})
		if err != nil {
			return nil, err
		}
		q.Caller = r.Caller
		out[i] = q
	}
	return out, nil
}
