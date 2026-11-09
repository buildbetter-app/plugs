package authz

// Frozen 8dab45e interpreter: deliberately does not invoke PreparedRequest,
// PreparedChecks, or finite prepared component methods. Differential chain
// tests supply independent legacy components, never the optimized fixed Eval.
import (
	"context"
	"errors"
)

func legacyChainCheck(c Chain, ctx context.Context, s Slice, reqs []CheckRequest) ([]Decision, error) {
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
	reqs, err = freezeChecks(reqs)
	if err != nil {
		return nil, unavailable(err)
	}
	valid := make([]bool, len(reqs))
	restrictions := make([][]FilterExpr, len(reqs))
	for i, r := range reqs {
		valid[i], err = prepare(ctx, s, c.Namespace, r)
		if err != nil {
			return nil, err
		}
		if valid[i] {
			valid[i], err = caveatsAllow(ctx, s, r)
			if err != nil {
				return nil, err
			}
		}
		if !valid[i] {
			out[i] = Decision{Effect: Deny, Reasons: []string{"invalid_context_or_caveat"}}
		}
	}
	for j, l := range c.Links {
		input, e := freezeChecks(reqs)
		if e != nil {
			return nil, unavailable(e)
		}
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
		input, e := freezeChecks(reqs)
		if e != nil {
			return nil, unavailable(e)
		}
		ds, e := bounded(ctx, b, func(call context.Context) ([]Decision, error) { return rules.Eval(call, s, input) })
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

func legacyChainCompile(c Chain, r ListRequest) (FilterTemplate, error) {
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
	r, err = FreezeList(r)
	if err != nil {
		return FilterTemplate{}, unavailable(err)
	}
	t := FilterTemplate{Namespace: c.Namespace, Request: r, Permit: FilterExpr{Op: False}, Forbid: FilterExpr{Op: False}}
	for j, l := range c.Links {
		input, err := FreezeList(r)
		if err != nil {
			return FilterTemplate{}, err
		}
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
		input, err = FreezeList(r)
		if err != nil {
			return FilterTemplate{}, err
		}
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
		input, err := FreezeList(r)
		if err != nil {
			return FilterTemplate{}, err
		}
		f, err := bounded(context.Background(), b, func(context.Context) (FilterTemplate, error) { return rules.Compile(input) })
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
	return t, t.Validate()
}
