package driver

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"reflect"
	"slices"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// These errors all retain ErrUnsupported compatibility, but their meanings
// are distinct. Unknown policy/plan nodes are hard refusals. Unavailable
// acceleration may use an exact existing path. A check-only host must use its
// separately bounded interpreter, never post-filter an already limited page.
var (
	ErrSelectionInvalid     = fmt.Errorf("%w: invalid selection plan", ErrUnsupported)
	ErrSelectionUnavailable = fmt.Errorf("%w: selection acceleration unavailable", ErrUnsupported)
	ErrSelectionCheckOnly   = fmt.Errorf("%w: selection requires bounded host checks", ErrUnsupported)
)

const SelectionCheckOnlyCap = 20000

type SelectionCheckOnlyError struct{ CandidateCap int }

func (e SelectionCheckOnlyError) Error() string {
	return fmt.Sprintf("%v (candidate cap %d)", ErrSelectionCheckOnly, e.CandidateCap)
}
func (e SelectionCheckOnlyError) Unwrap() error { return ErrSelectionCheckOnly }

// FreshSelectionExistsReader is optional on WriteTx ONLY, after Head. It is
// separate from pinned Select: one fresh, nonlocking committed observation
// excludes this transaction's staged image and has no candidate cap/page/
// offset/keyset/group/summary shortcut. The plan must use count mode, Cap=0, no Excluded refs,
// and current content=authority with a boundary rebound to ACTUAL current
// observations. A changed head/control refuses; implementations must never
// silently rewrite a pinned boundary to a newer head. No extra Lock is taken.
// PostgreSQL uses a fresh READ COMMITTED statement; SQLite preserves its
// existing reservation semantics. True forces caller restart. Every error
// returns false WITH that error; neither an error nor unavailable coverage can
// authorize a terminal null. This does not extend terminal eligibility policy.
type FreshSelectionExistsReader interface {
	FreshSelectionExists(context.Context, SelectionPlan) (bool, error)
}

// SelectionIndexWriter is deliberately separate from SelectionReader. A core
// publisher may use annotations only through this optional capability. Without
// it ordinary logical writes remain supported, but cannot certify selection
// coverage. The complete del/add call is validated before ANY staging, owns its
// inputs, and uses the existing table/key/value and lock/Head/bump contracts.
// Changed sources use the observed prepublication revision AND final staged
// byte digest; Commit rechecks and stamps changed source revisions atomically.
type SelectionIndexWriter interface {
	PutSelectionDerived(context.Context, DerivedTable, []DerivedRow, []DerivedRow) error
}

type SelectionChainMode string

const (
	SelectionChainCompiled SelectionChainMode = "compiled"
	SelectionChainNone     SelectionChainMode = "no_plugins"
)

type SelectionHostMode string

const (
	SelectionHostAbsent    SelectionHostMode = "absent"
	SelectionHostCompiled  SelectionHostMode = "compiled"
	SelectionHostCheckOnly SelectionHostMode = "check_only"
)

// SelectionBoundarySpec is supplied ONLY by trusted composition. PreparedBinding
// is the versioned digest of W2's immutable full caller (verification, exact
// epoch, sponsor and caveats), namespace, action/resource/method and policy.
// A separately domain-bound actual no-plugin host may use its own equivalent
// caller/method/installation binding; it cannot label that a W2 chain digest.
// It is not a pointer, credential or cached authorization decision. Sources
// record ALL current controls, including misses/tombstones and fact-source
// integrity; ACL generation alone cannot stand for source bytes. ViewRevision
// retains the existing authority binding's view revision (zero when absent).
//
// Chain compilation alone does not cover an extra AuthorizeRead callback.
// Composition must explicitly declare it absent, lower its complete semantics,
// or choose check_only. A check_only boundary cannot execute Select or gain the
// 100k summary exception. Callback functions/SQL/engine handles never cross here.
// Every Bind/Rebind obtains fresh current observations. A pinned result STILL
// requires the caller's fresh final authority fence, including empty results.
type SelectionBoundarySpec struct {
	Version                         uint8
	PreparedBinding                 string
	Incarnation                     string
	Branch                          model.Branch
	AuthorityRevision, ViewRevision model.Revision
	// A nonzero view revision identifies an actual present source witness;
	// it cannot be free metadata or be replaced by ACL-only equality.
	ViewSource *LockKey
	// InlineViewBinding is an exact typed definition identity owned and checked
	// by host composition. It is mutually exclusive with a stored ViewSource.
	// A driver schema constructor alone cannot attest that it is the used view.
	InlineViewBinding string
	Sources           []SelectionSource
	Chain             SelectionChainMode
	Authorization     SelectionExpr
	Host              SelectionHostMode
	HostExpression    *SelectionExpr
}

// SelectionBoundary owns a sealed description. Neither the builder's mutable
// inputs nor Description's output can change the binding used by a selector.
type SelectionBoundary struct {
	encoded []byte
	spec    SelectionBoundarySpec
}

func PrepareSelectionBoundary(s SelectionBoundarySpec) (*SelectionBoundary, error) {
	if err := s.validate(); err != nil {
		return nil, selectionError(err.Error())
	}
	encoded, err := selectionBytes(s)
	if err != nil {
		return nil, err
	}
	return &SelectionBoundary{encoded: encoded, spec: cloneSelectionBoundary(s)}, nil
}

func (b *SelectionBoundary) Description() (SelectionBoundarySpec, error) {
	var s SelectionBoundarySpec
	if b == nil || len(b.encoded) == 0 {
		return s, selectionError("missing trusted read boundary")
	}
	return cloneSelectionBoundary(b.spec), nil
}

// GobEncode binds the immutable description into the existing page binding.
// There is intentionally no exported mutator/GobDecode on this sealed value.
func (b *SelectionBoundary) GobEncode() ([]byte, error) {
	if b == nil || len(b.encoded) == 0 {
		return nil, selectionError("missing trusted read boundary")
	}
	return bytes.Clone(b.encoded), nil
}

func (s SelectionSource) Validate() error {
	if err := s.Key.Validate(); err != nil {
		return err
	}
	if s.Key.Ref != nil && !s.Key.Ref.Branch.IsZero() {
		return selectionError("source branch mismatch")
	}
	if _, err := s.Revision.Uint64(); err != nil {
		return err
	}
	data := s.Key.Kind == LockRecord || s.Key.Kind == LockDerived
	if s.Present && data {
		if !canonicalDigest(s.Digest) {
			return selectionError("invalid source digest")
		}
	} else if s.Digest != "" {
		return selectionError("digest without source bytes")
	}
	return nil
}

func (s SelectionBoundarySpec) validate() error {
	if s.Version != 1 || !canonicalDigest(s.PreparedBinding) || s.Incarnation == "" || s.Branch.Validate() != nil || !s.Branch.IsZero() {
		return selectionError("invalid prepared authority binding")
	}
	a, err := s.AuthorityRevision.Uint64()
	if err != nil {
		return err
	}
	v, err := s.ViewRevision.Uint64()
	if err != nil {
		return err
	}
	if v > a {
		return selectionError("view revision exceeds authority")
	}
	nodes := 0
	if err := validateExpr(s.Authorization, 0, &nodes); err != nil {
		return err
	}
	switch s.Chain {
	case SelectionChainCompiled:
	case SelectionChainNone:
		if s.Authorization.Op != SelectionTrue {
			return selectionError("invalid no-plugin boundary")
		}
	default:
		return selectionError("unknown chain mode")
	}
	switch s.Host {
	case SelectionHostAbsent, SelectionHostCheckOnly:
		if s.HostExpression != nil {
			return selectionError("unexpected host expression")
		}
	case SelectionHostCompiled:
		if s.HostExpression == nil {
			return selectionError("missing host expression")
		}
		if err := validateExpr(*s.HostExpression, 0, &nodes); err != nil {
			return err
		}
	default:
		return selectionError("unknown host mode")
	}
	if len(s.Sources) == 0 || len(s.Sources) > SelectionMaxNodes {
		return selectionError("missing or oversized authority observations")
	}
	seen := map[string]bool{}
	viewObserved := false
	if (s.ViewSource == nil) != vIsZero(v) {
		return selectionError("view revision requires its source")
	}
	viewKey := ""
	if s.InlineViewBinding != "" && (!canonicalDigest(s.InlineViewBinding) || s.ViewSource != nil) {
		return selectionError("invalid inline view binding")
	}
	if s.ViewSource != nil {
		if err := s.ViewSource.Validate(); err != nil {
			return err
		}
		viewKey, err = EncodeLockKey(*s.ViewSource)
		if err != nil {
			return err
		}
	}
	for _, source := range s.Sources {
		if err := source.Validate(); err != nil {
			return err
		}
		key, err := EncodeLockKey(source.Key)
		if err != nil {
			return err
		}
		if seen[key] {
			return selectionError("duplicate authority observation")
		}
		seen[key] = true
		n, _ := source.Revision.Uint64()
		if n > a {
			return selectionError("future authority observation")
		}
		if key == viewKey && source.Present && n == v {
			viewObserved = true
		}
	}
	if s.ViewSource != nil && !viewObserved {
		return selectionError("view source observation mismatch")
	}
	return nil
}

func vIsZero(v uint64) bool { return v == 0 }

func selectionExprBytes(e SelectionExpr) ([]byte, error) { return selectionBytes(e) }

// The closed encoding retains pointer presence (false/zero/empty), nil vs
// explicit empty vectors, and arbitrary logical key/string bytes. Encoding
// through gob/JSON would respectively lose pointer-zero or non-UTF8 identity.
// Callers validate the whole bounded AST before encoding or cloning.
func selectionBytes(value any) ([]byte, error) {
	out := []byte("plug.selection.encoding/2\x00")
	var encode func(reflect.Value) error
	write := func(s string) { out = binary.AppendUvarint(out, uint64(len(s))); out = append(out, s...) }
	encode = func(v reflect.Value) error {
		write(v.Type().String())
		// Ref owns a private presence discriminator. Encode its public semantic
		// union, including an explicitly selected empty arm, without traversing
		// private model state or changing raw logical identity bytes.
		if v.Type() == reflect.TypeOf(model.Ref{}) {
			r := v.Interface().(model.Ref)
			if r.IsID() {
				out = append(out, 1)
			} else {
				out = append(out, 2)
			}
			write(r.ID)
			write(r.Key)
			write(r.Branch.String())
			return nil
		}
		switch v.Kind() {
		case reflect.Pointer:
			if v.IsNil() {
				out = append(out, 0)
				return nil
			}
			out = append(out, 1)
			return encode(v.Elem())
		case reflect.Slice:
			if v.IsNil() {
				out = append(out, 0)
				return nil
			}
			out = append(out, 1)
			out = binary.AppendUvarint(out, uint64(v.Len()))
			for i := 0; i < v.Len(); i++ {
				if err := encode(v.Index(i)); err != nil {
					return err
				}
			}
		case reflect.Struct:
			out = binary.AppendUvarint(out, uint64(v.NumField()))
			for i := 0; i < v.NumField(); i++ {
				field := v.Type().Field(i)
				if !field.IsExported() {
					return selectionError("private field in closed encoding")
				}
				write(field.Name)
				if err := encode(v.Field(i)); err != nil {
					return err
				}
			}
		case reflect.String:
			write(v.String())
		case reflect.Bool:
			if v.Bool() {
				out = append(out, 1)
			} else {
				out = append(out, 0)
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			out = binary.AppendVarint(out, v.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			out = binary.AppendUvarint(out, v.Uint())
		default:
			return selectionError("non-closed encoding value")
		}
		return nil
	}
	if err := encode(reflect.ValueOf(value)); err != nil {
		return nil, err
	}
	return out, nil
}
func cloneSelectionKey(k LockKey) LockKey {
	if k.Ref != nil {
		r := *k.Ref
		k.Ref = &r
	}
	if k.Derived != nil {
		r := *k.Derived
		k.Derived = &r
	}
	return k
}
func cloneSelectionValue(value *SelectionValue) *SelectionValue {
	if value == nil {
		return nil
	}
	v := *value
	if v.String != nil {
		x := *v.String
		v.String = &x
	}
	if v.Bool != nil {
		x := *v.Bool
		v.Bool = &x
	}
	if v.Int != nil {
		x := *v.Int
		v.Int = &x
	}
	if v.Ref != nil {
		x := *v.Ref
		v.Ref = &x
	}
	return &v
}
func cloneSelectionExpr(e SelectionExpr) SelectionExpr {
	e.Args = slices.Clone(e.Args)
	for i := range e.Args {
		e.Args[i] = cloneSelectionExpr(e.Args[i])
	}
	e.Refs = slices.Clone(e.Refs)
	if e.Compare != nil {
		x := *e.Compare
		x.Value = cloneSelectionValue(x.Value)
		e.Compare = &x
	}
	if e.Exists != nil {
		x := *e.Exists
		x.Tests = slices.Clone(x.Tests)
		for i := range x.Tests {
			x.Tests[i].Value = cloneSelectionValue(x.Tests[i].Value)
		}
		e.Exists = &x
	}
	if e.Sources != nil {
		x := *e.Sources
		x.Refs = slices.Clone(x.Refs)
		x.Kinds = slices.Clone(x.Kinds)
		e.Sources = &x
	}
	return e
}
func cloneSelectionBoundary(s SelectionBoundarySpec) SelectionBoundarySpec {
	if s.ViewSource != nil {
		x := cloneSelectionKey(*s.ViewSource)
		s.ViewSource = &x
	}
	s.Sources = slices.Clone(s.Sources)
	for i := range s.Sources {
		s.Sources[i].Key = cloneSelectionKey(s.Sources[i].Key)
	}
	s.Authorization = cloneSelectionExpr(s.Authorization)
	if s.HostExpression != nil {
		x := cloneSelectionExpr(*s.HostExpression)
		s.HostExpression = &x
	}
	return s
}

func (p SelectionPlan) boundary() (SelectionBoundarySpec, error) {
	s, err := p.Boundary.Description()
	if err != nil {
		return s, err
	}
	if err := s.validate(); err != nil {
		return s, err
	}
	if s.Incarnation != p.Incarnation || s.Branch.String() != p.Branch.String() || s.AuthorityRevision.String() != p.AuthorityRevision.String() {
		return s, selectionError("prepared authority snapshot mismatch")
	}
	a, err := selectionExprBytes(p.Authorization)
	if err != nil {
		return s, err
	}
	b, err := selectionExprBytes(s.Authorization)
	if err != nil {
		return s, err
	}
	if !bytes.Equal(a, b) {
		return s, selectionError("authorization differs from sealed boundary")
	}
	return s, nil
}
