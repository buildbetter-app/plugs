package composition

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"reflect"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/dumpformat"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/integration/converterroundtrip"
	"github.com/buildbetter-app/plugs/internal/server/pluginhost"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	workflowplugin "github.com/buildbetter-app/plugs/plugins/workflow"
)

// RestoreSourcePlanner is trusted composition input. It receives the exact
// admitted archive and selected target's real lifetime. It must supply every
// run, principal, disposition and route; the host never infers those choices.
type RestoreSourcePlanner func(context.Context, io.Reader, driver.SourcePlaneTargetV1) (driver.SourcePlanePlanV1, error)

func (r *Runtime) installSourceWorkflow(ctx context.Context, profile driver.RestoreImportProfile, report converterroundtrip.Report) error {
	if profile.SourcePlanes == nil || report.SourcePlanes == nil || report.Receipt == nil || len(report.SourcePlanes.Providers) != 1 {
		return driver.ErrConflict
	}
	capability, err := driver.RequireWorkflowCheckpointStore(r.Store, r.Lease)
	if err != nil {
		return err
	}
	state, err := pluginhost.NewSourceRuntimeState(ctx, capability, r.Lease, report.SourcePlanes.Providers[0])
	if err != nil {
		return err
	}
	provider, err := workflowplugin.NewSourceRuntime(state, report.SourcePlanes.Providers[0])
	if err != nil {
		return err
	}
	if r.nativeImportOntology {
		if report.Archive == nil || report.Archive.Manifest().StatusCategoryContract != dumpformat.StatusCategoryContract || r.ontologyRuntime == nil {
			return driver.ErrUnsupported
		}
		meta, err := r.Store.Meta(ctx)
		if err != nil {
			return err
		}
		_, snapshot, err := loadConfiguredOntology(ctx, r.Store, meta, "", &OntologyConfig{Source: service.StaticOntologySource(model.RefByID(converterroundtrip.ImportedOntologySourceID, model.Natural64(0)))})
		if err != nil {
			return err
		}
		r.ontologyRuntime.Refresh(snapshot)
	}
	return r.Service.InstallSourceWorkflow(ctx, provider, profile, *report.SourcePlanes, *report.Receipt)
}

func (r *Runtime) recoverSourceWorkflow(ctx context.Context) error {
	profile, report, found, err := converterroundtrip.RecoverBoundSourceImport(ctx, r.Store)
	if err != nil || !found {
		return err
	}
	return r.installSourceWorkflow(ctx, profile, report)
}

func sourceAdmissionDigest(base, joint string) string {
	digest := sha256.Sum256([]byte("plug.restore.source-admission/1\x00" + base + "\x00" + joint))
	return hex.EncodeToString(digest[:])
}

func (a *restoreImportAdapter) importSourceArchive(ctx context.Context, principal model.PrincipalRef, data []byte, request RestoreImportRequest, archive *dumpformat.Archive) (converterroundtrip.Report, error) {
	if (archive.Manifest().StatusCategoryContract == dumpformat.StatusCategoryContract) != a.nativeOntology {
		return converterroundtrip.Report{}, driver.ErrUnsupported
	}
	if a.installSource == nil {
		return converterroundtrip.Report{}, converterroundtrip.ErrRestoreUnavailable
	}
	mode, actor, scope, authority, err := a.binding(ctx, principal, request.AgentLabel)
	if err != nil {
		return converterroundtrip.Report{}, err
	}
	// A lost response or a restarted host recovers the exact protected plan.
	// Replanning against the now-populated target would invent a new identity.
	profile, recovered, found, err := converterroundtrip.RecoverBoundSourceImport(ctx, a.store)
	if err != nil {
		return converterroundtrip.Report{}, err
	}
	if found {
		if profile.ArchiveDigest != archive.SHA256() || profile.IdempotencyKey != request.IdempotencyKey || profile.HistoryMode != request.HistoryMode || request.HistoryDigest != "" && profile.HistoryDigest != request.HistoryDigest || request.HistoryTime != "" && profile.HistoryTime != request.HistoryTime || profile.Admission.Mode != mode || !reflect.DeepEqual(profile.Admission.Actor, actor) || profile.Admission.Scope != scope || profile.Admission.Authority != authority {
			return converterroundtrip.Report{}, driver.ErrRestoreTargetNotEmpty
		}
		if request.SourcePlanes != nil {
			candidate := profile
			candidate.SourcePlanes = nil
			candidate, err = converterroundtrip.BindSourceImport(ctx, bytes.NewReader(data), candidate, *request.SourcePlanes)
			if err != nil || candidate.SourcePlanes.JointPlanDigest != profile.SourcePlanes.JointPlanDigest {
				return converterroundtrip.Report{}, driver.ErrConflict
			}
		}
		if err := a.admit(ctx, principal, request.AgentLabel, profile.Admission.AdmissionDigest, request.Presence); err != nil {
			return converterroundtrip.Report{}, err
		}
		if err := a.installSource(ctx, profile, recovered); err != nil {
			return converterroundtrip.Report{}, err
		}
		return recovered, nil
	}
	capability, err := driver.RequireWorkflowCheckpointStore(a.store, a.lease)
	if err != nil {
		return converterroundtrip.Report{}, err
	}
	meta, err := a.store.Meta(ctx)
	if err != nil {
		return converterroundtrip.Report{}, err
	}
	target := driver.SourcePlaneTargetV1{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, Head: 0, RequireEmpty: true, Lifetime: capability.WorkflowCheckpointLifetime(), RuntimeContract: driver.SourcePlaneRuntimeContractV1}
	var plan driver.SourcePlanePlanV1
	if request.SourcePlanes != nil {
		plan = *request.SourcePlanes
	} else if a.sourcePlanner != nil {
		plan, err = a.sourcePlanner(ctx, bytes.NewReader(data), target)
		if err != nil {
			return converterroundtrip.Report{}, err
		}
	} else {
		return converterroundtrip.Report{}, errors.New("source restore requires the complete trusted source plan")
	}
	if plan.Target != target {
		return converterroundtrip.Report{}, driver.ErrConflict
	}
	profile = driver.RestoreImportProfile{Admission: driver.RestoreImportAdmission{Mode: mode, Actor: actor, Scope: scope, Authority: authority, AdmissionDigest: strings.Repeat("0", 64)}, SourceStoreID: driver.StoreID(archive.Manifest().StoreID), Method: "offline.restore.import", IdempotencyKey: request.IdempotencyKey, ArchiveDigest: archive.SHA256(), HistoryMode: request.HistoryMode, HistoryDigest: request.HistoryDigest, HistoryTime: request.HistoryTime}
	profile, err = converterroundtrip.BindSourceImport(ctx, bytes.NewReader(data), profile, plan)
	if err != nil {
		return converterroundtrip.Report{}, err
	}
	base, err := importAdmissionDigest(mode, actor, scope, authority, request.HistoryMode, archive.Manifest().StoreID, archive.SHA256(), profile.DerivedDigest, request.IdempotencyKey)
	if err != nil {
		return converterroundtrip.Report{}, err
	}
	profile.Admission.AdmissionDigest = sourceAdmissionDigest(base, profile.SourcePlanes.JointPlanDigest)
	if err := a.admit(ctx, principal, request.AgentLabel, profile.Admission.AdmissionDigest, request.Presence); err != nil {
		return converterroundtrip.Report{}, err
	}
	// Presence verification does not replace the immediately-current identity.
	mode2, actor2, scope2, authority2, err := a.binding(ctx, principal, request.AgentLabel)
	if err != nil {
		return converterroundtrip.Report{}, err
	}
	if mode2 != mode || !reflect.DeepEqual(actor2, actor) || scope2 != scope || authority2 != authority {
		return converterroundtrip.Report{}, driver.ErrConflict
	}
	wantedHash, err := profile.RequestHash()
	if err != nil {
		return converterroundtrip.Report{}, err
	}
	report, err := converterroundtrip.RestoreImportWithLease(ctx, bytes.NewReader(data), a.store, a.lease, profile)
	if err != nil {
		var uncertain driver.ErrIndeterminate
		if !errors.As(err, &uncertain) && !errors.Is(err, driver.ErrRestoreTargetNotEmpty) {
			return report, err
		}
	}
	// Recovery proves the committed receipt and origin before live adoption.
	profile, report, found, err = converterroundtrip.RecoverBoundSourceImport(ctx, a.store)
	if err != nil || !found {
		if err == nil {
			err = driver.ErrConflict
		}
		return report, err
	}
	actualHash, err := profile.RequestHash()
	if err != nil || actualHash != wantedHash {
		return report, driver.ErrConflict
	}
	if err := a.installSource(ctx, profile, report); err != nil {
		return report, err
	}
	return report, nil
}
