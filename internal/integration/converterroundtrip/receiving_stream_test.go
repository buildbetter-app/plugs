package converterroundtrip

import (
 "bytes"
 "context"
 "encoding/json"
 "errors"
 "fmt"
 "io"
 "os"
 "reflect"
 "sort"
 "strings"
 "sync"
 "testing"
 "time"

 "github.com/buildbetter-app/plugs/internal/core/dumpformat"
 "github.com/buildbetter-app/plugs/internal/core/model"
 "github.com/buildbetter-app/plugs/internal/core/nativeprojection"
 "github.com/buildbetter-app/plugs/internal/storage/driver"
 "github.com/buildbetter-app/plugs/internal/storage/historyverify"
 "github.com/buildbetter-app/plugs/internal/storage/memory"
)

func nativeStreamingProfile(t *testing.T,archive *dumpformat.Archive,store driver.Store,lease driver.ActiveLease) driver.RestoreImportProfile {
 t.Helper()
 work,release:=archive.NativeDeltaAccountingScope()
 plan:=nativeHistoryPlanFromArchive(t,work,store,lease)
 release()
 plan.Dispositions[0].StorageClass=""
 for _,record:=range archive.Records("bead") {
  storage,present:=record.Source.Fields["storage_class"]
  if !plan.Dispositions[0].MatchesStorageClass(storage,present) {t.Fatal("genuine public source storage intent differs")}
 }
 statuses:=map[string]bool{}
 for _,project:=range archive.Manifest().Projects {for _,status:=range project.Statuses {statuses[status.Name]=true}}
 plan.Dispositions[0].Statuses=nil
 for status:=range statuses {plan.Dispositions[0].Statuses=append(plan.Dispositions[0].Statuses,driver.SourcePlaneStringPairV1{From:status,To:status})}
 sort.Slice(plan.Dispositions[0].Statuses,func(i,j int)bool{return plan.Dispositions[0].Statuses[i].From<plan.Dispositions[0].Statuses[j].From})
 p:=driver.RestoreImportProfile{Admission:driver.RestoreImportAdmission{Mode:"authenticated",Actor:model.PrincipalRef{Kind:"user",ID:"restore-admin"},Authority:"restore-admin",AdmissionDigest:strings.Repeat("a",64)},SourceStoreID:driver.StoreID(archive.Manifest().StoreID),Method:"offline.restore.import",IdempotencyKey:strings.Repeat("b",32),ArchiveDigest:archive.SHA256(),HistoryMode:driver.RestoreHistoryModeFull,HistoryTime:nativeFixtureTime.Format(time.RFC3339Nano)}
 bound,err:=BindNativeCaptureSetImport(context.Background(),archive,p,plan);if err!=nil {t.Fatal("production bind genuine delta archive",err)};return bound
}

func TestNativeReceivingVerifierStreamingProductionAllDrivers(t *testing.T) {
 for _,engine:=range []string{"memory","sqlite","postgres"} {
  t.Run(engine,func(t *testing.T){
   archive:=publicNativeDeltaArchive(t);defer archive.ReleaseNativeDecoded()
   ctx:=archive.WithNativeDeltaAccounting(context.Background())
   store,initial,reopen:=openIdentityTarget(t,engine)
   if err:=initial.Release(ctx);err!=nil {t.Fatal(err)}
   ctx,lease,release,err:=HoldRestoreLease(ctx,store,"native-stream-production");if err!=nil {t.Fatal(err)};release=sync.OnceValue(release);defer release()
   profile:=nativeStreamingProfile(t,archive,store,lease)
   observed:=nativeStreamObservedStore(t,store)
   report,err:=RestoreNativeCaptureSetImportWithLease(ctx,archive,observed,lease,profile);if err!=nil {t.Fatal("actual wrapper to verifier mint to real stream engine",err)}
   if report.Receipt==nil || report.Restored.Head!=model.Natural64(33) || report.Restored.HistoryFloor!=model.Natural64(0) {t.Fatal("stream receipt/range missing",report.Restored)}
   reader,err:=store.Read(ctx,driver.View{Incarnation:report.Restored.Incarnation});if err!=nil {t.Fatal(err)}
   rows,readErr:=reader.Get(ctx,[]model.Ref{model.RefByID(driver.OfflineHistoryEvidenceID,model.Natural64(0))});closeErr:=reader.Close();if readErr!=nil || closeErr!=nil || len(rows)!=1 {t.Fatal("real stream evidence missing",readErr,closeErr)}
   var marker struct{Version string `json:"streamVersion"`;History string `json:"historySha256"`;Mapping []struct{Parents []string `json:"parentIds"`} `json:"mapping"`}
   if json.Unmarshal(rows[0].Body,&marker)!=nil || marker.Version!=driver.OfflineHistoryStreamVersion || marker.History!=profile.HistoryDigest || len(marker.Mapping)!=32 {t.Fatal("production fell back from verified stream route")}
   if err=VerifyNativeCaptureSetReadback(ctx,store,archive,report);err!=nil {t.Fatal("independent real physical/current/history readback",err)}
   nativeStreamRecoveryMarkerControls(t,ctx,archive,store,profile,rows[0].Body)
   t.Run("original-key-indeterminate-recovery",func(t *testing.T){
    writes:=observed.writes;before:=snapshot(t,store)
    unknown:=driver.ErrIndeterminate{IdempotencyKey:profile.IdempotencyKey}
    call,cancel:=context.WithCancel(ctx);cancel()
    recovered,err:=recoverNativeReceivingIndeterminate(call,observed,profile,Verification{ArchiveSHA256:profile.ArchiveDigest,DerivedSHA256:profile.DerivedDigest},unknown)
    if recovered.Archive!=nil {defer recovered.Archive.ReleaseNativeDecoded()}
    if err!=nil || !reflect.DeepEqual(recovered.Receipt,report.Receipt) || observed.writes!=writes {t.Fatal("actual original durable receipt was not recovered without replay",err,observed.writes,writes)}
    wrong:=driver.ErrIndeterminate{IdempotencyKey:strings.Repeat("d",32)}
    _,err=recoverNativeReceivingIndeterminate(ctx,observed,profile,Verification{ArchiveSHA256:profile.ArchiveDigest,DerivedSHA256:profile.DerivedDigest},wrong)
    var retained driver.ErrIndeterminate;if !errors.As(err,&retained) || retained.IdempotencyKey!=wrong.IdempotencyKey {t.Fatal("wrong original key was resolved",err)}
    if !reflect.DeepEqual(before,snapshot(t,store)) || observed.writes!=writes {t.Fatal("indeterminate recovery mutated durable receipt or target")}
   })
   if err=release();err!=nil {t.Fatal(err)}
   store,_=reopen()
   recoveryCtx:=archive.WithNativeDeltaAccounting(context.Background())
   _,recovered,found,err:=RecoverBoundSourceImport(recoveryCtx,store)
   if recovered.Archive!=nil {defer recovered.Archive.ReleaseNativeDecoded()}
   if err!=nil || !found || !reflect.DeepEqual(recovered.Receipt,report.Receipt) {t.Fatal("original keyed stream receipt after actual engine reopen",err)}
   if err=VerifyNativeCaptureSetReadback(recoveryCtx,store,archive,recovered);err!=nil {t.Fatal("reopened independent stream/native readback",err)}
   if engine=="memory" {
    capability,ok:=store.(driver.WorkflowCheckpointStore);if !ok || capability.WorkflowCheckpointLifetime()!=driver.CheckpointLifetimeProcess {t.Fatal("memory checkpoint lifetime is not the real process lifetime")}
    fresh:=memory.New();defer fresh.Close()
    _,_,found,err:=RecoverBoundSourceImport(context.Background(),fresh);if err!=nil || found {t.Fatal("fresh memory instance retained another process receipt",err)}
   }
   t.Logf("NATIVE_STREAM_PRODUCTION engine=%s archive=%s stream=%s commits=32 receipt=original lifetime=%s",engine,archive.SHA256(),profile.HistoryDigest,map[bool]string{true:"process-preserved",false:"actual-close-reopen"}[engine=="memory"])
  })
 }
}

func TestNativeVerifierFinalBindingsRefuseBeforeTargetWriteAllDrivers(t *testing.T) {
 for _,engine:=range []string{"memory","sqlite","postgres"} {
  t.Run(engine,func(t *testing.T){
   archive:=publicNativeDeltaArchive(t);defer archive.ReleaseNativeDecoded()
   ctx:=archive.WithNativeDeltaAccounting(context.Background())
   store,initial,_:=openIdentityTarget(t,engine)
   if err:=initial.Release(ctx);err!=nil {t.Fatal(err)}
   ctx,lease,release,err:=HoldRestoreLease(ctx,store,"native-stream-refusals");if err!=nil {t.Fatal(err)};defer release()
   profile:=nativeStreamingProfile(t,archive,store,lease)
   when,_:=time.Parse(time.RFC3339Nano,profile.HistoryTime)
   meta,err:=store.Meta(ctx);if err!=nil {t.Fatal(err)}
   capability,err:=driver.RequireWorkflowCheckpointStore(store,lease);if err!=nil {t.Fatal(err)}
   before:=snapshot(t,store)
   for _,mismatch:=range []string{"archive","derived-current","intermediate-transcript","header","clock","full-request","zero-plan","plan-before-final-digest","lifetime","admitted-ceiling"} {
    t.Run(mismatch,func(t *testing.T){
     work,release:=archive.NativeDeltaAccountingScope();defer release()
     stream,err:=historyverify.PrepareNativeArchive(ctx,work,profile,when);if err!=nil {t.Fatal(err)};defer stream.Close()
     final:=profile
     if mismatch=="plan-before-final-digest" {final.HistoryDigest=""}
     plan,err:=nativeprojection.BuildWithProfile(work,&final,when,ctx);if err!=nil {t.Fatal("real final planner",err)}
     request:=driver.OfflineHistoryStreamRequest{Current:meta,Profile:profile,Header:stream.Header(),When:when,Lifetime:capability.WorkflowCheckpointLifetime(),Limits:historyverify.NativeArchiveLimits()}
     switch mismatch {
     case "archive":request.Profile.ArchiveDigest=strings.Repeat("1",64)
     case "derived-current":request.Profile.DerivedDigest=strings.Repeat("2",64)
     case "intermediate-transcript":request.Profile.HistoryDigest=hashBytes([]byte("other intermediate states and ordered parents; same final HEAD/current"))
     case "header":request.Header.Head++
     case "clock":request.When=request.When.Add(time.Nanosecond)
     case "full-request":request.Profile.IdempotencyKey=strings.Repeat("c",32)
     case "zero-plan":plan=nativeprojection.Plan{}
     case "admitted-ceiling":
      files:=receivingPublicFiles(t)
      alternate,e:=dumpformat.ReadNativeCaptureSet(context.Background(),bytes.NewReader(files["capture-set.json"]),bytes.NewReader(files["head.tar"]),func(_ context.Context,n uint64)(io.ReadCloser,error){return io.NopCloser(bytes.NewReader(files[fmt.Sprintf("parts/%020d.part",n)])),nil},dumpformat.NativeCaptureSetOptions{DecodedLimitBytes:1<<30,DecodedLimitOrigin:"same archive independently admitted at lower ceiling"})
      if e!=nil {t.Fatal(e)};defer alternate.ReleaseNativeDecoded()
      admitted,ok:=archive.NativeDecodedUsage();other,otherOK:=alternate.NativeDecodedUsage()
      if !ok || !otherOK || archive.SHA256()!=alternate.SHA256() || admitted.LimitBytes!=2<<30 || other.LimitBytes!=1<<30 || admitted.LimitBytes==other.LimitBytes {t.Fatal("foreign Plan test requires identical source under genuinely independent different admission ceilings",admitted,other)}
      plan,e=nativeprojection.BuildWithProfile(alternate,&profile,when,alternate.WithNativeDeltaAccounting(context.Background()));if e!=nil {t.Fatal(e)}
     case "lifetime":if request.Lifetime==driver.CheckpointLifetimeProcess {request.Lifetime=driver.CheckpointLifetimeRestart}else{request.Lifetime=driver.CheckpointLifetimeProcess}
     }
     _,err=stream.Seal(ctx,request,plan);if err==nil {t.Fatal("mismatched native checkpoint minted",mismatch)}
     var refused historyverify.ErrCheckpoint
     if !errors.As(err,&refused) && !errors.Is(err,driver.ErrConflict) {t.Fatal("missing typed pre-staging refusal",err)}
     if mismatch=="admitted-ceiling" && (!errors.As(err,&refused) || refused.Reason!="actual receiving archive ledger differs from reauthenticated originals") {t.Fatal("genuine foreign Plan did not reach the verifier's actual admitted-ceiling comparison",err)}
     if !reflect.DeepEqual(before,snapshot(t,store)) {t.Fatal("refusal mutated target/receipt/checkpoint",mismatch)}
    })
   }
  })
 }
}

// This wrapper preserves genuine engine transactions while observing that
// archive/verifier refusal precedes any writer and publication attempt.
type nativeStreamCountingStore struct {
 driver.Store
 driver.WorkflowCheckpointStore
 writes int
}
func nativeStreamObservedStore(t *testing.T,store driver.Store) *nativeStreamCountingStore {
 t.Helper();capability,ok:=store.(driver.WorkflowCheckpointStore);if !ok {t.Fatal("actual workflow capability missing")};return &nativeStreamCountingStore{Store:store,WorkflowCheckpointStore:capability}
}
func (s *nativeStreamCountingStore) Write(ctx context.Context,lease driver.ActiveLease)(driver.WriteTx,error){s.writes++;return s.Store.Write(ctx,lease)}
func (s *nativeStreamCountingStore) Persistent() bool {
 // Preserve the selected engine's real persistence capability. The workflow
 // lifetime marker alone cannot authorize a restart-persistent wrapper.
 actual,ok:=s.Store.(driver.PersistentCheckpointStore);return ok && actual.Persistent()
}

type nativeStreamPart struct {io.Reader;close func()error}
func (p *nativeStreamPart) Close()error{return p.close()}

func TestNativeReceivingVerifierInputDriftTerminalCloseAndCancellationConserveAllDrivers(t *testing.T) {
 for _,engine:=range []string{"memory","sqlite","postgres"} {
  t.Run(engine,func(t *testing.T){
   store,initial,_:=openIdentityTarget(t,engine)
   ctx:=context.Background();if err:=initial.Release(ctx);err!=nil {t.Fatal(err)}
   op,lease,release,err:=HoldRestoreLease(ctx,store,"native-stream-source-refusal");if err!=nil {t.Fatal(err)};defer release()
   observed:=nativeStreamObservedStore(t,store)
   before:=snapshot(t,store)
   for _,fault:=range []string{"opener-drift","trailing-part-bytes","source-close","cancel-source-close"} {
    t.Run(fault,func(t *testing.T){
     files:=receivingPublicFiles(t);armed:=false;closes:=0
     call,cancel:=context.WithCancel(op);defer cancel()
     closeFailure:=errors.New("actual original part Close failed")
     a,err:=dumpformat.ReadNativeCaptureSet(call,bytes.NewReader(files["capture-set.json"]),bytes.NewReader(files["head.tar"]),func(_ context.Context,n uint64)(io.ReadCloser,error){
      raw:=bytes.Clone(files[fmt.Sprintf("parts/%020d.part",n)])
      if armed && fault=="opener-drift" {raw[len(raw)/2]^=1}
      if armed && fault=="trailing-part-bytes" {raw=append(raw,'x')}
      return &nativeStreamPart{Reader:bytes.NewReader(raw),close:func()error{closes++;if armed && fault=="source-close" {return closeFailure};if armed && fault=="cancel-source-close" {cancel()};return nil}},nil
     },dumpformat.NativeCaptureSetOptions{})
     if err!=nil {t.Fatal("initial genuine archive admission",err)};defer a.ReleaseNativeDecoded()
     profile:=nativeStreamingProfile(t,a,store,lease)
     // Binding has an authentic private staging cache. Discard it to require
     // a new production staging pass against the now changed original opener.
     a.ClearNativeDeltaPreparation();baseline,_:=a.NativeDecodedUsage();priorCloses:=closes;armed=true
     _,err=RestoreNativeCaptureSetImportWithLease(a.WithNativeDeltaAccounting(call),a,observed,lease,profile)
     if err==nil || closes<=priorCloses {t.Fatal("changed source minted or source was not closed",fault,err,closes)}
     if fault=="source-close" && !errors.Is(err,closeFailure) {t.Fatal("lost actual source close error",err)}
     if fault=="cancel-source-close" && !errors.Is(err,context.Canceled) {t.Fatal("lost cancellation",err)}
     if observed.writes!=0 || !reflect.DeepEqual(before,snapshot(t,store)) {t.Fatal("source failure touched writer/target/receipt",observed.writes)}
     after,_:=a.NativeDecodedUsage();if baseline.CurrentAccountedDecodedBytes!=after.CurrentAccountedDecodedBytes || !reflect.DeepEqual(baseline.CurrentCategories,after.CurrentCategories) || !reflect.DeepEqual(baseline.CurrentCharges,after.CurrentCharges) {t.Fatal("failed candidate retained decoded reservations",baseline,after)}
    })
   }
  })
 }
}

type nativeStreamFaultSource struct {
 driver.OfflineHistoryStreamSource
 fault string
 used bool
 closes int
 cancel context.CancelFunc
 closeFailure error
}
func (s *nativeStreamFaultSource) Next(ctx context.Context,limits driver.OfflineHistoryStreamLimits)(driver.OfflineHistoryStreamFrame,error){
 if !s.used && s.fault=="cancel-pull" {s.used=true;s.cancel();return driver.OfflineHistoryStreamFrame{},context.Canceled}
 frame,err:=s.OfflineHistoryStreamSource.Next(ctx,limits)
 if err==nil && !s.used && frame.Table!=nil {
  s.used=true
  if s.fault=="physical-frame" {v:=*frame.Table.PhysicalAfter;v.Rows++;frame.Table.PhysicalAfter=&v}
  if s.fault=="truncated" {return driver.OfflineHistoryStreamFrame{},io.EOF}
 }
 return frame,err
}
func(s *nativeStreamFaultSource) Close()error{s.closes++;return errors.Join(s.OfflineHistoryStreamSource.Close(),s.closeFailure)}

func TestNativeReceivingProductionMintStreamFailuresReleaseRealEngineStagingAllDrivers(t *testing.T) {
 for _,engine:=range []string{"memory","sqlite","postgres"} {
  t.Run(engine,func(t *testing.T){
   a:=publicNativeDeltaArchive(t);defer a.ReleaseNativeDecoded()
   ctx:=a.WithNativeDeltaAccounting(context.Background());store,initial,_:=openIdentityTarget(t,engine)
   if err:=initial.Release(ctx);err!=nil {t.Fatal(err)}
   op,lease,release,err:=HoldRestoreLease(ctx,store,"native-stream-stage-refusal");if err!=nil {t.Fatal(err)};defer release()
   p:=nativeStreamingProfile(t,a,store,lease);when,_:=time.Parse(time.RFC3339Nano,p.HistoryTime)
   before:=snapshot(t,store);meta,err:=store.Meta(op);if err!=nil {t.Fatal(err)}
   capability,err:=driver.RequireWorkflowCheckpointStore(store,lease);if err!=nil {t.Fatal(err)}
   for _,fault:=range []string{"cancel-pull","close-failure","truncated","physical-frame"} {
    t.Run(fault,func(t *testing.T){
     scratch:=t.TempDir();t.Setenv("TMPDIR",scratch)
     work,drop:=a.NativeDeltaAccountingScope();defer drop()
     plan,err:=nativeprojection.BuildWithProfile(work,&p,when,op);if err!=nil {t.Fatal(err)}
     stream,err:=historyverify.PrepareNativeArchive(op,work,p,when);if err!=nil {t.Fatal(err)};defer stream.Close()
     req:=driver.OfflineHistoryStreamRequest{Current:meta,Profile:p,Header:stream.Header(),When:when,Lifetime:capability.WorkflowCheckpointLifetime(),Limits:historyverify.NativeArchiveLimits()}
     seal,err:=stream.Seal(op,req,plan);if err!=nil {t.Fatal("actual production verifier mint",err)}
     locks,err:=stream.LockRequests(op,plan,work);if err!=nil {t.Fatal(err)}
     tx,err:=store.Write(op,lease);if err!=nil {t.Fatal(err)}
     if _,err=tx.Lock(op,locks);err!=nil {t.Fatal(err)};if _,err=tx.Head(op);err!=nil {t.Fatal(err)}
     actual,err:=nativeReceivingStage(tx,seal);if err!=nil {t.Fatal(err)}
     pull,cancel:=context.WithCancel(op);defer cancel()
     source:=&nativeStreamFaultSource{OfflineHistoryStreamSource:stream,fault:fault,cancel:cancel}
     if fault=="close-failure" {source.closeFailure=errors.New("original stream Close failed")}
     prepared,err:=driver.PrepareOfflineHistoryStream(pull,req,source,actual)
     if prepared!=nil || err==nil || source.closes!=1 {t.Fatal("failed real minted stream became publishable",fault,err,source.closes)}
     if fault=="cancel-pull" && !errors.Is(err,context.Canceled) {t.Fatal("lost pull cancellation",err)}
     if fault=="close-failure" && !errors.Is(err,source.closeFailure) {t.Fatal("lost stream Close error",err)}
     if err:=tx.Close();err!=nil {t.Fatal(err)}
     entries,err:=os.ReadDir(scratch);if err!=nil || len(entries)!=0 {t.Fatal("private real-engine spool/scratch survived refusal",entries,err)}
     if !reflect.DeepEqual(before,snapshot(t,store)) {t.Fatal("failed stream mutated target/receipt/history")}
    })
   }
  })
 }
}

func nativeStreamRecoveryMarkerControls(t *testing.T,ctx context.Context,a *dumpformat.Archive,store driver.Store,p driver.RestoreImportProfile,original []byte) {
 t.Helper();when,_:=time.Parse(time.RFC3339Nano,p.HistoryTime)
 stream,err:=historyverify.PrepareNativeArchive(ctx,a,p,when);if err!=nil {t.Fatal(err)};defer stream.Close()
 if err:=stream.ValidateEvidence(ctx,p,original);err!=nil {t.Fatal("actual persisted engine evidence",err)}
 before:=snapshot(t,store)
 for _,attack:=range []string{"ordered-parents","mapping-commit","physical-census","intermediate-image","missing-source-frame"} {
  t.Run("recovery-"+attack,func(t *testing.T){
   var marker map[string]json.RawMessage;if err:=json.Unmarshal(original,&marker);err!=nil {t.Fatal(err)}
   var mapping []map[string]json.RawMessage;if err:=json.Unmarshal(marker["mapping"],&mapping);err!=nil || len(mapping)<2 {t.Fatal(err)}
   switch attack {
   case "ordered-parents":mapping[1]["parentIds"]=json.RawMessage(`["another-ordered-parent"]`)
   case "mapping-commit":mapping[1]["commitIds"]=json.RawMessage(`["another-intermediate-commit"]`)
   }
   marker["mapping"],err=json.Marshal(mapping);if err!=nil {t.Fatal(err)}
   var frames []map[string]json.RawMessage;if err:=json.Unmarshal(marker["sourceFrames"],&frames);err!=nil || len(frames)==0 {t.Fatal(err)}
   if attack=="missing-source-frame" {frames=frames[:len(frames)-1]}
   if attack=="physical-census" || attack=="intermediate-image" {
    changed:=false
    for _,entry:=range frames {
     var frame driver.OfflineHistoryStreamFrame;if err:=json.Unmarshal(entry["frame"],&frame);err!=nil {t.Fatal(err)}
     if attack=="physical-census" && frame.Commit!=nil && frame.Commit.Sequence==2 {frame.Commit.Physical.Tables++;changed=true}
     if attack=="intermediate-image" && frame.Table!=nil && frame.Table.After!=nil {frame.Table.After.SHA256=strings.Repeat("4",64);changed=true}
     if changed {entry["frame"],err=json.Marshal(frame);if err!=nil {t.Fatal(err)};break}
    }
    if !changed {t.Fatal("actual source-frame attack did not alter a preimage")}
   }
   marker["sourceFrames"],err=json.Marshal(frames);if err!=nil {t.Fatal(err)}
   raw,err:=json.Marshal(marker);if err!=nil {t.Fatal(err)}
   err=stream.ValidateEvidence(ctx,p,raw);var refusal historyverify.ErrCheckpoint
   if !errors.As(err,&refusal) {t.Fatal("altered original marker accepted",attack,err)}
   if !reflect.DeepEqual(before,snapshot(t,store)) {t.Fatal("recovery refusal mutated actual target/receipt")}
  })
 }
}
