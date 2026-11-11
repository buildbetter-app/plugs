package converterroundtrip

import (
 "context"
 "encoding/json"
 "errors"
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
)

func nativeStreamingProfile(t *testing.T,archive *dumpformat.Archive,store driver.Store,lease driver.ActiveLease) driver.RestoreImportProfile {
 t.Helper()
 plan:=nativeHistoryPlanFromArchive(t,archive,store,lease)
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
   report,err:=RestoreNativeCaptureSetImportWithLease(ctx,archive,store,lease,profile);if err!=nil {t.Fatal("actual wrapper to verifier mint to real stream engine",err)}
   if report.Receipt==nil || report.Restored.Head!=model.Natural64(33) || report.Restored.HistoryFloor!=model.Natural64(0) {t.Fatal("stream receipt/range missing",report.Restored)}
   reader,err:=store.Read(ctx,driver.View{Incarnation:report.Restored.Incarnation});if err!=nil {t.Fatal(err)}
   rows,readErr:=reader.Get(ctx,[]model.Ref{model.RefByID(driver.OfflineHistoryEvidenceID,model.Natural64(0))});closeErr:=reader.Close();if readErr!=nil || closeErr!=nil || len(rows)!=1 {t.Fatal("real stream evidence missing",readErr,closeErr)}
   var marker struct{Version string `json:"streamVersion"`;History string `json:"historySha256"`;Mapping []struct{Parents []string `json:"parentIds"`} `json:"mapping"`}
   if json.Unmarshal(rows[0].Body,&marker)!=nil || marker.Version!=driver.OfflineHistoryStreamVersion || marker.History!=profile.HistoryDigest || len(marker.Mapping)!=32 {t.Fatal("production fell back from verified stream route")}
   if err=VerifyNativeCaptureSetReadback(ctx,store,archive,report);err!=nil {t.Fatal("independent real physical/current/history readback",err)}
   if err=release();err!=nil {t.Fatal(err)}
   store,_=reopen()
   recoveryCtx:=archive.WithNativeDeltaAccounting(context.Background())
   _,recovered,found,err:=RecoverBoundSourceImport(recoveryCtx,store)
   if recovered.Archive!=nil {defer recovered.Archive.ReleaseNativeDecoded()}
   if err!=nil || !found || !reflect.DeepEqual(recovered.Receipt,report.Receipt) {t.Fatal("original keyed stream receipt after actual engine reopen",err)}
   if err=VerifyNativeCaptureSetReadback(recoveryCtx,store,archive,recovered);err!=nil {t.Fatal("reopened independent stream/native readback",err)}
   t.Logf("NATIVE_STREAM_PRODUCTION engine=%s archive=%s stream=%s commits=32 receipt=original reopen=actual",engine,archive.SHA256(),profile.HistoryDigest)
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
   for _,mismatch:=range []string{"archive","derived-current","intermediate-transcript","header","clock","full-request","zero-plan","plan-before-final-digest","lifetime"} {
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
     case "lifetime":if request.Lifetime==driver.CheckpointLifetimeProcess {request.Lifetime=driver.CheckpointLifetimeRestart}else{request.Lifetime=driver.CheckpointLifetimeProcess}
     }
     _,err=stream.Seal(ctx,request,plan);if err==nil {t.Fatal("mismatched native checkpoint minted",mismatch)}
     var refused historyverify.ErrCheckpoint
     if !errors.As(err,&refused) && !errors.Is(err,driver.ErrConflict) {t.Fatal("missing typed pre-staging refusal",err)}
     if !reflect.DeepEqual(before,snapshot(t,store)) {t.Fatal("refusal mutated target/receipt/checkpoint",mismatch)}
    })
   }
  })
 }
}
