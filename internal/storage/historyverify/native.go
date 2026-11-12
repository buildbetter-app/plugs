package historyverify

import (
 "context"
 "crypto/sha256"
 "encoding/hex"
 "errors"
 "io"
 "os"
 "sort"
 "time"

 "github.com/buildbetter-app/plugs/internal/core/dumpformat"
 "github.com/buildbetter-app/plugs/internal/core/model"
 "github.com/buildbetter-app/plugs/internal/core/nativeprojection"
 "github.com/buildbetter-app/plugs/internal/storage/driver"
)

// NativeArchiveStream is a private disk-backed transcript produced by the real
// archive verifier. No caller transcript or raw projection can construct one.
// Preparation authenticates the capture; Seal binds the final admitted request
// and actual immutable receiving Plan. Ownership transfers to the driver port.
type NativeArchiveStream struct {
 archive *dumpformat.Archive
 release func()
 file *os.File
 path string
 header driver.OfflineHistoryStreamHeader
 when time.Time
 profileHash, digest string
 head Root
 nativeCurrent map[string]checkpointRecord
 revisions map[string]model.Revision
 files map[string]dumpformat.File
 peakFrame, peakChunk int
 sealed, closed bool
 closeErr error
 pullRelease func()
 originalDir string
 cleanup func() error
}

type checkpointRecord struct {
 ref model.Ref
 kind string
 bytes uint64
 hash string
}
func nativeSHA(data []byte) string { h:=sha256.Sum256(data); return hex.EncodeToString(h[:]) }
func checkpointKey(ref model.Ref) (string,error) { return driver.EncodeLockKey(driver.LockKey{Kind:driver.LockRecord, Ref:&ref}) }
func convertedRecord(r driver.Record) checkpointRecord { return checkpointRecord{r.Ref,r.Kind,uint64(len(r.Body)),nativeSHA(r.Body)} }
func addCheckpoint(records map[string]checkpointRecord,r driver.Record) error {
 key,err:=checkpointKey(r.Ref)
 if err!=nil || !r.Ref.Branch.IsZero() || r.Ref.ID==driver.OfflineHistoryEvidenceID { return ErrCheckpoint{"invalid checkpoint record identity"} }
 if _,ok:=records[key];ok { return ErrCheckpoint{"duplicate checkpoint record identity"} }
 records[key]=convertedRecord(r);return nil
}
func convertedRoot(records map[string]checkpointRecord) (Root,error) {
 keys:=make([]string,0,len(records));for key:=range records {keys=append(keys,key)};sort.Strings(keys)
 h:=NewRootHasher();for _,key:=range keys { r:=records[key];if err:=h.Add(r.ref,r.kind,r.bytes,r.hash);err!=nil {return Root{},err} };return h.Root(),nil
}
func nativeReserveProfile(owner *dumpformat.Archive,phase string,p driver.RestoreImportProfile) error {
 n,err:=dumpformat.NativeDeltaJSONSize(p);if err!=nil{return err}
 raw:=uint64(0);if p.SourcePlanes!=nil {raw=uint64(len(p.SourcePlanes.CanonicalPlan))}
 if n>(^uint64(0)-4096)/32 || raw>(^uint64(0)-n*32-4096)/64 {return ErrCheckpoint{"profile accounting overflow"}}
 // Detached/RequestHash validate and decode the canonical source plan several
 // times. Reserve both wire preimages and their decoded candidates up front.
 return owner.ChargeNativeDeltaBytes(phase,n*32+raw*64+4096)
}
func nativePreparationHash(owner *dumpformat.Archive,p driver.RestoreImportProfile) (string,error) {
 if err:=nativeReserveProfile(owner,"verifier_profile_hash",p);err!=nil{return "",err}
 // Only HistoryDigest is an output. Every other admission/source/target/clock
 // field is fixed before preparation. The final Plan binds the full profile.
 p.HistoryDigest="";return p.RequestHash()
}

// PrepareNativeArchive produces every frame itself, including ordered parent
// IDs and physical preimages. Results stay private through all terminal EOF,
// source Close and recheck boundaries. A digest grants no checkpoint authority.
func PrepareNativeArchive(ctx context.Context,archive *dumpformat.Archive,profile driver.RestoreImportProfile,when time.Time) (_ *NativeArchiveStream,err error) {
 if ctx==nil {return nil,ErrCheckpoint{"missing verification context"}};if err:=ctx.Err();err!=nil {return nil,err}
 if archive==nil || !archive.HasNativeDelta() || archive.SHA256()!=profile.ArchiveDigest || driver.StoreID(archive.Manifest().StoreID)!=profile.SourceStoreID {return nil,ErrCheckpoint{"native archive binding differs"}}
 owner,release:=archive.NativeDeltaAccountingScope()
 s:=&NativeArchiveStream{archive:owner,release:release,when:when,files:map[string]dumpformat.File{},nativeCurrent:map[string]checkpointRecord{},revisions:map[string]model.Revision{}}
 defer func(){if err!=nil {err=errors.Join(err,s.Close())}}()
 if err=nativeReserveProfile(owner,"verifier_profile_detach",profile);err!=nil{return nil,err}
 p,err:=profile.Detached();if err!=nil {return nil,err}
 if p.SourcePlanes==nil || p.HistoryMode!=driver.RestoreHistoryModeFull || when.IsZero() || p.HistoryTime!=when.UTC().Format(time.RFC3339Nano) {return nil,ErrCheckpoint{"native admitted request is incomplete"}}
 s.profileHash,err=nativePreparationHash(owner,p);if err!=nil {return nil,err}
 s.file,err=os.CreateTemp("","plug-native-history-*.frames");if err!=nil {return nil,err};s.path=s.file.Name()
 b,err:=newNativeFrames(ctx,s);if err!=nil {return nil,err}
 // The archive alone issues this immutable view after the complete combined
 // traversal has authenticated EOF, source Close and original-file recheck.
 // Its detached callbacks cannot replace or mutate the authentic candidates.
 prepared,err:=archive.PrepareNativeDelta(ctx);if err!=nil {return nil,err}
 if err=prepared.Visit(ctx,dumpformat.NativeDeltaVisitor{Commit:b.commit,TableState:b.checkpoint,File:b.captureFile});err!=nil {return nil,err}
 if err=b.finish();err!=nil {return nil,err};if err=ctx.Err();err!=nil {return nil,err}
 if p.HistoryDigest!="" && p.HistoryDigest!=s.digest {return nil,ErrCheckpoint{"archive transcript digest differs"}}
 if _,err=s.file.Seek(0,io.SeekStart);err!=nil {return nil,err};return s,nil
}
func (s *NativeArchiveStream) Header() driver.OfflineHistoryStreamHeader {if s==nil {return driver.OfflineHistoryStreamHeader{}};return s.header}
func (s *NativeArchiveStream) Digest() string {if s==nil {return ""};return s.digest}

// RecordRevision reports the source-mapped revision of a current retained-native
// record; zero means the record is created by final current-image publication.
func (s *NativeArchiveStream) RecordRevision(ref model.Ref) model.Revision {
 if s==nil {return model.Natural64(0)};key,err:=checkpointKey(ref);if err!=nil {return model.Natural64(0)};if revision,ok:=s.revisions[key];ok {return revision};return model.Natural64(0)
}

// Seal accepts only the actual immutable receiving Plan built with the FINAL
// full profile. Its entire current and auxiliary image becomes engine proof;
// detached caller snapshots cannot replace any domain or confer authority.
func (s *NativeArchiveStream) Seal(ctx context.Context,request driver.OfflineHistoryStreamRequest,plan nativeprojection.Plan) (Checkpoint,error) {
 if ctx==nil {return Checkpoint{},ErrCheckpoint{"missing verification context"}};if err:=ctx.Err();err!=nil {return Checkpoint{},err}
 if s==nil || s.closed || s.sealed || s.digest=="" || !plan.Built() {return Checkpoint{},ErrCheckpoint{"missing or consumed verified native transcript"}}
 hash,err:=nativePreparationHash(s.archive,request.Profile);if err!=nil {return Checkpoint{},errors.Join(ErrCheckpoint{"invalid final admitted request"},err)}
 if hash!=s.profileHash || request.Profile.HistoryDigest!=s.digest || request.Header!=s.header || !request.When.Equal(s.when) || plan.ArchiveSHA256()!=s.archive.SHA256() || plan.ArchiveSHA256()!=request.Profile.ArchiveDigest || plan.DerivedSHA256()!=request.Profile.DerivedDigest || request.Limits.MaxFrameBytes<s.peakFrame || request.Limits.MaxChunkBytes<s.peakChunk {return Checkpoint{},ErrCheckpoint{"archive, transcript, clock or receiving plan binding differs"}}
 if err:=nativeReserveProfile(s.archive,"verifier_final_request_checks",request.Profile);err!=nil{return Checkpoint{},err}
 bound,clock,ok,err:=plan.RequestBinding();if err!=nil {return Checkpoint{},err}
 want,err:=request.Profile.RequestHash();if err!=nil {return Checkpoint{},err};got,err:=bound.RequestHash()
 if err!=nil || !ok || got!=want || !clock.Equal(request.When) {return Checkpoint{},ErrCheckpoint{"receiving plan does not bind the final full request"}}
 // Refuse target/lifetime policy before engine scratch creation. Begin repeats
 // this check against the actual locked writer.
 if _,err=driver.PrepareSourcePlaneHistoryProfile(request.Profile,request.Current,request.Lifetime);err!=nil {return Checkpoint{},err}
 image,err:=plan.Snapshot();if err!=nil {return Checkpoint{},err};current,auxiliary,err:=s.planRoots(request,image);if err!=nil {return Checkpoint{},err}
 if err=ctx.Err();err!=nil {return Checkpoint{},err};if err=nativeReserveProfile(s.archive,"verifier_checkpoint_request_hash",request.Profile);err!=nil{return Checkpoint{},err};proof,err:=mint(request,s.head,current,auxiliary,true);if err!=nil {return Checkpoint{},err};s.sealed=true;return proof,nil
}

// Close is independent of cancellation and has no store/receipt handle. It
// releases only private resources, including after a possibly committed result.
func (s *NativeArchiveStream) Close() error {
 if s==nil {return nil};if s.closed {return s.closeErr};s.closed=true
 if s.pullRelease!=nil {s.pullRelease();s.pullRelease=nil};if s.file!=nil {s.closeErr=s.file.Close()};if s.path!="" {s.closeErr=errors.Join(s.closeErr,os.Remove(s.path))};s.archive=nil;s.file=nil;s.nativeCurrent=nil;s.revisions=nil;s.files=nil;if s.release!=nil {s.release();s.release=nil};if s.cleanup!=nil {s.closeErr=errors.Join(s.closeErr,s.cleanup());s.cleanup=nil};return s.closeErr
}
