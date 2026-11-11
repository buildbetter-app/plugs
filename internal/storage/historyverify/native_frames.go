package historyverify

import (
 "context"
 "crypto/sha256"
 "encoding/binary"
 "encoding/hex"
 "encoding/json"
 "fmt"
 "hash"
 "reflect"
 "sort"

 "github.com/buildbetter-app/plugs/internal/core/dumpformat"
 "github.com/buildbetter-app/plugs/internal/core/nativeprojection"
 "github.com/buildbetter-app/plugs/internal/core/model"
 "github.com/buildbetter-app/plugs/internal/storage/driver"
)

const nativeFrameBytes = 1 << 20
const nativeChunkBytes = 16 << 10

// NativeArchiveLimits bounds each pull, not aggregate history. Metadata that
// cannot fit is refused without truncation. Bodies are emitted in fixed chunks
// so transcript identity does not depend on the consumer's larger limits.
func NativeArchiveLimits() driver.OfflineHistoryStreamLimits {
 return driver.OfflineHistoryStreamLimits{MaxFrameBytes:nativeFrameBytes,MaxChunkBytes:nativeChunkBytes}
}
type nativeGraph struct {id string;parents []string}
type nativeFrames struct {
 ctx context.Context
 stream *NativeArchiveStream
 digest hash.Hash
 graph []nativeGraph
 source, project string
 classes map[string]string
 counts map[string]int
 physical map[string]driver.OfflineHistorySourceTable
 replay map[string]checkpointRecord
 proofs []dumpformat.NativeDeltaCommitProof
 head map[string]checkpointRecord
 seen map[string]bool
 headDone,currentDone bool
 commits,tables,decoded uint64
}
func newNativeFrames(ctx context.Context,s *NativeArchiveStream) (*nativeFrames,error) {
 b:=&nativeFrames{ctx:ctx,stream:s,digest:sha256.New(),classes:map[string]string{},counts:map[string]int{},physical:map[string]driver.OfflineHistorySourceTable{},head:map[string]checkpointRecord{},replay:map[string]checkpointRecord{},seen:map[string]bool{}}
 scopes:=0
 err:=s.archive.VisitNativeSourceHistories(ctx,func(scope dumpformat.NativeHistory)error {
  scopes++;if scopes!=1 || len(scope.Projects)!=1 {return ErrCheckpoint{"native source range differs"}}
  if err:=nativeprojection.NativePolicyRefusal(driver.RestoreHistoryModeFull,s.archive,scope);err!=nil {return err}
  b.source,b.project=scope.StoreID,scope.Projects[0]
  s.header=driver.OfflineHistoryStreamHeader{Version:driver.OfflineHistoryStreamVersion,Source:driver.NativeTableHistorySource,Verified:true,Complete:scope.Complete,First:1,Head:scope.VerifiedImageCount()}
  if scope.HistoryCoverage!=nil {s.header.NativeBoundaries=1}
  if err:=s.archive.ChargeNativeDeltaBytes("verifier_graph_slots",uint64(len(scope.Commits))*64);err!=nil {return err}
  for _,c:=range scope.Commits {
   if err:=s.archive.ChargeNativeDeltaJSON("verifier_ordered_parents",c.Parents);err!=nil {return err}
   b.graph=append(b.graph,nativeGraph{c.ID,append([]string(nil),c.Parents...)})
  }
  if err:=s.archive.ChargeNativeDeltaBytes("verifier_current_census",uint64(len(scope.Current))*512);err!=nil{return err}
  var err error;b.classes,b.counts,err=nativeprojection.NativeCurrentClasses(scope);return err
 })
 if err!=nil {return nil,err};if scopes!=1 || s.header.Head==0 || uint64(len(b.graph))!=s.header.Head {return nil,ErrCheckpoint{"native commit census differs"}}
 _,_=b.digest.Write([]byte(driver.OfflineHistoryStreamVersion+"\n"))
 raw,err:=b.marshal(s.header);if err!=nil {return nil,err};_,_=b.digest.Write(raw);_,_=b.digest.Write([]byte{'\n'})
 // The independently authenticated physical census is retained by admission
 // and rechecked by the complete preparation. Reserve its detached copy first.
 usage,ok:=s.archive.NativeDecodedUsage();if !ok {return nil,ErrCheckpoint{"missing native verification owner"}}
 proofBytes:=usage.CurrentCategories["retained_commit_proofs"]
 if proofBytes==0 || proofBytes>(^uint64(0)-2048)/8 {return nil,ErrCheckpoint{"missing or oversized retained physical census"}}
 // JSON retained bytes bound every map key/value and commit slot. The
 // conservative factor also reserves detached map/slice representation.
 if err:=s.archive.ChargeNativeDeltaBytes("verifier_physical_proof_copy",proofBytes*8+2048);err!=nil {return nil,err}
 proof,ok:=s.archive.NativeDeltaVerification();if !ok || len(proof.Commits)!=len(b.graph) {return nil,ErrCheckpoint{"missing authenticated physical census"}}
 b.proofs=proof.Commits
 metadata,err:=s.archive.NativeSourceHistoryMetadata();if err!=nil {return nil,err}
 if len(metadata)!=1 {return nil,ErrCheckpoint{"native metadata census differs"}}
 if coverage:=metadata[0].HistoryCoverage;coverage!=nil {
  boundary:=driver.NativeHistoryBoundary{SourceStoreID:b.source,Project:b.project,VerifiedCommitCount:s.header.Head,BoundarySHA256:dumpformat.NativeHistoryCoverageSHA256(*coverage)}
  if err=b.frame(driver.OfflineHistoryStreamFrame{Kind:"boundary",Boundary:&boundary},nil);err!=nil {return nil,err}
 }
 return b,nil
}
func (b *nativeFrames) marshal(value any) ([]byte,error) {
 n,err:=dumpformat.NativeDeltaJSONSize(value);if err!=nil {return nil,err}
 if n==0 || n>nativeFrameBytes+1 {return nil,ErrCheckpoint{"indivisible native stream metadata exceeds pull bound"}}
 if err=b.stream.archive.ChargeNativeDeltaBytes("verifier_frame_encoding",n-1);err!=nil {return nil,err};return json.Marshal(value)
}
func (b *nativeFrames) frame(f driver.OfflineHistoryStreamFrame,commitHash hash.Hash) error {
 if err:=b.ctx.Err();err!=nil {return err}
 work,release:=b.stream.archive.NativeDeltaAccountingScope();defer release()
 save:=b.stream.archive;b.stream.archive=work;raw,err:=b.marshal(f);b.stream.archive=save;if err!=nil {return err}
 if len(raw)>b.stream.peakFrame {b.stream.peakFrame=len(raw)}
 if f.Chunk!=nil && len(f.Chunk.Data)>b.stream.peakChunk {b.stream.peakChunk=len(f.Chunk.Data)}
 var prefix [8]byte;binary.BigEndian.PutUint64(prefix[:],uint64(len(raw)))
 if _,err=b.stream.file.Write(prefix[:]);err!=nil {return err};if _,err=b.stream.file.Write(raw);err!=nil {return err}
 if f.Kind!="end" {_,_=b.digest.Write(raw);_,_=b.digest.Write([]byte{'\n'})}
 if commitHash!=nil {_,_=commitHash.Write(raw);_,_=commitHash.Write([]byte{'\n'})};return nil
}
type nativeChange struct {name,key string;change driver.SourceHistoryChange}
func nativePhysical(meta *dumpformat.NativeRowMeta) (driver.OfflineHistorySourceTable,error) {
 if meta==nil || meta.IdentityKind!=dumpformat.NativeRowD3Physical || meta.PhysicalTableSHA256==nil {return driver.OfflineHistorySourceTable{},ErrCheckpoint{"native metadata lacks a physical preimage"}}
 return driver.OfflineHistorySourceTable{Present:true,Rows:meta.RowCount,SchemaSHA256:meta.SchemaID,TableSHA256:*meta.PhysicalTableSHA256},nil
}
func (b *nativeFrames) identify(owner *dumpformat.Archive,record *driver.Record) (string,*dumpformat.NativeRowMeta,error) {
 if record==nil {return "",nil,nil}
 meta,row,err:=owner.DecodeNativeRowRecord(*record);if err!=nil {return "",nil,err}
 if meta!=nil {
  if meta.Source.StoreID!=b.source || meta.Source.Project!=b.project || meta.History!="retained-native-commit" {return "",nil,ErrCheckpoint{"native metadata provenance differs"}}
  return meta.Table,meta,nil
 }
 if row.Source.StoreID!=b.source || row.Source.Project!=b.project || row.History!="retained-native-commit" {return "",nil,ErrCheckpoint{"native row provenance differs"}}
 return row.Table,nil,nil
}
func (b *nativeFrames) commit(c dumpformat.NativeDeltaRowCommit) error {
 if c.Sequence!=b.commits+1 || c.Sequence>b.stream.header.Head || c.CommitID!=b.graph[c.Sequence-1].id {return ErrCheckpoint{"native ordered graph differs"}}
 work,release:=b.stream.archive.NativeDeltaAccountingScope();defer release()
 if err:=work.ChargeNativeDeltaBytes("verifier_commit_index",uint64(len(c.Changes))*192);err!=nil {return err}
 changes:=make([]nativeChange,0,len(c.Changes));before:=map[string]*driver.OfflineHistorySourceTable{}
 for _,change:=range c.Changes {
  name,left,err:=b.identify(work,change.Before);if err!=nil {return err};afterName,right,err:=b.identify(work,change.After);if err!=nil {return err}
  if name=="" {name=afterName};if name=="" || afterName!="" && afterName!=name {return ErrCheckpoint{"native converted table identity differs"}}
  if _,seen:=before[name];!seen {if old,present:=b.physical[name];present {v:=old;before[name]=&v}else{before[name]=nil}}
  if left!=nil {
   physical,err:=nativePhysical(left);if err!=nil {return err};old,present:=b.physical[name]
   if !present || old!=physical {return ErrCheckpoint{"native preceding imported physical preimage differs"}}
  }
  if left!=nil || right!=nil {
   if right==nil {delete(b.physical,name)}else{physical,err:=nativePhysical(right);if err!=nil {return err};if _,present:=b.physical[name];!present {if err:=b.stream.archive.ChargeNativeDeltaBytes("verifier_physical_table_index",uint64(len(name))+384);err!=nil{return err}}
    b.physical[name]=physical}
  }
  record:=change.After;if record==nil {record=change.Before};key,err:=checkpointKey(record.Ref);if err!=nil {return err}
  old,hadOld:=b.replay[key]
  if change.Before==nil && hadOld || change.Before!=nil && (!hadOld || old!=convertedRecord(*change.Before)) {return ErrCheckpoint{"converted preceding record differs from authentic transcript"}}
  if change.After==nil {delete(b.replay,key)}else{
   if !hadOld {if err:=b.stream.archive.ChargeNativeDeltaBytes("verifier_replayed_head_index",uint64(len(key))+256);err!=nil{return err}}
   b.replay[key]=convertedRecord(*change.After)
  }
  changes=append(changes,nativeChange{name,key,change})
  if change.After==nil {delete(b.stream.revisions,key)}else{
   if _,present:=b.stream.revisions[key];!present {if err:=b.stream.archive.ChargeNativeDeltaBytes("verifier_current_revision_index",uint64(len(key))+96);err!=nil{return err}}
   b.stream.revisions[key]=model.Natural64(c.Sequence)
  }
 }
 // This census comes from every real table's rolling metadata, including
 // unchanged tables. Converted row/meta count remains a distinct quantity.
 if err:=work.ChargeNativeDeltaBytes("verifier_physical_census",uint64(len(b.physical))*256);err!=nil{return err}
 census:=map[string]dumpformat.NativeDeltaTableDigest{}
 for name,table:=range b.physical {census[name]=dumpformat.NativeDeltaTableDigest{Rows:table.Rows,SHA256:table.TableSHA256}}
 root,err:=dumpformat.NativeDeltaRootSHA256(census);if err!=nil {return err}
 authentic:=b.proofs[c.Sequence-1]
 if authentic.Sequence!=c.Sequence || authentic.CommitID!=c.CommitID || authentic.RootSHA256!=root || !reflect.DeepEqual(authentic.Tables,census) {return ErrCheckpoint{"physical commit census differs from independently authenticated preimages"}}
 sort.Slice(changes,func(i,j int)bool{if changes[i].name!=changes[j].name{return changes[i].name<changes[j].name};return changes[i].key<changes[j].key})
 graph:=b.graph[c.Sequence-1]
 frame:=driver.OfflineHistoryStreamCommit{SourceStoreID:b.source,Project:b.project,SourceSequence:c.Sequence,Sequence:c.Sequence,CommitIDs:1,ParentIDs:uint64(len(graph.parents)),Tables:uint64(len(changes)),Physical:&driver.OfflineHistorySourceCommit{Root:root,Tables:uint64(len(census)),TablesSHA256:root}}
 hash:=sha256.New();_,_=hash.Write([]byte("plug-offline-history-commit/1\n"))
 if err=b.frame(driver.OfflineHistoryStreamFrame{Kind:"commit",Commit:&frame},hash);err!=nil {return err}
 if err=b.frame(driver.OfflineHistoryStreamFrame{Kind:"id",ID:&driver.OfflineHistoryStreamID{Kind:"commit",Value:c.CommitID}},hash);err!=nil {return err}
 for _,id:=range graph.parents {if err=b.frame(driver.OfflineHistoryStreamFrame{Kind:"id",ID:&driver.OfflineHistoryStreamID{Kind:"parent",Value:id}},hash);err!=nil {return err}}
 for _,change:=range changes {
  old:=before[change.name];next,present:=b.physical[change.name]
  if !present {
   // Absence is committed under the actual resulting physical census root,
   // not confused with row deletion or an empty present table.
   raw,err:=json.Marshal(struct{Name,Root string}{change.name,root});if err!=nil {return err}
   next=driver.OfflineHistorySourceTable{TableSHA256:nativeSHA(append([]byte("plug-native-table-absence/1\n"),raw...))}
  }
  r:=change.change.After;if r==nil {r=change.change.Before}
  table:=driver.OfflineHistoryStreamTable{Name:change.name,Ref:r.Ref,Kind:r.Kind,Before:nativeImage(change.change.Before),After:nativeImage(change.change.After),PhysicalBefore:old,PhysicalAfter:&next}
  if err=b.frame(driver.OfflineHistoryStreamFrame{Kind:"table",Table:&table},hash);err!=nil {return err}
  for i,r:=range []*driver.Record{change.change.Before,change.change.After} {
   if r==nil {continue};side:="before";if i==1 {side="after"}
   for offset:=0;offset<len(r.Body); {
    end:=offset+nativeChunkBytes;if end>len(r.Body) {end=len(r.Body)}
    chunk:=driver.OfflineHistoryStreamChunk{Image:side,Offset:uint64(offset),Data:r.Body[offset:end]}
    if err=b.frame(driver.OfflineHistoryStreamFrame{Kind:"chunk",Chunk:&chunk},nil);err!=nil {return err};b.decoded+=uint64(end-offset);offset=end
   }
  }
  if err=b.frame(driver.OfflineHistoryStreamFrame{Kind:"table-end"},nil);err!=nil {return err};b.tables++
 }
 if err=b.frame(driver.OfflineHistoryStreamFrame{Kind:"commit-end",CommitEnd:&driver.OfflineHistoryStreamCommitEnd{SHA256:hex.EncodeToString(hash.Sum(nil))}},nil);err!=nil {return err};b.commits++;return nil
}
func nativeImage(r *driver.Record) *driver.OfflineHistoryStreamImage {if r==nil {return nil};return &driver.OfflineHistoryStreamImage{Bytes:uint64(len(r.Body)),SHA256:nativeSHA(r.Body)}}
func (b *nativeFrames) checkpoint(state dumpformat.NativeDeltaTableState) error {
 if state.Kind=="commit" {return nil}
 if state.Complete {
  switch state.Kind {case "head-checkpoint":if b.headDone {return ErrCheckpoint{"duplicate independent HEAD"}};b.headDone=true
  case "current":if !b.headDone || b.currentDone {return ErrCheckpoint{"independent current order differs"}};b.currentDone=true
  default:return ErrCheckpoint{"unexpected checkpoint kind"}};return nil
 }
 if b.currentDone || state.Kind!="head-checkpoint" && state.Kind!="current" {return ErrCheckpoint{"unexpected checkpoint table"}}
 history:="retained-native-commit";var keep func(dumpformat.Row)(bool,error)
 if state.Kind=="current" {
  history=b.classes[state.Schema.Name]
  if !b.headDone || history=="" || b.seen[state.Schema.Name] || len(state.Rows)!=b.counts[state.Schema.Name] {return ErrCheckpoint{"independent current census differs"}}
  b.seen[state.Schema.Name]=true
  if state.Schema.Name=="leases" {keep=func(row dumpformat.Row)(bool,error){return nativeprojection.NativeLeaseUnexpired(row,b.stream.when)}}
 }else if b.headDone {return ErrCheckpoint{"independent HEAD table after terminal"}}
 owner,release:=b.stream.archive.NativeDeltaAccountingScope();defer release()
 records,_,err:=owner.NativeDeltaTableRecords(state,history,keep);if err!=nil {return err}
 for _,record:=range records {
  if err=b.stream.archive.ChargeNativeDeltaBytes("verifier_checkpoint_index",uint64(len(record.Ref.ID))+256);err!=nil {return err}
  if err=addCheckpoint(b.stream.nativeCurrent,record);err!=nil {return err}
  if state.Kind=="head-checkpoint" {if err=addCheckpoint(b.head,record);err!=nil {return err}}
 }
 return nil
}
func (b *nativeFrames) captureFile(name string,raw []byte) error {
 if _,duplicate:=b.stream.files[name];duplicate {return ErrCheckpoint{"duplicate original capture file"}}
 if err:=b.stream.archive.ChargeNativeDeltaBytes("verifier_capture_ledger",uint64(len(name))+192);err!=nil {return err}
 b.stream.files[name]=dumpformat.File{Size:int64(len(raw)),SHA256:nativeSHA(raw)};return nil
}
func (b *nativeFrames) finish() error {
 if b.commits!=b.stream.header.Head || !b.headDone || !b.currentDone || len(b.seen)!=len(b.classes) || len(b.stream.files)<3 {return ErrCheckpoint{"native independent verification is incomplete"}}
 var err error;b.stream.head,err=convertedRoot(b.head);if err!=nil {return err}
 replay,err:=convertedRoot(b.replay);if err!=nil {return err};if replay!=b.stream.head {return ErrCheckpoint{"exact transcript HEAD differs from independent checkpoint"}}
 b.stream.digest=hex.EncodeToString(b.digest.Sum(nil))
 end:=driver.OfflineHistoryStreamEnd{Commits:b.commits,Tables:b.tables,DecodedBytes:b.decoded}
 if err=b.frame(driver.OfflineHistoryStreamFrame{Kind:"end",End:&end},nil);err!=nil {return err}
 if err=b.stream.file.Sync();err!=nil {return fmt.Errorf("native transcript spool: %w",err)};return nil
}
