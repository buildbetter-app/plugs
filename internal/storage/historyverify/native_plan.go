package historyverify

import (
 "encoding/json"
 "reflect"
 "strings"

 "github.com/buildbetter-app/plugs/internal/core/nativeprojection"
 "github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (s *NativeArchiveStream) planRoots(request driver.OfflineHistoryStreamRequest,image nativeprojection.Snapshot,currentGraphFacts ...driver.ReadFact) (Root,Root,error) {
 owner,releaseOwner:=s.archive.NativeDeltaAccountingScope();defer releaseOwner()
 usage,ok:=owner.NativeDecodedUsage();if !ok {return Root{},Root{},ErrCheckpoint{"missing verifier admission ceiling"}}
 if image.DerivedDigest!=request.Profile.DerivedDigest {return Root{},Root{},ErrCheckpoint{"actual rebuilt derived image differs"}}
 // Indexes contain only the actual one current image, not historical bodies.
 n:=len(image.Records)+len(image.Derived)+len(image.Allocators)+len(image.Blobs)+len(image.Sentinels)
 if err:=owner.ChargeNativeDeltaBytes("verifier_plan_projection_indexes",uint64(n)*512);err!=nil {return Root{},Root{},err}
 current:=map[string]checkpointRecord{};nativeSeen:=0;ledgerSeen:=false
 for _,record:=range image.Records {
  validation,release:=owner.NativeDeltaAccountingScope()
  if err:=validation.ChargeNativeDeltaBytes("verifier_current_record_validation",uint64(len(record.Body))*8+1024);err!=nil {release();return Root{},Root{},err}
  err:=request.Profile.ValidateRecord(record,request.When);release();if err!=nil{return Root{},Root{},err}
  if record.Ref.ID==driver.SourcePlaneCommitRecordID {return Root{},Root{},ErrCheckpoint{"receiving plan contains an engine-time publication record"}}
  if err:=addCheckpoint(current,record);err!=nil {return Root{},Root{},err}
  if strings.HasPrefix(record.Ref.ID,driver.NativeTableHistoryRecordPrefix) || strings.HasPrefix(record.Ref.ID,"source/native-current/") {
   key,_:=checkpointKey(record.Ref);want,ok:=s.nativeCurrent[key]
   if !ok || want!=convertedRecord(record) {return Root{},Root{},ErrCheckpoint{"complete rebuilt native current checkpoint differs"}};nativeSeen++
  }
  if record.Ref.ID=="restore/archive" {
   var evidence nativeprojection.ArchiveEvidence
   if ledgerSeen || json.Unmarshal(record.Body,&evidence)!=nil || evidence.ArchiveSHA256!=request.Profile.ArchiveDigest || evidence.DerivedSHA256!=request.Profile.DerivedDigest || evidence.NativeDecodedLimitBytes!=usage.LimitBytes || !reflect.DeepEqual(evidence.NativeCaptureFiles,s.files) {return Root{},Root{},ErrCheckpoint{"actual receiving archive ledger differs from reauthenticated originals"}}
   ledgerSeen=true
  }
 }
 if !ledgerSeen || nativeSeen!=len(s.nativeCurrent) {return Root{},Root{},ErrCheckpoint{"receiving image omits authenticated native current or originals"}}
 auxiliary:=Auxiliary{Derived:map[string]driver.LockState{},Allocators:map[string]driver.OfflineAllocator{},Parents:map[string]driver.ParentCounter{},Blobs:map[driver.Hash][]byte{},Bumps:map[string]driver.LockKey{}}
 for _,row:=range image.Derived {
  key:=driver.LockKey{Kind:driver.LockDerived,Derived:&driver.DerivedRowKey{Table:row.Table,Key:row.Key}}
  encoded,err:=driver.EncodeLockKey(key);if err!=nil {return Root{},Root{},err}
  if _,duplicate:=auxiliary.Derived[encoded];duplicate {return Root{},Root{},ErrCheckpoint{"duplicate actual derived row"}}
  value:=driver.DerivedRow{Key:row.Key,Value:row.Value};auxiliary.Derived[encoded]=driver.LockState{Key:key,Present:true,Derived:&value}
 }
 for _,allocator:=range image.Allocators {
  if _,duplicate:=auxiliary.Allocators[allocator.Prefix];duplicate {return Root{},Root{},ErrCheckpoint{"duplicate actual allocator"}}
  auxiliary.Allocators[allocator.Prefix]=allocator
  key,err:=driver.AllocatorLock(allocator.Prefix);if err!=nil {return Root{},Root{},err};encoded,err:=driver.EncodeLockKey(key);if err!=nil {return Root{},Root{},err};auxiliary.Bumps[encoded]=key
 }
 // RestoreAllocators stages the complete allocator (including Children) and
 // its fact bump. It does not stage separate ParentCounter writes. Any such
 // extra write is refused by the full auxiliary equality in the engines.
 for _,sentinel:=range image.Sentinels {
  key:=driver.LockKey{Kind:driver.LockSentinel,Sentinel:sentinel};encoded,err:=driver.EncodeLockKey(key);if err!=nil {return Root{},Root{},err};auxiliary.Bumps[encoded]=key
 }
 // Trusted core supplies this fact only after complete semantic validation
 // and typed-writer preflight. Bind the exact extra bump into the checkpoint;
 // do not weaken auxiliary equality or silently accept arbitrary extra writes.
 for _,fact:=range currentGraphFacts {
  key:=driver.LockKey{Kind:driver.LockReadFact,Fact:fact};encoded,err:=driver.EncodeLockKey(key);if err!=nil{return Root{},Root{},err};auxiliary.Bumps[encoded]=key
  graph:=driver.GraphBlockingSentinelLock;encoded,err=driver.EncodeLockKey(graph);if err!=nil{return Root{},Root{},err};auxiliary.Bumps[encoded]=graph
 }
 for digest,data:=range image.Blobs {auxiliary.Blobs[driver.Hash(digest)]=data}
 currentRoot,err:=convertedRoot(current);if err!=nil {return Root{},Root{},err}
 work,release:=owner.NativeDeltaAccountingScope();defer release()
 if err=work.ChargeNativeDeltaJSON("verifier_auxiliary_preimage",auxiliary);err!=nil{return Root{},Root{},err}
 auxiliaryRoot,err:=auxiliaryRoot(auxiliary,s.files);if err!=nil {return Root{},Root{},err};return currentRoot,auxiliaryRoot,nil
}
