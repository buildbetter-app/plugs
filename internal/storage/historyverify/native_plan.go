package historyverify

import (
 "encoding/json"
 "reflect"
 "strings"

 "github.com/buildbetter-app/plugs/internal/core/nativeprojection"
 "github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (s *NativeArchiveStream) planRoots(request driver.OfflineHistoryStreamRequest,image nativeprojection.Snapshot) (Root,Root,error) {
 if image.DerivedDigest!=request.Profile.DerivedDigest {return Root{},Root{},ErrCheckpoint{"actual rebuilt derived image differs"}}
 // Indexes contain only the actual one current image, not historical bodies.
 n:=len(image.Records)+len(image.Derived)+len(image.Allocators)+len(image.Blobs)+len(image.Sentinels)
 if err:=s.archive.ChargeNativeDeltaBytes("verifier_plan_projection_indexes",uint64(n)*512);err!=nil {return Root{},Root{},err}
 current:=map[string]checkpointRecord{};nativeSeen:=0;ledgerSeen:=false
 for _,record:=range image.Records {
  if err:=request.Profile.ValidateRecord(record,request.When);err!=nil {return Root{},Root{},err}
  if record.Ref.ID==driver.SourcePlaneCommitRecordID {return Root{},Root{},ErrCheckpoint{"receiving plan contains an engine-time publication record"}}
  if err:=addCheckpoint(current,record);err!=nil {return Root{},Root{},err}
  if strings.HasPrefix(record.Ref.ID,driver.NativeTableHistoryRecordPrefix) || strings.HasPrefix(record.Ref.ID,"source/native-current/") {
   key,_:=checkpointKey(record.Ref);want,ok:=s.nativeCurrent[key]
   if !ok || want!=convertedRecord(record) {return Root{},Root{},ErrCheckpoint{"complete rebuilt native current checkpoint differs"}};nativeSeen++
  }
  if record.Ref.ID=="restore/archive" {
   var evidence nativeprojection.ArchiveEvidence
   if ledgerSeen || json.Unmarshal(record.Body,&evidence)!=nil || evidence.ArchiveSHA256!=request.Profile.ArchiveDigest || evidence.DerivedSHA256!=request.Profile.DerivedDigest || !reflect.DeepEqual(evidence.NativeCaptureFiles,s.files) {return Root{},Root{},ErrCheckpoint{"actual receiving archive ledger differs from reauthenticated originals"}}
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
 for digest,data:=range image.Blobs {auxiliary.Blobs[driver.Hash(digest)]=data}
 currentRoot,err:=convertedRoot(current);if err!=nil {return Root{},Root{},err}
 work,release:=s.archive.NativeDeltaAccountingScope();defer release()
 if err=work.ChargeNativeDeltaJSON("verifier_auxiliary_preimage",auxiliary);err!=nil{return Root{},Root{},err}
 auxiliaryRoot,err:=auxiliaryRoot(auxiliary,s.files);if err!=nil {return Root{},Root{},err};return currentRoot,auxiliaryRoot,nil
}
