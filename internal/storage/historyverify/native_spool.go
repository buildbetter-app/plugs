package historyverify

import (
 "context"
 "encoding/binary"
 "encoding/json"
 "errors"
 "io"
 "os"
 "sort"

 "github.com/buildbetter-app/plugs/internal/core/model"
 "github.com/buildbetter-app/plugs/internal/core/dumpformat"
 "github.com/buildbetter-app/plugs/internal/core/nativeprojection"
 "github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (s *NativeArchiveStream) readFrame(ctx context.Context,input io.Reader,limits driver.OfflineHistoryStreamLimits) (driver.OfflineHistoryStreamFrame,func(),error) {
 zero:=driver.OfflineHistoryStreamFrame{}
 if ctx==nil {return zero,nil,ErrCheckpoint{"missing pull context"}};if err:=ctx.Err();err!=nil {return zero,nil,err}
 var prefix [8]byte;n,err:=io.ReadFull(input,prefix[:]);if n==0 && err==io.EOF {return zero,nil,io.EOF};if err!=nil {return zero,nil,err}
 size:=binary.BigEndian.Uint64(prefix[:])
 if size==0 || limits.MaxFrameBytes<=0 || limits.MaxChunkBytes<=0 || size>uint64(limits.MaxFrameBytes) || size>nativeFrameBytes {return zero,nil,ErrCheckpoint{"native spool frame exceeds pull limit"}}
 owner,release:=s.archive.NativeDeltaAccountingScope()
 // Reserve encoding plus a conservative decoded representation before either
 // allocation. This is the real retained transfer batch, so the existing
 // offline batch high-water gauge includes streamed receiving too. It survives
 // until the next borrowed pull/Close, separately from raw capture staging.
 if err=owner.ChargeNativeDeltaBytes("offline_batches",size*8+1024);err!=nil {release();return zero,nil,err}
 raw:=make([]byte,int(size));if _,err=io.ReadFull(input,raw);err!=nil {release();return zero,nil,err}
 var frame driver.OfflineHistoryStreamFrame
 if err=json.Unmarshal(raw,&frame);err!=nil {release();return zero,nil,err}
 if frame.Chunk!=nil && len(frame.Chunk.Data)>limits.MaxChunkBytes {release();return zero,nil,ErrCheckpoint{"native spool chunk exceeds pull limit"}}
 return frame,release,nil
}
func (s *NativeArchiveStream) Next(ctx context.Context,limits driver.OfflineHistoryStreamLimits) (driver.OfflineHistoryStreamFrame,error) {
 if s==nil || s.closed || !s.sealed {return driver.OfflineHistoryStreamFrame{},ErrCheckpoint{"unsealed or closed native stream"}}
 if s.pullRelease!=nil {s.pullRelease();s.pullRelease=nil}
 frame,release,err:=s.readFrame(ctx,s.file,limits);if err!=nil {return driver.OfflineHistoryStreamFrame{},err};s.pullRelease=release;return frame,nil
}
func (s *NativeArchiveStream) walk(ctx context.Context,visit func(driver.OfflineHistoryStreamFrame)error) (err error) {
 if s==nil || s.closed || s.digest=="" {return ErrCheckpoint{"missing native transcript"}}
 input,err:=os.Open(s.path);if err!=nil {return err};defer func(){err=errors.Join(err,input.Close())}()
 for {
  frame,release,e:=s.readFrame(ctx,input,NativeArchiveLimits());if e==io.EOF {return nil};if e!=nil {return e}
  e=visit(frame);release();if e!=nil {return e}
 }
}

// LockRequests reads verifier-produced identities from private disk, never
// accumulating historical bodies. The receiving host acquires these complete
// update locks before Head. Returned lock data conveys no checkpoint authority.
func (s *NativeArchiveStream) LockRequests(ctx context.Context,plan nativeprojection.Plan,owners ...*dumpformat.Archive) ([]driver.LockRequest,error) {
 if s==nil || s.closed || !plan.Built() || plan.ArchiveSHA256()!=s.archive.SHA256() {return nil,ErrCheckpoint{"lock plan belongs to another archive"}}
 image,err:=plan.Snapshot();if err!=nil {return nil,err};keys:=map[string]driver.LockKey{}
 add:=func(key driver.LockKey)error {
  encoded,err:=driver.EncodeLockKey(key);if err!=nil {return err}
  if _,ok:=keys[encoded];!ok {if err=s.archive.ChargeNativeDeltaBytes("verifier_receiving_lock_index",uint64(len(encoded))*2+384);err!=nil {return err};keys[encoded]=key};return nil
 }
 record:=func(ref model.Ref)error{return add(driver.LockKey{Kind:driver.LockRecord,Ref:&ref})}
 for _,r:=range image.Records {if err=record(r.Ref);err!=nil {return nil,err}}
 for _,row:=range image.Derived {if err=add(driver.LockKey{Kind:driver.LockDerived,Derived:&driver.DerivedRowKey{Table:row.Table,Key:row.Key}});err!=nil {return nil,err}}
 for _,allocator:=range image.Allocators {key,e:=driver.AllocatorLock(allocator.Prefix);if e!=nil {return nil,e};if err=add(key);err!=nil {return nil,err}}
 for _,sentinel:=range image.Sentinels {if err=add(driver.LockKey{Kind:driver.LockSentinel,Sentinel:sentinel});err!=nil {return nil,err}}
 for _,id:=range []string{driver.OfflineHistoryEvidenceID,driver.SourcePlaneCommitRecordID} {if err=record(model.RefByID(id,model.Natural64(0)));err!=nil {return nil,err}}
 err=s.walk(ctx,func(frame driver.OfflineHistoryStreamFrame)error{if frame.Table!=nil{return record(frame.Table.Ref)};return nil});if err!=nil {return nil,err}
 if err=s.archive.ChargeNativeDeltaBytes("verifier_receiving_sorted_locks",uint64(len(keys))*256);err!=nil {return nil,err}
 ordered:=make([]string,0,len(keys));for key:=range keys {ordered=append(ordered,key)};sort.Strings(ordered)
 owner:=s.archive;if len(owners)>0 && owners[0]!=nil {owner=owners[0]}
 if err=owner.ChargeNativeDeltaBytes("receiving_retained_lock_slots",uint64(len(keys))*256);err!=nil{return nil,err}
 requests:=make([]driver.LockRequest,0,len(keys));for _,key:=range ordered {
  if err=owner.ChargeNativeDeltaJSON("receiving_retained_lock_keys",keys[key]);err!=nil{return nil,err}
  requests=append(requests,driver.LockRequest{Key:keys[key],Mode:driver.LockUpdate})
 };return requests,nil
}

var _ driver.OfflineHistoryStreamSource = (*NativeArchiveStream)(nil)
