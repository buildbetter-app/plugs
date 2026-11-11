package cardb20

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/publicnative"
)

type metadataNative struct {
	r *nativeRecorder
	binary, dir string
	env []string
	sequence int
}

func (n *metadataNative) invoke(t *testing.T, label string, args ...string) ([]byte, int) {
	t.Helper()
	n.sequence++
	ctx,cancel := context.WithTimeout(context.Background(),2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx,n.binary,args...)
	cmd.Dir,cmd.Env=n.dir,n.env
	var stdout,stderr bytes.Buffer
	cmd.Stdout,cmd.Stderr=&stdout,&stderr
	started := time.Now().UTC()
	err := cmd.Run()
	exit := 0
	if err!=nil { exit=-1; if e,ok:=err.(*exec.ExitError);ok { exit=e.ExitCode() } }
	stem:=fmt.Sprintf("%03d-%s",n.sequence,label)
	for name,data:=range map[string][]byte{stem+".stdout":stdout.Bytes(),stem+".stderr":stderr.Bytes()} {
		if err:=os.WriteFile(filepath.Join(n.r.root,name),data,0600);err!=nil { t.Fatal(err) }
	}
	// Raw request and outputs are frozen before any JSON decoding.
	n.r.writeJSON(filepath.Join(n.r.root,stem+".request.json"),map[string]any{"argv":cmd.Args,"directory":n.dir,"environment":n.env,"exit":exit,"error":fmt.Sprint(err),"stdout_sha256":hashBytes(stdout.Bytes()),"stderr_sha256":hashBytes(stderr.Bytes()),"started":started,"finished":time.Now().UTC()})
	return stdout.Bytes(),exit
}

func (n *metadataNative) command(t *testing.T,label string,args ...string) []byte {
	t.Helper()
	raw,exit:=n.invoke(t,label,args...)
	if exit!=0 { t.Fatalf("%s: actual native exit%d; original outputs retained",label,exit) }
	return raw
}

func nativeMetadata(t *testing.T,raw []byte) map[string]json.RawMessage {
	t.Helper()
	var rows []struct { Metadata json.RawMessage `json:"metadata"` }
	if err:=json.Unmarshal(raw,&rows);err!=nil || len(rows)!=1 { t.Fatalf("native show did not return one row: %s %v",raw,err) }
	var metadata map[string]json.RawMessage
	if err:=json.Unmarshal(rows[0].Metadata,&metadata);err!=nil { t.Fatalf("native metadata is not an object: %s %v",raw,err) }
	return metadata
}

func TestB17B18CanonicalNativeMetadataRawTokens(t *testing.T) {
	root:=os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	checkout:=os.Getenv("PLUG_SOURCE_MODULE_ROOT")
	if !filepath.IsAbs(root) || !filepath.IsAbs(checkout) { t.Fatal("original RunsOn source/output roots required") }
	r:=&nativeRecorder{t:t,root:root,target:"//cmd/bd:bd"}
	actual:=strings.TrimSpace(string(r.buildCommand(checkout,"candidate-head","/usr/bin/git",[]string{"rev-parse","HEAD"})))
	if actual!=os.Getenv("PLUG_B17_B18_REQUEST_SHA") || actual=="" { t.Fatal("exact candidate request mismatch") }
	r.writeJSON(filepath.Join(root,"request-run-binding.json"),map[string]any{"candidate":actual,"target":os.Getenv("TEST_TARGET"),"run":os.Getenv("GITHUB_RUN_ID"),"attempt":os.Getenv("GITHUB_RUN_ATTEMPT"),"workflow_sha":os.Getenv("GITHUB_SHA"),"native_source":pinnedSource,"qualification":"PUBLIC_OPERATIONAL_REFERENCE_ONLY"})
	binary:=r.buildCanonicalSource(pinnedSource)
	server:=publicnative.StartDolt(t,root)
	workspace:=t.TempDir()
	home,tmp,dir:=filepath.Join(workspace,"home"),filepath.Join(workspace,"tmp"),filepath.Join(workspace,"project")
	for _,p:=range []string{home,tmp,dir} { if err:=os.Mkdir(p,0700);err!=nil { t.Fatal(err) } }
	env:=server.ChildEnv([]string{"PATH=/tmp/plug-swarm/bin/worker-safe:/usr/bin:/bin","HOME="+home,"USER=b17-native","TZ=UTC","LC_ALL=C","NO_COLOR=1","CI=true","GIT_CONFIG_NOSYSTEM=1","GIT_CONFIG_GLOBAL=/dev/null","GIT_AUTHOR_NAME=B17 Public","GIT_AUTHOR_EMAIL=b17@example.invalid","GIT_COMMITTER_NAME=B17 Public","GIT_COMMITTER_EMAIL=b17@example.invalid","TMPDIR="+tmp,"BEADS_ACTOR=b17-native","BEADS_NO_DAEMON=1"})
	n:=&metadataNative{r:r,binary:"/usr/bin/git",dir:dir,env:env}
	n.command(t,"git-init","-c","core.hooksPath=/dev/null","init","--quiet","--template=")
	n.binary=binary
	n.command(t,"version","version","--json")
	n.command(t,"init","init","--server","--external","--server-host","127.0.0.1","--server-port",strconv.Itoa(server.Port),"--database","b17_precision","--prefix","b17","--skip-hooks","--skip-agents","--non-interactive","--quiet")
	server.RequireDatabaseDir(t,"b17_precision")
	n.command(t,"enable-history","config","set","versioned-history.enabled","true")
	n.command(t,"create","create","--id","b17-native","--type","task","--title","native literal metadata","--json")
	n.command(t,"seed-sibling","update","b17-native","--set-metadata","sibling=true","--json")
	const key="literal.dot/key"
	type observation struct { Name,Requested,Stored,PhysicalJSON string; WriteExit int; Exact bool }
	observations:=[]observation{}
	for _,vector:=range []struct{name,value string}{
		{"stored-null","null"},{"integer-one","1"},{"decimal-one","1.0"},{"exponent-one","1e0"},{"negative-zero","-0.0"},{"wide-integer","9007199254740993"},
	} {
		t.Run(vector.name,func(t *testing.T) {
			_,writeExit:=n.invoke(t,vector.name+"-write","update","b17-native","--set-metadata",key+"="+vector.value,"--json")
			metadata:=nativeMetadata(t,n.command(t,vector.name+"-show","show","b17-native","--json"))
			if string(metadata["sibling"])!="true" { t.Fatal("native metadata edit changed unrelated sibling") }
			// Query the original JSON cell as text: no map/float conversion may
			// stand in for what the native database actually stored.
			physical:=n.command(t,vector.name+"-physical","--readonly","sql","SELECT CAST(metadata AS CHAR) AS metadata_json FROM issues WHERE id = 'b17-native'","--json")
			var rows []struct { Metadata string `json:"metadata_json"` }
			if err:=json.Unmarshal(physical,&rows);err!=nil || len(rows)!=1 { t.Fatalf("physical JSON text: %s %v",physical,err) }
			var cell map[string]json.RawMessage
			if err:=json.Unmarshal([]byte(rows[0].Metadata),&cell);err!=nil { t.Fatal(err) }
			if string(cell[key])!=string(metadata[key]) { t.Fatalf("show differs from actual stored cell: %s vs%s",metadata[key],cell[key]) }
			exact:=writeExit==0 && string(cell[key])==vector.value
			observations=append(observations,observation{vector.name,vector.value,string(cell[key]),rows[0].Metadata,writeExit,exact})
			if writeExit!=0 {
				t.Errorf("NATIVE_METADATA_WRITE_REFUSED request=%s exit=%d; actual unchanged/readback is recorded separately from numeric admission",vector.value,writeExit)
			} else if !exact {
				t.Errorf("NATIVE_LEXICAL_PRECISION_LOSS request=%s actualStored=%s; raw source cell/output retained, no token repair",vector.value,cell[key])
			}
		})
	}
	n.command(t,"unset-literal-key","update","b17-native","--unset-metadata",key,"--json")
	after:=nativeMetadata(t,n.command(t,"show-after-unset","show","b17-native","--json"))
	if _,present:=after[key];present || string(after["sibling"])!="true" { t.Fatal("native unset did not preserve absence/sibling") }
	n.command(t,"versions","versions","b17-native","--json")
	r.writeJSON(filepath.Join(root,"native-literal-observations.json"),map[string]any{"observations":observations,"native_stored_null":"present null; adopted Plug setMeta(null) deletes instead","public_interface":"update --set-metadata, not an invented CLI CAS","numeric_history_admission":"separate from lexical metadata comparison","native_full_card":"NOT_QUALIFIED"})
}
