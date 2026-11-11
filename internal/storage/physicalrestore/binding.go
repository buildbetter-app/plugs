package physicalrestore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"unicode/utf8"

	"github.com/buildbetter-app/plugs/internal/storage/physicalcapture"
)

const SourceFormat = "beads-physical-source/1"
const CapturePort347 = "physicalcapture.Image/347f269fbade3a37d717aac78ae93099f8c6544f"

// SourceBinding carries the original typed capture, not normalized NativeTable
// rows. The destination authenticates this complete binding against its actual
// preserved root before any delete. Immutable capture cannot invent working-set
// tables which it did not observe; incomplete user-table input is refused.
type SourceBinding struct {
	Format        string          `json:"format"`
	Port          string          `json:"port"`
	CommitID      string          `json:"commit_id"`
	ReadOnlyScope bool            `json:"read_only_scope"`
	Image         Manifest        `json:"image"`
	SHA256        string          `json:"sha256"`
	Working       *WorkingBinding `json:"working,omitempty"`
}

func BindSource(image physicalcapture.Image) (SourceBinding, error) {
	if !image.ReadOnlyScope || !regexp.MustCompile(`^[0-9a-v]{32}$`).MatchString(image.CommitID) {
		return SourceBinding{}, &Error{Kind: "source-binding", Row: -1, Cause: errors.New("authenticated immutable typed capture required")}
	}
	manifest, err := FromImage(image)
	if err != nil {
		return SourceBinding{}, err
	}
	binding := SourceBinding{Format: SourceFormat, Port: CapturePort347, CommitID: image.CommitID, ReadOnlyScope: true, Image: manifest}
	binding.SHA256, err = bindingDigest(binding)
	return binding, err
}

func bindingDigest(binding SourceBinding) (string, error) {
	binding.SHA256 = ""
	data, err := json.Marshal(binding)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func validateBinding(binding SourceBinding) error {
	if binding.Format != SourceFormat || binding.Port != CapturePort347 || !binding.ReadOnlyScope || !regexp.MustCompile(`^[0-9a-v]{32}$`).MatchString(binding.CommitID) {
		return &Error{Kind: "source-binding", Row: -1, Cause: errors.New("supported original typed capture port required")}
	}
	digest, err := bindingDigest(binding)
	if err != nil || binding.SHA256 != digest {
		return &Error{Kind: "source-binding", Row: -1, Cause: errors.New("typed capture digest mismatch")}
	}
	_, err = decode(binding.Image)
	if err != nil {
		return err
	}
	if binding.Working != nil {
		working := binding.Working
		if working.Port != WorkingCapturePort || working.Format != physicalcapture.WorkingImageFormat || !working.Complete || !working.ReadOnlyScope || working.Database == "" || working.HeadCommitID != binding.CommitID || !regexp.MustCompile(`^[0-9a-v]{32}$`).MatchString(working.WorkingRootHash) {
			return &Error{Kind: "source-binding", Row: -1, Cause: errors.New("complete working-root binding identity differs")}
		}
		_, err = decode(working.Image)
	}
	return err
}

func ReadSource(reader io.Reader) (SourceBinding, error) {
	var binding SourceBinding
	if err := readEnvelope(reader, &binding); err != nil {
		return binding, err
	}
	return binding, validateBinding(binding)
}

func readEnvelope(reader io.Reader, target any) error {
	data, err := io.ReadAll(io.LimitReader(reader, (64<<20)+1))
	if err != nil {
		return err
	}
	if len(data) > 64<<20 || !utf8.Valid(data) || !json.Valid(data) {
		return errors.New("invalid or oversized typed envelope")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := checkValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("trailing typed envelope")
	}
	decoder = json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

const WorkingCapturePort = "physicalcapture.WorkingImage/f7919ad526b33a319e8fe87e66c75ab437309c27"

type WorkingBinding struct {
	Format          string   `json:"format"`
	Port            string   `json:"port"`
	Database        string   `json:"database"`
	HeadCommitID    string   `json:"head_commit_id"`
	WorkingRootHash string   `json:"working_root_hash"`
	ReadOnlyScope   bool     `json:"read_only_scope"`
	Complete        bool     `json:"complete"`
	Image           Manifest `json:"image"`
}

// FromWorkingImage explicitly accepts the complete current-only typed port.
// It never relabels a WorkingImage as an immutable Image or normalized archive.
func FromWorkingImage(image physicalcapture.WorkingImage) (Manifest, error) {
	if image.Format != physicalcapture.WorkingImageFormat || !image.Complete || !image.ReadOnlyScope || image.Database == "" || !regexp.MustCompile(`^[0-9a-v]{32}$`).MatchString(image.HeadCommitID) || !regexp.MustCompile(`^[0-9a-v]{32}$`).MatchString(image.WorkingRootHash) {
		return Manifest{}, &Error{Kind: "source-completeness", Row: -1, Cause: errors.New("authenticated complete typed working-root port required")}
	}
	manifest := Manifest{Format: Format, HistoryMode: "snapshot", Tables: make([]Table, 0, len(image.Tables))}
	for _, source := range image.Tables {
		table := Table{Name: source.Name, Columns: make([]Column, 0, len(source.Columns)), Rows: make([][]Cell, len(source.Rows))}
		for _, column := range source.Columns {
			table.Columns = append(table.Columns, Column{Name: column.Name, SQLType: column.DatabaseTypeName})
		}
		for ri, row := range source.Rows {
			if len(row) != len(source.Columns) {
				return Manifest{}, &Error{Kind: "row-width", Table: source.Name, Column: "*", Row: ri, Cause: errors.New("typed working row width differs")}
			}
			table.Rows[ri] = make([]Cell, len(row))
			for ci, cell := range row {
				encoded, err := encodeCell(cell)
				if err != nil {
					return Manifest{}, &Error{Kind: "cell-type", Table: source.Name, Column: source.Columns[ci].Name, Row: ri, Cause: err}
				}
				table.Rows[ri][ci] = encoded
			}
		}
		manifest.Tables = append(manifest.Tables, table)
	}
	if _, err := decode(manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

// BindWorkingSource binds TWO distinct observed ports at the same original
// HEAD. Target validation independently checks database, original working-root
// hash, complete physical census, SQL types and cells before any deletion. These
// content identities do not invent an external project identity or Plug census.
func BindWorkingSource(original physicalcapture.Image, working physicalcapture.WorkingImage) (SourceBinding, error) {
	binding, err := BindSource(original)
	if err != nil {
		return SourceBinding{}, err
	}
	manifest, err := FromWorkingImage(working)
	if err != nil {
		return SourceBinding{}, err
	}
	if working.HeadCommitID != original.CommitID {
		return SourceBinding{}, &Error{Kind: "source-binding", Row: -1, Cause: errors.New("immutable and complete working captures have different original HEADs")}
	}
	binding.Working = &WorkingBinding{Format: physicalcapture.WorkingImageFormat, Port: WorkingCapturePort, Database: working.Database, HeadCommitID: working.HeadCommitID, WorkingRootHash: working.WorkingRootHash, ReadOnlyScope: working.ReadOnlyScope, Complete: working.Complete, Image: manifest}
	binding.SHA256, err = bindingDigest(binding)
	return binding, err
}
