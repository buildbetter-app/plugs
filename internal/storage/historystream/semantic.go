package historystream

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Historical images are syntax-checked independently, then projected one
// comment at a time through the existing authoritative profile validator. Large
// body/unknown strings and comment arrays are scanned rather than decoded into
// memory. Authority/provenance metadata is bounded to one protocol-sized field;
// an unrepresentable field explicitly refuses instead of skipping validation.
const commentMetadataBytes = 64 * 1024

type countedReader struct {
	input io.Reader
	n     int64
}

func (c *countedReader) Read(b []byte) (int, error) {
	n, e := c.input.Read(b)
	c.n += int64(n)
	return n, e
}

type projection struct {
	jsonSyntax
	input io.ReaderAt
	count *countedReader
	ctx   context.Context
	limit int64
}

func (p *projection) position() int64 { return p.count.n - int64(p.r.Buffered()) }
func (p *projection) raw(start, end int64, limit int64) ([]byte, error) {
	if end-start > limit {
		return nil, fmt.Errorf("%w: historical comment metadata field exceeds bounded representation", driver.ErrUnsupported)
	}
	return io.ReadAll(ContextReader(p.ctx, io.NewSectionReader(p.input, start, end-start)))
}
func (p *projection) key() (string, error) {
	if e := p.space(); e != nil {
		return "", e
	}
	start := p.position()
	if e := p.text(); e != nil {
		return "", e
	}
	end := p.position()
	// Unknown keys may themselves be arbitrarily large. They are consumed but
	// never retained; no recognized field name can occupy this many raw bytes.
	if end-start > 512 {
		return "", nil
	}
	raw, e := p.raw(start, end, 512)
	if e != nil {
		return "", e
	}
	var key string
	e = json.Unmarshal(raw, &key)
	return key, e
}
func (p *projection) object(visit func(string) error) error {
	if e := p.space(); e != nil {
		return e
	}
	if e := p.byte('{'); e != nil {
		return e
	}
	if e := p.space(); e != nil {
		return e
	}
	b, e := p.r.Peek(1)
	if e != nil {
		return e
	}
	if b[0] == '}' {
		_, _ = p.r.ReadByte()
		return nil
	}
	for {
		key, e := p.key()
		if e != nil {
			return e
		}
		if e = p.space(); e != nil {
			return e
		}
		if e = p.byte(':'); e != nil {
			return e
		}
		if e = p.space(); e != nil {
			return e
		}
		if e = visit(key); e != nil {
			return e
		}
		if e = p.space(); e != nil {
			return e
		}
		c, e := p.r.ReadByte()
		if e != nil {
			return e
		}
		if c == '}' {
			return nil
		}
		if c != ',' {
			return invalid("invalid historical object delimiter")
		}
	}
}
func (p *projection) comments(validate func([]byte) error) error {
	b, e := p.r.Peek(1)
	if e != nil {
		return e
	}
	if b[0] == 'n' {
		return p.value(0)
	}
	if e = p.byte('['); e != nil {
		return e
	}
	if e = p.space(); e != nil {
		return e
	}
	b, e = p.r.Peek(1)
	if e != nil {
		return e
	}
	if b[0] == ']' {
		_, _ = p.r.ReadByte()
		return nil
	}
	for {
		fields := map[string]json.RawMessage{}
		e = p.object(func(key string) error {
			recognized := ""
			for _, field := range []string{"principal", "time", "origin", "sourceAuthor", "sourceTime", "body"} {
				if strings.EqualFold(key, field) {
					recognized = field
					break
				}
			}
			if recognized == "" {
				return p.value(0)
			}
			if _, seen := fields[recognized]; seen {
				return invalid("duplicate historical comment metadata")
			}
			if recognized == "body" {
				b, e := p.r.Peek(1)
				if e != nil {
					return e
				}
				if b[0] != '"' && b[0] != 'n' {
					return invalid("historical comment body must be a string")
				}
				if e = p.value(0); e != nil {
					return e
				}
				fields[recognized] = json.RawMessage(`""`)
				return nil
			}
			start := p.position()
			if e := p.value(0); e != nil {
				return e
			}
			raw, e := p.raw(start, p.position(), p.limit)
			if e != nil {
				return e
			}
			fields[recognized] = raw
			return nil
		})
		if e != nil {
			return e
		}
		raw, e := json.Marshal(struct {
			Comments []map[string]json.RawMessage `json:"comments"`
		}{[]map[string]json.RawMessage{fields}})
		if e != nil {
			return e
		}
		if e = validate(raw); e != nil {
			return e
		}
		if e = p.space(); e != nil {
			return e
		}
		c, e := p.r.ReadByte()
		if e != nil {
			return e
		}
		if c == ']' {
			return nil
		}
		if c != ',' {
			return invalid("invalid historical comments delimiter")
		}
		if e = p.space(); e != nil {
			return e
		}
	}
}
func (s *Stage) validateHistoricalRecord(ctx context.Context, image Image) (err error) {
	defer func() {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
	}()
	input := io.NewSectionReader(s.file, image.Offset, image.Bytes)
	count := &countedReader{input: ContextReader(ctx, input)}
	actor, e := json.Marshal(s.binding.Profile.Admission.Actor)
	if e != nil {
		return e
	}
	limit := int64(commentMetadataBytes)
	if int64(len(actor))*16 > limit {
		limit = int64(len(actor)) * 16
	}
	p := projection{jsonSyntax: jsonSyntax{r: bufio.NewReaderSize(count, PageBytes)}, input: input, count: count, ctx: ctx, limit: limit}
	if e = p.space(); e != nil {
		return e
	}
	b, e := p.r.Peek(1)
	if e != nil {
		return e
	}
	if b[0] == 'n' {
		return p.value(0)
	}
	seen := false
	return p.object(func(key string) error {
		if !strings.EqualFold(key, "comments") {
			return p.value(0)
		}
		if seen {
			return invalid("duplicate historical comments field")
		}
		seen = true
		return p.comments(func(raw []byte) error {
			return s.binding.Profile.ValidateRecord(driver.Record{Kind: "bead", Body: raw}, s.binding.When)
		})
	})
}
