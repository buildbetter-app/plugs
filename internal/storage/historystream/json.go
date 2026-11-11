package historystream

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"unicode/utf8"
)

// validateJSON checks syntax and original UTF-8 without materializing a string,
// number, array or image. Its reader buffer is fixed; depth is capped at the
// same 10,000 nesting levels as encoding/json. Lexical numbers remain untouched.
func validateJSON(ctx context.Context, input io.Reader) (err error) {
	defer func() {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
	}()
	p := jsonSyntax{r: bufio.NewReaderSize(ContextReader(ctx, input), PageBytes)}
	if err := p.value(0); err != nil {
		return err
	}
	if err := p.space(); err != nil {
		return err
	}
	if _, err := p.r.ReadByte(); err != io.EOF {
		if err != nil {
			return err
		}
		return fmt.Errorf("history image has trailing JSON data")
	}
	return nil
}

type jsonSyntax struct{ r *bufio.Reader }

func (p *jsonSyntax) space() error {
	for {
		b, err := p.r.Peek(1)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if b[0] != ' ' && b[0] != '\t' && b[0] != '\r' && b[0] != '\n' {
			return nil
		}
		_, _ = p.r.ReadByte()
	}
}

func (p *jsonSyntax) byte(want byte) error {
	b, err := p.r.ReadByte()
	if err != nil || b != want {
		return fmt.Errorf("invalid history JSON: expected %q", want)
	}
	return nil
}

func (p *jsonSyntax) text() error {
	if err := p.byte('"'); err != nil {
		return err
	}
	for {
		b, err := p.r.ReadByte()
		if err != nil {
			return fmt.Errorf("unterminated history JSON string: %w", err)
		}
		switch {
		case b == '"':
			return nil
		case b < 0x20:
			return fmt.Errorf("control byte in history JSON string")
		case b == '\\':
			e, err := p.r.ReadByte()
			if err != nil {
				return err
			}
			if e == 'u' {
				for i := 0; i < 4; i++ {
					c, err := p.r.ReadByte()
					if err != nil || !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
						return fmt.Errorf("invalid history JSON unicode escape")
					}
				}
			} else if e != '"' && e != '\\' && e != '/' && e != 'b' && e != 'f' && e != 'n' && e != 'r' && e != 't' {
				return fmt.Errorf("invalid history JSON escape")
			}
		case b >= utf8.RuneSelf:
			_ = p.r.UnreadByte()
			r, n, err := p.r.ReadRune()
			if err != nil || r == utf8.RuneError && n == 1 {
				return fmt.Errorf("invalid original history JSON UTF-8")
			}
		}
	}
}

func (p *jsonSyntax) digits(required bool) error {
	n := 0
	for {
		b, err := p.r.Peek(1)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if b[0] < '0' || b[0] > '9' {
			break
		}
		_, _ = p.r.ReadByte()
		n++
	}
	if required && n == 0 {
		return fmt.Errorf("missing history JSON number digits")
	}
	return nil
}

func (p *jsonSyntax) number() error {
	b, _ := p.r.Peek(1)
	if len(b) != 0 && b[0] == '-' {
		_, _ = p.r.ReadByte()
	}
	b, err := p.r.Peek(1)
	if err != nil {
		return err
	}
	if b[0] == '0' {
		_, _ = p.r.ReadByte()
	} else if b[0] >= '1' && b[0] <= '9' {
		if err = p.digits(true); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("invalid history JSON number")
	}
	b, _ = p.r.Peek(1)
	if len(b) != 0 && b[0] == '.' {
		_, _ = p.r.ReadByte()
		if err = p.digits(true); err != nil {
			return err
		}
	}
	b, _ = p.r.Peek(1)
	if len(b) != 0 && (b[0] == 'e' || b[0] == 'E') {
		_, _ = p.r.ReadByte()
		b, _ = p.r.Peek(1)
		if len(b) != 0 && (b[0] == '+' || b[0] == '-') {
			_, _ = p.r.ReadByte()
		}
		if err = p.digits(true); err != nil {
			return err
		}
	}
	return nil
}

func (p *jsonSyntax) value(depth int) error {
	if depth > 10000 {
		return fmt.Errorf("history JSON nesting exceeds 10000")
	}
	if err := p.space(); err != nil {
		return err
	}
	b, err := p.r.Peek(1)
	if err != nil {
		return err
	}
	switch b[0] {
	case '"':
		return p.text()
	case 'n', 't', 'f':
		word := "null"
		if b[0] == 't' {
			word = "true"
		} else if b[0] == 'f' {
			word = "false"
		}
		for i := range word {
			if err = p.byte(word[i]); err != nil {
				return err
			}
		}
		return nil
	case '[', '{':
		object := b[0] == '{'
		end := byte(']')
		if object {
			end = '}'
		}
		_, _ = p.r.ReadByte()
		if err = p.space(); err != nil {
			return err
		}
		b, err = p.r.Peek(1)
		if err != nil {
			return err
		}
		if b[0] == end {
			_, _ = p.r.ReadByte()
			return nil
		}
		for {
			if object {
				if err = p.space(); err != nil {
					return err
				}
				if err = p.text(); err != nil {
					return err
				}
				if err = p.space(); err != nil {
					return err
				}
				if err = p.byte(':'); err != nil {
					return err
				}
			}
			if err = p.value(depth + 1); err != nil {
				return err
			}
			if err = p.space(); err != nil {
				return err
			}
			c, err := p.r.ReadByte()
			if err != nil {
				return err
			}
			if c == end {
				return nil
			}
			if c != ',' {
				return fmt.Errorf("invalid history JSON delimiter")
			}
		}
	default:
		return p.number()
	}
}
