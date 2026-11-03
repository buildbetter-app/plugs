package viewops

import (
	"strings"
	"unicode"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Label expressions stay private to the shared matcher. Scalar terms remain
// separate root conjuncts so Run retains its existing project conflict checks.
type labelPredicate struct {
	op          byte
	value       string
	left, right *labelPredicate
}

func (p *labelPredicate) matches(labels []string) bool {
	switch p.op {
	case '=':
		return contains(labels, p.value)
	case '!':
		return !p.left.matches(labels)
	case '&':
		return p.left.matches(labels) && p.right.matches(labels)
	case '|':
		return p.left.matches(labels) || p.right.matches(labels)
	default:
		return false
	}
}

// A nil labels member means this expression contains a scalar. Such an
// expression may be a root conjunction, but never an OR/NOT operand or group.
type parsedLabelQuery struct {
	terms  []queryTerm
	labels *labelPredicate
}

func labelQueryNode(predicate *labelPredicate) parsedLabelQuery {
	return parsedLabelQuery{
		terms:  []queryTerm{{key: "labels", labels: predicate}},
		labels: predicate,
	}
}

type labelQueryParser struct{ rest string }

func parseLabelQuery(query string) ([]queryTerm, error) {
	p := labelQueryParser{rest: strings.TrimSpace(query)}
	if p.rest == "" {
		return nil, nil
	}
	node, err := p.or(false)
	p.space()
	if err != nil {
		return nil, err
	}
	if p.rest != "" {
		return nil, invalidLabelQuery()
	}
	return node.terms, nil
}

func invalidLabelQuery() error {
	// Keep the existing scalar syntax error, including its public hint.
	return failure("invalid_params", "unsupported view query; use equality clauses joined by AND")
}

func (p *labelQueryParser) or(labelsOnly bool) (parsedLabelQuery, error) {
	left, err := p.and(labelsOnly)
	if err != nil {
		return parsedLabelQuery{}, err
	}
	for p.keyword("OR") {
		if left.labels == nil {
			return parsedLabelQuery{}, invalidLabelQuery()
		}
		right, err := p.and(true)
		if err != nil {
			return parsedLabelQuery{}, err
		}
		if left.labels == nil || right.labels == nil {
			return parsedLabelQuery{}, invalidLabelQuery()
		}
		left = labelQueryNode(&labelPredicate{op: '|', left: left.labels, right: right.labels})
	}
	return left, nil
}

func (p *labelQueryParser) and(labelsOnly bool) (parsedLabelQuery, error) {
	left, err := p.unary(labelsOnly)
	if err != nil {
		return parsedLabelQuery{}, err
	}
	for p.keyword("AND") {
		right, err := p.unary(labelsOnly)
		if err != nil {
			return parsedLabelQuery{}, err
		}
		left.terms = append(left.terms, right.terms...)
		if left.labels != nil && right.labels != nil {
			left.labels = &labelPredicate{op: '&', left: left.labels, right: right.labels}
		} else {
			left.labels = nil
		}
	}
	return left, nil
}

func (p *labelQueryParser) unary(labelsOnly bool) (parsedLabelQuery, error) {
	p.space()
	if p.keyword("NOT") {
		child, err := p.unary(true)
		if err != nil {
			return parsedLabelQuery{}, err
		}
		if child.labels == nil {
			return parsedLabelQuery{}, invalidLabelQuery()
		}
		return labelQueryNode(&labelPredicate{op: '!', left: child.labels}), nil
	}
	if strings.HasPrefix(p.rest, "(") {
		p.rest = p.rest[1:]
		child, err := p.or(true)
		if err != nil {
			return parsedLabelQuery{}, err
		}
		p.space()
		if child.labels == nil || !strings.HasPrefix(p.rest, ")") {
			return parsedLabelQuery{}, invalidLabelQuery()
		}
		p.rest = p.rest[1:]
		return labelQueryNode(child.labels), nil
	}
	for _, key := range []string{"labels", "label"} {
		if !strings.HasPrefix(p.rest, key) {
			continue
		}
		tail := p.rest[len(key):]
		if tail != "" && !strings.ContainsAny(tail[:1], "=!<>()") && !unicode.IsSpace(firstQueryRune(tail)) {
			continue
		}
		p.rest = tail
		p.space()
		if !strings.HasPrefix(p.rest, "=") {
			return parsedLabelQuery{}, invalidLabelQuery()
		}
		p.rest = p.rest[1:]
		value, err := p.value()
		if err != nil {
			return parsedLabelQuery{}, err
		}
		return labelQueryNode(&labelPredicate{op: '=', value: value}), nil
	}
	if labelsOnly {
		return parsedLabelQuery{}, invalidLabelQuery()
	}
	// Consume a legacy scalar as one opaque strings.Fields token. In
	// particular, title=OR, title=(foo), id=a=b and quote bytes are untouched.
	word := p.word()
	terms, err := parseScalarQuery(word)
	if err != nil {
		return parsedLabelQuery{}, err
	}
	if len(terms) != 1 {
		return parsedLabelQuery{}, invalidLabelQuery()
	}
	return parsedLabelQuery{terms: terms}, nil
}

func (p *labelQueryParser) value() (string, error) {
	p.space()
	if p.rest == "" {
		return "", invalidLabelQuery()
	}
	if quote := p.rest[0]; quote == '\'' || quote == '"' {
		p.rest = p.rest[1:]
		var value strings.Builder
		for p.rest != "" {
			c := p.rest[0]
			p.rest = p.rest[1:]
			if c == quote {
				if value.Len() == 0 {
					return "", invalidLabelQuery()
				}
				return value.String(), nil
			}
			if c == '\\' {
				if p.rest == "" {
					return "", invalidLabelQuery()
				}
				c, p.rest = p.rest[0], p.rest[1:]
				switch c {
				case 'n':
					c = '\n'
				case 't':
					c = '\t'
				case '\\', '\'', '"':
				default:
					return "", invalidLabelQuery()
				}
			}
			value.WriteByte(c) // Preserve decoded UTF-8 bytes, not rune(byte).
		}
		return "", invalidLabelQuery()
	}
	end := strings.IndexFunc(p.rest, func(r rune) bool {
		return unicode.IsSpace(r) || r == '(' || r == ')'
	})
	if end < 0 {
		end = len(p.rest)
	}
	value := p.rest[:end]
	if value == "" || strings.EqualFold(value, "AND") || strings.EqualFold(value, "OR") || strings.EqualFold(value, "NOT") {
		return "", invalidLabelQuery()
	}
	for i, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' ||
			(i > 0 && strings.ContainsRune("-.:/", r)) {
			continue
		}
		return "", invalidLabelQuery()
	}
	p.rest = p.rest[end:]
	return value, nil
}

func (p *labelQueryParser) space() { p.rest = strings.TrimLeftFunc(p.rest, unicode.IsSpace) }

func firstQueryRune(s string) rune {
	for _, r := range s {
		return r
	}
	return 0
}

func (p *labelQueryParser) keyword(word string) bool {
	p.space()
	if len(p.rest) < len(word) || !strings.EqualFold(p.rest[:len(word)], word) {
		return false
	}
	tail := p.rest[len(word):]
	if tail != "" && !unicode.IsSpace(firstQueryRune(tail)) && tail[0] != '(' && tail[0] != ')' {
		return false
	}
	p.rest = tail
	return true
}

func (p *labelQueryParser) word() string {
	end := strings.IndexFunc(p.rest, unicode.IsSpace)
	if end < 0 {
		end = len(p.rest)
	}
	word := p.rest[:end]
	p.rest = p.rest[end:]
	return word
}

// Each existing logical label row is complete for exactly one source ID.
// The closed driver contract has no candidate-relative/family operand.
func (p *labelPredicate) selection(id string) driver.SelectionExpr {
	switch p.op {
	case '=':
		value := driver.SelectionString(p.value)
		return driver.SelectionExpr{Op: driver.SelectionExistsOp, Exists: &driver.SelectionExists{
			Row: driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: id}, Kind: driver.SelectionLabelFact,
			Tests: []driver.SelectionFactTest{{Column: driver.SelectionFactRef, Candidate: driver.SelectionID}, {Column: driver.SelectionFactName, Value: &value}},
		}}
	case '!':
		return driver.SelectionExpr{Op: driver.SelectionNot, Args: []driver.SelectionExpr{p.left.selection(id)}}
	case '&', '|':
		op := driver.SelectionAll
		if p.op == '|' {
			op = driver.SelectionAny
		}
		return driver.SelectionExpr{Op: op, Args: []driver.SelectionExpr{p.left.selection(id), p.right.selection(id)}}
	default:
		return driver.SelectionExpr{Op: driver.SelectionFalse}
	}
}
