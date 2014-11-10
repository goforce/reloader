package soqlparser

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode"
)

type scanner struct {
	reader *strings.Reader
	buff   []rune
}

func SoqlFields(soql string) []string {
	aggr := 0
	fields := make([]string, 0)
	scanner := &scanner{reader: strings.NewReader(soql), buff: make([]rune, 0, len(soql))}
	token := scanner.read()
	var next string
	if !strings.EqualFold(token, "select") {
		panic(message(soql, "no select keyword", token))
	}
	// parse fields until from clause
	for {
		token = scanner.read()
		if token == "" {
			panic(message(soql, "no from clause", token))
		} else if token == "(" {
			// subselect
			if !scanner.skipInParentheses() {
				panic(message(soql, "no closing parenthesis", token))
			}
			for {
				next = scanner.read()
				if next == "," || next == "" || strings.EqualFold(next, "from") {
					break
				}
			}
		} else {
			next = scanner.read()
			if next == "(" {
				fname := scanner.read()
				if !scanner.skipInParentheses() {
					panic(message(soql, "no closing parenthesis", fname))
				}
				if strings.EqualFold(token, "convertcurrency") {
					token = fname
					next = scanner.read()
				} else {
					alias := scanner.read()
					if alias == "," || strings.EqualFold(alias, "from") {
						token = "expr" + strconv.Itoa(aggr)
						next = alias
						aggr++
					} else {
						token = alias
						next = scanner.read()
					}
				}
			}
			fields = append(fields, token)
		}
		if next == "" {
			panic(message(soql, "no from clause", token))
		} else if strings.EqualFold(next, "from") {
			break
		} else if next != "," {
			panic(message(soql, "no comma, only aggregate functions can be aliased", next))
		}
	}
	// parsing from to resolve aliases
	aliases := make(map[string]string)
	for {
		token = scanner.read()
		next = scanner.read()
		if token == "" {
			break
		} else {
			if next != "," {
				if strings.IndexRune(token, '.') < 0 {
					token = ""
				}
				aliases[strings.ToLower(next)] = token
				next = scanner.read()
				if next != "," {
					break
				}
			}
		}
	}
	// replace aliases in aliases themselves
	for k, v := range aliases {
		parts := strings.Split(v, ".")
		for i := 0; i < len(parts)-1; i++ {
			if s, ok := aliases[parts[i]]; ok {
				parts[i] = s
			}
		}
		aliases[k] = strings.Join(parts, ".")
	}
	// replace aliases with actual object names
	if len(aliases) > 0 {
		for i, f := range fields {
			parts := strings.SplitN(f, ".", 2)
			if len(parts) > 1 {
				if actual, ok := aliases[strings.ToLower(parts[0])]; ok {
					fields[i] = actual + "." + parts[1]
				}
			}
		}
	}
	return fields
}

func message(soql string, cause string, token string) string {
	return fmt.Sprint("failed to parse soql: ", soql, "\n>>> ", cause, ": ", token)
}

func (sc *scanner) read() string {
	sc.buff = sc.buff[:0]
	var r rune
	var err error
	for {
		r, _, err = sc.reader.ReadRune()
		if err == io.EOF {
			break
		}
		if !unicode.IsSpace(r) {
			break
		}
	}
	if r == '(' || r == ')' || r == ',' {
		return string(r)
	}
	for {
		sc.buff = append(sc.buff, r)
		r, _, err = sc.reader.ReadRune()
		if err == io.EOF {
			break
		}
		if r == '(' || r == ')' || r == ',' {
			sc.reader.UnreadRune()
			break
		} else if unicode.IsSpace(r) {
			break
		}
	}
	return string(sc.buff)
}

// SkipInParentheses skips reader until matching brackets
func (sc *scanner) skipInParentheses() bool {
	var r rune
	var err error
	for {
		r, _, err = sc.reader.ReadRune()
		if err == io.EOF {
			break
		}
		if r == ')' {
			return true
		} else if r == '(' {
			if !sc.skipInParentheses() {
				break
			}
		} else if r == '\'' {
			if !sc.skipStringLiteral() {
				break
			}
		}
	}
	return false
}

// SkipStringLiteral skips soal string literal in reader and advances to the next rune after the literal
// It returns true if literal is correctly ended and false if there is no matching single quote
func (sc *scanner) skipStringLiteral() bool {
	var r rune
	var err error
	for {
		r, _, err = sc.reader.ReadRune()
		if err == io.EOF {
			break
		}
		if r == '\'' {
			return true
		} else if r == '\\' {
			// skip next rune
			r, _, err = sc.reader.ReadRune()
			if err == io.EOF {
				break
			}
		}
	}
	return false
}
