package discover

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type Config map[string]string

func Parse(s string) (Config, error) {
	return parse(s)
}

func (c Config) String() string {

	var keys []string
	for k := range c {
		if k != "provider" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	keys = append([]string{"provider"}, keys...)

	quote := func(s string) string {
		if strings.ContainsAny(s, ` "\`) {
			return strconv.Quote(s)
		}
		return s
	}

	var vals []string
	for _, k := range keys {
		v := c[k]
		if v == "" {
			continue
		}
		vals = append(vals, quote(k)+"="+quote(v))
	}
	return strings.Join(vals, " ")
}

func parse(in string) (Config, error) {
	m := Config{}
	s := []rune(strings.TrimSpace(in))
	state := stateKey
	key := ""
	for {

		if len(s) == 0 {
			break
		}

		item, val, n := lex(s)
		s = s[n:]

		switch state {

		case stateKey:
			switch item {
			case itemText:
				key = val
				if _, exists := m[key]; exists {
					return nil, fmt.Errorf("%s: duplicate key", key)
				}
				state = stateEqual
			default:
				return nil, fmt.Errorf("%s: %s", key, val)
			}

		case stateEqual:
			switch item {
			case itemEqual:
				state = stateVal
			default:
				return nil, fmt.Errorf("%s: missing '='", key)
			}

		case stateVal:
			switch item {
			case itemText:
				m[key] = val
				state = stateKey
			case itemError:
				return nil, fmt.Errorf("%s: %s", key, val)
			default:
				return nil, fmt.Errorf("%s: missing value", key)
			}
		}
	}

	switch state {
	case stateEqual:
		return nil, fmt.Errorf("%s: missing '='", key)
	case stateVal:
		return nil, fmt.Errorf("%s: missing value", key)
	}
	if len(m) == 0 {
		return nil, nil
	}
	return m, nil
}

type itemType string

const (
	itemText  itemType = "TEXT"
	itemEqual          = "EQUAL"
	itemError          = "ERROR"
)

func (t itemType) String() string {
	return string(t)
}

type state string

const (
	stateStart    state = "start"
	stateEqual          = "equal"
	stateText           = "text"
	stateQText          = "qtext"
	stateQTextEnd       = "qtextend"
	stateQTextEsc       = "qtextesc"

	stateKey = "key"
	stateVal = "val"
)

func lex(s []rune) (itemType, string, int) {
	isEqual := func(r rune) bool { return r == '=' }
	isEscape := func(r rune) bool { return r == '\\' }
	isQuote := func(r rune) bool { return r == '"' }
	isSpace := func(r rune) bool { return r == ' ' }

	unquote := func(r []rune) (string, error) {
		v := strings.TrimSpace(string(r))
		return strconv.Unquote(v)
	}

	var quote rune
	state := stateStart
	for i, r := range s {

		switch state {
		case stateStart:
			switch {
			case isSpace(r):

			case isEqual(r):
				state = stateEqual
			case isQuote(r):
				quote = r
				state = stateQText
			default:
				state = stateText
			}

		case stateEqual:
			return itemEqual, "", i

		case stateText:
			switch {
			case isEqual(r) || isSpace(r):
				v := strings.TrimSpace(string(s[:i]))
				return itemText, v, i
			default:

			}

		case stateQText:
			switch {
			case r == quote:
				state = stateQTextEnd
			case isEscape(r):
				state = stateQTextEsc
			default:

			}

		case stateQTextEsc:
			state = stateQText

		case stateQTextEnd:
			v, err := unquote(s[:i])
			if err != nil {
				return itemError, err.Error(), i
			}
			return itemText, v, i
		}
	}

	switch state {
	case stateEqual:
		return itemEqual, "", len(s)
	case stateQText:
		return itemError, "unbalanced quotes", len(s)
	case stateQTextEsc:
		return itemError, "unterminated escape sequence", len(s)
	case stateQTextEnd:
		v, err := unquote(s)
		if err != nil {
			return itemError, err.Error(), len(s)
		}
		return itemText, v, len(s)
	default:
		return itemText, strings.TrimSpace(string(s)), len(s)
	}
}
