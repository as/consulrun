package columnize

import (
	"bytes"
	"fmt"
	"strings"
)

type Config struct {
	Delim string

	Glue string

	Prefix string

	Empty string
}

func DefaultConfig() *Config {
	return &Config{
		Delim:  "|",
		Glue:   "  ",
		Prefix: "",
		Empty:  "",
	}
}

func MergeConfig(a, b *Config) *Config {
	var result Config = *a

	if a == nil || b == nil {
		return &result
	}

	if b.Delim != "" {
		result.Delim = b.Delim
	}
	if b.Glue != "" {
		result.Glue = b.Glue
	}
	if b.Prefix != "" {
		result.Prefix = b.Prefix
	}
	if b.Empty != "" {
		result.Empty = b.Empty
	}

	return &result
}

func stringFormat(c *Config, widths []int, columns int) string {

	buf := bytes.NewBuffer(make([]byte, 0, (6+len(c.Glue))*columns))

	buf.WriteString(c.Prefix)

	for i := 0; i < columns && i < len(widths); i++ {
		if i == columns-1 {
			buf.WriteString("%s\n")
		} else {
			fmt.Fprintf(buf, "%%-%ds%s", widths[i], c.Glue)
		}
	}
	return buf.String()
}

func elementsFromLine(config *Config, line string) []interface{} {
	seperated := strings.Split(line, config.Delim)
	elements := make([]interface{}, len(seperated))
	for i, field := range seperated {
		value := strings.TrimSpace(field)

		if value == "" && config.Empty != "" {
			value = config.Empty
		}
		elements[i] = value
	}
	return elements
}

func widthsFromLines(config *Config, lines []string) []int {
	widths := make([]int, 0, 8)

	for _, line := range lines {
		elems := elementsFromLine(config, line)
		for i := 0; i < len(elems); i++ {
			l := len(elems[i].(string))
			if len(widths) <= i {
				widths = append(widths, l)
			} else if widths[i] < l {
				widths[i] = l
			}
		}
	}
	return widths
}

func Format(lines []string, config *Config) string {
	conf := MergeConfig(DefaultConfig(), config)
	widths := widthsFromLines(conf, lines)

	glueSize := len(conf.Glue)
	var size int
	for _, w := range widths {
		size += w + glueSize
	}
	size *= len(lines)

	buf := bytes.NewBuffer(make([]byte, 0, size))

	fmtCache := make(map[int]string, 16)

	for _, line := range lines {
		elems := elementsFromLine(conf, line)

		numElems := len(elems)
		stringfmt, ok := fmtCache[numElems]
		if !ok {
			stringfmt = stringFormat(conf, widths, numElems)
			fmtCache[numElems] = stringfmt
		}

		fmt.Fprintf(buf, stringfmt, elems...)
	}

	result := buf.String()

	if n := len(result); n > 0 && result[n-1] == '\n' {
		result = result[:n-1]
	}

	return result
}

func SimpleFormat(lines []string) string {
	return Format(lines, nil)
}
