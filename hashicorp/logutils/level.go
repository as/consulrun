// Package logutils augments the standard log package with levels.
package logutils

import (
	"bytes"
	"io"
	"sync"
)

type LogLevel string

//
type LevelFilter struct {
	Levels []LogLevel

	MinLevel LogLevel

	Writer io.Writer

	badLevels map[LogLevel]struct{}
	once      sync.Once
}

func (f *LevelFilter) Check(line []byte) bool {
	f.once.Do(f.init)

	var level LogLevel
	x := bytes.IndexByte(line, '[')
	if x >= 0 {
		y := bytes.IndexByte(line[x:], ']')
		if y >= 0 {
			level = LogLevel(line[x+1 : x+y])
		}
	}

	_, ok := f.badLevels[level]
	return !ok
}

func (f *LevelFilter) Write(p []byte) (n int, err error) {

	if !f.Check(p) {
		return len(p), nil
	}

	return f.Writer.Write(p)
}

func (f *LevelFilter) SetMinLevel(min LogLevel) {
	f.MinLevel = min
	f.init()
}

func (f *LevelFilter) init() {
	badLevels := make(map[LogLevel]struct{})
	for _, level := range f.Levels {
		if level == f.MinLevel {
			break
		}
		badLevels[level] = struct{}{}
	}
	f.badLevels = badLevels
}
