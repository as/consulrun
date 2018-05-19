package logger

import (
	"bytes"

	"github.com/hashicorp/go-syslog"
	"github.com/hashicorp/logutils"
)

var levelPriority = map[string]gsyslog.Priority{
	"TRACE": gsyslog.LOG_DEBUG,
	"DEBUG": gsyslog.LOG_INFO,
	"INFO":  gsyslog.LOG_NOTICE,
	"WARN":  gsyslog.LOG_WARNING,
	"ERR":   gsyslog.LOG_ERR,
	"CRIT":  gsyslog.LOG_CRIT,
}

type SyslogWrapper struct {
	l    gsyslog.Syslogger
	filt *logutils.LevelFilter
}

func (s *SyslogWrapper) Write(p []byte) (int, error) {

	if !s.filt.Check(p) {
		return 0, nil
	}

	var level string
	afterLevel := p
	x := bytes.IndexByte(p, '[')
	if x >= 0 {
		y := bytes.IndexByte(p[x:], ']')
		if y >= 0 {
			level = string(p[x+1 : x+y])
			afterLevel = p[x+y+2:]
		}
	}

	priority, ok := levelPriority[level]
	if !ok {
		priority = gsyslog.LOG_NOTICE
	}

	err := s.l.WriteLevel(priority, afterLevel)
	return len(p), err
}
