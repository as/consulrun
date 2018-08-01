package logger

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/as/consulrun/hashicorp/go-syslog"
	"github.com/as/consulrun/hashicorp/logutils"
	"github.com/mitchellh/cli"
)

type Config struct {
	LogLevel string

	EnableSyslog bool

	SyslogFacility string
}

//
//
func Setup(config *Config, ui cli.Ui) (*logutils.LevelFilter, *GatedWriter, *LogWriter, io.Writer, bool) {

	logGate := &GatedWriter{
		Writer: &cli.UiWriter{Ui: ui},
	}

	logFilter := LevelFilter()
	logFilter.MinLevel = logutils.LogLevel(strings.ToUpper(config.LogLevel))
	logFilter.Writer = logGate
	if !ValidateLevelFilter(logFilter.MinLevel, logFilter) {
		ui.Error(fmt.Sprintf(
			"Invalid log level: %s. Valid log levels are: %v",
			logFilter.MinLevel, logFilter.Levels))
		return nil, nil, nil, nil, false
	}

	var syslog io.Writer
	if config.EnableSyslog {
		retries := 12
		delay := 5 * time.Second
		for i := 0; i <= retries; i++ {
			l, err := gsyslog.NewLogger(gsyslog.LOG_NOTICE, config.SyslogFacility, "consul")
			if err == nil {
				syslog = &SyslogWrapper{l, logFilter}
				break
			}

			ui.Error(fmt.Sprintf("Syslog setup error: %v", err))
			if i == retries {
				timeout := time.Duration(retries) * delay
				ui.Error(fmt.Sprintf("Syslog setup did not succeed within timeout (%s).", timeout.String()))
				return nil, nil, nil, nil, false
			}

			ui.Error(fmt.Sprintf("Retrying syslog setup in %s...", delay.String()))
			time.Sleep(delay)
		}
	}

	logWriter := NewLogWriter(512)
	var logOutput io.Writer
	if syslog != nil {
		logOutput = io.MultiWriter(logFilter, logWriter, syslog)
	} else {
		logOutput = io.MultiWriter(logFilter, logWriter)
	}
	return logFilter, logGate, logWriter, logOutput, true
}
