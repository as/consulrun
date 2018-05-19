package gsyslog

type Priority int

const (
	LOG_EMERG Priority = iota
	LOG_ALERT
	LOG_CRIT
	LOG_ERR
	LOG_WARNING
	LOG_NOTICE
	LOG_INFO
	LOG_DEBUG
)

type Syslogger interface {
	WriteLevel(Priority, []byte) error

	Write([]byte) (int, error)

	Close() error
}
