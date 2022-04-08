package logger

import (
	"fmt"
	"io"
	"log"
	"os"
)

// 项目的日志记录器

type (
	LogLevel int
	LogType  int
)

// | 或位运算用来标识配置是否enable， config & LogDebug != 0 代表启用了 LogDebug 级别

const (
	LogFatal   = LogType(1 << iota) //LogType(0x1)  == 1
	LogError                        //= LogType(0x2)2
	LogWarning                      //= LogType(0x4)4
	LogInfo                         //= LogType(0x8)8
	LogDebug                        //= LogType(0x10)16
)

const (
	LogLevelNone  = LogLevel(0x0)
	LogLevelFatal = LogLevelNone | LogLevel(LogFatal)
	LogLevelError = LogLevelFatal | LogLevel(LogError)
	LogLevelWarn  = LogLevelError | LogLevel(LogWarning)
	LogLevelInfo  = LogLevelWarn | LogLevel(LogInfo)
	LogLevelDebug = LogLevelInfo | LogLevel(LogDebug)
	LogLevelAll   = LogLevelDebug
)

var _log = New()

func init() {
	SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	// SetHighlighting(runtime.GOOS != "windows")
	SetHighlighting(true)
}

func New() *Logger {
	return NewLogger(os.Stderr, "")
}

func NewLogger(w io.Writer, prefix string) *Logger {
	var level LogLevel
	if l := os.Getenv("LOG_LEVEL"); len(l) != 0 {
		level = StringToLogLevel(os.Getenv("GET_LEVEL"))
	} else {
		level = LogLevelInfo
	}
	return &Logger{
		_log:         log.New(w, prefix, log.LstdFlags),
		level:        level,
		highlighting: true,
	}
}

// 初始化一个默认的全局日志记录器_log
func GlobalLogger() *log.Logger {
	return _log._log
}

func SetLevel(level LogLevel) {
	_log.SetLevel(level)
}

func GetLogLevel() LogLevel {
	return _log.level
}

func SetFlags(flag int) {
	_log._log.SetFlags(flag)
}

func Info(v ...any) {
	_log.Info(v...)
}

func Infof(format string, v ...any) {
	_log.Infof(format, v...)
}

func Panic(v ...any) {
	_log.Panic(v...)
}

func Panicf(format string, v ...any) {
	_log.Panicf(format, v...)
}

func Debug(v ...any) {
	_log.Debug(v...)
}

func Debugf(format string, v ...any) {
	_log.Debugf(format, v...)
}

func Warn(v ...any) {
	_log.Warn(v...)
}

func Warnf(format string, v ...any) {
	_log.Warnf(format, v...)
}

func Error(v ...any) {
	_log.Error(v...)
}

func Errorf(format string, v ...any) {
	_log.Errorf(format, v...)
}

func Fatal(v ...any) {
	_log.Fatal(v...)
}

func Fatalf(format string, v ...any) {
	_log.Fatalf(format, v...)
}

func SetLevelByString(level string) {
	_log.SetLevelByString(level)
}

func SetHighlighting(highlighting bool) {
	_log.SetHighlighting(highlighting)
}

// ========================================Logger struct=======================================

type Logger struct {
	_log         *log.Logger
	level        LogLevel
	highlighting bool
}

func (l *Logger) SetHighlighting(highlighting bool) {
	l.highlighting = highlighting
}

func (l *Logger) SetFlags(flags int) {
	l._log.SetFlags(flags)
}

func (l *Logger) Flags() int {
	return l._log.Flags()
}

func (l *Logger) SetLevel(level LogLevel) {
	l.level = level
}

func (l *Logger) SetLevelByString(level string) {
	l.level = StringToLogLevel(level)
}

func (l *Logger) log(t LogType, v ...any) {
	l.logf(t, "%v\n", v)
}

func (l *Logger) logf(t LogType, format string, v ...any) {
	if l.level|LogLevel(t) != l.level {
		return
	}

	logStr, logColor := LogTypeToString(t)
	var s string
	if l.highlighting {
		s = "\033" + logColor + "m[" + logStr + "] " + fmt.Sprintf(format, v...) + "\033[0m"
	} else {
		s = "[" + logStr + "] " + fmt.Sprintf(format, v...)
	}
	_ = l._log.Output(4, s)
}

func (l *Logger) Fatal(v ...any) {
	l.log(LogFatal, v...)
	os.Exit(-1)
}

func (l *Logger) Fatalf(format string, v ...any) {
	l.logf(LogFatal, format, v...)
	os.Exit(-1)
}

func (l *Logger) Panic(v ...any) {
	l._log.Panic(v...)
}

func (l *Logger) Panicf(format string, v ...any) {
	l._log.Panicf(format, v...)
}

func (l *Logger) Error(v ...any) {
	l.log(LogError, v...)
}

func (l *Logger) Errorf(format string, v ...any) {
	l.logf(LogError, format, v...)
}

func (l *Logger) Warn(v ...any) {
	l.log(LogWarning, v...)
}

func (l *Logger) Warnf(format string, v ...any) {
	l.logf(LogWarning, format, v...)
}

func (l *Logger) Debug(v ...any) {
	l.log(LogDebug, v...)
}

func (l *Logger) Debugf(format string, v ...any) {
	l.logf(LogDebug, format, v...)
}

func (l *Logger) Info(v ...any) {
	l.log(LogInfo, v...)
}

func (l *Logger) Infof(format string, v ...any) {
	l.logf(LogInfo, format, v...)
}

func StringToLogLevel(level string) LogLevel {
	switch level {
	case "fatal":
		return LogLevelFatal
	case "error":
		return LogLevelError
	case "warning":
		return LogLevelWarn
	case "warn":
		return LogLevelWarn
	case "info":
		return LogLevelInfo
	case "debug":
		return LogLevelDebug
	}
	return LogLevelAll
}

func LogTypeToString(t LogType) (string, string) {
	switch t {
	case LogFatal:
		return "fatal", "[0;31"
	case LogError:
		return "error", "[0;31"
	case LogWarning:
		return "warning", "[0;33"
	case LogDebug:
		return "debug", "[0;36"
	case LogInfo:
		return "info", "[0;37"
	}
	return "unkown", "[0;37"
}
