package common

import (
	"context"
	"fmt"
	"os"
	"time"

	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
)

var (
	servName      string
	defaultLogger *logrus.Logger
)

func init() {
	defaultLogger = &logrus.Logger{
		Out:          os.Stdout,
		Formatter:    new(logrus.TextFormatter),
		Hooks:        make(logrus.LevelHooks),
		Level:        logrus.InfoLevel,
		ExitFunc:     os.Exit,
		ReportCaller: true,
	}
}

func InitServiceLogHook(serviceName string, maxRemainCnt uint) {
	servName = serviceName
	defaultLogger.AddHook(newLfsHook(logrus.InfoLevel, "server", maxRemainCnt))
	defaultLogger.AddHook(newLfsHook(logrus.WarnLevel, "error", maxRemainCnt))
}

func NewLoggerWithLfsHook(logName string) *logrus.Logger {
	tmpLogger := &logrus.Logger{
		Out:          os.Stdout,
		Formatter:    new(logrus.TextFormatter),
		Hooks:        make(logrus.LevelHooks),
		Level:        logrus.InfoLevel,
		ExitFunc:     os.Exit,
		ReportCaller: true,
	}
	tmpLogger.AddHook(newLfsHook(logrus.InfoLevel, logName, 30))
	return tmpLogger
}

func AnonymousLogger() *logrus.Entry {
	return logrus.NewEntry(defaultLogger)
}

func Logger(ctx context.Context) *logrus.Entry {
	entry := logrus.NewEntry(defaultLogger)
	fields := logrus.Fields{}
	for field := range loggerFields {
		if val := ctx.Value(field); val != nil {
			fields[field] = val
		}
	}
	if len(fields) == 0 {
		return entry
	}
	return entry.WithFields(fields)
}

func newLfsHook(logLevel logrus.Level, logName string, maxRemainCnt uint) logrus.Hook {
	logPath := fmt.Sprintf("/var/log/%s/%s.log", servName, logName)
	writer, err := rotatelogs.New(
		logPath+".%Y%m%d%H",
		rotatelogs.WithLinkName(logPath),
		rotatelogs.WithRotationTime(24*time.Hour),
		rotatelogs.WithMaxAge(-1),
		rotatelogs.WithRotationCount(maxRemainCnt),
	)

	if err != nil {
		logrus.Errorf("config local file system for logger error: %v", err)
	}

	logrus.SetLevel(logLevel)
	writerMap := lfshook.WriterMap{}
	for level := logrus.PanicLevel; level <= logLevel; level += 1 {
		writerMap[level] = writer
	}

	lfsHook := lfshook.NewHook(
		writerMap,
		&logrus.TextFormatter{DisableColors: true},
	)
	return lfsHook
}
