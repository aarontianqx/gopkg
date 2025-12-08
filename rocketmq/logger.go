package rocketmq

import (
	"context"
	"log/slog"
	"os"

	"github.com/aarontianqx/gopkg/common"
	rmq_client "github.com/apache/rocketmq-clients/golang/v5"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// SlogSyncer implements zapcore.WriteSyncer
type SlogSyncer struct {
	logger *slog.Logger
}

// Write sends the log line to the underlying slog.Logger
func (s *SlogSyncer) Write(p []byte) (n int, err error) {
	// Convert p to string and log
	msg := string(p)
	// Default to Info level; zapcore will handle level via core
	s.logger.LogAttrs(context.Background(), slog.LevelInfo, msg)
	return len(p), nil
}

// Sync is a no-op or can flush buffers if needed
func (s *SlogSyncer) Sync() error {
	// slog.Logger does not require sync by default
	return nil
}

var zlogger *zap.Logger

func init() {
	encoderCfg := zapcore.EncoderConfig{
		TimeKey:       "T",
		LevelKey:      "L",
		NameKey:       "N",
		CallerKey:     "C",
		MessageKey:    "M",
		StacktraceKey: "S",
		LineEnding:    zapcore.DefaultLineEnding,
		EncodeLevel:   zapcore.CapitalLevelEncoder,
		EncodeTime:    zapcore.ISO8601TimeEncoder,
		EncodeCaller:  zapcore.ShortCallerEncoder,
	}
	zlogger = NewSlogZapLogger(common.Logger(), zapcore.DebugLevel, encoderCfg)
	//defer zlogger.Sync()

	os.Setenv(rmq_client.ENABLE_CONSOLE_APPENDER, "true")
	rmq_client.ResetLogger()
	rmq_client.WithZapLogger(zlogger)
}

func NewSlogZapLogger(slogger *slog.Logger, level zapcore.Level, encoderCfg zapcore.EncoderConfig) *zap.Logger {
	// Create encoder
	encoder := zapcore.NewConsoleEncoder(encoderCfg)
	// Wrap slog.Logger in WriteSyncer
	syncer := &SlogSyncer{logger: slogger}
	// Create core
	core := zapcore.NewCore(encoder, syncer, level)
	// Build zap.Logger
	return zap.New(core)
}
