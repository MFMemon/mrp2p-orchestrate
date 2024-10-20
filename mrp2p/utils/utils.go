package utils

import (
	"go.uber.org/zap"
)

func Logger() *zap.SugaredLogger {
	zapLogger, _ := zap.NewProduction()
	// defer zapLogger.Sync()
	logger := zapLogger.Sugar()
	return logger
}
