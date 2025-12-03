package sop

import (
	"log/slog"
	"os"
)

var logLevel = new(slog.LevelVar)

// ConfigureLogging sets up the global default logger with a TextHandler
// and configures the log level based on the SOP_LOG_LEVEL environment variable.
// It defaults to Info level if not specified.
//
// This function should be called by the application at startup if it wants
// to use the default SOP logging configuration.
func ConfigureLogging() {
	// Default to Info
	logLevel.Set(slog.LevelInfo)

	// Check environment variable for log level
	lvl := os.Getenv("SOP_LOG_LEVEL")
	switch lvl {
	case "DEBUG":
		logLevel.Set(slog.LevelDebug)
	case "WARN":
		logLevel.Set(slog.LevelWarn)
	case "ERROR":
		logLevel.Set(slog.LevelError)
	}

	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	})
	slog.SetDefault(slog.New(handler))
}

// SetLogLevel sets the logging level for the logger configured by ConfigureLogging.
func SetLogLevel(level slog.Level) {
	logLevel.Set(level)
}
