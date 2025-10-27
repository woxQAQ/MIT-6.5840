package logger

import (
	"log"
	"log/slog"
	"os"
)

func init() {
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
		AddSource: true,
	})
	slog.SetDefault(slog.New(handler))
	log.SetOutput(slog.NewLogLogger(handler,slog.LevelInfo).Writer())
}
