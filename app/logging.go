package main

import (
	"context"
	"log/slog"
	"os"
)

const (
	Trace      = "trace"
	ContextMap = "ctxmap"
)

func SetCtxMapKey(ctx context.Context, key string, value string) context.Context {
	ctxmap, _ := ctx.Value(ContextMap).(map[string]string)

	if ctxmap == nil {
		ctxmap = make(map[string]string)
	}
	ctxmap[key] = value

	return context.WithValue(ctx, ContextMap, ctxmap)
}

func TracedContext(ctx context.Context, trace string) context.Context {
	return SetCtxMapKey(ctx, Trace, trace)
}

type LogRecordHandler struct {
	slog.Handler
	staticLogData
}

type staticLogData struct {
	serviceName     string
	awsRegion       string
	awsExecutionEnv string
}

func NewLogRecordHandler(serviceName string, h slog.Handler) slog.Handler {
	return &LogRecordHandler{
		Handler:         h,
		serviceName:     serviceName,
		awsRegion:       os.Getenv("AWS_REGION"),
		awsExecutionEnv: os.Getenv("AWS_EXECUTION_ENV"),
	}
}

func (h *LogRecordHandler) Handle(ctx context.Context, r slog.Record) error {
	ctxmap, ok := ctx.Value(ContextMap).(map[string]string)
	if ok {
		for key, value := range ctxmap {
			r.Add(key, value)
		}
	}

	// propagates common AWS environment data into the logs for easier debugging. if details are not provided, does not fail
	if h.staticLogData.serviceName != "" {
		r.Add("serviceName", h.staticLogData.serviceName)
	}
	if h.staticLogData.awsRegion != "" {
		r.Add("awsRegion", h.staticLogData.awsRegion)
	}
	if h.staticLogData.awsExecutionEnv != "" {
		r.Add("awsExecutionEnv", h.staticLogData.awsExecutionEnv)
	}

	return h.Handler.Handle(ctx, r)
}
