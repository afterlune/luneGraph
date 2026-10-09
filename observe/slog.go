// Package observe provides adapters for graph execution observers.
package observe

import (
	"context"
	"log/slog"

	graph "github.com/afterlune/luneGraph"
)

// NewSlog reports public calls at Info and callbacks and Store calls at Debug.
// Events with errors use Error. A nil logger selects slog.Default(). The logger
// and its handler must support concurrent use. No application state is logged;
// error messages are supplied by callbacks and stores and may contain user data.
func NewSlog(logger *slog.Logger) graph.Observer {
	if logger == nil {
		logger = slog.Default()
	}
	return &slogObserver{logger: logger}
}

type slogObserver struct{ logger *slog.Logger }

func (o *slogObserver) Observe(ctx context.Context, event graph.Event) {
	level := slog.LevelDebug
	switch event.Operation {
	case graph.OperationStart, graph.OperationResume, graph.OperationRecover:
		level = slog.LevelInfo
	}
	if event.Err != nil {
		level = slog.LevelError
	}
	if !o.logger.Enabled(ctx, level) {
		return
	}
	attrs := []slog.Attr{
		slog.String("operation", string(event.Operation)),
		slog.String("phase", string(event.Phase)),
		slog.Uint64("operation_id", event.OperationID),
		slog.String("run_id", event.RunID),
		slog.String("machine_id", event.MachineID),
		slog.Uint64("revision", event.Revision),
		slog.Time("event_time", event.Time),
	}
	if event.InvocationID != "" {
		attrs = append(attrs, slog.String("invocation_id", event.InvocationID))
	}
	if event.CallID != "" {
		attrs = append(attrs, slog.String("call_id", event.CallID))
	}
	if event.Node != "" {
		attrs = append(attrs, slog.String("node", event.Node))
	}
	if event.Continuation != "" {
		attrs = append(attrs, slog.String("continuation", event.Continuation))
	}
	if event.Phase == graph.PhaseFinished {
		attrs = append(attrs, slog.Duration("duration", event.Duration))
	}
	if event.Outcome != "" {
		attrs = append(attrs, slog.String("outcome", string(event.Outcome)))
	}
	if event.Status != "" {
		attrs = append(attrs, slog.String("status", string(event.Status)))
	}
	if event.Action != 0 {
		attrs = append(attrs, slog.Int("action", int(event.Action)))
	}
	if event.Err != nil {
		attrs = append(attrs, slog.String("error", event.Err.Error()))
	}
	o.logger.LogAttrs(ctx, level, "graph execution", attrs...)
}
