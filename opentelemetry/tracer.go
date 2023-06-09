package opentelemetry

import (
	"context"
	"database/sql/driver"

	"github.com/luna-duclos/instrumentedsql"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	instrumentationName = "github.com/luna-duclos/instrumentedsql/opentelemetry"
)

var (
	otelAttributes = map[string]attribute.Key{
		"query": semconv.DBStatementKey,
		"args":  attribute.Key("db.statement.args"),
	}
)

type tracer struct {
	traceOrphans bool
	t            trace.Tracer
}

type span struct {
	tracer
	ctx    context.Context
	parent trace.Span
}

// NewTracer returns a tracer that will fetch spans using OpenTELemetry's SpanFromContext function
// if traceOrphans is set to true, then spans with no parent will be traced anyway, if false, they will not be.
func NewTracer(traceOrphans bool, options ...trace.TracerOption) instrumentedsql.Tracer {
	return tracer{
		traceOrphans: traceOrphans,
		t:            otel.GetTracerProvider().Tracer(instrumentationName, options...),
	}
}

// GetSpan returns a span
func (t tracer) GetSpan(ctx context.Context) instrumentedsql.Span {
	// To prevent trace.ContextWithSpan from panicking.
	// It doesn't panic with otel/sdk/trace, but it does with otel/api/trace.
	if ctx == nil {
		ctx = context.Background()
	}
	parentSpan := trace.SpanFromContext(ctx)
	if !parentSpan.SpanContext().IsValid() {
		return span{parent: nil, ctx: ctx, tracer: t}
	}
	return span{parent: parentSpan, ctx: ctx, tracer: t}
}

func (s span) NewChild(name string) instrumentedsql.Span {
	if s.parent == nil && !s.traceOrphans {
		return s
	}
	ctx, newSpan := s.t.Start(s.ctx, name,
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	return span{parent: newSpan, ctx: ctx, tracer: s.tracer}
}

func (s span) SetLabel(k, v string) {
	if s.parent == nil {
		return
	}
	kv := attribute.String(k, v)
	// Translate given labels keys to opentelemetry semconv attributes as much as possible
	if attr, found := otelAttributes[k]; found {
		kv.Key = attr
	}
	s.parent.SetAttributes(kv)
}

func (s span) SetError(err error) {
	if err == nil || err == driver.ErrSkip {
		return
	}

	if s.parent == nil {
		return
	}

	s.parent.SetStatus(codes.Error, err.Error())
	s.parent.RecordError(err)
}

func (s span) Finish() {
	if s.parent == nil {
		return
	}
	s.parent.End()
}
