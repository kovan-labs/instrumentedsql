package opentelemetry

import (
	"context"
	"database/sql"
	"fmt"

	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/luna-duclos/instrumentedsql"
	"go.opentelemetry.io/otel"
)

// ExampleWrapDriver_opentelemetry demonstrates how to call wrapDriver and register a new driver.
// This example uses MySQL and OpenTelemetry to illustrate this
func ExampleWrapDriver_opentelemetry() {
	sql.Register("instrumented-mysql", instrumentedsql.WrapDriver(mysql.MySQLDriver{}, instrumentedsql.WithTracer(NewTracer(false))))
	db, err := sql.Open("instrumented-mysql", "connString")

	// Proceed to handle connection errors and use the database as usual
	_, _ = db, err
}

func TestSpanWithParent(t *testing.T) {
	ctx, parentSpan := otel.GetTracerProvider().Tracer("").Start(context.Background(), "parent_span")
	defer parentSpan.End()

	tr := NewTracer(true)
	span := tr.GetSpan(ctx)
	span.SetLabel("key", "value")

	child := span.NewChild("child")
	child.SetLabel("child_key", "child_value")
	child.SetError(fmt.Errorf("my error"))
	child.Finish()

	span.Finish()
}

func TestSpanWithoutParent(t *testing.T) {
	ctx := context.Background() // Background has no span
	tr := NewTracer(true)
	span := tr.GetSpan(ctx)
	span.SetLabel("key", "value")

	child := span.NewChild("child")
	child.SetLabel("child_key", "child_value")
	child.SetError(fmt.Errorf("my error"))
	child.Finish()

	span.Finish()
}
