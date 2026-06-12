package natsx

import (
	"context"
	"time"
)

type HealthStatusValue string

const (
	HealthHealthy   HealthStatusValue = "healthy"
	HealthDegraded  HealthStatusValue = "degraded"
	HealthUnhealthy HealthStatusValue = "unhealthy"
)

type HealthStatus struct {
	Name      string            `json:"name"`
	Status    HealthStatusValue `json:"status"`
	Message   string            `json:"message,omitempty"`
	CheckedAt time.Time         `json:"checked_at"`
	LatencyMs int64             `json:"latency_ms"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

func (c *Client) HealthCheck(ctx context.Context) HealthStatus {
	start := time.Now()
	status := HealthStatus{Name: "natsx", Status: HealthUnhealthy, CheckedAt: time.Now()}
	if c != nil {
		status.Name = c.cfg.withDefaults().Name
	}
	finish := func() HealthStatus {
		status.LatencyMs = time.Since(start).Milliseconds()
		if c != nil && c.metrics != nil {
			c.metrics.SetGauge(MetricClientHealthStatus, healthGaugeValue(status.Status), map[string]string{"name": status.Name, "status": string(status.Status)})
			c.metrics.ObserveHistogram(MetricClientHealthLatencyMS, float64(status.LatencyMs), map[string]string{"name": status.Name, "status": string(status.Status)})
		}
		return status
	}
	if ctx == nil {
		status.Message = "context is required"
		return finish()
	}
	if err := ctx.Err(); err != nil {
		status.Message = err.Error()
		return finish()
	}
	if c == nil || c.conn == nil {
		status.Message = "client is not connected"
		return finish()
	}
	if c.conn.IsClosed() {
		status.Message = "connection is closed"
		return finish()
	}
	if !c.conn.IsConnected() {
		status.Status = HealthDegraded
		status.Message = "connection is reconnecting or disconnected"
		return finish()
	}
	status.Status = HealthHealthy
	status.Message = "ok"
	status.Metadata = map[string]string{"server_id": c.conn.ConnectedServerId(), "server_url": c.conn.ConnectedUrl()}
	return finish()
}
func healthGaugeValue(status HealthStatusValue) float64 {
	if status == HealthHealthy {
		return 1
	}
	if status == HealthDegraded {
		return 0.5
	}
	return 0
}
