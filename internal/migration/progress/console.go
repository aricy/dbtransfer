package progress

import (
	"fmt"
	"strings"
	"time"

	"dbtransfer/internal/i18n"

	"github.com/sirupsen/logrus"
)

// ConsoleReporter reports migration progress to the console
type ConsoleReporter struct {
	interval       time.Duration
	lastUpdateTime time.Time
	mu             bool // simple mutex replacement
}

// NewConsoleReporter creates a new ConsoleReporter with the specified interval
func NewConsoleReporter(intervalSeconds int) *ConsoleReporter {
	return &ConsoleReporter{
		interval: time.Duration(intervalSeconds) * time.Second,
	}
}

// Report sends the current progress state to the console
func (r *ConsoleReporter) Report(progress MigrationProgress) {
	// Build progress bar
	barWidth := 30
	filled := int(float64(barWidth) * progress.Percent / 100)
	bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)

	// Format estimates
	estimatedLabel := ""
	if progress.IsEstimated {
		estimatedLabel = i18n.Tr(" (预估)", " (estimated)")
	}

	// Format remaining time
	remainingStr := FormatRemainingTime(progress.RemainingTime)
	if remainingStr == i18n.Tr("0秒", "0s") {
		remainingStr = i18n.Tr("计算中...", "Calculating...")
	}

	// Format speed
	speedStr := FormatSpeed(progress.Speed)

	// Build status line
	status := progress.Status
	if status == "" {
		status = i18n.Tr("迁移中", "Migrating")
	}

	logrus.Infof(
		i18n.Tr("[%s] %s %d/%d%s %s 速率: %s 剩余: %s",
			"[%s] %s %d/%d%s %s Rate: %s Remaining: %s"),
		bar,
		status,
		progress.MigratedCount,
		progress.TotalCount,
		estimatedLabel,
		fmt.Sprintf("%.2f%%", progress.Percent),
		speedStr,
		remainingStr,
	)
}

// SetTotalRows sets the total number of rows (for databases that get it later)
func (r *ConsoleReporter) SetTotalRows(rows int64) {
	// No-op for console reporter
}

// SetSpeed sets the current migration speed
func (r *ConsoleReporter) SetSpeed(speed float64) {
	// No-op for console reporter, speed is passed in Report()
}

// SetRemainingTime sets the estimated remaining time
func (r *ConsoleReporter) SetRemainingTime(time time.Duration) {
	// No-op for console reporter, time is passed in Report()
}

// SetStatus sets the current status message
func (r *ConsoleReporter) SetStatus(status string) {
	// No-op for console reporter, status is passed in Report()
}

// ShouldReport determines if a report should be sent based on interval
func (r *ConsoleReporter) ShouldReport() bool {
	now := time.Now()
	if now.Sub(r.lastUpdateTime) >= r.interval {
		r.lastUpdateTime = now
		return true
	}
	return false
}
