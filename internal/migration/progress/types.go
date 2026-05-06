package progress

import (
	"fmt"
	"time"

	"dbtransfer/internal/i18n"
)

// MigrationProgress represents the current state of a migration task
type MigrationProgress struct {
	TotalCount    int64
	MigratedCount int64
	Speed         float64 // records per second
	RemainingTime time.Duration
	Percent       float64
	Status        string
	IsEstimated   bool // true if TotalCount is an estimate (not exact)
}

// FormatRemainingTime formats the remaining time in human-readable format
func FormatRemainingTime(d time.Duration) string {
	if d <= 0 {
		return i18n.Tr("0秒", "0s")
	}

	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	if hours > 0 {
		return fmt.Sprintf(i18n.Tr("%d小时%d分钟%d秒", "%dh %dm %ds"), hours, minutes, seconds)
	}
	if minutes > 0 {
		return fmt.Sprintf(i18n.Tr("%d分钟%d秒", "%dm %ds"), minutes, seconds)
	}
	return fmt.Sprintf(i18n.Tr("%d秒", "%ds"), seconds)
}

// FormatSpeed formats the speed with appropriate unit
func FormatSpeed(speed float64) string {
	if speed <= 0 {
		return i18n.Tr("0条/秒", "0 rows/sec")
	}
	return fmt.Sprintf(i18n.Tr("%.1f条/秒", "%.1f rows/sec"), speed)
}
