package progress

import "time"

// ProgressReporter defines the interface for reporting migration progress
type ProgressReporter interface {
	// Report sends the current progress state
	Report(progress MigrationProgress)
	// SetTotalRows sets the total number of rows (for databases that get it later)
	SetTotalRows(rows int64)
	// SetSpeed sets the current migration speed
	SetSpeed(speed float64)
	// SetRemainingTime sets the estimated remaining time
	SetRemainingTime(time time.Duration)
	// SetStatus sets the current status message
	SetStatus(status string)
}

// NullReporter is a reporter that does nothing (for testing or when no reporting needed)
type NullReporter struct{}

func (n *NullReporter) Report(progress MigrationProgress)            {}
func (n *NullReporter) SetTotalRows(rows int64)                      {}
func (n *NullReporter) SetSpeed(speed float64)                      {}
func (n *NullReporter) SetRemainingTime(time time.Duration)         {}
func (n *NullReporter) SetStatus(status string)                      {}

// CallbackTrigger represents a callback to be triggered at specific progress milestones
type CallbackTrigger struct {
	Milestone int
	Callback  func(progress MigrationProgress)
	Triggered bool
}