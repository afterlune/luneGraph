package model

// ValidateCheckpointHeader checks the fields shared by all Store writes.
func ValidateCheckpointHeader[S any](value Checkpoint[S]) error {
	if !ValidName(value.RunID) || !ValidName(value.MachineID) || value.FormatVersion != CheckpointFormatVersion || value.Revision == 0 {
		return ErrInvalidCheckpoint
	}
	return nil
}
