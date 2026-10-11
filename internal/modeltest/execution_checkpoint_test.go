package modeltest

func mustCheckpoint[T any](execution interface{ Checkpoint() (T, error) }) T {
	checkpoint, err := execution.Checkpoint()
	if err != nil {
		panic(err)
	}
	return checkpoint
}
