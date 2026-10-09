package model

import "errors"

// DeletionIDs validates the entire batch before any mutation and returns an
// owned, deduplicated list. It never retains the caller's slice.
func DeletionIDs(ids []string) ([]string, error) {
	seen := make(map[string]bool, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if !ValidName(id) {
			return nil, errors.New("run ID must be non-empty and have no surrounding whitespace")
		}
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out, nil
}
