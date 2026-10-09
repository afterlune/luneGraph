package main

import (
	"fmt"
	"io"
	"math"
	"path"
)

const minimumCoverage = 85

type totals struct{ covered, statements uint64 }

func checkCoverage(blocks map[blockKey]blockCoverage, packages []string, out io.Writer) error {
	byPackage := make(map[string]totals, len(packages))
	for _, name := range packages {
		byPackage[name] = totals{}
	}
	for key, block := range blocks {
		name := path.Dir(key.file)
		sum, included := byPackage[name]
		if !included {
			continue
		}
		if block.statements > math.MaxUint64/100-sum.statements {
			return fmt.Errorf("statement total overflows for %s", name)
		}
		sum.statements += block.statements
		if block.covered {
			sum.covered += block.statements
		}
		byPackage[name] = sum
	}
	failed := false
	for _, name := range packages {
		sum := byPackage[name]
		if sum.statements == 0 {
			if _, err := fmt.Fprintf(out, "%s: FAIL missing effective statement coverage\n", name); err != nil {
				return err
			}
			failed = true
			continue
		}
		status := "PASS"
		// Compare integers before formatting; 84.999% must never round into a pass.
		if sum.covered*100 < minimumCoverage*sum.statements {
			status, failed = "FAIL", true
		}
		if _, err := fmt.Fprintf(out, "%s: %s %.3f%% (%d/%d statements; minimum %d%%)\n", name, status, 100*float64(sum.covered)/float64(sum.statements), sum.covered, sum.statements, minimumCoverage); err != nil {
			return err
		}
	}
	if failed {
		return fmt.Errorf("production package coverage gate failed")
	}
	return nil
}
