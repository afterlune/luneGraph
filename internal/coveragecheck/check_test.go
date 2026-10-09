package main

import (
	"fmt"
	"io"
	"strings"
	"testing"
)

func TestGateUsesExactStatementRatio(t *testing.T) {
	for _, tc := range []struct {
		covered, total uint64
		pass           bool
	}{{85, 100, true}, {84999, 100000, false}, {85000, 100000, true}, {86, 100, true}, {0, 100, false}} {
		t.Run(fmt.Sprintf("%d/%d", tc.covered, tc.total), func(t *testing.T) {
			blocks := map[blockKey]blockCoverage{{file: "example/p/a.go", startLine: 1}: {statements: tc.covered, covered: true}, {file: "example/p/a.go", startLine: 2}: {statements: tc.total - tc.covered}}
			err := checkCoverage(blocks, []string{"example/p"}, io.Discard)
			if (err == nil) != tc.pass {
				t.Fatalf("gate=%v want pass=%v", err, tc.pass)
			}
		})
	}
}

type brokenWriter struct{}

func (brokenWriter) Write([]byte) (int, error) { return 0, fmt.Errorf("write failed") }

func TestGateRejectsMissingPackagesAndOverflow(t *testing.T) {
	var output strings.Builder
	blocks, err := readProfile(strings.NewReader("mode: set\nexample/a/a.go:1.1,2.1 9 1\nexample/a/a.go:3.1,4.1 1 0\nexample/b/b.go:1.1,2.1 1 0\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkCoverage(blocks, []string{"example/a", "example/b", "example/new"}, &output); err == nil {
		t.Fatal("aggregate coverage hid package failures")
	}
	if !strings.Contains(output.String(), "example/a: PASS 90.000%") || !strings.Contains(output.String(), "example/b: FAIL 0.000%") || !strings.Contains(output.String(), "example/new: FAIL missing") {
		t.Fatal(output.String())
	}
	if err := checkCoverage(blocks, []string{"example/a"}, brokenWriter{}); err == nil {
		t.Fatal("ignored writer error")
	}
	if err := checkCoverage(nil, []string{"example/a"}, brokenWriter{}); err == nil {
		t.Fatal("ignored missing-data writer error")
	}
	big := ^uint64(0) / 100
	if err := checkCoverage(map[blockKey]blockCoverage{{file: "example/a/a.go", startLine: 1}: {statements: big}, {file: "example/a/a.go", startLine: 2}: {statements: 1}}, []string{"example/a"}, io.Discard); err == nil {
		t.Fatal("ignored overflow")
	}
	if err := checkCoverage(map[blockKey]blockCoverage{{file: "excluded/a.go"}: {statements: 1}}, []string{"example/a"}, io.Discard); err == nil {
		t.Fatal("excluded package hid missing production data")
	}
}
