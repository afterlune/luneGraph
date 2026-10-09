package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestProductionDiscoveryIncludesNewPackagesAndExamples(t *testing.T) {
	var input strings.Builder
	for _, tc := range []struct {
		name                  string
		goFile, cgoFile, main bool
	}{
		{"example", true, false, true}, {"example/internal/new", true, false, true}, {"example/examples/app", true, false, true},
		{"example/internal/storetest", true, false, true}, {"example/internal/coveragecheck", true, false, true},
		{"example/internal/capacitytest", false, false, true}, {"example/cgo", false, true, true}, {"other/dependency", true, false, false},
	} {
		p := packageInfo{ImportPath: tc.name}
		p.Module = &struct {
			Path string
			Main bool
		}{"example", tc.main}
		if tc.goFile {
			p.GoFiles = []string{"a.go"}
		}
		if tc.cgoFile {
			p.CgoFiles = []string{"a.go"}
		}
		if err := json.NewEncoder(&input).Encode(p); err != nil {
			t.Fatal(err)
		}
	}
	// go list can also contain packages without module metadata.
	input.WriteString("{}\n")
	got, err := productionPackages(strings.NewReader(input.String()))
	if err != nil || fmt.Sprint(got) != "[example example/cgo example/examples/app example/internal/new]" {
		t.Fatalf("packages=%v %v", got, err)
	}
	for _, data := range []string{"", "{", "{\"GoFiles\":[\"a.go\"],\"Module\":{\"Path\":\"example\",\"Main\":true}}"} {
		if _, err := productionPackages(strings.NewReader(data)); err == nil {
			t.Fatalf("accepted invalid metadata: %s", data)
		}
	}
}
