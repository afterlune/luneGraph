package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"sort"
)

type packageInfo struct {
	ImportPath string
	GoFiles    []string
	CgoFiles   []string
	Module     *struct {
		Path string
		Main bool
	}
}

func listProduction() ([]string, error) {
	cmd := exec.Command("go", "list", "-json", "./...")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	data, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("go list production packages: %w: %s", err, stderr.String())
	}
	return productionPackages(bytes.NewReader(data))
}

func productionPackages(input io.Reader) ([]string, error) {
	var packages []string
	decoder := json.NewDecoder(input)
	for {
		var p packageInfo
		if err := decoder.Decode(&p); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("decode go list: %w", err)
		}
		if p.Module == nil || !p.Module.Main || len(p.GoFiles)+len(p.CgoFiles) == 0 {
			continue
		}
		// These two packages provide test and verification infrastructure;
		// all other source-bearing packages, including examples, participate.
		if p.ImportPath == p.Module.Path+"/internal/storetest" || p.ImportPath == p.Module.Path+"/internal/coveragecheck" {
			continue
		}
		if p.ImportPath == "" || p.Module.Path == "" {
			return nil, fmt.Errorf("go list contains an invalid production package")
		}
		packages = append(packages, p.ImportPath)
	}
	if len(packages) == 0 {
		return nil, fmt.Errorf("go list found no production packages")
	}
	sort.Strings(packages)
	return packages, nil
}
