package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

type blockKey struct {
	file                string
	startLine, startCol uint64
	endLine, endCol     uint64
}

type blockCoverage struct {
	statements uint64
	covered    bool
}

func readProfile(input io.Reader) (map[blockKey]blockCoverage, error) {
	scanner := bufio.NewScanner(input)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return nil, fmt.Errorf("read coverage header: %w", err)
		}
		return nil, fmt.Errorf("coverage profile is empty")
	}
	switch scanner.Text() {
	case "mode: set", "mode: count", "mode: atomic":
	default:
		return nil, fmt.Errorf("invalid coverage mode %q", scanner.Text())
	}
	blocks := make(map[blockKey]blockCoverage)
	line := 1
	for scanner.Scan() {
		line++
		key, value, err := parseBlock(scanner.Text())
		if err != nil {
			return nil, fmt.Errorf("coverage line %d: %w", line, err)
		}
		if previous, exists := blocks[key]; exists {
			if previous.statements != value.statements {
				return nil, fmt.Errorf("coverage line %d: inconsistent statement count for %s", line, key.file)
			}
			value.covered = value.covered || previous.covered
		}
		blocks[key] = value
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read coverage profile: %w", err)
	}
	return blocks, nil
}

func parseBlock(line string) (blockKey, blockCoverage, error) {
	var key blockKey
	var value blockCoverage
	fields := strings.Fields(line)
	if len(fields) != 3 {
		return key, value, fmt.Errorf("expected location, statement count and execution count")
	}
	colon := strings.LastIndexByte(fields[0], ':')
	if colon <= 0 {
		return key, value, fmt.Errorf("invalid source location %q", fields[0])
	}
	key.file = strings.ReplaceAll(fields[0][:colon], "\\", "/")
	location := fields[0][colon+1:]
	coordinates := strings.FieldsFunc(location, func(c rune) bool { return c == '.' || c == ',' })
	if len(coordinates) != 4 {
		return key, value, fmt.Errorf("invalid source coordinates %q", location)
	}
	positions := []*uint64{&key.startLine, &key.startCol, &key.endLine, &key.endCol}
	for i, part := range coordinates {
		n, err := strconv.ParseUint(part, 10, 64)
		if err != nil || n == 0 {
			return key, value, fmt.Errorf("invalid source coordinate %q", part)
		}
		*positions[i] = n
	}
	if location != fmt.Sprintf("%d.%d,%d.%d", key.startLine, key.startCol, key.endLine, key.endCol) || key.endLine < key.startLine || (key.endLine == key.startLine && key.endCol < key.startCol) {
		return key, value, fmt.Errorf("invalid source range %q", location)
	}
	statements, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil || statements > math.MaxUint64/100 {
		return key, value, fmt.Errorf("invalid statement count %q", fields[1])
	}
	count, err := strconv.ParseUint(fields[2], 10, 64)
	if err != nil {
		return key, value, fmt.Errorf("invalid execution count %q", fields[2])
	}
	value.statements, value.covered = statements, count > 0
	return key, value, nil
}
