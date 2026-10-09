package main

import (
	"errors"
	"io"
	"strings"
	"testing"
)

func TestProfileMergesRepeatedSourceBlocks(t *testing.T) {
	for _, mode := range []string{"set", "count", "atomic"} {
		t.Run(mode, func(t *testing.T) {
			blocks, err := readProfile(strings.NewReader("mode: " + mode + "\nexample/p/a.go:1.1,2.1 85 0\nexample/p/a.go:1.1,2.1 85 9\nexample/p/a.go:1.1,2.1 85 0\nexample/p/a.go:3.1,4.1 15 0\n"))
			if err != nil || len(blocks) != 2 {
				t.Fatalf("blocks=%v %v", blocks, err)
			}
			var output strings.Builder
			if err := checkCoverage(blocks, []string{"example/p"}, &output); err != nil || !strings.Contains(output.String(), "85/100 statements") {
				t.Fatalf("union=%s %v", output.String(), err)
			}
		})
	}
}

func TestInvalidProfiles(t *testing.T) {
	for _, data := range []string{
		"", "mode: unsupported\n", "mode: set\nbad\n", "mode: set\na 1 1\n",
		"mode: set\na.go:1.1,2 1 1\n", "mode: set\na.go:0.1,2.1 1 1\n",
		"mode: set\na.go:x.1,2.1 1 1\n", "mode: set\na.go:2.1,1.1 1 1\n",
		"mode: set\na.go:1.2,1.1 1 1\n", "mode: set\na.go:1..1,2.1 1 1\n",
		"mode: set\na.go:1.1,2.1 -1 1\n", "mode: set\na.go:1.1,2.1 18446744073709551615 1\n",
		"mode: set\na.go:1.1,2.1 1 -1\n", "mode: set\na.go:1.1,2.1 1 0\na.go:1.1,2.1 2 1\n",
	} {
		t.Run(data, func(t *testing.T) {
			if _, err := readProfile(strings.NewReader(data)); err == nil {
				t.Fatal("accepted malformed profile")
			}
		})
	}
}

type brokenReader struct{}

func (brokenReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }

func TestProfileReadErrorsAndWindowsPath(t *testing.T) {
	for _, input := range []io.Reader{brokenReader{}, io.MultiReader(strings.NewReader("mode: set\n"), brokenReader{})} {
		if _, err := readProfile(input); err == nil {
			t.Fatal("ignored read error")
		}
	}
	blocks, err := readProfile(strings.NewReader("mode: set\nexample\\p\\a.go:1.1,2.1 1 1\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkCoverage(blocks, []string{"example/p"}, io.Discard); err != nil {
		t.Fatal(err)
	}
}
