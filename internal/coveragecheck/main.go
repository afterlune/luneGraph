// Command coveragecheck enforces statement coverage for the module's production
// packages. Run it from the repository root with a combined -coverpkg profile.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("coveragecheck", flag.ContinueOnError)
	flags.SetOutput(out)
	filename := flags.String("profile", "", "combined Go coverage profile")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *filename == "" || flags.NArg() != 0 {
		return fmt.Errorf("usage: coveragecheck -profile <coverage.out> (from repository root)")
	}
	file, err := os.Open(*filename)
	if err != nil {
		return fmt.Errorf("open coverage profile: %w", err)
	}
	defer file.Close()
	blocks, err := readProfile(file)
	if err != nil {
		return err
	}
	packages, err := listProduction()
	if err != nil {
		return err
	}
	return checkCoverage(blocks, packages, out)
}
