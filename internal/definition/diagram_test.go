package definition

import (
	"context"
	"github.com/afterlune/luneGraph/internal/model"
	"strings"
	"testing"
)

func TestDiagramNamesMountsAndRecursiveReferences(t *testing.T) {
	run := model.Node[int](func(context.Context, model.CallInfo, int) (model.Transition[int], error) {
		return model.EndBranch[int](), nil
	})
	root := New[int]("a/b")
	for _, name := range []string{"a/b", "a-b", "a_b", "quote\"<&>"} {
		if err := root.AddNode(model.NodeSpec[int]{Name: name, Run: run}); err != nil {
			t.Fatal(err)
		}
	}
	child := New[int]("a_b")
	if err := child.AddNode(model.NodeSpec[int]{Name: "a_b", Run: run}); err != nil {
		t.Fatal(err)
	}
	if err := root.AddSubgraph("left", child); err != nil {
		t.Fatal(err)
	}
	if err := root.AddSubgraph("right", child); err != nil {
		t.Fatal(err)
	}
	out, err := root.ExportMermaid()
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]bool{}
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "v") {
			at := strings.IndexAny(line, "[({")
			if at > 0 {
				id := line[:at]
				if ids[id] {
					t.Fatal("duplicate node", id, out)
				}
				ids[id] = true
			}
		}
	}
	if len(ids) != 6 || !strings.Contains(out, "&quot;&lt;&amp;&gt;") {
		t.Fatal(out)
	}
	if err := child.AddSubgraph("recursive", root); err != nil {
		t.Fatal(err)
	}
	if _, err := root.ExportMermaid(); err == nil {
		t.Fatal("recursive export accepted")
	}
}
