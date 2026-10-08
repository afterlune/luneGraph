package definition

import (
	"context"
	"strings"
	"testing"

	"github.com/afterlune/luneGraph/internal/model"
)

func TestRegisterJSONContinuationDecodesTypedPayload(t *testing.T) {
	type payload struct {
		Delta int `json:"delta"`
	}
	g := New[int]("entry")
	if err := RegisterJSONContinuation[int, payload](g, "resume-json", func(_ context.Context, call model.CallInfo, state int, input payload) (int, error) {
		if call.CallID != "c2" {
			t.Fatalf("continuation call ID = %q", call.CallID)
		}
		return state + input.Delta, nil
	}); err != nil {
		t.Fatal(err)
	}
	continuation := g.continuations["resume-json"]
	value, err := continuation.Decode([]byte(`{"delta":4}`))
	if err != nil {
		t.Fatal(err)
	}
	got, err := continuation.Apply(context.Background(), model.CallInfo{CallID: "c2"}, 3, value)
	if err != nil || got != 7 {
		t.Fatalf("Apply = %d, %v; want 7", got, err)
	}
	if _, err := continuation.Decode([]byte("{")); err == nil {
		t.Fatal("malformed JSON payload decoded successfully")
	}
}

func TestExportMermaidRendersSortedNestedGraph(t *testing.T) {
	var nilGraph *Graph[int]
	if nilGraph.ExportMermaid() != "" {
		t.Fatal("nil graph exported a diagram")
	}

	validNode := model.Node[int](func(context.Context, model.CallInfo, int) (model.Transition[int], error) {
		return model.EndExecution(0), nil
	})
	root := New[int]("root-node")
	addDefinitionNode(t, root, "root-node", validNode)
	addDefinitionNode(t, root, "finish", validNode)
	addDefinitionJoin(t, root, model.JoinSpec[int]{Name: "join-node", From: "root-node", Merge: func(context.Context, model.CallInfo, []int) (int, error) { return 0, nil }})
	child := New[int]("child-entry")
	addDefinitionNode(t, child, "child-entry", validNode)
	addDefinitionNode(t, child, "child-finish", validNode)
	addDefinitionEdge(t, child, "child-entry", "child-finish")
	if err := root.AddSubgraph("mounted-part", child); err != nil {
		t.Fatal(err)
	}
	addDefinitionEdge(t, root, "root-node", "finish")
	addDefinitionEdge(t, root, "root-node", "mounted-part")

	diagram := root.ExportMermaid()
	for _, expected := range []string{
		`root_node(["root-node (entry)"])`,
		`join_node{{"join-node (join)"}}`,
		`subgraph mounted_part ["mounted-part"]`,
		`child_entry(["child-entry (entry)"])`,
		`child_entry --> child_finish`,
		`root_node --> finish`,
		`root_node --> mounted_part`,
	} {
		if !strings.Contains(diagram, expected) {
			t.Errorf("Mermaid output missing %q:\n%s", expected, diagram)
		}
	}
	if repeated := root.ExportMermaid(); repeated != diagram {
		t.Fatalf("Mermaid output is not deterministic:\n%s\n---\n%s", diagram, repeated)
	}
}
