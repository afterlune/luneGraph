package diagram

import "testing"

func TestNamesAndLabels(t *testing.T) {
	var n Names
	seen := map[string]bool{}
	for _, name := range []string{"a/b", "a-b", "a_b", "other/a_b"} {
		id := n.ID(name)
		if seen[id] || n.ID(name) != id {
			t.Fatalf("unstable or duplicate ID: %s", id)
		}
		seen[id] = true
	}
	if got := Label("a\"<&>\n\rb"); got != "a&quot;&lt;&amp;&gt;  b" {
		t.Fatal(got)
	}
}
