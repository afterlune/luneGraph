// Package diagram renders graph names without treating them as Mermaid syntax.
package diagram

import (
	"fmt"
	"strings"
)

// Names assigns stable, unique IDs in the caller's deterministic traversal order.
type Names struct{ ids map[string]string }

func (n *Names) ID(path string) string {
	if n.ids == nil {
		n.ids = make(map[string]string)
	}
	if id, ok := n.ids[path]; ok {
		return id
	}
	id := fmt.Sprintf("v%d", len(n.ids))
	n.ids[path] = id
	return id
}

func Label(value string) string {
	r := strings.NewReplacer("&", "&amp;", "\"", "&quot;", "<", "&lt;", ">", "&gt;", "\r", " ", "\n", " ")
	return r.Replace(value)
}
