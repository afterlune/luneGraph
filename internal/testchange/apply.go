// Package testchange materializes deltas for test oracles and fault stores.
package testchange

import (
	"cmp"
	"github.com/afterlune/luneGraph/internal/model"
	"slices"
)

func Apply[S any](base model.Checkpoint[S], change model.Change[S]) model.Checkpoint[S] {
	cp := change.Checkpoint()
	cp.Invocations = nil
	cp.Groups = nil
	if cp.Completed {
		return cp
	}
	inv := make(map[string]model.Invocation[S])
	groups := make(map[string]model.ActivationGroup)
	for _, v := range base.Invocations {
		inv[v.ID] = v
	}
	for _, g := range base.Groups {
		groups[g.ID] = g
	}
	for _, id := range change.DeleteInvocations {
		delete(inv, id)
	}
	for _, v := range change.UpsertInvocations {
		inv[v.ID] = v
	}
	for _, id := range change.DeleteGroups {
		delete(groups, id)
	}
	for _, g := range change.UpsertGroups {
		groups[g.ID] = g
	}
	for _, v := range inv {
		cp.Invocations = append(cp.Invocations, v)
	}
	for _, g := range groups {
		cp.Groups = append(cp.Groups, g)
	}
	slices.SortFunc(cp.Invocations, func(a, b model.Invocation[S]) int { return cmp.Compare(a.ID, b.ID) })
	slices.SortFunc(cp.Groups, func(a, b model.ActivationGroup) int { return cmp.Compare(a.ID, b.ID) })
	return cp
}
