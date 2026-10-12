package main

import (
	"context"
	"errors"
	"strconv"

	graph "github.com/afterlune/luneGraph"
)

const (
	modelLead   = "local/lead-model"
	modelSearch = "local/search-model"
	modelPlan   = "local/planning-model"
)

type task struct {
	Target string
	ID     string
	Model  string
	Depth  int
}

// teamState is the serializable execution state shared by Lead and Workers.
type teamState struct {
	Plan     []task
	TaskID   string
	Model    string
	Depth    int
	MaxDepth int
	Results  []string
	Approved bool
}

func cloneTeamState(s teamState) (teamState, error) {
	s.Plan = append([]task(nil), s.Plan...)
	s.Results = append([]string(nil), s.Results...)
	return s, nil
}

func newTeamRunner() (*graph.Runner[teamState], error) {
	g := graph.New[teamState]("lead")
	if err := addTeamDefinition(g); err != nil {
		return nil, err
	}

	return g.Compile(graph.Config[teamState]{MachineID: "offline-team-v1", Clone: cloneTeamState})
}

func addTeamDefinition(g *graph.Graph[teamState]) error {
	if err := g.AddNode(graph.NodeSpec[teamState]{Name: "lead", Run: func(_ context.Context, _ graph.CallInfo, s teamState) (graph.Transition[teamState], error) {
		s.Model = modelLead
		branches := make([]graph.Branch[teamState], 0, len(s.Plan))
		for _, assignment := range s.Plan {
			child := s
			child.TaskID, child.Model, child.Depth, child.Results = assignment.ID, assignment.Model, assignment.Depth, nil
			branches = append(branches, graph.Branch[teamState]{Target: assignment.Target, State: child})
		}
		return graph.FanOut(s, branches...), nil
	}}); err != nil {
		return err
	}
	if err := g.AddNode(graph.NodeSpec[teamState]{Name: "search", Run: func(_ context.Context, _ graph.CallInfo, s teamState) (graph.Transition[teamState], error) {
		if s.Model != modelSearch {
			return graph.Transition[teamState]{}, errors.New("search Worker received an incompatible model assignment")
		}
		s.Results = []string{s.TaskID + " via " + s.Model}
		return graph.CompleteBranch(s), nil
	}}); err != nil {
		return err
	}
	if err := g.AddNode(graph.NodeSpec[teamState]{Name: "planner", Run: func(_ context.Context, _ graph.CallInfo, s teamState) (graph.Transition[teamState], error) {
		if s.Model != modelPlan {
			return graph.Transition[teamState]{}, errors.New("planning Worker received an incompatible model assignment")
		}
		if s.Depth < s.MaxDepth {
			search, nestedPlan := s, s
			search.TaskID, search.Model, search.Depth, search.Results = s.TaskID+"/research", modelSearch, s.Depth+1, nil
			nestedPlan.TaskID, nestedPlan.Model, nestedPlan.Depth, nestedPlan.Results = s.TaskID+"/plan", modelPlan, s.Depth+1, nil
			return graph.FanOut(s,
				graph.Branch[teamState]{Target: "search", State: search},
				graph.Branch[teamState]{Target: "planner", State: nestedPlan}), nil
		}
		s.Results = []string{s.TaskID + " via " + s.Model}
		return graph.CompleteBranch(s), nil
	}}); err != nil {
		return err
	}
	if err := g.AddJoin(graph.JoinSpec[teamState]{Name: "worker-join", From: "planner", OnError: graph.FailExecution, Merge: mergeTeamResults}); err != nil {
		return err
	}
	if err := g.AddNode(graph.NodeSpec[teamState]{Name: "worker-return", Run: func(_ context.Context, _ graph.CallInfo, s teamState) (graph.Transition[teamState], error) {
		return graph.CompleteBranch(s), nil
	}}); err != nil {
		return err
	}
	if err := g.AddJoin(graph.JoinSpec[teamState]{Name: "lead-join", From: "lead", OnError: graph.FailExecution, Merge: mergeTeamResults}); err != nil {
		return err
	}
	if err := g.AddNode(graph.NodeSpec[teamState]{Name: "approval", Run: func(_ context.Context, _ graph.CallInfo, s teamState) (graph.Transition[teamState], error) {
		return graph.Wait(s, "approval", "finish"), nil
	}}); err != nil {
		return err
	}
	if err := g.AddNode(graph.NodeSpec[teamState]{Name: "finish", Run: func(_ context.Context, _ graph.CallInfo, s teamState) (graph.Transition[teamState], error) {
		return graph.EndExecution(s), nil
	}}); err != nil {
		return err
	}
	for _, edge := range [][2]string{
		{"lead", "search"}, {"lead", "planner"}, {"lead-join", "approval"},
		{"planner", "search"}, {"planner", "planner"}, {"worker-join", "worker-return"},
		{"approval", "finish"},
	} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			return err
		}
	}
	if err := graph.RegisterJSONContinuation(g, "approval", func(_ context.Context, _ graph.CallInfo, s teamState, approved bool) (teamState, error) {
		s.Approved = approved
		return s, nil
	}); err != nil {
		return err
	}
	return nil
}

func mergeTeamResults(_ context.Context, _ graph.CallInfo, parent teamState, branches []teamState) (teamState, error) {
	parent.Results = nil
	for _, branch := range branches {
		parent.Results = append(parent.Results, branch.Results...)
	}
	return parent, nil
}

func makePlan(count, maxDepth int) []task {
	plan := make([]task, 0, count)
	for i := range count {
		worker, model := "search", modelSearch
		if i%2 == 1 {
			worker, model = "planner", modelPlan
		}
		plan = append(plan, task{Target: worker, ID: "task-" + strconv.Itoa(i), Model: model})
	}
	return plan
}
