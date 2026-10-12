package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	graph "github.com/afterlune/luneGraph"
)

const (
	modelLead   = "local/lead-model"
	modelSearch = "local/search-model"
	modelPlan   = "local/planning-model"
	maxTasks    = 16

	roleLead    = "lead"
	roleSearch  = "search"
	rolePlanner = "planner"
)

// TaskAssignment is the provider-neutral task shape returned by a Lead or a
// Planner adapter. Target selects a graph node; ModelID selects its adapter.
type TaskAssignment struct {
	Target      string
	ID          string
	ModelID     string
	Instruction string
}

// ModelRequest contains only serializable application data. Adapter instances
// are injected into the runner and are never stored in checkpoints.
type ModelRequest struct {
	Role           string
	Goal           string
	TaskID         string
	Instruction    string
	Depth          int
	MaxDepth       int
	RequestedTasks int
	Results        []string
}

// ModelResponse is normalized by each provider adapter. Assignments are used
// by the Lead and by Planners that delegate sub-tasks; Text is a leaf result.
type ModelResponse struct {
	Text        string
	Assignments []TaskAssignment
}

// ModelAdapter performs one synchronous model request. The same adapter may be
// called concurrently and must honor context cancellation. CallInfo carries
// the stable RunID and CallID needed to reconcile at-least-once replay.
type ModelAdapter interface {
	Generate(context.Context, graph.CallInfo, ModelRequest) (ModelResponse, error)
}

// ModelRegistry maps the model IDs stored in tasks to runtime adapter values.
type ModelRegistry map[string]ModelAdapter

// teamState stores the serializable task data needed to recover model work.
type teamState struct {
	Goal           string
	RequestedTasks int
	TaskID         string
	Instruction    string
	ModelID        string
	Depth          int
	MaxDepth       int
	Results        []string
	Failures       []string
	Approved       bool
}

func cloneTeamState(s teamState) (teamState, error) {
	s.Results = slices.Clone(s.Results)
	s.Failures = slices.Clone(s.Failures)
	return s, nil
}

func newTeamRunner(models ModelRegistry) (*graph.Runner[teamState], error) {
	registry, err := copyModelRegistry(models)
	if err != nil {
		return nil, err
	}
	g := graph.New[teamState](roleLead)
	if err := addTeamDefinition(g, registry); err != nil {
		return nil, err
	}
	return g.Compile(graph.Config[teamState]{MachineID: "team-model-adapters-v2", Clone: cloneTeamState})
}

func copyModelRegistry(models ModelRegistry) (ModelRegistry, error) {
	if len(models) == 0 {
		return nil, errors.New("model registry must not be empty")
	}
	registry := make(ModelRegistry, len(models))
	for id, adapter := range models {
		if strings.TrimSpace(id) == "" || adapter == nil {
			return nil, errors.New("model registry contains an empty ID or nil adapter")
		}
		registry[id] = adapter
	}
	return registry, nil
}

func addTeamDefinition(g *graph.Graph[teamState], models ModelRegistry) error {
	if err := g.AddNode(graph.NodeSpec[teamState]{Name: roleLead, Run: func(ctx context.Context, call graph.CallInfo, s teamState) (graph.Transition[teamState], error) {
		if s.RequestedTasks < 0 || s.RequestedTasks > maxTasks {
			return graph.Transition[teamState]{}, fmt.Errorf("requested task count must be between 0 and %d", maxTasks)
		}
		if s.MaxDepth < 0 {
			return graph.Transition[teamState]{}, errors.New("maximum task depth must not be negative")
		}
		response, err := generate(ctx, call, models, s.ModelID, modelRequest(roleLead, s))
		if err != nil {
			return graph.Transition[teamState]{}, err
		}
		if err := validateAssignments(response.Assignments, models); err != nil {
			return graph.Transition[teamState]{}, fmt.Errorf("Lead returned invalid assignments: %w", err)
		}
		return graph.FanOut(s, assignmentBranches(s, response.Assignments, 0)...), nil
	}}); err != nil {
		return err
	}
	if err := g.AddNode(graph.NodeSpec[teamState]{Name: roleSearch, Run: func(ctx context.Context, call graph.CallInfo, s teamState) (graph.Transition[teamState], error) {
		response, err := generate(ctx, call, models, s.ModelID, modelRequest(roleSearch, s))
		if err != nil {
			return graph.Transition[teamState]{}, err
		}
		if len(response.Assignments) != 0 {
			return graph.Transition[teamState]{}, errors.New("Search adapter returned sub-task assignments")
		}
		s.Results = []string{response.Text}
		return graph.CompleteBranch(s), nil
	}}); err != nil {
		return err
	}
	if err := g.AddNode(graph.NodeSpec[teamState]{Name: rolePlanner, Run: func(ctx context.Context, call graph.CallInfo, s teamState) (graph.Transition[teamState], error) {
		response, err := generate(ctx, call, models, s.ModelID, modelRequest(rolePlanner, s))
		if err != nil {
			return graph.Transition[teamState]{}, err
		}
		if len(response.Assignments) != 0 {
			if s.Depth >= s.MaxDepth {
				return graph.Transition[teamState]{}, fmt.Errorf("Planner exceeded maximum depth %d", s.MaxDepth)
			}
			if err := validateAssignments(response.Assignments, models); err != nil {
				return graph.Transition[teamState]{}, fmt.Errorf("Planner returned invalid assignments: %w", err)
			}
			return graph.FanOut(s, assignmentBranches(s, response.Assignments, s.Depth+1)...), nil
		}
		s.Results = []string{response.Text}
		return graph.CompleteBranch(s), nil
	}}); err != nil {
		return err
	}
	if err := g.AddJoin(graph.JoinSpec[teamState]{Name: "worker-join", From: rolePlanner, OnError: graph.FailExecution, Merge: mergeTeamResults}); err != nil {
		return err
	}
	if err := g.AddNode(graph.NodeSpec[teamState]{Name: "worker-return", Run: func(_ context.Context, _ graph.CallInfo, s teamState) (graph.Transition[teamState], error) {
		return graph.CompleteBranch(s), nil
	}}); err != nil {
		return err
	}
	if err := g.AddJoin(graph.JoinSpec[teamState]{Name: "lead-join", From: roleLead, OnError: graph.FailExecution, Merge: mergeTeamResults}); err != nil {
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
		{roleLead, roleSearch}, {roleLead, rolePlanner}, {"lead-join", "approval"},
		{rolePlanner, roleSearch}, {rolePlanner, rolePlanner}, {"worker-join", "worker-return"},
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

func generate(ctx context.Context, call graph.CallInfo, models ModelRegistry, modelID string, request ModelRequest) (ModelResponse, error) {
	adapter, ok := models[modelID]
	if !ok {
		return ModelResponse{}, fmt.Errorf("unknown model %q", modelID)
	}
	response, err := adapter.Generate(ctx, call, request)
	if err != nil {
		return ModelResponse{}, fmt.Errorf("model %q: %w", modelID, err)
	}
	return response, nil
}

func modelRequest(role string, s teamState) ModelRequest {
	return ModelRequest{
		Role:           role,
		Goal:           s.Goal,
		TaskID:         s.TaskID,
		Instruction:    s.Instruction,
		Depth:          s.Depth,
		MaxDepth:       s.MaxDepth,
		RequestedTasks: s.RequestedTasks,
		Results:        slices.Clone(s.Results),
	}
}

func validateAssignments(assignments []TaskAssignment, models ModelRegistry) error {
	if len(assignments) > maxTasks {
		return fmt.Errorf("assignment count exceeds %d", maxTasks)
	}
	seen := make(map[string]struct{}, len(assignments))
	for index, assignment := range assignments {
		if strings.TrimSpace(assignment.ID) == "" {
			return fmt.Errorf("assignment %d has an empty task ID", index)
		}
		if _, exists := seen[assignment.ID]; exists {
			return fmt.Errorf("duplicate task ID %q", assignment.ID)
		}
		seen[assignment.ID] = struct{}{}
		if assignment.Target != roleSearch && assignment.Target != rolePlanner {
			return fmt.Errorf("assignment %q has unknown target %q", assignment.ID, assignment.Target)
		}
		if _, exists := models[assignment.ModelID]; !exists {
			return fmt.Errorf("assignment %q selects unknown model %q", assignment.ID, assignment.ModelID)
		}
	}
	return nil
}

func assignmentBranches(parent teamState, assignments []TaskAssignment, depth int) []graph.Branch[teamState] {
	branches := make([]graph.Branch[teamState], len(assignments))
	for index, assignment := range assignments {
		child := teamState{
			Goal:        parent.Goal,
			TaskID:      assignment.ID,
			Instruction: assignment.Instruction,
			ModelID:     assignment.ModelID,
			Depth:       depth,
			MaxDepth:    parent.MaxDepth,
		}
		branches[index] = graph.Branch[teamState]{Target: assignment.Target, State: child}
	}
	return branches
}

func mergeTeamResults(_ context.Context, _ graph.CallInfo, parent teamState, branches []graph.BranchResult[teamState]) (teamState, error) {
	parent.Results = nil
	parent.Failures = nil
	for _, branch := range branches {
		if branch.OK() {
			parent.Results = append(parent.Results, branch.State.Results...)
			parent.Failures = append(parent.Failures, branch.State.Failures...)
		} else if branch.Failed() {
			parent.Failures = append(parent.Failures, branch.Target+": "+branch.Error)
		}
	}
	return parent, nil
}

func offlineModelRegistry() ModelRegistry {
	return ModelRegistry{
		modelLead: modelAdapterFunc(func(_ context.Context, _ graph.CallInfo, request ModelRequest) (ModelResponse, error) {
			assignments := make([]TaskAssignment, 0, request.RequestedTasks)
			for i := range request.RequestedTasks {
				target, modelID := roleSearch, modelSearch
				if i%2 == 1 {
					target, modelID = rolePlanner, modelPlan
				}
				assignments = append(assignments, TaskAssignment{
					Target:      target,
					ID:          "task-" + strconv.Itoa(i),
					ModelID:     modelID,
					Instruction: "Complete sub-task " + strconv.Itoa(i),
				})
			}
			return ModelResponse{Assignments: assignments}, nil
		}),
		modelSearch: modelAdapterFunc(func(_ context.Context, _ graph.CallInfo, request ModelRequest) (ModelResponse, error) {
			return ModelResponse{Text: request.TaskID + " via " + modelSearch}, nil
		}),
		modelPlan: modelAdapterFunc(func(_ context.Context, _ graph.CallInfo, request ModelRequest) (ModelResponse, error) {
			if request.Depth < request.MaxDepth {
				return ModelResponse{Assignments: []TaskAssignment{
					{Target: roleSearch, ID: request.TaskID + "/research", ModelID: modelSearch, Instruction: "Research " + request.TaskID},
					{Target: rolePlanner, ID: request.TaskID + "/plan", ModelID: modelPlan, Instruction: "Plan from the research"},
				}}, nil
			}
			return ModelResponse{Text: request.TaskID + " via " + modelPlan}, nil
		}),
	}
}

type modelAdapterFunc func(context.Context, graph.CallInfo, ModelRequest) (ModelResponse, error)

func (f modelAdapterFunc) Generate(ctx context.Context, call graph.CallInfo, request ModelRequest) (ModelResponse, error) {
	return f(ctx, call, request)
}
