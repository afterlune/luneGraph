package main

import (
	"errors"
	"fmt"
)

func teamSystemPrompt(role string, models []ModelOption) (string, error) {
	exampleModelID := "configured-worker-model"
	if len(models) != 0 {
		exampleModelID = models[0].ModelID
	}
	switch role {
	case roleLead:
		return "You are the Lead in a task team. Break the goal into the requested number of independent tasks, unless the requested count is zero. Return JSON with an assignments array. Each assignment must contain target (search or planner), a unique id, model_id, and instruction. Choose model_id exactly from the available_models in the task input; use its description to match model strengths to the task. Never invent a model ID. Example JSON: {\"assignments\":[{\"target\":\"search\",\"id\":\"research-1\",\"model_id\":\"" + exampleModelID + "\",\"instruction\":\"Find relevant evidence\"}]}", nil
	case rolePlanner:
		return "You are a Planner worker. Complete the task with text or delegate smaller tasks with assignments. Respect depth and max_depth; when depth is at the maximum, complete the task. If delegating, choose each model_id exactly from available_models in the task input and never invent a model ID. Return JSON with both text and assignments fields: set text to a non-empty answer and assignments to an empty array when completing; set text to an empty string and assignments to a non-empty array when delegating. Examples: {\"text\":\"A concise plan\",\"assignments\":[]} or {\"text\":\"\",\"assignments\":[{\"target\":\"search\",\"id\":\"research\",\"model_id\":\"" + exampleModelID + "\",\"instruction\":\"Find evidence\"}]}", nil
	case roleSearch:
		return "You are a Search worker. Complete the assigned research task and return JSON with a non-empty text field containing findings. Do not delegate tasks. Example JSON: {\"text\":\"Relevant findings\"}.", nil
	default:
		return "", fmt.Errorf("unknown model role %q", role)
	}
}

func teamResponseSchema(role string, models []ModelOption) (map[string]any, error) {
	textSchema := map[string]any{"type": "string"}
	properties := map[string]any{}
	required := []string{}
	assignmentSchema := func() (map[string]any, error) {
		if len(models) == 0 {
			return nil, errors.New("assignment response schema needs at least one worker model")
		}
		modelIDs := make([]string, len(models))
		for index, model := range models {
			modelIDs[index] = model.ModelID
		}
		item := map[string]any{
			"type": "object",
			"properties": map[string]any{
				"target":      map[string]any{"type": "string", "enum": []string{roleSearch, rolePlanner}},
				"id":          map[string]any{"type": "string"},
				"model_id":    map[string]any{"type": "string", "enum": modelIDs},
				"instruction": map[string]any{"type": "string"},
			},
			"required":             []string{"target", "id", "model_id", "instruction"},
			"additionalProperties": false,
		}
		return map[string]any{"type": "array", "items": item}, nil
	}
	switch role {
	case roleLead:
		assignments, err := assignmentSchema()
		if err != nil {
			return nil, err
		}
		properties["assignments"] = assignments
		required = []string{"assignments"}
	case rolePlanner:
		assignments, err := assignmentSchema()
		if err != nil {
			return nil, err
		}
		properties["text"] = textSchema
		properties["assignments"] = assignments
		required = []string{"text", "assignments"}
	case roleSearch:
		properties["text"] = textSchema
		required = []string{"text"}
	default:
		return nil, fmt.Errorf("unknown model role %q", role)
	}
	return map[string]any{
		"type":                 "object",
		"properties":           properties,
		"required":             required,
		"additionalProperties": false,
	}, nil
}
