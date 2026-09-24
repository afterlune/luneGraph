// Package graph builds and executes typed stateful graphs.
package graph

import (
	"context"

	"lune-graph/internal/definition"
	"lune-graph/internal/executor"
	"lune-graph/internal/model"
)

type (
	Action            = model.Action
	ActivationGroup   = model.ActivationGroup
	Checkpoint[S any] = model.Checkpoint[S]
	Clone[S any]      = model.Clone[S]
	Config[S any]     = model.Config[S]
	Failure           = model.Failure
	FailureScope      = model.FailureScope
	Graph[S any]      = definition.Graph[S]
	Invocation[S any] = model.Invocation[S]
	InvocationStatus  = model.InvocationStatus
	JoinSpec[S any]   = model.JoinSpec[S]
	Merge[S any]      = model.Merge[S]
	Node[S any]       = model.Node[S]
	NodeSpec[S any]   = model.NodeSpec[S]
	Options[S any]    = model.Options[S]
	PanicError        = model.PanicError
	Result[S any]     = model.Result[S]
	ResumeInput       = model.ResumeInput
	Runner[S any]     = executor.Runner[S]
	Status            = model.Status
	Store[S any]      = model.Store[S]
	Terminal[S any]   = model.Terminal[S]
	Transition[S any] = model.Transition[S]
	TransitionError   = model.TransitionError
)

const (
	ActionContinue     = model.ActionContinue
	ActionWait         = model.ActionWait
	ActionEndBranch    = model.ActionEndBranch
	ActionEndExecution = model.ActionEndExecution

	FailInvocation = model.FailInvocation
	FailGroup      = model.FailGroup
	FailExecution  = model.FailExecution

	InvocationReady   = model.InvocationReady
	InvocationWaiting = model.InvocationWaiting
	InvocationGroup   = model.InvocationGroup
	InvocationJoined  = model.InvocationJoined
	InvocationEnded   = model.InvocationEnded
	InvocationFailed  = model.InvocationFailed

	StatusCompleted             = model.StatusCompleted
	StatusCompletedWithFailures = model.StatusCompletedWithFailures
	StatusWaiting               = model.StatusWaiting
	StatusBudget                = model.StatusBudget
	StatusFailed                = model.StatusFailed
	StatusCancelled             = model.StatusCancelled

	CheckpointFormatVersion = model.CheckpointFormatVersion
)

var (
	ErrConflict          = model.ErrConflict
	ErrExecutionLimit    = model.ErrExecutionLimit
	ErrInvalidCheckpoint = model.ErrInvalidCheckpoint
)

// New creates a graph builder with the given entry node name.
func New[S any](entry string) *Graph[S] { return definition.New[S](entry) }

// RegisterContinuation adapts a typed continuation handler to persisted bytes.
func RegisterContinuation[S, P any](g *Graph[S], key string, decode func([]byte) (P, error), apply func(context.Context, S, P) (S, error)) error {
	return definition.RegisterContinuation(g, key, decode, apply)
}

func To[S any](state S, targets ...string) Transition[S] {
	return model.To(state, targets...)
}

func Wait[S any](state S, continuation string, targets ...string) Transition[S] {
	return model.Wait(state, continuation, targets...)
}

func EndBranch[S any](state S) Transition[S] { return model.EndBranch(state) }

func EndExecution[S any](state S) Transition[S] { return model.EndExecution(state) }
