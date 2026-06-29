package workflow

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
)

// ErrMissingSuccessor 表示 strict routing 模式下 action 没有匹配的后继节点。
var ErrMissingSuccessor = errors.New("workflow missing successor")

// Stage 标识错误发生的执行阶段。
type Stage string

const (
	StagePrep     Stage = "prep"
	StageExec     Stage = "exec"
	StageFallback Stage = "fallback"
	StagePost     Stage = "post"
	StageFlow     Stage = "flow"
	StageBatch    Stage = "batch"
	StagePanic    Stage = "panic"
)

// ErrorCode 是稳定的轻量错误码，便于业务平台分类处理。
type ErrorCode string

const (
	ErrCodePrepFailed     ErrorCode = "prep_failed"
	ErrCodeExecFailed     ErrorCode = "exec_failed"
	ErrCodePostFailed     ErrorCode = "post_failed"
	ErrCodeFallbackFailed ErrorCode = "fallback_failed"
	ErrCodeFlowFailed     ErrorCode = "flow_failed"
	ErrCodeBatchFailed    ErrorCode = "batch_failed"
	ErrCodeTimeout        ErrorCode = "timeout"
	ErrCodeCancelled      ErrorCode = "cancelled"
	ErrCodePanic          ErrorCode = "panic"
	ErrCodeValidation     ErrorCode = "validation_failed"
	ErrCodeRoutingFailed  ErrorCode = "routing_failed"
)

// WorkflowError 使用工作流阶段和节点上下文包装底层错误。
type WorkflowError struct {
	Code   ErrorCode
	Stage  Stage
	NodeID string
	Msg    string
	Cause  error
	Stack  string
}

// Error 将阶段、节点、消息和原因格式化为错误字符串。
func (e *WorkflowError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf(
			"workflow error: code=%s stage=%s node=%s msg=%s cause=%v",
			e.Code,
			e.Stage,
			e.NodeID,
			e.Msg,
			e.Cause,
		)
	}

	return fmt.Sprintf(
		"workflow error: code=%s stage=%s node=%s msg=%s",
		e.Code,
		e.Stage,
		e.NodeID,
		e.Msg,
	)
}

// wrapPanic 将 panic 转换为 WorkflowError，并保留调用栈便于诊断。
func wrapPanic(stage Stage, nodeID string, msg string, recovered any) error {
	return &WorkflowError{
		Code:   ErrCodePanic,
		Stage:  stage,
		NodeID: nodeID,
		Msg:    msg,
		Cause:  fmt.Errorf("panic: %v", recovered),
		Stack:  string(debug.Stack()),
	}
}

// Unwrap 返回底层原因，以支持 errors.Is 和 errors.As。
func (e *WorkflowError) Unwrap() error {
	return e.Cause
}

// wrapErr 为 err 增加工作流上下文，同时保留错误解包语义。
func wrapErr(stage Stage, nodeID string, msg string, err error) error {
	if err == nil {
		return nil
	}

	return &WorkflowError{
		Code:   errorCodeFor(stage, err),
		Stage:  stage,
		NodeID: nodeID,
		Msg:    msg,
		Cause:  err,
	}
}

func errorCodeFor(stage Stage, err error) ErrorCode {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return ErrCodeTimeout
	case errors.Is(err, context.Canceled):
		return ErrCodeCancelled
	}

	switch stage {
	case StagePrep:
		return ErrCodePrepFailed
	case StageExec:
		return ErrCodeExecFailed
	case StageFallback:
		return ErrCodeFallbackFailed
	case StagePost:
		return ErrCodePostFailed
	case StageBatch:
		return ErrCodeBatchFailed
	case StagePanic:
		return ErrCodePanic
	case StageFlow:
		if errors.Is(err, ErrMissingSuccessor) {
			return ErrCodeRoutingFailed
		}
		return ErrCodeFlowFailed
	default:
		return ErrCodeFlowFailed
	}
}
