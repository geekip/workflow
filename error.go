package workflow

import (
	"fmt"
	"runtime/debug"
)

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

// WorkflowError 使用工作流阶段和节点上下文包装底层错误。
type WorkflowError struct {
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
			"workflow error: stage=%s node=%s msg=%s cause=%v",
			e.Stage,
			e.NodeID,
			e.Msg,
			e.Cause,
		)
	}

	return fmt.Sprintf(
		"workflow error: stage=%s node=%s msg=%s",
		e.Stage,
		e.NodeID,
		e.Msg,
	)
}

// wrapPanic 将 panic 转换为 WorkflowError，并保留调用栈便于诊断。
func wrapPanic(stage Stage, nodeID string, msg string, recovered any) error {
	return &WorkflowError{
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
		Stage:  stage,
		NodeID: nodeID,
		Msg:    msg,
		Cause:  err,
	}
}
