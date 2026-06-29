package workflow

import (
	"fmt"
	"sort"
	"strings"
)

// ValidationIssue 描述工作流静态校验发现的问题。
type ValidationIssue struct {
	NodeID string
	Action string
	Msg    string
}

// ValidationError 聚合工作流静态校验发现的全部问题。
type ValidationError struct {
	Issues []ValidationIssue
}

// Error 返回适合日志输出的校验错误摘要。
func (e *ValidationError) Error() string {
	if e == nil || len(e.Issues) == 0 {
		return "workflow validation failed"
	}

	parts := make([]string, 0, len(e.Issues))
	for _, issue := range e.Issues {
		if issue.Action != "" {
			parts = append(parts, fmt.Sprintf("node=%s action=%s msg=%s", issue.NodeID, issue.Action, issue.Msg))
			continue
		}
		parts = append(parts, fmt.Sprintf("node=%s msg=%s", issue.NodeID, issue.Msg))
	}

	return "workflow validation failed: " + strings.Join(parts, "; ")
}

// Validate 对可达节点图执行静态校验。
//
// 当前校验覆盖起始节点、节点 Core、节点 ID、重复节点 ID、空后继节点和环路。
// 如果业务需要显式循环，应跳过该校验或在更高层提供循环次数/退出条件保护。
func (f *Flow) Validate() error {
	if f == nil {
		return &ValidationError{Issues: []ValidationIssue{{Msg: "flow is nil"}}}
	}
	if f.Start == nil {
		return &ValidationError{Issues: []ValidationIssue{{Msg: "flow start node is nil"}}}
	}

	v := flowValidator{
		state:   map[*CoreNode]int{},
		seenIDs: map[string]*CoreNode{},
	}
	v.visit(f.Start, nil, "")

	if len(v.issues) == 0 {
		return nil
	}

	return &ValidationError{Issues: v.issues}
}

type flowValidator struct {
	issues  []ValidationIssue
	state   map[*CoreNode]int
	seenIDs map[string]*CoreNode
}

func (v *flowValidator) visit(node Node, parent *CoreNode, action string) {
	if node == nil {
		v.addIssue(parentID(parent), action, "successor node is nil")
		return
	}

	core := node.Core()
	if core == nil {
		v.addIssue(parentID(parent), action, "node core is nil")
		return
	}

	meta := core.Meta()
	if meta.ID == "" {
		v.addIssue(parentID(parent), action, "node id is empty")
		return
	}

	if existing := v.seenIDs[meta.ID]; existing != nil && existing != core {
		v.addIssue(meta.ID, "", "duplicate node id")
		return
	}
	v.seenIDs[meta.ID] = core

	switch v.state[core] {
	case 1:
		v.addIssue(meta.ID, action, "workflow graph contains a cycle")
		return
	case 2:
		return
	}

	v.state[core] = 1

	successors := core.SuccessorsSnapshot()
	actions := make([]string, 0, len(successors))
	for nextAction := range successors {
		actions = append(actions, nextAction)
	}
	sort.Strings(actions)

	for _, nextAction := range actions {
		next := successors[nextAction]
		if next == nil {
			v.addIssue(meta.ID, nextAction, "successor node is nil")
			continue
		}
		v.visit(next, core, nextAction)
	}

	v.state[core] = 2
}

func (v *flowValidator) addIssue(nodeID string, action string, msg string) {
	v.issues = append(v.issues, ValidationIssue{
		NodeID: nodeID,
		Action: action,
		Msg:    msg,
	})
}

func parentID(parent *CoreNode) string {
	if parent == nil {
		return ""
	}
	return parent.Meta().ID
}
