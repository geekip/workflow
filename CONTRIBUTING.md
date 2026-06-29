# 贡献指南

## 项目定位

`workflow` 是轻量工作流执行内核，不是完整工作流平台。新增能力应优先满足以下条件：

- 属于节点执行、流程编排、错误处理、取消、超时、重试、校验或测试能力。
- 不强绑定数据库、队列、Web API、权限、多租户、UI 或调度平台。
- 不引入非必要外部依赖。
- 不破坏现有公开 API，除非有清晰迁移理由。

## 提交前检查

```bash
gofmt -w .
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
go test -coverprofile=coverage.out -count=1 ./...
rm -f coverage.out
```

## 测试要求

新增行为应覆盖：

- 成功路径。
- 错误路径。
- context 取消或 timeout。
- 并发场景的 race 测试。
- 公开 API 的链式调用或兼容性。

## 文档要求

公开 API 或语义变化需要同步更新：

- `README.md`
- `docs/USAGE.md`
- `docs/DESIGN.md`
- `CHANGELOG.md`

## 兼容性

优先新增 API，不轻易删除或重命名已有 API。行为变化需要在 `CHANGELOG.md` 中说明。
