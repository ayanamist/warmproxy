# warmproxy

放在 magpie 前面的 Go 反向代理，对 magpie 上的 Claude Sonnet/Opus 做提示缓存预热。用户对话使用中文。

## 命令

- 测试：`go vet ./... && go test -race -count=1 ./...`
- 本机无网络依赖时用模块缓存：`export GOFLAGS=-mod=mod GOPROXY=file://$(go env GOMODCACHE)/cache/download GOSUMDB=off`
- 在终端里用 `!` 的路径（如模块缓存里的 `!burnt!sushi`）前先 `set +H`。

## 约定

- 凭证只存在内存和 `state_dir/runs`（0600），不进日志、不进 ledger。日志与 ledger 不记录对话内容。
- 预热请求只发往配置里的 `upstream`，不接受请求中任何地址。
- 价格不要凭记忆填，用 `https://models.dev/api.json` 核对（`anthropic.models.<id>.cost`）。
- 安全窗口（1 小时/30 分钟）从真实请求算起，预热不延长；恢复快照时沿用 `started_at`。

## 设计文档

改动 `internal/` 下的包、会话键或预热策略前，先读 [docs/design.md](docs/design.md)（包布局、取舍、待验证项）；用法见 [README.md](README.md)。
