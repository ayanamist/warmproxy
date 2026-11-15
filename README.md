# warmproxy

放在 magpie 前面的反向代理，用 Go 写，对 `claude-sonnet*` 和 `claude-opus*` 的 `/v1/messages` 请求做提示缓存预热，策略移植自 pi 的 `cache-warming`。其他请求原样透传。

```
客户端(agent) → warmproxy :3426 → magpie :3425 → Anthropic
                        └─ 到期前把上一次请求重发一次（max_tokens=1）
```

为什么是代理而不是 magpie 插件：代理能拿到客户端自己的凭证和完整请求头、能读到响应里的 `usage`，定时器也不受插件宿主重载影响。

## 用法

```bash
go build -o ~/.local/bin/warmproxy ./cmd/warmproxy
mkdir -p ~/.config/warmproxy && cp config.example.toml ~/.config/warmproxy/config.toml
warmproxy            # 或 warmproxy -config <path> -debug
```

把 agent 的 Anthropic base URL 改成 `http://127.0.0.1:3426`。systemd user 单元见 `contrib/warmproxy.service`。

状态：`curl http://127.0.0.1:3426/_warm/status`（仅回环地址）。每次预热一行记录在 `<state_dir>/ledger.jsonl`，不含请求内容。

## 工作方式

1. `POST /v1/messages` 且模型匹配 `match`：缓冲请求体（上限 `max_body_bytes`），其余原样转发。
2. 应答边转发边解析 `usage` 和 `stop_reason`（SSE 或 JSON）。只有 200 且正常结束的应答（流要到 `message_stop`）才布防；客户端中途断开不布防。
3. 布防：按会话保留一个 run，新请求覆盖旧 run。在 TTL 的 90%（至少留 10 秒余量）处重发。TTL 从真实请求发出时算起（应答耗时也在内），预热从发出时重新计时；失败的预热不延长缓存，只在原条目剩余寿命内每 10 秒重试。客户端不跟随重定向。
4. 每次刷新前检查：模式、迟到保护（错过安全窗口就不发）、最长时长（流式 1 小时、空闲 30 分钟，从真实请求算起，预热不延长）、经济性。
5. 经济性公式与 pi 相同：`P(继续) × max(0, miss − hit) − warm ≥ min_savings_usd`。`miss` 用 cache write 价，`hit` 用 cache read 价，`warm` 是一次 cache read、一个输出 token，再加上最后一个缓存断点之后的未缓存后缀（`input_tokens`）每次按普通输入价重付的费用。没有价格的模型不预热。
6. 预热后读不到一半以上的缓存算 miss，连续 `stop_after_misses` 次就停止这个 run。

**阶段近似**：代理看不到 agent 是否还在运行，`stop_reason` 为 `tool_use` 按 streaming（P=1），其余按 idle（P=0.15）。`mode = "streaming"` 只预热前者。

**不预热的请求**：没有 `cache_control`；`thinking.type = "enabled"`（预算式 thinking 的 `budget_tokens` 随 `max_tokens` 变，重发会变成另一个请求）；带 `thread` 字段的请求（续用服务端状态）。

## 会话键

`sha256(x-api-key | authorization | system | tools | 会话 id)`。会话 id 按 `session_headers` 顺序取第一个存在的头（magpie 的 `sessionOf` 读的就是这些：`X-Magpie-Session` 优先，然后是 agent 自己的），都没有时改用首条消息的内容。`system` 和 `tools` 进键，是因为子 agent 会沿用父 agent 的会话 id，两者的缓存前缀不同，必须各有各的 run。模型不进键：同一会话换模型时，新请求写入的才是最新缓存，直接覆盖。

## 重启

每个 run 落盘在 `<state_dir>/runs/<key>.json`（元数据）和 `.body`（请求体），权限 0600，目录 0700，原子写入。**请求头里的凭证随快照落盘**。启动时恢复仍在安全窗口内的 run，沿用原来的 `started_at` 和迟到保护；缓存条目已经过期的直接丢弃。重启间隔超过剩余 TTL 窗口（5 分钟 TTL 约 4 分钟）就没有恢复意义。

## 已知限制

- 预热请求仍经过 magpie 路由，落到另一个账号就不会命中。假定 Claude 在 magpie 里是单账号。出现 miss 时兑底会自动止损，但根因要到 magpie 里查。
- magpie 对发往 Anthropic 的请求如果每次都改写（占位符、时间戳），预热也会 miss。
- 代理不检查前缀是否延伸。agent 有会话 id 时压缩后的新请求键不变，直接覆盖旧 run；没有会话 id 时旧 run 靠 1 小时和 30 分钟的上限及连续 miss 止损。
- 同一会话里主 agent 和子 agent 各占一个 run，都计入 `max_sessions`。

## 开发

```bash
go vet ./... && go test -race ./...
```

尽量离线：TOML 库 `github.com/BurntSushi/toml` 在本机模块缓存里。代码布局和设计取舍见 [docs/design.md](docs/design.md)。
