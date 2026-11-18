# warmproxy

放在 magpie 前面的 Go 反向代理，对 `claude-sonnet*` 和 `claude-opus*` 的 `/v1/messages` 和 `/v1/chat/completions` 请求做提示缓存预热，策略移植自 pi 的 `cache-warming`。其他请求透传。

```
客户端(agent) → warmproxy :3426 → magpie :3425 → Anthropic
                        └─ 到期前重发上一次请求，输出上限为一个 token
```

为什么是代理而不是 magpie 插件：代理能拿到客户端自己的凭证和完整请求头、能读到响应里的 `usage`，定时器也不受插件宿主重载影响。

## 用法

```bash
go build -o ~/.local/bin/warmproxy ./cmd/warmproxy
mkdir -p ~/.config/warmproxy && cp config.example.toml ~/.config/warmproxy/config.toml
warmproxy            # 或 warmproxy -config <path> -debug
```

Anthropic 客户端的 Base URL 用 `http://127.0.0.1:3426`，Chat Completions 客户端用 `http://127.0.0.1:3426/v1`。

systemd user 单元见 [contrib/warmproxy.service](contrib/warmproxy.service)。程序把启动、上游响应状态、预热排期、刷新结果和关闭等日志写到 stderr，示例单元将 stderr 接入 journal。

```bash
journalctl --user -u warmproxy.service -n 50
journalctl --user -u warmproxy.service -f
```

想查看跳过预热的原因时，在服务的 `ExecStart` 后加 `-debug`。日志不记录请求体、凭证或 URL 查询参数。

状态：`curl http://127.0.0.1:3426/_warm/status`（仅回环地址）。每次预热一行记录在 `<state_dir>/ledger.jsonl`，不含请求内容。

## 工作方式

1. 检查 `POST /v1/messages` 和 `POST /v1/chat/completions` 请求体，按 `match` 筛选模型。Chat 请求体达到 `max_body_bytes` 检查上限时继续透传。
2. 应答边转发边解析 `usage` 和结束原因。只有 HTTP 200 且正常结束的应答才可布防，Messages SSE 要收到 `message_stop`，Chat SSE 要收到 `finish_reason` 和 `[DONE]`。客户端中途断开不布防。
3. 布防：按会话保留一个 run，新请求覆盖旧 run。在 TTL 的 90%（至少留 10 秒余量）处重发。TTL 取请求里最短的 `cache_control`；应答的 `usage.cache_creation` 里只要有 `ephemeral_5m_input_tokens`，就说明上游只给了 5 分钟，TTL 以 5 分钟为准（请求要 1 小时也一样）。应答只读缓存、没有写入时看不出 TTL，等某次预热写入后再按它校正。TTL 从真实请求发出时算起（应答耗时也在内），预热从发出时重新计时；失败的预热不延长缓存，只在原条目剩余寿命内每 10 秒重试。客户端不跟随重定向。
4. 每次刷新前检查：模式、迟到保护（错过安全窗口就不发）、最长时长（流式 1 小时、空闲 30 分钟，从真实请求算起，预热不延长）、经济性。
5. 经济性公式与 pi 相同：`P(继续) × max(0, miss − hit) − warm ≥ min_savings_usd`。`miss` 用 cache write 价，`hit` 用 cache read 价，`warm` 是一次 cache read、一个输出 token，再加上最后一个缓存断点之后的未缓存后缀（`input_tokens`）每次按普通输入价重付的费用。没有价格的模型不预热。
6. 预热后读不到一半以上的缓存算 miss，连续 `stop_after_misses` 次就停止这个 run。

**阶段近似**：Messages 的 `tool_use` 和 Chat 的 `tool_calls`（含旧版 `function_call`）按 streaming（P=1），其余按 idle（P=0.15）。`mode = "streaming"` 只预热前者。

**Messages** 要求请求带 `cache_control`。预算式 thinking 和续用服务端 `thread` 的请求不预热。

**Chat Completions** 依赖 magpie 转译为 Anthropic 请求时添加缓存标记。响应的 `usage.prompt_tokens_details` 必须报告正数的 `cached_tokens` 或 `cache_write_tokens` 才会布防。Chat 响应缺少 TTL 拆分，排期按 5 分钟。预热沿用原路径，关闭流式输出，移除 `stream_options`，将 `max_tokens` 或 `max_completion_tokens` 设为 1。显式推理设置（`reasoning_effort: "none"` 除外）、服务端会话、网页搜索和多候选请求跳过预热。

## 会话键

`sha256(x-api-key | authorization | system | tools | 会话 id)`。会话 id 按 `session_headers` 顺序取第一个存在的头（magpie 的 `sessionOf` 读的就是这些：`X-Magpie-Session` 优先，然后是 agent 自己的），都没有时改用首条消息的内容。`system` 和 `tools` 进键，是因为子 agent 会沿用父 agent 的会话 id，两者的缓存前缀不同，必须各有各的 run。模型不进键：同一会话换模型时，新请求写入的才是最新缓存，直接覆盖。

Chat 的会话键带协议前缀，用 `system` 和 `developer` 消息识别指令，并纳入 `prompt_cache_key`。

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
