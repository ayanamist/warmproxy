# 设计说明

面向用户的用法和工作流程见 [README.md](../README.md)，这里记录包布局、关键设计取舍和未验证的假设。

## 包布局

- `internal/policy`：pi `cache-warming.ts` 的移植，纯函数：延迟、经济性公式。
- `internal/anthropic`：解析请求（TTL、是否可重放）、构造预热体（`WarmBody`，除 `max_tokens` 和 `stream` 外字节不变）、解析 SSE 和 JSON 应答的 usage（含 `cache_creation` 的 5m/1h 拆分，用来校正实际 TTL）。
- `internal/warmer`：`Manager` 按会话维护 run、定时刷新、落盘恢复（`Store`）、记账（`Ledger`）。依赖 `clock.Clock` 和 `Doer`，测试用 `clock.Fake`。
- `internal/proxy`：`ReverseProxy` 加响应 tap，边转发边解析，完整结束后才布防。
- `internal/config`：TOML 配置，拒绝未知键；`config.example.toml` 是示例。

## 会话键

键是 `sha256(凭证 | system | tools | 会话 id)`；没有会话 id 时把首条消息代替会话 id 放进去。

- 子 agent 会沿用父 agent 的会话 id，但 `system` 和 `tools` 不同。不把这两项放进键，子 agent 的请求会覆盖主 agent 的 run，并行子 agent 只剩最后一个，子 agent 的请求被跳过（例如没有价格的 haiku）时还会删掉主 agent 的 run。
- 代价：一个会话可能同时持有多个 run，占用 `max_sessions`，每个 run 各自空闲到 30 分钟或 1 小时上限，除非经济性不通过。
- 请求被跳过时只删除自己的键，不影响同会话其他 run。模型不匹配 `match` 的请求成功完成后也走 `Disarm`，结束同键（同凭证、system、tools、会话 id）的 run，因为新请求才是写入最新缓存的那一个。

## 时间记账

- `NextWarmAt` 是绝对时刻，从最后一次读写缓存的请求**发出时**算起（真实请求用代理收到请求的时刻，预热用 `send` 开始的时刻），不用应答结束时刻。排队前和取得并发名额后都走完整的 `checkLocked`（缓存期限 `deadlineOf`、安全窗口、经济性），排队时间可能超过任一项。
- 预热失败（429、5xx、网络错误）不延长缓存，不改 `NextWarmAt`，只每 `retryDelay` 重试一次，超过期限就停止。
- 预热请求的 URL 直接调用 `httputil.ProxyRequest.SetURL` 构造，路径（包括转义和尾部斜杠）和查询参数与普通转发一致；`warmer.NewClient` 不跟随重定向。

## 实际 TTL

`Info.TTL` 只是请求想要的寿命。上游（magpie 或 Anthropic）可能把 1 小时降成 5 分钟，所以 run 的 `TTL` 取 `Usage.TTL(请求 TTL)`：应答 `usage.cache_creation.ephemeral_5m_input_tokens > 0` 时按 5 分钟；有 1 小时写入拆分时沿用请求值；没有 TTL 拆分信息时保守地按 5 分钟。实际 TTL 永远不会比请求更长。

- 只有写入了缓存的应答才带这个信息。全部读缓存的应答看不出 TTL，`Arm` 会按 5 分钟处理；若之后某次预热写入报告 1 小时拆分，则沿用当前 TTL，不会把它升回去。若报告 5 分钟写入，`refresh` 会把 `TTL` 和 `delay` 降下来，从这次预热起按 5 分钟排期。
- 预热的费用按应答里 5m/1h 的写入拆分分别按对应的 cache write 价计；拆分里没有的部分按 run 当前的 TTL 计。经济性检查（`EvaluateSplit`）用 run 的 TTL 取 cache write 价。
- 上游不返回 `cache_creation` 拆分（非 Anthropic 后端）时按 5 分钟 TTL 处理。

## 经济性

`usage.input_tokens` 是最后一个缓存断点之后的未缓存后缀，重放时仍按输入价计费，不会变成 cache read。run 分别记 `Tokens`（全部）和 `Uncached`，`policy.EvaluateSplit` 只用前缀算可避免的 miss 成本，把后缀计入每次预热成本；hit 的判断也只看前缀。

## 压缩（compaction）

pi 的预热在旧消息不再是新消息前缀时停止。代理没有这个检查，因为它没有收益：

- 有会话 id：压缩后的请求键不变（`system` 和 `tools` 通常不变），`Arm` 会无条件替换旧 run。
- 没有会话 id：首条消息变了，键也变，旧 run 无法与新请求关联。同一凭证、`system`、`tools` 下的另一条对话与压缩后的对话无法区分，所以不按这三项归并；旧 run 靠 1 小时、30 分钟上限和连续 miss 止损。

基于成本的压缩（冷缓存检测、150k 阈值）只存在于 p2 的 Familiar 插件，不在代理范围内。

## 待验证（未用真实流量试过）

- 真实经 magpie 的预热是否命中：看 `ledger.jsonl` 的 `hit`、`usage.cache_read_input_tokens`。
- magpie 对哪些 agent 传了会话 id 头（见 `session_headers`）；Pi 需要 `compat.sendSessionAffinityHeaders`，否则退化为按对话开头区分。
- magpie 是否在重发的请求上保持与原请求相同的缓存前缀。
