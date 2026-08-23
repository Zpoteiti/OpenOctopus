# Py8a Admin Shared-service Server MCP 设计

**状态：** accepted，待实现
**Milestone：** Py8a Server shared MCP
**依赖：** 已完成的 Py7 Client workspace restriction 与 Device MCP
**目标协议：** Protocol v3 不变；Server MCP 不经过 Device WebSocket

本设计是 Python-main 中 Py8a 的 implementation authority。它把
`DECISIONS.md`、`SCHEMA.md`、`API.yaml` 和 `TOOLS.md` 中仍标为 deferred、
future 或沿用旧 Rust 实现的 Server MCP 文字收敛为一个完整契约，并取代：

- ADR-047/049 中旧 Rust `rmcp`、Device 可阻止 Admin 同名安装的 cross-install schema
  collision 和 synchronous restart 的细节；
- `system_config.server_mcp` 只是 config array、没有 revision/catalog 的占位形状；
- `/api/admin/server-mcp` 只有 GET/PUT 名称、没有 CAS、redaction 或 runtime 状态的占位；
- Py8 milestone 中“Server sandbox”会引入 bwrap/容器隔离的旧暗示；
- “一个 FastMCP client 等于所有调用只能串行”的错误假设。

Py7 design 继续是 Device MCP 的权威历史设计。Py8a 复用它已经实现并验证的 config、
FastMCP transport、四 surface discovery、catalog canonicalization、result mapping 和
no-replay 语义；本设计只在明确写出的 Server tenancy、namespace reservation、容量优先、
公平 admission 和 timeout drain 处覆盖 Py7。

## 1. 结果与边界

Py8a 允许管理员配置一组对整个部署共享的 MCP：

- stdio MCP 以 OpenOctopus Server OS 用户身份运行；
- Streamable HTTP 与 legacy SSE MCP 从 Server 主机发起连接；
- 每个配置只有一个进程级 `RuntimeSlot`、一个 FastMCP client/session 和一个 active
  generation；所有用户共享它们；
- tools、static resources、resource templates、prompts 四个 surface 都包装为
  `mcp_<server>_<alias>` Agent tools，并以
  `openoctopus_device="server"` 暴露；
- 配置与完整 last-good catalog 持久化。runtime down、Server restart 或 remote 暂时
  不可达不会让 Provider schema 消失；调用返回普通 bounded tool error；
- 一个显式有界公平队列协调并发；500 个 Agent 同时搜索时，只有有限 active 与 waiting，
  其余明确得到 `tool_mcp_busy`，不会制造 500 个无界 semaphore waiter。

Server MCP 的产品定位是管理员为全体用户安装的共享、轻量、无状态或低状态能力，例如
计算器、SearXNG 搜索和共享知识库。管理员不能把个人 OAuth、Slack 登录、浏览器/IDE
状态、REPL、长时间任务或按用户隔离的数据源安装成 Server MCP。此限制是明确的信任与
产品合同，不是 OpenOctopus 能通过 discovery 自动验证的属性。

用户身份、session id、workspace、Device token 或 OpenOctopus credential 不会被自动
注入 MCP request。MCP 只收到其声明参数和管理员配置的 env/headers。Provider-visible
tool input/result 仍按正常聊天合同持久化到 transcript；“不记录 MCP 参数/结果”仅指
application log、telemetry 和 diagnostics，不表示破坏 tool-use/result 历史。

## 2. 已确定的决策

1. **只有 admin shared-service 与 Device 两种 tenancy。** 不增加 user-scoped、
   session-scoped 或 per-chat Server MCP。
2. **一个配置一个共享 runtime/client。** 不做 client pool、per-user process、
   per-session session 或 `pool_size`。
3. **FastMCP 固定 3.4.7。** Server 增加
   `fastmcp-slim[client]==3.4.7`、与 Py7 相同的 MCP SDK/URI template 固定版本；
   不使用 FastMCP 4 beta、transport inference 或 multi-server config。
4. **三种显式 transport。** `stdio`、`streamable_http`、`sse`；SSE 只为旧服务兼容，
   不从 HTTP 自动 fallback。
5. **四个完整 surface。** tools、static resources、resource templates、prompts 分别
   discover、分页、建模和调用；不把 Py7 已实现的基础 surface 延后到 Py14。
6. **统一 final namespace。** 沿用 `mcp_<server>_<alias>` 和
   `enabled_capabilities` 三态；surface 只存在于 immutable route metadata，不从名称反解析。
7. **whole-list admin API + CAS。** 只有 `GET/PUT /api/admin/server-mcp`。PUT 全量替换并
   必须携带 `base_config_revision`；不增加逐项 POST/PATCH/DELETE。
8. **单 JSONB envelope。** `system_config.server_mcp` 原子保存 schema version、
   `config_revision`、完整 configs 与完整 last-good catalog。runtime state 不持久化。
9. **validate before save。** 新增或修改的 server 必须真实 initialize、完整 discovery、
   catalog validation 成功后才 commit；纯删除不需要被删 runtime 可达。
10. **管理员 server name 保留整个逻辑 namespace。** 只要 authoritative Server config
    中存在 name `x`，所有 Device catalog 中 `entry.server == "x"` 的能力都不进入
    Provider schema 或 dispatch route。判断使用结构化 `server` 字段，不做字符串 prefix
    解析；因此 name `foo` 不会误伤 name `foo_bar`。
11. **已有 Device 配置只 shadow，不删除。** 管理员新增 name `x` 时，既有 Device
    `x` config/catalog/Client runtime 保持运行；仅 Provider/dispatch 抑制。管理员移除
    name 后，仍符合 Device 自身 allowlist/catalog 的能力自动恢复，无 Device 写入、
    reconnect 或重新 discovery。
12. **以后禁止新增或修改保留 name。** Device config candidate 新增或有效修改当前
    Server 已保留的 name 返回 409；原样保留、纯删除或 rename 到未保留 name 允许。
    commit 前必须重验 reservation，消除 admin PUT 与 Device PATCH 的竞态。
13. **Server Provider 容量优先。** Server enabled capabilities 必须独立满足 256 names /
    256 KiB Provider schema 上限；它们永不因 Device catalog 被截掉。剩余 Device 能力
    依稳定算法选择，超出的只做 Provider/dispatch suppression，且在 Device API 可见。
14. **有界公平 admission。** 每个 runtime 用户内 FIFO、用户间 round-robin；同一
    Server MCP 的 tools/resources/templates/prompts 共用一个队列和 concurrency limit。
15. **唯一 per-server 调优项是 `max_concurrent_calls`。** stdio 默认 1，HTTP/SSE 默认
    8；管理员可配置 `1..32`。queue size、queue wait、global/per-user cap 和 timeout 都是
    Py8a 固定合同，不再暴露 config。
16. **remote timeout 不立即炸掉共享 session。** 已发 remote call 在用户可见 deadline
    后 shield drain 最多 60 秒并继续持有 permits；最终仍不收敛才 retire generation。
    stdio 已发调用 timeout/Stop 直接 retire process tree。
17. **Server MCP 是可选依赖。** 单个或全部 runtime 启动失败不阻止 OpenOctopus
    startup，不使 `/health` 返回 503；last-good schema 保留，runtime 后台恢复。
18. **stdio 是管理员信任的同 UID 代码。** Py8a 不引入 bwrap、容器、seccomp 或 OS
    sandbox，也不声称能安全运行不可信 npm/pip package。
19. **可能发出的调用绝不自动 replay。** timeout、disconnect、Stop、generation
    replacement 和 Server restart 都不能触发 OpenOctopus 自动重发。

## 3. 范围

### 3.1 包含

- Server 侧 FastMCP 3.4.7 dependency、bounded stdio/HTTP/SSE transports；
- admin Server MCP config DTO、secret redaction、whole-list GET/PUT 与 CAS；
- `system_config.server_mcp` authoritative envelope、last-good catalog 和 digest；
- 四 surface bounded discovery、canonical wrapping、result mapping；
- process-lifetime supervisor、runtime generation、backoff、drift、replacement 与 cleanup；
- per-runtime 公平队列、global/per-user admission、timeout/drain/no-replay；
- Server namespace reservation、已有 Device shadow、Device mutation rejection；
- Server-first Provider capacity 与 deterministic Device suppression/API projection；
- Agent-turn immutable global/device route snapshot 与 dispatch revalidation；
- degraded startup、admin runtime diagnostics、health contract；
- fake stdio/Streamable HTTP/SSE、500-user load、Docker E2E 和真实 remote smoke。

### 3.2 不包含

- Py8b Client→Client 单文件 bridge 或 Py8c recursive directory transfer；
- user/session-scoped Server MCP、个人 credential、OAuth、浏览器登录或人工授权回调；
- runtime/client pool、load balancer、multi-worker coordination 或 distributed queue；
- bwrap、container、namespace、seccomp、SELinux/AppArmor profile 或其它 OS jail；
- Agent shell、Python/eval、让 Agent 安装/修改 Server MCP；
- MCP sampling、elicitation、roots、completion、tasks、background job、resource
  subscription 或 Apps/UI；
- per-call approval、per-user allowlist、计费/rate limit、priority queue；
- configurable queue size、queue timeout、global/user cap、invocation timeout 或 drain timeout；
- OAuth transport、custom CA、`verify_tls=false`、redirect following 或 transport inference；
- MCP session 状态跨 Server restart 恢复，或任何 issued call replay；
- frontend MCP 管理 UI、installer、marketplace 和自动安装 npm/pip package；
- 自动判断一个 MCP 是否无状态、安全或“足够轻量”。

## 4. 不可破坏的不变量

1. 只有 admin 可读写 Server MCP config；普通用户和 Agent 没有配置 API/tool。
2. config、revision 与 complete last-good catalog 在同一 JSONB row、同一 DB commit 中
   改变；不得观察到新 config 配旧 catalog或反之。
3. Provider schema 只来自 durable last-good catalog，不来自临时 runtime discovery。
4. runtime availability 不改变 schema；authoritative config/name/filter/catalog commit
   才能改变 Provider shape。
5. authoritative Server name reservation 与 runtime 是否 READY、是否启用任何能力无关；
   down/drifted/`enabled_capabilities=null` 仍保留该 name。
6. reservation 以 persisted config 的结构化 server name 判定，不解析 final name。
7. shadow/suppression 只影响 Provider schema 和新 dispatch；不删除 Device config/catalog，
   不发 corrective config，不停止 Client runtime。
8. Server capabilities 先完整进入每个用户的 MCP Provider budget；若它们单独超限，
   admin candidate 整体失败，绝不截断 Server catalog。
9. 与 Server exact final name 冲突的既有 Device logical group由Server优先并整体抑制；
   不能生成重复Provider tool name或让Device覆盖Server route。
10. Device capacity suppression 是 deterministic、Provider/dispatch 一致、API 可见的；
   不能只从 schema 隐藏却仍允许名称或旧 route 调用。
11. 每个 Provider iteration 从同一逻辑 snapshot 冻结 Server revision/catalog、Device
    catalogs、suppression decision 和 exact routes。Provider 返回后不绑定到新 generation。
12. pre-send revision/generation/reservation 失效返回 unavailable；issued 或可能 issued 后
    失去结果返回 outcome unknown。两者不能混淆。
13. queue waiting、active、draining、global active 和 per-user active 全部有硬上界；没有
    与请求数量成正比的无界 semaphore waiter、task、future 或 response buffer。
14. `tool_mcp_busy` 只用于明确尚未发送的 queue full/wait expiry，可以安全稍后重试；
    Server 本身不自动重试 tool invocation。
15. remote public timeout/Stop 后的 drain task 必须 shield caller cancellation；permit 只有
    在真实结果被消费或 generation 被最终关闭后释放。
16. 一个 remote call timeout 在 60 秒 drain 窗口内不能关闭同 generation 的其它健康
    calls；hard drain expiry/transport-wide failure 才能 retire generation。
17. stdio direct argv 不经 shell；child 只收到 safe baseline + configured env，且任何
    `OPENOCTOPUS_*` 都被删除。
18. env/header values 不进入 REST 明文、catalog、Provider schema/prompt、exception、
    application log、telemetry 或 admin runtime error；PostgreSQL 与目标 MCP transport
    是明确例外。
19. MCP tool input/result 按正常 Provider transcript 合同持久化，但不写 lifecycle log。
20. remote MCP 和 stdio child 不受 `web_fetch_denylist` 控制；文档不得把该 denylist
    描述成 Server egress firewall。
21. Server shutdown、config replacement 和 handler cancellation 都执行 bounded、
    cancellation-safe cleanup；不得遗留 root child、HTTP stream 或 generation task。

## 5. Authoritative persistence

### 5.1 单 JSONB envelope

Py8a 不新增表。`system_config` 继续是全局 key-value store；key `server_mcp` 的 value
从旧占位 array 定稿为：

```json
{
  "version": 1,
  "config_revision": 7,
  "mcp_servers": [],
  "mcp_catalog": {
    "version": 1,
    "digest": "<lowercase sha256>",
    "servers": []
  }
}
```

`mcp_servers` 保存 canonical complete config，包括可逆明文 env/header values；
`mcp_catalog` 保存完整 bounded last-good discovery，包括 disabled entries 与
Server-assigned UUIDv7 `entry_id`，但不含 secret、runtime generation、queue 或 error。

数据库没有该 row 时，读取层合成 version 1、`config_revision=1`、空 config 与 canonical
空 catalog，不在 bootstrap seed row。第一次 effective PUT 写 revision 2。删除至空配置
也保留 envelope 与递增 revision，不能删 row 后把 CAS 倒退到 1。精确 no-op 不写 row、
不更新 `updated_at`、不递增 revision。

存储解析使用 strict models、`extra="forbid"`、bounded canonical JSON。version、revision、
digest、config/catalog 对应关系损坏属于 authoritative DB corruption；Server startup
可以失败，而 transport/MCP endpoint 不可达只能 degraded，不能与 corruption 混为一类。

### 5.2 Config shape

Py8a 定义 Server-only `ServerMcpServerConfig` tagged union。它复用 Py7
`McpServerConfig` 的 transport 字段与校验，但不改 Device DTO，并在三个 Server
transport 上增加：

```json
{"max_concurrent_calls": 8}
```

- input 可省略；stdio canonical effective default 为 1，HTTP/SSE 为 8；
- GET 与 storage projection 总是返回/保存 effective integer，避免 omitted 与显式默认
  产生伪 config change；
- 值必须为 `1..32`，bool 不作为 integer 接受；
- 它限制同一个 shared `ClientSession` 上同时 issued/draining 的调用数量，不是 pool size；
- tools、resources、templates 和 prompts 共用该值；
- 任何 effective config change，包括 filter-only 或 concurrency-only change，都按完整
  candidate replacement 处理；这样 `RuntimeSlot` 与 scheduler config 在 generation
  内 immutable，不实现 live-resize semaphore。

Device `McpServerConfig` 不接受 `max_concurrent_calls`；Device MCP 继续由既有
per-Device execution contract 管理，Py8a 不顺带改变它的并发模型。

其余 limits 与 Py7 一致：最多 16 个唯一 server names；每个 name 匹配
`^[a-z][a-z0-9_]{0,31}$`；whole config canonical JSON 最多 256 KiB；stdio command/args/
cwd/env、remote URL/headers、HTTPS secret、forbidden headers、no redirect 和 strict TLS
规则不变。

`enabled_capabilities` 继续是：

```text
null / omitted -> 全部禁用
[]             -> 显式全部启用
[names...]      -> 精确启用这些 final wrapped names
```

discovery 总是保存四 surface 的完整 catalog；disabled entries 不进入 Provider/dispatch，
但仍参与 catalog digest 和 admin discovery view。

### 5.3 Secret redaction

Server MCP secrets 与 Py7 Device MCP 使用同一合同：

- PostgreSQL/备份可读取明文；Py8a 不增加 master key 或 envelope encryption；
- internal models 使用 `SecretStr` 等 secret-aware representation，validation error/repr
  不含 value；
- GET/PUT response 保留 env/header keys，所有 values 都是 `"<redacted>"`；
- PUT 的 marker 只可保留同 name、同 transport、同 sink 的同 key。stdio sink 是
  `(name, transport, command, args, cwd)`；remote sink 是
  `(name, transport, exact stored URL)`；
- `enabled_capabilities` 与 `max_concurrent_calls` 不属于 sink identity，可在不重新提交
  secret 时改变；
- sink change、rename、新 key 或新 server 必须提交真实 values；marker 跨 sink 返回
  `config_validation_failed`；省略 key 表示删除；字面 secret `"<redacted>"` 不可保存；
- marker resolution 发生在 canonical compare、candidate spawn 和 DB write 之前；
- admin 是授权边界，不是泄漏许可。API 仍不返回明文 secret。

## 6. Admin REST API 与 CAS

### 6.1 GET

`GET /api/admin/server-mcp` 只允许 admin，返回 DB authoritative state 与一个匹配该
revision 的 in-memory runtime snapshot：

```json
{
  "config_revision": 7,
  "mcp_servers": [
    {
      "name": "search",
      "transport": "streamable_http",
      "url": "https://mcp.example/mcp",
      "headers": {"authorization": "<redacted>"},
      "enabled_capabilities": [],
      "max_concurrent_calls": 8
    }
  ],
  "mcp_catalog_digest": "<sha256>",
  "mcp_discovered": {
    "search": {
      "tools": [{"raw_name": "search", "final_name": "mcp_search_search", "enabled": true}],
      "resources": [],
      "resource_templates": [],
      "prompts": []
    }
  },
  "runtimes": {
    "search": {
      "configured": true,
      "active": {
        "state": "ready",
        "origin": "persisted",
        "config_revision": 7,
        "catalog_digest": "<sha256>",
        "runtime_generation": "<uuid-new>",
        "max_concurrent_calls": 8,
        "active_calls": 2,
        "waiting_calls": 6,
        "draining_calls": 0,
        "restart_attempt": 0,
        "last_error": null
      },
      "draining": {
        "state": "draining",
        "origin": "persisted",
        "config_revision": 6,
        "catalog_digest": "<old-sha256>",
        "runtime_generation": "<uuid-old>",
        "max_concurrent_calls": 8,
        "active_calls": 0,
        "waiting_calls": 0,
        "draining_calls": 1,
        "restart_attempt": 0,
        "last_error": null
      }
    }
  }
}
```

`mcp_discovered` 是 complete last-good catalog 的 stable、non-secret REST projection；
它不返回 `entry_id`、raw invocation URI/template、output schema 或 hidden route。
`mcp_catalog_digest` 让 UI 判断 discovery 是否改变。

每个name的slot projection固定为`configured`、`active: RuntimeStatus | null`与
`draining: RuntimeStatus | null`，因此replacement期间新active与旧draining generation不会
互相覆盖；每项仍最多一个。Active state固定为`starting | discovering | ready |
unavailable | backoff | drifted`，draining state固定为`draining | cleanup_blocked`。
Counters是瞬时只读diagnostics，不是CAS input，也不承诺跨两个GET单调。
`origin`固定为`persisted | candidate`。Persisted status带非null
`config_revision/catalog_digest`；未promote candidate只可出现在`draining`，两字段为null，
不能伪装成DB authority。`runtime_generation`在generation尚未创建时为null；一旦candidate
打开transport/child就先分配generation，因此其draining status也带generation。
`last_error` 只能是 `{code, message}`，message 由 OpenOctopus 生成、bounded、sanitized；
不得透传 third-party exception、HTTP body、stderr、URL query 或 JSON-RPC error data。

`runtimes` 包含所有configured names，以及已经从config删除但仍在bounded drain/cleanup的
name；后者 `configured=false, active=null`，其`draining.config_revision/catalog_digest`
表示被retire的旧authority。cleanup结束后该项消失。这样admin看到同name暂时不能重新
添加的真实原因，而不是只得到一个无来源的409。

未promote candidate若cleanup卡住也投影在其name的`draining`：`origin=candidate`、
`state=cleanup_blocked`、`config_revision=null`、`catalog_digest=null`。若同name仍有persisted
active，则两者同时展示；若是新增name，则`configured=false, active=null`。该candidate不进入
`mcp_servers/mcp_discovered`、不能dispatch，也不泄漏其command、URL、headers或env。

若 DB revision 与 supervisor snapshot 暂时不一致，API 重新捕获一次；仍不一致时按 DB
state 返回配置，并把相应`active`标为`starting`；旧generation若仍存在只能放在
`draining`，不能把旧counters冒充新revision。

### 6.2 PUT

`PUT /api/admin/server-mcp` request：

```json
{
  "base_config_revision": 7,
  "mcp_servers": []
}
```

PUT success直接返回transition后的同一GET projection，不增加全体Device owner的同步
`impact`统计。Server-first suppression继续在各owner schema按需重算/失效；admin mutation
不在global MCP commit fence内扫描所有Device catalogs。

其余契约如下：

- body `extra="forbid"`，两个字段都 required；`mcp_servers` 是 whole-list replacement；
- `base_config_revision` 在 candidate 工作前和最终 commit 内各比较一次；stale 返回
  409 `server_mcp_config_conflict`，不保存、不 publish candidate；
- marker resolution 后精确 config no-op 且 runtime/catalog 未 drift 时直接返回，不
  initialize、不 bump revision、不替换 runtime；
- 新增或任意有效修改的 server 做真实 connect/initialize、完整 four-surface discovery
  和 Server catalog validation；任一失败使整个 PUT 不保存；
- Server candidate validation 先用 whole-list candidate names 建立 prospective reservation，
  并且只对Server candidate自身/fixed reserved namespace做collision与Provider-bound
  validation。任何既有Device entry，包括不同server name但exact final name相同者，都不能
  阻止Admin PUT；Admin candidate仍必须在自身Server configs之间满足唯一name、collision
  与Server-only Provider bounds。Device影响由commit后的shadow/suppression投影处理；
- 纯删除只从 config/catalog 确定性移除 server，不要求它当前可达；混合删除与修改仍是
  一个原子 candidate。唯一例外是同name已有draining/cleanup-blocked前代，此时删除也按
  下述single-draining规则409；
- exact config 在 runtime `drifted` 时表示管理员显式 refresh：重新 validation；若
  discovery 不同则保存新 catalog、revision +1 并替换 generation；
- exact config 在 runtime unavailable/backoff 时也触发一次显式 recovery candidate。
  fresh catalog 不同时与 drift refresh 一样原子保存并 revision +1；相同时只替换
  runtime、DB revision 不变；失败仍不改变 DB/active state；
- validation 阶段不触碰 active runtime/queue。DB commit 成功后 candidate 才 publish；
  removed/replaced slot 停止接新 call并进入 bounded drain；
- validation 可被 HTTP cancellation 取消，但必须 shield cleanup。DB transition 一旦启动，
  handler cancellation 不能取消它，必须收敛到 commit 或 rollback；
- 任一已打开transport、child或request task的未promote candidate，在validation failure、
  cancellation、最终stale CAS或whole-candidate失败时，必须在第一次cleanup await前原子登记为
  该name唯一的`draining(origin=candidate)`。Bounded close成功后删除；不收敛则转
  `cleanup_blocked`并由supervisor继续重试。请求结束、task cancellation或异常都不能遗忘它；
- DB commit 成功而 publish 遇到进程内异常时，DB 仍是 authority；slot 标为 unavailable
  并由 supervisor 按 persisted state recovery，不能回滚已经成功的 commit；
- 每个 server 最多 active + 一个 draining generation。相同name前代仍在draining/
  cleanup-blocked时，任何会add、replace或delete该name的PUT都返回409
  `server_mcp_config_conflict`；不能用第二个draining generation覆盖前代。前代cleanup完成后
  管理员重试原PUT。

独立 MCP candidate 最多并行 4 个。每 server connect+initialize 30 秒、完整 discovery
额外 30 秒，whole PUT candidate 300 秒，沿用 Py7 经验证的最坏预算。管理员配置 API
允许耗时 validation；这不占 tool-call queue，不阻塞 chat event loop。

### 6.3 事务与并发

一个 process-wide mutation guard 串行 Server MCP PUT transition。最终 DB transaction
对 `system_config.server_mcp` row 做 `SELECT ... FOR UPDATE`，再次 CAS；不存在 row 时用
固定 advisory transaction lock 序列化 first writer。

Device MCP commit 与 Server MCP commit 还共享一个短时 global MCP catalog advisory
fence。remote validation 不持 DB/advisory lock；只在最终 recheck/commit 持有。锁顺序
固定为 global MCP fence，再是 Device owner fence或 Server row，禁止反向获取。

Device candidate 在开始和 commit fence 内读取当前 Server names：

- candidate 新增当前 reserved name：409 `mcp_name_reserved_by_server`；
- candidate 的 stored config projection 对 reserved name 有任何有效修改：同样 409；
- whole-list 中原样保留既有 reserved config不算修改；
- 删除 reserved config允许；rename 视为删除旧 name + 新增新 name，新 name 未保留即可；
- admin reservation 在 Device validation 期间新增时，最终 recheck 使 Device commit
  失败并清理 candidate，不能让竞态穿透。
- Device add/modify discovery后若任一enabled final name与当前Server enabled final name
  exact冲突，即使server config name不同，也返回409 `mcp_schema_collision`；原样保留既有
  config或纯删除允许。Admin后来制造的该类collision只抑制既有Device capability，不写回。

Py8a 仍是单 ASGI worker。上述 DB fence 保留 authority correctness，但不承诺 Py13
multi-worker supervisor/queue；不得据此宣称多个 workers 可安全各自启动一套 shared MCP。

## 7. Discovery、catalog 与 Server wrapping

### 7.1 FastMCP session boundary

Server-local `openoctopus_server/mcp/` 实现 runtime、transport、catalog、result、scheduler
和 supervisor；Server 不 import `openoctopus_client`，也不新建 common Python package。
相同算法通过 shared contract fixtures 对齐。

使用 FastMCP `Client` 管理显式 transport/context/initialize/close，通过公开
`client.session` 调 MCP SDK raw APIs：

```text
list_tools / list_resources / list_resource_templates / list_prompts
send_request(ClientRequest(CallToolRequest), CallToolResult)
read_resource / get_prompt
```

不调用会注入 telemetry/执行额外 outputSchema validation 的 FastMCP convenience API；
不创建 client spans，不向 MCP 注入 `traceparent`/`tracestate`。FastMCP/MCP/httpx/httpcore
第三方 logger 在 application handler 前丢弃；OpenOctopus 只生成 sanitized lifecycle log。

### 7.2 Bounds 与 canonical catalog

四 surface 的分页、schema、RFC 6570、name normalization、result identities 与 Py7 完全
相同：

| 范围 | 上限 |
|---|---:|
| 每 surface / server pages | 16 |
| 每 server 四 surface capability 总数 | 256 |
| Server complete catalog capability 总数 | 512 |
| 单 capability canonical JSON | 256 KiB |
| Server complete catalog | 2 MiB |
| 单 raw stdio record / HTTP entity / SSE event | 12 MiB pre-decode |
| cursor | 4096 bytes |
| description | 4096 Unicode chars |
| raw name / URI | 256 / 4096 Unicode chars |
| JSON/schema nesting | 32 |
| enabled Server logical Provider names | 256 |
| Server MCP Provider schemas canonical JSON | 256 KiB |

任何 page/cursor/item/depth/raw/decoded byte 超限或任一 advertised surface list 失败，使
whole candidate 失败；不保存 partial catalog。disabled entries计入完整 catalog limits，
不计 Provider limits。

final name 继续按 NFKC/lower/fold/trim 生成 `mcp_<server>_<alias>`，不截断、不 suffix、
不 hash。四 surface 共用 namespace；filter 后同一 Server config 内的 intra/cross-surface
collision 整体拒绝。Server configs 的 names 唯一；不同 Server names 若因 underscore
边界等原因生成完全相同的 final name，Admin whole candidate 也整体拒绝，不能按字符串
反解析合并、覆盖或 suffix。`mcp_` 仍为 dynamic MCP 保留，fixed tools 不准使用该前缀。

catalog entry 与 digest projection 沿用 Py7：Server-assigned UUIDv7 entry id、明确 surface、
raw name/invocation identity、provider description、canonical input/output schema、enabled；
digest 排除随机 entry id、envelope digest和 secrets，数组先 canonical sort，SHA-256
lowercase。逻辑相同的 unchanged entries复用 entry id。

### 7.3 Provider schema

每个 enabled Server entry 生成一个 Provider tool，required routing field 固定为：

```json
{
  "openoctopus_device": {
    "type": "string",
    "enum": ["server"],
    "x-openoctopus-device": true
  }
}
```

Provider-visible description/input schema 与 Py7 Device 同 logical entry 必须使用同一
canonical builder。Server namespace reservation意味着同 name 的 Device entry不参与
equal-site merge；即使 schema 完全相同，也只展示 Server route，而不是
`["server", "laptop"]`。管理员移除 reservation 后，Device sites按 Py7 原规则恢复合并。

Provider 看不到 transport、URL、command、cwd、env/header、entry id、config revision、
catalog digest、runtime state、queue counters、suppression reason 或 generation。

## 8. Namespace reservation 与 Device suppression

### 8.1 Reservation

reservation set 精确等于当前 persisted Server config 的 `server.name` 集合。它不取决于：

- Server runtime 是否 ready/down/drifted；
- catalog 是否有 enabled capability；
- `enabled_capabilities` 是 null、空或非空；
- Device capability 与 Server capability 是否同 schema；
- Device 在线、runtime 是否已注册。

对每个 Device persisted entry，先检查 `entry.server in reserved_names`。命中者标记
`server_namespace_reserved`，不进入后续 Device merge、Provider capacity 或 route table。
Client registration仍按 Py7 正常验证 revision/digest/generation；Server 可保留 binding
用于 reservation 移除后的下一次 snapshot，不向 Client 发送 stop/remove。

admin PUT 新增 reservation 时：

- 尚未跨 Device WS issued boundary 的旧 Device route必须 pre-send fail；
- 已经 issued 的 Device call按 Py7 generation/outcome 语义完成，绝不 replay；
- Device queue、runtime、config/catalog和 config revision不改变；
- tool shape cache以 Server config revision为 key立即失效。

admin PUT 移除 reservation 时，下一次 schema build重新包含未被容量抑制的 Device entry；
无需 Device write/config push/discovery/reconnect。若 Device offline，schema仍来自 last-good，
调用按既有规则返回 `tool_device_unreachable`。

### 8.2 Server-first capacity

Provider MCP budget仍是每 user、每 Provider iteration 256 enabled logical names 和
256 KiB canonical Provider schemas，不包括 fixed built-ins。

构建算法固定为：

1. 从 Server last-good catalog生成全部 enabled Server schemas，按 final name排序；
2. 若 Server schemas本身超过任一 budget，说明持久 invariant损坏；正常 admin candidate
   必须在 commit 前以 `mcp_server_schema_limit` 拒绝，不能落库；
3. 从该 user全部 paired Device last-good catalogs收集 enabled entries；先移除 namespace
   shadow，再把final name已存在于Server schema set的既有Device entries整组标记
   `server_final_name_collision`，其余按Py7 logical identity/schema合并install sites；
4. Device logical groups按 final name升序；每组的 Device names也升序，先生成完整
   `openoctopus_device` enum；
5. 依次尝试把整组追加到 Server schemas。追加后 count和完整 canonical JSON都不超限
   才保留；否则该整组标记 `provider_capacity` 并继续检查后续组；不拆 install sites，
   不截断 schema，不因一个大 schema阻止后面的较小 schema；
6. Provider schemas与hidden routes只包含最终保留组，且共享同一个 suppression snapshot。

这是一种 stable greedy selection：输入 catalogs、names、Server revision不变时结果逐 byte
不变。Device create/delete/rename/catalog commit或Server config/catalog commit使对应 cache
失效。online/offline、runtime availability、queue counters不改变 selection。

Py7 的 Device-only hard bounds仍保留：一个 Device最多 512 complete entries/2 MiB catalog，
一个 owner 的未考虑 Server budget前 Device enabled aggregate仍须满足既有 256/256 KiB
validation。Py8a 的 suppression只处理“合法 Device catalog + 优先 Server catalog”后的
组合超限，不允许用户借此保存原本就违反 Py7 Device bounds 的 candidate。

### 8.3 Device API 可见性

`GET/PATCH /api/devices/{name}/config` response 使用独立的 response-only MCP config
projection；request/storage DTO 不增加这些派生字段。每个 `mcp_servers[]` entry 增加：

```json
{
  "effective_status": "shadowed_by_server",
  "shadowed_by": "search"
}
```

`effective_status` 只能是 `active | shadowed_by_server`。server name 命中当前
reservation 时返回 `shadowed_by_server`，且 `shadowed_by` 是 exact Server config name；
否则返回 `active` 与 `shadowed_by=null`。capacity或exact-final-name suppression只影响
capability，不改变config-level status。字段不持久化，也不允许出现在 PATCH input。

每个 `mcp_discovered` capability 保留原
`enabled`，新增：

```json
{
  "provider_visible": false,
  "suppression_reason": "server_namespace_reserved"
}
```

`suppression_reason` 只能是 `server_namespace_reserved | server_final_name_collision |
provider_capacity | null`：

- disabled entry：`enabled=false, provider_visible=false, suppression_reason=null`；
- enabled且进入当前 owner Provider snapshot：`provider_visible=true, ...=null`；
- enabled但被 namespace/final-name/capacity抑制：`provider_visible=false` 并给出精确
  reason。

Device list summary新增 `mcp_provider_visible_capability_count` 与
`mcp_suppressed_capability_count`，原 `mcp_enabled_capability_count` 继续表示配置选择，
不静默改义。三个 count 都按 Device catalog entry 计数，并始终满足
`visible + suppressed = enabled`。API projection读取同一个 Server revision和owner catalogs
snapshot，不能与实际 registry使用不同排序/容量算法。

API不写 suppression到 Device row/catalog；它是 global Server config + owner catalogs 的
派生状态。Server restart或admin removal后可自动重新计算。

## 9. Runtime 与 transport lifecycle

### 9.1 `RuntimeSlot`

每个process-reserved Server name对应一个process-lifetime slot；集合是当前configured names、
已删除但persisted draining/cleanup尚未完成的names，以及未promote candidate仍在draining/
cleanup的names之并集：

```text
persisted config/catalog | bounded candidate-cleanup metadata
active RuntimeGeneration | null
at most one draining generation
fair waiting queues keyed by user_id
active/draining counters
retry/list_changed tasks
sanitized last_error
```

一个 generation拥有恰好一个 FastMCP client/session。MCP SDK通过 JSON-RPC request id在
同一 session上 multiplex并发 request；`max_concurrent_calls` 是该 session的并发 permit，
不是创建多个 client。

Process-reserved name slots硬上限同config names为16。Admin candidate在validation开始与
最终mutation guard内都检查：

```text
candidate configured names
  ∪ existing removed-or-candidate-cleanup-not-finished names
  ∪ retiring current names
```

其中`retiring current names = current configured names - candidate configured names`，只要旧
generation在commit前未确认closed就计入。超过16返回409
`server_mcp_config_conflict`，不保存。因此16个旧name不能在一个PUT中全量换成16个新name；
管理员先删除、等待cleanup释放credit，再添加。Removed或unpromoted-candidate slot直到cleanup
结束才释放name credit；Server restart从durable config重建，旧进程内retired slots不恢复。

状态迁移为：

```text
STARTING -> DISCOVERING -> READY
                       -> DRIFTED
                       -> UNAVAILABLE -> BACKOFF -> STARTING
READY -> DRAINING -> closed
close cannot converge -> CLEANUP_BLOCKED
```

candidate validation runtime与active slot隔离；成功 DB commit后才 promote。每个新增/修改
name在开始validation前都必须确认没有draining generation；否则不启动candidate并返回409。
未promote candidate按上文先登记再cleanup，因此同name仍满足最多一个draining。Removed/replaced
slot立即拒绝/清空未发送 queue；active calls仍绑定旧 generation并按第 11 节收敛。

### 9.2 Startup 与 recovery

PostgreSQL/RustFS等 required startup完成后，Server读取并严格验证 envelope，立即以 durable
catalog建立 Provider authority，然后异步启动各 MCP runtime。lifespan不等待所有 MCP
connect/discovery；因此坏掉的 npx package或远程搜索不会阻止 HTTP/chat ready。

每次 startup/recovery都重新 initialize并完整 discover：

- fresh digest等于 persisted server catalog：generation READY；
- fresh digest不同：DRIFTED，保留旧 Provider schema但不以旧 route调用新 runtime；
- connect/discovery失败：UNAVAILABLE/BACKOFF，保留旧 schema；
- 401/403、missing executable、invalid protocol/config等 permanent failure suspended，
  等待 admin PUT或Server restart；普通网络/child exit使用 1/2/4/8/.../60 秒 capped
  exponential backoff + jitter；
- `list_changed` debounce/coalesce后做 bounded full rediscovery；相同保持 READY，不同
  DRIFTED；不自动接受动态 schema；
- static resource内容更新不改变 schema，不订阅/持久化资源内容。

独立 runtime 的 startup/recovery connect attempt 全局最多并行 4 个；retry task 可同时
存在但必须先取得该 admission，不能在 endpoint 集体故障时同时打开 16 套连接。

runtime unavailable时Provider仍看到 durable tool；dispatch返回 `tool_mcp_unavailable`。
管理员可对相同 config做 PUT refresh并以 CAS接受新 catalog。

### 9.3 Stdio transport

Server只支持 Linux部署，但仍使用独立 bounded stdio transport而不是 stock transport的
无界读取路径：

- `asyncio.create_subprocess_exec(command, *args, shell=False)`，direct argv；
- 新 process session/group，stdin/stdout专用 pipe，stderr固定到 Server持有的 devnull；
- child env只取 POSIX safe baseline `HOME, LOGNAME, PATH, SHELL, TERM, USER` 加 config
  overlay；合并前后删除大小写折叠 `OPENOCTOPUS_*`；不继承数据库、LLM、object storage、
  JWT、admin token或其它 Server ambient env；
- default cwd是 Server OS user home；配置 cwd展开 `~` 后必须是存在的绝对 directory；
- stdout按 LF record做 12 MiB pre-decode cap；没有 LF时也持续计数；
- close先拒绝新 request并给 protocol close/stdin EOF 2 秒，再 terminate process group 3 秒，
  最后 kill 5 秒；每阶段shield cancellation，总计最多10秒；
- cleanup不能确认收敛则 `cleanup_blocked`，同 sink不启动replacement，防止child累积。

stdio timeout/issued Stop立即 retire整个 generation并执行上述tree cleanup；其它并发stdio
调用也成为 outcome unknown。stdio默认 concurrency=1可避免通常的连带影响；管理员显式
提高到 2..32 即接受共享stdio程序的并发安全和generation-wide failure blast radius。

### 9.4 Remote transport

Streamable HTTP/SSE复用Py7 bounded HTTP client factory：

- `follow_redirects=false, trust_env=false, verify=true`；不支持custom CA/OAuth；
- non-empty headers要求HTTPS；禁止hop-by-hop/MCP transport-owned headers；
- `Accept-Encoding: identity`，任何其它 Content-Encoding在读body前拒绝；
- JSON response entity或单个完整SSE event做12 MiB pre-decode cap；长连接累计bytes不作为
  单 event cap；
- configured endpoint可访问private/loopback/metadata网络，不应用Server
  `web_fetch_denylist`；这是admin-gated能力；
- transport意外关闭使该generation全部可能issued calls outcome unknown，未发送queue
  返回 unavailable，随后bounded close/backoff；不重发JSON-RPC request。

remote FastMCP/client/HTTP close 使用 10 秒 shielded cleanup deadline。cleanup 仍不收敛时
slot进入 `cleanup_blocked`，继续持有受影响permits并禁止同name replacement；不能通过
遗忘旧client来恢复表面可用性。

Streamable HTTP SDK同session stream resumption若不重发JSON-RPC request，可作为transport
delivery recovery；它不是OpenOctopus invocation replay，也不创建第二generation。

## 10. 有界公平 admission

### 10.1 固定数值

| 范围 | 合同 |
|---|---:|
| per-runtime active | `max_concurrent_calls`；stdio默认1，remote默认8，范围1..32 |
| per-runtime waiting capacity | `min(128, max(8, 4 * max_concurrent_calls))` |
| waiting deadline | 5 s |
| queue + invocation public deadline | 60 s |
| all Server MCP global active/draining | 32 |
| one user across all Server MCP active/draining | 4 |
| remote post-public-timeout drain | 额外60 s |

`active/draining`都占per-runtime、global和per-user permit。Device MCP、web_fetch、LLM和
其它admission有自己的既有caps，不计入这32/4。

### 10.2 Scheduler

每个slot维护 `dict[user_id, deque[QueuedCall]]` 与一个ready-user ring：

- 同user按单调enqueue sequence FIFO；
- 每次成功dispatch一个call后把仍有queue的user移到ring尾部；
- scheduler只有在per-runtime、global和该user三个counter都可非阻塞reserve时才把call
  标为issued；不能先持有一个permit再作为另一个semaphore的waiter；
- global/user counters由一个进程内coordinator在同一async lock下reserve/release；
- global capacity释放会唤醒有queue的slots；跨不同MCP不承诺业务priority，但单slot内
  不允许一个user用大量session饿死其它user；
- queue只保存bounded metadata、args reference和一个completion future；总waiting受
  每slot cap，process-reserved slot总数（含removed cleanup）硬限16，因此process task/
  memory有确定上界；
- full queue立即 `tool_mcp_busy`；等待5秒未issued也返回同code；两者都保证MCP未收到；
- caller在queue中取消/Stop时原子移除，保证未发送；
- route/config/generation在send前失效时移除并返回 `tool_mcp_unavailable`；
- config replacement不迁移old queue。old slot所有waiting以unavailable收敛；new slot使用
  fresh empty queue；
- 60秒public deadline从进入Server MCP dispatch开始，包含queue时间。等待5秒后issued的
  call最多只剩55秒transport预算。

单独使用 `asyncio.Semaphore` 不满足本合同，因为它既不能限制waiter数量，也不提供这里
定义的user FIFO/round-robin与non-blocking multi-cap reservation。

### 10.3 500-user语义

queue是backpressure，不是capacity expansion。500个Agent同时调用默认remote搜索时：

- runtime active不超过8，waiting不超过32；
- global active不超过32，每user active不超过4；
- 超出waiting capacity者立即busy；进入queue但5秒内未拿到permit者busy；
- `/health`、chat、其它MCP/event loop仍响应，task/future/memory/FD保持有界；
- queue清空后runtime恢复正常。

验收不要求500次调用全部成功。若业务要求全部成功，管理员必须确认remote MCP/SearXNG
吞吐并提高单session concurrency；单session仍不足时需要Py14 runtime pool/load balancing，
不能把排队描述成扩容。

## 11. Invocation、timeout 与 drain

### 11.1 Send boundary

调用先验证本次Agent iteration冻结的route，再进入queue。scheduler拿齐permits后立即在
同一critical transition中标记 `issued` 并创建transport invocation task。此点之前的
失败可证明未发送；此点之后包括task cancellation、connection failure和write结果不明
都按“可能发送”处理。

四surface调用API与Py7一致：tool用raw
`send_request(ClientRequest(CallToolRequest), CallToolResult)`；resource用
`read_resource`；prompt用`get_prompt`。OpenOctopus只校验自己的envelope、route和bounds，
不在每次调用重新执行动态inputSchema。

### 11.2 Remote public timeout / Stop

remote invocation创建独立task，caller以 `asyncio.shield` 等待到60秒public deadline：

- deadline前成功/已知错误：正常map result并释放全部permits；
- public timeout：caller得到 `tool_execution_outcome_unknown`；underlying task不取消，
  进入最多额外60秒drain；
- Agent Stop/caller cancellation：调用对Agent停止，但underlying task同样shield drain；
  turn repair沿现有`user_cancelled`合同，MCP side effect outcome仍未知；
- late result在drain内到达：完整读取、bounded parse/map后丢弃，不追加到已结束turn，
  释放permits；
- 额外60秒仍不结束：retire generation。关闭session会使同generation其它未完成calls
  outcome unknown；supervisor随后reconnect；
- drain task/counter/future都由slot持有并有硬上界，不能成为orphan task；
- Server不依据tool名猜测幂等性，不自动retry。

### 11.3 Stdio timeout / Stop

stdio issued call达到public deadline或收到Stop时立即retire process generation，不做额外
remote-style 60秒protocol drain。process tree按2/3/5秒close收敛；permits直到tree/result
边界明确关闭才释放。所有受影响issued calls返回/记录outcome unknown，queued calls
unavailable，之后才可启动replacement。

### 11.4 Replacement 与 shutdown

config commit后old generation停止接新call，queue先失败；issued calls允许最多60秒
generation drain。deadline内完成的result仍只交给原pending caller；deadline后force
close。route/result包含generation identity，late completion不能污染new generation。

Server shutdown先停止admin mutation和新Server MCP admission，清空waiting，再对active/
draining generations执行同样bounded close。lifespan cleanup shield cancellation并最终
关闭stdio trees、HTTP clients、scheduler和retry/list_changed tasks。

## 12. Result mapping 与稳定错误

### 12.1 Result mapping

Py8a直接复用Py7的确定性all-or-nothing mapping：

- TextContent、四种safe image MIME、ResourceLink、text/blob EmbeddedResource、
  structuredContent和prompt role/order使用相同labels/canonical JSON；
- Audio/其它binary为 `tool_unsupported_media`；invalid base64/JSON/unknown block为
  `tool_mcp_invalid_result`；
- MCP `isError=true` 和deadline内JSON-RPC error为 `tool_mcp_error`；不透传third-party
  error message/data；
- 空成功结果为 `(no output)`；最终Provider text默认16,000 chars，image/frame使用既有
  credit；超限all-or-nothing `tool_result_too_large`；
- 所有Provider-facing结果先加现有Server-authored untrusted tool-result warning；
- runtime共享不改变conversation ownership：每个mapped result只进入发起它的turn。

### 12.2 Stable codes

| 场景 | code / HTTP |
|---|---|
| queue full或5秒wait expiry，明确未发送 | `tool_mcp_busy` |
| runtime starting/down/drifted、old queue退役、pre-send route失效 | `tool_mcp_unavailable` |
| MCP isError或已知JSON-RPC error | `tool_mcp_error` |
| raw MCP message超限 | `tool_mcp_message_too_large` |
| unsupported media | `tool_unsupported_media` |
| invalid typed/result content | `tool_mcp_invalid_result` |
| final encoded result超credit | `tool_result_too_large` |
| issued/maybe-issued后timeout、Stop、disconnect、hard retire | `tool_execution_outcome_unknown` |
| admin stale CAS或同name generation仍cleanup | `server_mcp_config_conflict` / 409 |
| Device stale CAS | 既有 `device_config_conflict` / 409 |
| Device新增/修改Server-reserved name | `mcp_name_reserved_by_server` / 409 |
| Device新增/修改能力与Server exact final name冲突 | `mcp_schema_collision` / 409 |
| config syntax、secret marker、discovery shape错误 | `config_validation_failed` / 422 |
| spawn/initialize失败 | `mcp_spawn_failed` / 422 |
| candidate raw message超限 | `mcp_message_too_large` / 422 |
| enabled intra/cross-surface collision | `mcp_within_server_collision` / 409 |
| 不同 Server configs 的 exact final-name collision | `mcp_schema_collision` / 409 |
| Server Provider count/schema超限 | `mcp_server_schema_limit` / 409 |
| Device自身owner aggregate collision/limit | 既有 `mcp_schema_collision` / `mcp_owner_schema_limit` |

`tool_mcp_busy`与`tool_mcp_unavailable`都保证当前调用未发送，Agent可根据上下文稍后重试
或选择Device tool；只有outcome unknown必须明确提示不要盲目重放。所有tool errors是
普通bounded result，Agent loop继续。

## 13. Agent-turn snapshot 与 dispatch

每个Provider iteration在一次逻辑DB snapshot中读取：

- global Server MCP config revision与last-good catalog；
- 当前user所有paired Device revision/catalog；
- structured reservation set；
- deterministic Device suppression结果；
- supervisor中匹配global revision/catalog的active runtime generations。

schema builder顺序固定为fixed tools、全部Server enabled entries、选择后的Device logical
groups。Provider shape cache key至少包含global catalog shape key、reservation revision和
owner Device shape key；runtime availability/counters不进入shape key。

hidden Server route冻结：

```text
install_site=server
entry_id
server name + surface + source identity + final name
server config_revision + catalog_digest
runtime_generation（READY时；否则null）
```

hidden Device route继续冻结Py7 fields，并额外捕获global Server config revision，确保admin
新增reservation后旧Device route在send前失败。dispatch顺序：

1. 核对caller user/session ownership与Provider final name；
2. 核对当前global revision/catalog digest；
3. Server route核对entry仍enabled、未suppressed、slot generation仍exact READY；
4. Device route核对当前name未reserved，再执行Py7 Device revision/digest/binding checks；
5. 移除`openoctopus_device`，仅把source args交给目标runtime；
6. Server route进入第10节queue；Device route仍走现有Device WS。

任何check失败都不得把old args绑定到new schema/generation。issued call继续由其原generation
持有；admin PUT不能把late result匹配到新route。

## 14. Degraded startup、health 与 diagnostics

`/health`继续只检查required PostgreSQL与RustFS：

- DB/RustFS healthy时始终200和现有 `status=ok`，即使所有Server MCP unavailable；
- MCP failure不新增503条件，不把load balancer诱导进restart loop；
- `/health`不connect、discover、ping或等待remote MCP，也不暴露MCP names/errors；
- DB authoritative envelope corruption仍可使startup fail，因为这不是optional endpoint
  outage。

Server MCP aggregate/per-runtime degraded状态只在admin GET呈现。普通用户通过稳定tool error
观察availability；不新增公开status endpoint、SSE event或每用户runtime diagnostics。

runtime state/counters采集必须是non-blocking immutable snapshot；health/admin GET不能争抢
tool queue permit或等待MCP network。

## 15. 安全与信任边界

Py8a的真实模型是“admin显式安装可信Server代码”：

- stdio child与OpenOctopus同UID。它可读取该UID可达文件、访问网络、观察同UID进程或
  主动泄漏configured secret；safe env不是OS isolation；
- 因此管理员只能安装审计过的package/command。普通用户与Agent不能提交command；
- direct argv、safe env、no stderr/log、bounded frames和process-group cleanup用于减少
  意外泄漏/失控，不把恶意child变安全；
- remote MCP可访问Server网络，包括内网；admin已有stdio代码执行权限，因此不把
  `web_fetch_denylist`错误复用为MCP SSRF policy；
- env/header credential是全体用户通过声明tools共享的service account credential。
  个人Slack/OAuth、个人数据或按用户授权的API必须安装为Device MCP；
- OpenOctopus不向MCP隐式传user id、email、session、workspace、Device token、JWT、LLM
  key、object-storage key或admin token；
- MCP output仍是不可信内容，保持统一untrusted result warning，不因admin安装而信任
  output指令。

允许的lifecycle log fields：server name、transport、state、attempt、elapsed、capability
counts、digest前12 chars、active/waiting/draining counts和stable code。禁止command args、
cwd、完整URL/query、headers、env、stderr、tool input/output、raw JSON-RPC、third-party
exception/body和user id。公平性测试可用不可逆/ephemeral test label，不在生产log输出
user队列身份。

Py8 milestone在canonical map中改称“Server MCP security boundary + shared runtime”；Py8a
不交付“Server sandbox”。若未来要运行不可信admin package，必须另写容器/bwrap等OS隔离
设计，不能在本spec上追加一句“best effort sandbox”。

## 16. TDD 实现顺序

### Slice A：Models、persistence 与 Admin API

先写失败测试覆盖：absent-row revision1、single-envelope strict parse、config/catalog atomic、
whole-list CAS两次check、no-op、deletion、secret redact/retain/cross-sink reject、concurrency
defaults/limits、HTTP cancellation before/after transition。再实现Server-local DTO/service/route
与stable errors，不启动runtime。

### Slice B：Server transport、discovery 与 result

把FastMCP/MCP/uritemplate版本pin到Server；用真实fake stdio、Streamable HTTP、SSE先测
initialize、四surface pagination、12MiB pre-decode caps、safe env、direct argv、stderr/log/
OTel discard、no redirect/trust_env、2/3/5 close。复用Py7 canonical fixtures验证name、
RFC6570、digest、entry-id reuse、all-or-nothing result mapping；不复制出第二套语义。

### Slice C：Supervisor 与 validate-before-save

先测candidate隔离、parallelism4、30/30/300 deadlines、跨Server exact final-name collision、
commit-before-publish、DB commit成功
后activation failure recovery、startup degraded、backoff/permanent failure、drift/list_changed、
same-config refresh、active+one-draining bound和cancellation-safe shutdown。用barriers覆盖CAS、
commit和generation replacement每个await boundary；GET projection必须同时展示同name的
new active与old draining status；old draining存在时同name delete/replace/add都409，cleanup后
可重试。无前代draining的删除展示`active=null`直到当前generation cleanup结束。Discovery
failure、HTTP cancellation与最终stale CAS分别注入close不收敛，验证未promote candidate以
`origin=candidate, cleanup_blocked`占用唯一draining/name credit，且GET不伪造persisted revision/
catalog；cleanup完成后才消失并允许重试。

### Slice D：Fair scheduler 与 timeout drain

先用fake multiplex session测stdio/remote defaults、1..32、queue公式/full、5秒expiry、60秒
overall、user FIFO、cross-user round-robin、global32/user4、queued cancellation和route retire。
再测remote public timeout/Stop shield drain、late consume、额外60秒hard retire、permit直到
drain才释放，以及stdio timeout立即tree retire。所有clock用fake monotonic clock/barrier，
不写sleep-based flaky test。

### Slice E：Registry reservation、capacity 与 dispatch

先测admin name对既有Device四surface全shadow、runtime/register不停止、unavailable Server仍
reserve、结构化name不误伤prefix、同名Device schema不阻止Admin PUT、admin removal自动恢复、
Device add/modify 409、unchanged/delete/rename允许和admin-vs-Device commit race。再测
Server-first count/byte budget、Server-vs-既有Device exact final-name suppression、Device
add/modify collision 409、stable greedy Device suppression、config-level shadow projection、
capability reasons/counts、cache invalidation、frozen global revision和
late old-generation result。

### Slice F：Canonical docs、CI 与 E2E

同步第18节canonical docs与contract snapshots；运行Server full pytest、Ruff、strict mypy。
Docker PostgreSQL/RustFS/Server + fake provider + 三transport MCP完成真实REST/Agent E2E，
再运行500-user bounded-pressure gate与process/FD/task/connection leak检查。

## 17. 测试矩阵与合并门禁

### 17.1 自动测试

Linux Server CI必须覆盖：

- PostgreSQL-backed envelope/CAS/config+catalog atomic transaction；
- GET redaction、runtime diagnostics sanitization与admin authorization；
- stdio real child及HTTP/SSE real ASGI/TCP transport，不以纯mock替代transport boundary；
- four-surface多页、cursor循环、schema/catalog/raw limits和所有result mapping；
- runtime startup/recovery/drift/replacement/shutdown及cancellation/late result；
- fair queue deterministic order、所有caps、timeout/drain、no replay；
- Device namespace reservation mutation races、shadow/restore、capacity suppression/API projection；
- Agent loop在busy/unavailable/error/outcome unknown后继续，并保持tool-use/result pairing；
- `/health`在Server MCP全down时仍200，DB/RustFS down时仍按现有合同503；
- TRACE级root/application logger、recording OTel exporter和secret sentinel无泄漏。

### 17.2 容量与资源

边界测试至少包括：

- 16 configs、每server 256、complete catalog 512/2MiB、Server Provider 256/256KiB；
- concurrency 1/8/32对应queue 8/32/128；第129个waiting立即busy；
- global active 32、每user active 4，跨16 runtimes仍不越界；
- 一个user连续100 calls、其它user后到时round-robin不饥饿；同user保持FIFO；
- 500 unique users同时搜索，记录accepted/queued/busy/expired，task/future/queue/RSS/FD/HTTP
  connection high-water保持bound；不要求500次全成功；
- remote timeout后60秒内迟到不伤其它call，hard expiry只保留一个replacement；
- repeated add/replace/delete/crash/recovery后无child、process group、HTTP stream、task、
  future、queue或generation leak；
- 16个不同name的removed cleanup-blocked slots耗尽process name credits，第17个add以409拒绝；
  任一cleanup完成后credit恢复；
- 当前16个name全量替换成16个新name因prospective 32 credits而409；纯删除可进入16个retiring
  slots，全部cleanup后再添加16个新name；
- Server capabilities占用不同count/byte预算时，Device suppression顺序和API reason稳定。

### 17.3 真实 smoke

合并前执行一次：

1. Docker启动PostgreSQL、RustFS、OpenOctopus Server和Anthropic-compatible fake provider；
2. 注册admin与至少两个普通用户，各配一个Device MCP；
3. admin GET revision1空state，PUT添加stdio/HTTP/SSE并验证secret redaction/CAS；
4. 验证tool、static resource、template、prompt都以`server`site被Agent调用；
5. 添加与既有Device相同name的Server MCP，确认Device config/runtime仍在、Provider被shadow；
6. 删除Server name，确认Device capability无需重连自动恢复；
7. 关闭remote/killchild，确认schema保留、tool unavailable、health 200和后台恢复；
8. 制造schema drift，用相同config PUT显式接受新catalog；
9. 运行500-user搜索gate，确认bounded active/waiting/busy和Server响应；
10. 制造remote timeout与late response，确认shield drain/no replay；制造stdio timeout，确认
    process tree retire；
11. shutdown后确认无MCP child/connection/task/container残留。

至少一次remote smoke使用真实SearXNG或等价真实search MCP wrapper，验证单shared session
并发；确定性CI仍使用本地fake，不依赖公网。任何credential只通过admin API临时提交，
不写fixture、artifact、commit或log。

## 18. Canonical docs 同步

实现PR必须同时更新，而不是让本design长期覆盖旧契约：

- `docs/DECISIONS.md`：ADR-047/049/114/133、ADR-072 admin exception、Py8 milestone名称/
  exit gate、queue/degraded/no-sandbox决策；删除Server MCP deferred措辞；
- `docs/SCHEMA.md`：`system_config.server_mcp`单envelope、revision/config/catalog/secret合同；
- `docs/API.yaml`：admin GET/PUT schemas/CAS/redaction/runtime counters/errors，以及Device
  config-level shadow projection、`provider_visible`/`suppression_reason`/summary counts；
- `docs/TOOLS.md`：Server四surface、namespace reservation、Server-first suppression、queue/
  timeout/result/errors和`server`route；
- `docs/SYSTEM_PROMPT.md`：动态MCP catalog可包含Server install site；只描述用户可采取的
  tool行为，不塞admin implementation narration；
- `docs/PROTOCOL.md`：审计并移除“Server MCP仍不存在”的文字；Protocol v3 wire不变，
  不新增frame、不升级v4；
- `docs/reference/adr-audit-python-main.md`：把Py8 placeholder更新为本spec结论；
- Server dependency/README、OpenAPI snapshots、error-code/catalog fixtures与测试说明。

Py7历史design只在顶部增加“Server MCP由Py8a扩展”的短指针（若实现时确有歧义），不
改写其Device MCP历史决策。

## 19. Acceptance gate

Py8a只有同时满足以下条件才完成：

- admin whole-list GET/PUT、CAS、single-envelope atomic persistence和secret redaction通过；
- 三transport、四surface与FastMCP 3.4.7真实integration通过；
- 每configured MCP始终只有一个shared active client/session，无pool/per-user runtime；
- name reservation对既有Device只shadow、不stop/delete，new/modified拒绝，removal自动恢复；
- Server-first Provider budget和deterministic Device suppression在schema、dispatch、API三处
  完全一致；
- Admin candidate不被任何Device collision阻止；Server间exact final name拒绝，既有Device
  exact final-name collision由Server优先抑制且新/修改Device candidate返回409；
- queue/user fairness/global caps/500-user backpressure以high-water artifact证明有界；
- remote shield drain60秒、stdio retire、generation replacement、late result和no replay
  都有deterministic regression test；
- admin GET在replacement期间分别返回同name的active/draining generation diagnostics，不把
  两代counter/error互相覆盖；
- 每name最多一个draining generation，process-reserved names含removed与candidate cleanup且总数
  不超过16；
- MCP全down时Server degraded运行、last-good schema保留且`/health`仍遵守既有required
  dependency合同；
- trusted same-UID stdio、无OS sandbox、remote network边界在code/docs/UI wording中诚实；
- Server full pytest、Ruff、strict mypy、Docker E2E与真实search smoke通过；
- 第18节canonical docs全部与实现一致；
- implementation diff不提前实现Py8b/Py8c、OAuth、pool、multi-worker或frontend。

## 20. 决策状态

本spec已闭合Py8a开始实现前的产品与结构决策；没有要求实现者暗选的开放分支。实现按
第16节TDD slices推进。若实现验证发现FastMCP 3.4.7不能在单session上安全multiplex已
声明的并发，必须停止并以实测证据修订本spec；不得静默退化成无界串行队列、创建client
pool或谎称500-user gate已满足。

参考的一手契约：

- [FastMCP 3.4.7 release](https://github.com/PrefectHQ/fastmcp/releases/tag/v3.4.7)
- [FastMCP Client](https://gofastmcp.com/clients/client)
- [FastMCP transports](https://gofastmcp.com/clients/transports)
- [MCP cursor pagination](https://modelcontextprotocol.io/specification/2025-11-25/server/utilities/pagination)
- [MCP schema reference](https://modelcontextprotocol.io/specification/2025-11-25/schema)
- [uritemplate 4.2.0](https://pypi.org/project/uritemplate/4.2.0/)
