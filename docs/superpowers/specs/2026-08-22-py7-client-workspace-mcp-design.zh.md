# Py7 Client Workspace Restriction 与 Client MCP 设计

**状态：** accepted，开始实现
**Milestone：** Py7 Client workspace restriction + client-side MCP
**依赖：** 已完成的 Py5 Python Client/device files 与 Py6 cross-platform exec
**目标协议：** Protocol v3

本设计在 Python-main 中取代以下旧约定：

- `sandbox_mode` 及其同时控制文件、exec 和网络的旧语义；
- ADR-047/048/049/099/100/105 中面向 Rust、stdio-only、三 surface、
  typed-infix、`enabled_tools`、离线乐观保存和 per-WS catalog 的部分；
- Py6 “只有 `sandbox_mode=false` 才暴露 exec/PTY”以及首次连接永久重试的约定；
- `API.yaml` 中仅供未来参考的 `McpServerConfig(name + command:list)`；
- `PROTOCOL.md` 中 Protocol v2 不支持 MCP 的边界。

Py8 的 admin shared-service Server MCP 仍然保留，不在 Py7 提前实现。

## 1. 结果与边界

Py7 完成两个相互独立的能力：

1. 把含义过重的 `sandbox_mode` 改为诚实的
   `restrict_to_workspace`。它只约束 OpenOctopus 自己解析的本地文件路径和
   exec/PTY 的初始 `working_dir`，不声称提供 OS sandbox。
2. 允许用户通过 Server API 为自己的在线 Device 配置 MCP；MCP runtime 真正运行
   在 Client 上，支持 stdio、Streamable HTTP 和 legacy SSE，并把 tools、静态
   resources、resource templates、prompts 四类能力统一包装成 Agent tools。

此外，Py7 把 Server 自身 `web_fetch` 的 SSRF denylist 纳入现有 admin config
热配置，并把 Client 初次连接失败与已成功运行后的断线重连语义分开。

Py7 不引入 OS jail、命令文本分析、exec 网络过滤、OAuth、server-side MCP 或
MCP session 的磁盘恢复。用户安装的 stdio MCP、exec 和 PTY 都是以 Client 宿主
用户权限运行的可信代码边界。

## 2. 已确定的决策

1. **直接升级 Protocol v3。** 不兼容 v2，不做 fallback、版本协商或字段 alias。
2. **字段直接改名。** DB、REST、wire、代码和文档只接受
   `restrict_to_workspace`；旧 `sandbox_mode` 作为 unknown field 拒绝。
3. **workspace restriction 不是 OS sandbox。** `true` 只限制结构化路径与 shell
   的初始 cwd；shell command 和 stdio MCP 仍可读取宿主其它路径并联网。
4. **网络策略独立。** `ssrf_denylist` 只约束同一 install site 的 `web_fetch`；
   exec/PTY、stdio MCP、remote MCP 和 Client→Server 连接不受它控制。
5. **Server 无 exec。** Server 只为自己的 `web_fetch` 使用 admin 热更新 denylist，
   不引入 bwrap。
6. **Client MCP 由 Server 托管配置。** `env` 与 HTTP headers 明文存 PostgreSQL；
   REST、日志和错误全部脱敏，目标 Device 的私有 WSS config frame 携带明文。
7. **FastMCP 固定版本。** 使用 `fastmcp-slim[client]==3.4.7`；不采用 FastMCP 4
   beta，不使用 root `fastmcp` 包，也不让 FastMCP 自动猜 transport。
8. **三种显式 transport。** `stdio`、`streamable_http`、`sse`；HTTP 是推荐项，
   SSE 仅兼容旧服务，不自动 fallback。
9. **四个 discovery surface。** tools、static resources、resource templates、
   prompts 分别发现、分页、建模和调用。
10. **统一名称。** 四类能力都使用 `mcp_<server>_<alias>`；名称中没有
    `_tool_`、`_resource_` 或 `_prompt_`。surface 由内部 route 和 description
    表示，不靠名称反解析。
11. **统一 allowlist。** `enabled_capabilities` 的语义固定为：

    ```text
    null / omitted  -> 全部启用
    []              -> 全部禁用
    ["..."]         -> 只启用列出的最终 wrapped names
    ```

    列表对四个 surface 一视同仁，精确匹配，无 glob。
12. **在线 validate-before-save。** 新增或修改 MCP 必须在当前 Client 在线时完成
    真实 initialize 与完整 discovery；任一步失败都不写 DB、不改变 active runtime。
13. **离线只允许删除 MCP。** 纯删除可离线提交；任何混合的新增或修改使整个 PATCH
    原子失败。非 MCP 字段仍遵循各自原有的离线修改规则。
14. **持久 last-good catalog。** Server 保存最后一次验证成功的有界 catalog。
    Device 离线时能力仍可出现在 Agent schema；调用返回
    `tool_device_unreachable`。
15. **断线不删配置、不重放调用。** MCP runtime 暂时断开时后台重连；可能已经
    发出的调用绝不自动重放。
16. **首次 Server 不可达即退出。** Client 进程首次完成 ready 前只做一次有界真实
    WS handshake；失败退出。至少成功 ready 一次后，普通断线才无限 backoff 重连。

## 3. 范围

### 3.1 包含

- `sandbox_mode` → `restrict_to_workspace` 的全契约替换；
- Client file tools、device Workspace REST、file transfer、MarkItDown 输入路径的
  workspace guard；
- exec/PTY 在所有已配对 Client Device 上可见，以及初始 `working_dir` guard；
- Client 与 Server `web_fetch` 各自独立的 SSRF denylist；
- `/api/admin/config` 中 Server `web_fetch_denylist` 的校验与热更新；
- Device MCP config、secret redaction、last-good catalog、REST read/patch；
- FastMCP 3.4.7 的 stdio、Streamable HTTP、SSE client transport；
- initialize、四 surface cursor pagination、catalog bounds、统一 wrapping/collision；
- Protocol v3 validation、activation、catalog registration 和 MCP dispatch route；
- runtime crash/drop、schema drift、WS reconnect、Client shutdown 与 config removal；
- MCP text/image/resource/prompt 结果到现有 safe tool-result blocks 的映射；
- Linux、macOS、Windows source/frozen tests，以及 Docker Server + native Client E2E。

### 3.2 延后

- Linux bwrap、macOS Seatbelt、Windows AppContainer/Job-based isolation 作为
  security boundary，或任何统一 OS jail；
- command denylist、shell command AST/regex、`curl` URL 检测、DNS interception、
  exec/PTY network sandbox；
- Server-side/admin shared-service MCP（Py8）；
- OAuth、浏览器登录、人类授权回调、keyring、自定义 CA、`verify_tls=false`；
- MCP sampling、elicitation、roots、completion、tasks、resource subscription；
- MCP session 状态跨 Client 重启恢复，或已发送调用的自动 replay；
- MCP Apps/UI、audio 结果、任意 binary blob 下载、自动跟随 ResourceLink；
- 手工 capability alias、自动 hash/suffix/version collision 修复；
- 前端 MCP 配置页面。Py7 只提供完整 REST 契约。

## 4. 不可破坏的不变量

1. Device 与其 MCP config/catalog 只属于该 Device owner；所有 REST、schema build、
   route snapshot 和 dispatch 都重新验证 ownership。
2. OpenOctopus 自身不会把 MCP `env`/header values 主动序列化到 REST
   明文响应、repr、异常、日志、SSE、Provider prompt、tool description 或
   catalog；PostgreSQL 和发给目标 Client 的私有 config frame 是明确
   例外。用户信任的 MCP 本身可以读取这些值，并可能在 discovery
   metadata 或 tool/resource/prompt 结果中主动返回它们；Py7 不声称
   能阻止这个 trusted-boundary 行为。
3. 新增/修改 MCP 只有在相同 Device 当前 WS generation 上真实 initialize、完整
   discovery、Server catalog validation 全部成功后才可 commit。
4. validation 是候选态：失败、超时、connection replacement、late result，
   或在 cancellation-safe commit transition 启动前的 HTTP cancellation，都不能改
   active config/runtime/catalog。transition 一旦原子启动就必须收敛到明确
   commit/rollback；此后 HTTP cancellation 不能取消它。
5. config 与 last-good catalog 在同一个 DB commit 中改变。不得出现“新 config + 旧
   catalog”或“旧 config + 新 catalog”。
6. Provider schema 来自持久 last-good catalog，不来自临时在线内存。online/offline
   或短暂 MCP availability 变化不使工具名称抖动。
7. Provider-visible MCP name 永远映射到显式 immutable entry identity；Server 和
   Client 都不得从 `mcp_<server>_<alias>` 反解析 surface 或原始 MCP identity。
8. 同名能力只有在 logical identity 与 canonical provider-visible schema 都相同
   时才能跨 install site 合并。其它 collision 整体拒绝，不截断、不覆盖、不加后缀。
9. discovery 的 page、item、schema 和 total-byte limit 任一超限，整个候选 MCP
   validation 失败；不得发布 partial catalog。
10. 已进入或可能进入 MCP transport 的调用超时/断线后只返回 outcome unknown；
    OpenOctopus 不自动重放。
11. 旧 config revision、旧 catalog digest 或旧 MCP entry 的 call/result 只能被对应
    generation 消费；late result 不能关闭或污染健康的新 connection/runtime。
12. MCP 错误是普通 bounded tool result，不关闭 Device WS、不停止 Agent loop。
13. `restrict_to_workspace=true` 不限制 shell command、stdio MCP 或网络；任何文档、
    prompt 或错误都不能称它为安全 sandbox。
14. `OPENOCTOPUS_*` 不进入 stdio MCP child；Client token 不写 DB 中的 MCP config，
    不作为 MCP header/env 自动转发。
15. Server 进程永远不执行 Agent shell；Py7 MCP tool 的 `server` install site 仍不存在。

## 5. Workspace restriction 与网络边界

### 5.1 字段语义

Device 持久字段改为：

```text
restrict_to_workspace BOOLEAN NOT NULL DEFAULT TRUE
```

没有 DB migration 或 compatibility shim；开发数据库/bootstrap/schema 直接更新。
REST、Protocol、fixtures、prompt、变量名和文档同步改名。

当值为 `true`：

- 相对文件路径以 canonical `workspace_path` 为根；
- 绝对文件路径也必须位于 canonical workspace 内；
- exec/PTY 的初始 `working_dir` 使用同一规则；未指定时为 workspace root；
- file transfer 的 Client source/destination 各自在对应端应用该规则。

当值为 `false`：

- 相对路径仍以 workspace 为根；
- 允许宿主 OS 可达的绝对路径；
- 仍保留 NUL、路径类型、symlink/reparse-point、no-follow、原子发布等普通正确性
  检查。`false` 不是“关闭全部路径检查”。

两种值都不允许 workspace root 自身为 symlink、junction 或其它 reparse point。

### 5.2 适用矩阵

| 能力 | `restrict_to_workspace` 的作用 |
|---|---|
| Client shared file tools | 约束所有本地 path |
| Device Workspace REST | 与同名 Client file tool 共用 resolver |
| file transfer | 分别约束该 Client 的 source/destination |
| MarkItDown | 上游传 validated canonical path；worker 保留 no-follow reopen/identity 检查 |
| exec / PTY | 只约束初始 `working_dir` |
| Client `web_fetch` | 无作用；只看 `ssrf_denylist` |
| stdio MCP | 无作用；用户安装的 trusted child |
| HTTP/SSE MCP | 无作用；remote transport |
| Client→OpenOctopus Server | 无作用 |

`restrict_to_workspace=true` 的 shell 仍可执行 `cd /`、Python、`curl`、读取用户有权
访问的任意文件或启动后代。stdio MCP 同样拥有宿主用户权限和宿主网络。

### 5.3 路径竞态的诚实边界

统一 `WorkspacePaths` 继续阻止静态 `..`、已观察到的 symlink/junction/reparse
escape，并在可用处使用 no-follow、文件 identity、same-directory temp 与 atomic
publish 缩小竞态窗口。

它不是 handle-relative OS confinement：同账户恶意进程仍可能在检查和
open/rename/walk 之间替换父目录；workspace 内 hardlink 也不能证明 inode 的原始
来源。Py7 的验收措辞只能是“阻止普通/静态路径逃逸”，不能声称抵御恶意本地进程。
MarkItDown 不为此额外复制全文到 helper IPC；继续传 canonical path，并由
worker 使用现有 no-follow reopen/文件 identity 检查。

在途的有限 file/transfer/MarkItDown 调用捕获 immutable policy snapshot；
`false→true` 后允许这些已开始的调用完成，新调用使用新策略。exec session 生命周期
更长，workspace/restriction/shell/env policy 改变继续按 Py6 fence 终止旧 session。

### 5.4 Client `web_fetch`

Client `ssrf_denylist` 与 workspace 字段完全解耦：

- Device 创建时省略 denylist，始终使用现有 private/reserved 默认列表；
- 显式 `[]` 允许访问内网地址；
- 每个 redirect hop 都重新解析并应用同一次调用捕获的 immutable denylist；
- host、host:port 与 CIDR 规则保持精确匹配，不支持 regex；
- 每个 hop 只做一次 DNS resolution；只要任一返回 IP 命中 denylist，
  整个 target 被拒绝；允许时连接到已验证并 pin 的 IP，同时保留
  原 hostname 的 Host/SNI；
- HTTP client 固定 `trust_env=false`，不让环境代理绕过 DNS pin/denylist；
  OS 级 TUN/VPN 仍可正常工作；
- Agent 可通过 remote MCP、stdio MCP 或 exec 绕过该 denylist，这是被接受的边界。

Server/Client 不分析 shell command 中是否包含 `curl`，也不尝试从任意程序流量反推
URL。此类 best-effort 检查只会制造虚假安全感。

### 5.5 Server `web_fetch_denylist`

现有 `GET/PATCH /api/admin/config` 新增：

```json
{
  "web_fetch_denylist": ["10.0.0.0/8", "metadata.example", "example.net:8080"]
}
```

契约如下：

- 最多 256 项，每项 UTF-8 最多 512 bytes；canonical 后必须唯一；
- 接受 IPv4/IPv6 CIDR、单个 IP、IDNA DNS hostname、hostname:port；不接受 URL、
  wildcard、regex、path 或 userinfo；
- IP canonical 为 `/32` 或 `/128`，network 使用 `strict=false` 后的 canonical CIDR，
  hostname IDNA/lowercase/去末尾点，port 范围 1..65535；
- 省略字段表示不变，显式 `[]` 表示允许所有可解析 HTTP(S) 地址；
- 默认值复用当前 private/reserved/loopback/link-local/metadata denylist；
- 整个 candidate config 先验证后写入；非法项使 PATCH 失败且不保存任何字段；
- GET 返回 effective canonical list；修改立即影响后续调用，无需重启；
- 每次 Server `web_fetch` 在发请求前从 PostgreSQL 读一次 policy，关闭 DB session，
  再使用 immutable tuple 完成全部 redirects。没有进程内配置 cache 或多 worker
  失效问题；
- 每个 hop 只做一次 DNS resolution；只要任一返回地址命中 denylist 就拒绝整个
  target，允许时连接到已验证并 pin 的地址，同时保持正确 Host/SNI。
- Server HTTP client 同样固定 `trust_env=false`；不从 ambient proxy 重新解析
  或转发 target。

Server 不为 `web_fetch` 引入 bwrap；denylist、DNS pin、redirect revalidation、已有
response/time/admission bounds 已是 Py7 的完整 Server network boundary。

## 6. Device persistence 与 REST API

### 6.1 Device row

Py7 的相关字段为：

```text
restrict_to_workspace BOOLEAN NOT NULL DEFAULT TRUE
mcp_servers           JSONB   NOT NULL DEFAULT '[]'
mcp_catalog           JSONB   NOT NULL DEFAULT
  '{"version":1,"digest":"d5f4bb30627f342c5625dfe6a6d7a282874bd8121b32dbdd2004756e4b1ad8cf","servers":[]}'
config_revision       BIGINT  NOT NULL DEFAULT 1 CHECK (config_revision >= 1)
```

`mcp_servers` 是 Server 托管的 authoritative config；`mcp_catalog` 是最近一次成功
validation 的完整 bounded discovery snapshot，包含 disabled capability，但不含任何
env/header value。catalog 内含 SHA-256 digest。任意 effective Device config/name
改变都使 `config_revision` 加一。

删除某个 MCP 时，Server 可离线确定性地从 config 和 catalog 同时移除该 server，
重新计算 digest 并 commit。Device 删除由现有 row cascade 一并清理 config/catalog。

### 6.2 MCP config tagged union

所有模型 `extra="forbid"`。每个 Device 最多 16 个 config，`name` 唯一且匹配
`^[a-z][a-z0-9_]{0,31}$`。

stdio：

```json
{
  "name": "github",
  "transport": "stdio",
  "command": "npx",
  "args": ["-y", "@example/github-mcp"],
  "cwd": null,
  "env": {"GITHUB_TOKEN": "secret"},
  "enabled_capabilities": null
}
```

- `command` 是单个 executable string，不经用户可控 shell parsing；Windows
  `.cmd`/`.bat` shim 只使用第 7.2 节的固定 OS launcher；1..4096 chars；
- `args` 最多 64 项，每项最多 4096 chars，允许空字符串但禁止 NUL；
- `cwd=null` 表示 Device 用户 home；非空值展开 `~` 后必须是已存在的绝对目录；
  不接受相对 cwd，也不受 `restrict_to_workspace` 约束；
- `env` 最多 64 项；key 1..128 chars，禁止 control/NUL/`=`、大小写折叠重复和
  `OPENOCTOPUS_` 前缀；value 最多 16 KiB 且禁止 NUL。

Streamable HTTP：

```json
{
  "name": "corp",
  "transport": "streamable_http",
  "url": "https://mcp.example.com/mcp",
  "headers": {"Authorization": "Bearer secret"},
  "enabled_capabilities": ["mcp_corp_search"]
}
```

legacy SSE：

```json
{
  "name": "legacy",
  "transport": "sse",
  "url": "http://10.0.0.20:8000/sse",
  "headers": {},
  "enabled_capabilities": []
}
```

- URL 必须是完整 `http`/`https` endpoint，最长 4096 chars；保留 path、query 和
  trailing slash；拒绝 userinfo 与 fragment；
- HTTP 允许用于用户明确配置的内网 MCP；Py7 不施加 SSRF denylist；
  但只要 `headers` 非空，URL 必须是 `https`；
- remote MCP 不跟随 redirect。用户必须填写最终 endpoint，避免 custom auth header
  被转发到另一个 origin；
- `headers` 最多 64 项；name 必须是 ASCII HTTP `tchar`，canonical
  lowercase、大小写折叠唯一；value 最多 16 KiB，禁止 CR/LF/NUL；
- 禁止 `host`、`content-length`、`connection`、`transfer-encoding`、
  `proxy-authorization`、`proxy-authenticate`、`te`、`trailer`、`upgrade`、
  `keep-alive`、`accept`、`content-type`、`mcp-protocol-version`、
  `mcp-session-id` 和 `last-event-id`；它们只能由 transport 管理；
- Bearer 直接写为 `authorization: Bearer ...`；没有单独 token alias；
- 使用 HTTPX `verify=True` 的默认 certifi CA bundle，不声称读取 OS/
  enterprise trust store；不支持关闭校验或 custom CA；
- query、command、args、cwd 都是可见配置，禁止把 secret 放入这些字段。

整个 `mcp_servers` canonical JSON 最多 256 KiB。

### 6.3 Secret 存储与脱敏

Py7 明确接受 `env` 和 `headers` 的所有 value 以可逆明文存 PostgreSQL；数据库管理员
和数据库备份可读取它们。这里不增加 master key 或 envelope encryption。

边界如下：

- Pydantic/internal DTO 使用 secret-aware representation；repr 与 validation error
  不包含 value；
- REST GET/PATCH response 保留 key，但 value 一律返回 `"<redacted>"`；
- whole-list PATCH 中，`"<redacted>"` 只可在 secret sink identity 完全未变且
  key 相同时保留当前明文。remote sink identity 是
  `(name, transport, exact stored URL)`；stdio sink identity 是
  `(name, transport, command, args, cwd)`。任一 sink 字段改变都必须重新
  提交该 server 的全部 secret values；新 server/key、rename 或 marker 跨 sink
  使用均为 422；
- 省略一个 env/header key 表示删除，真实 string 表示替换；不允许把字面值
  `"<redacted>"` 保存为 secret；
- header key 以 canonical lowercase 匹配；env key 精确匹配；
- marker resolution 在 candidate compare 和 Client validation 之前完成；精确 no-op
  不发 frame、不重启 runtime；
- Device WSS 的 `hello_ack`/`config_update`/`config_validate` 对目标 Client 发送明文；
- config 含任一非空 MCP env/header value 时，Server 只能在 ASGI
  `scope["scheme"] == "wss"` 的连接上发送这些 frame；即使 direct peer 是
  loopback，明文 `ws` 也没有例外。应用不直接信任 raw
  `X-Forwarded-Proto`；TLS reverse
  proxy 部署复用 Uvicorn 的 `proxy_headers` + `FORWARDED_ALLOW_IPS`，只把明确
  的 proxy IP/CIDR 列入信任集，由 Uvicorn 在 ASGI scope 中恢复 `wss`。不新增
  OpenOctopus proxy 配置，不建议生产使用 wildcard trusted proxy；
- 其它 WS 连接在发送 secret-bearing payload 前被拒绝/退役；Client 继续要求
  `OPENOCTOPUS_SERVER_URL` 是 HTTP(S) origin，并由现有配置层派生 `/ws/device`。
  secret-bearing activation 要求 origin 为 `https://`、派生 WebSocket 为 `wss://`；
  `http://` 派生的 `ws://` 必须拒绝；
- registry 在 handshake 时记录 immutable `secret_transport_safe`。已持久 secret 的
  Client 重连时，Server 在 `hello_ack` 前 fail closed；REST add/modify 需要发送
  secret candidate 而当前 handle 不安全时返回 409
  `mcp_secret_transport_insecure`，不发 frame/不保存。纯删除不发送旧 secret，
  仍可完成；
- 即使 TRACE，也不得记录这些 frame 的 raw JSON。日志只含 device id、server name、
  transport、state、elapsed 和稳定 error code。

### 6.4 REST routes 与 response

- `POST /api/devices` 不接受 `mcp_servers`。新 Device 必须先用 token 上线，再 PATCH
  配置 MCP，满足在线真实 validation 不变量。
- `GET /api/devices` 保持轻量：每行只增加 `config_revision`、
  `mcp_config_count`、`mcp_enabled_capability_count` 和 `mcp_catalog_digest`；
  不内联 `mcp_servers` 或完整 catalog。
- 新增 `GET /api/devices/{name}/config`，返回一个 `DeviceConfigResponse`：

  ```json
  {
    "device": {"name": "laptop", "online": false, "config_revision": 7},
    "mcp_servers": [{"name": "github", "env": {"TOKEN": "<redacted>"}}],
    "mcp_catalog_digest": "<sha256>",
    "mcp_discovered": {
      "github": {
        "tools": [{"raw_name": "search", "final_name": "mcp_github_search", "enabled": true}],
        "resources": [],
        "resource_templates": [],
        "prompts": []
      }
    }
  }
  ```

- `PATCH /api/devices/{name}/config` 返回同一稳定 envelope；`mcp_servers` 是 whole-field
  replacement，其它 top-level 字段仍为 partial update。
- 每个 PATCH body 必须带 GET 返回的 `base_config_revision`；它不是
  Device config 字段。Server 在 secret marker resolution 前及最终 commit 前各比较
  一次；stale revision 返回 409，不发 validation frame、不保存。
- PATCH 中 `mcp_servers=[]` 删除全部；纯删除可在 Device offline 时成功。
- candidate 中任何新增、transport/url/command/args/cwd/env/headers/filter 修改都要求
  Device online 并触发完整真实 validation；filter-only 修改也不例外。
- 混合删除和修改时，只要存在修改就要求在线；失败时包括其它 top-level field 在内
  整个 PATCH 不 commit。
- validation/spawn/discovery/device-limit/unknown enabled name 返回 422；wrapped-name、
  cross-install schema collision 或 owner aggregate schema limit 返回 409；Device
  offline/replaced/config race 或 insecure secret transport 返回 409。
  所有错误内容经过脱敏。

## 7. FastMCP 3.4.7 集成

### 7.1 依赖

Client 直接固定：

```text
fastmcp-slim[client]==3.4.7
mcp==1.26.0
uritemplate==4.2.0
```

当前项目没有 lockfile，因此显式固定 FastMCP 3.4.7 实际验证的 MCP SDK 版本。
`uritemplate` 只用于 RFC 6570 resource template 的变量提取与展开；它不是
严格语法 validator，Py7 在调用它之前执行第 8.4 节的 bounded parser。

FastMCP 3.4.7 是稳定 3.x；4.x beta、server extra、FastMCP multi-server config、
transport inference 都不进入 Py7。官方 3.4.7 Client 在 context entry 时完成 MCP
initialize，stdio/HTTP/SSE 都有显式 transport class。

### 7.2 Transport 构造

- stdio 使用一个实现 FastMCP `ClientTransport` 的薄
  `BoundedStdioTransport`，不直接使用 stock `StdioTransport`。它新建专用
  双向 MCP process adapter，在 session 生命期内保持 stdin/stdout；只复用 Py6
  的 Job/process-group/terminate/kill primitives，不复用或改变会立即关 stdin
  的 exec `spawn_pipe`。它仍把建好的 `ClientSession` 交给 FastMCP
  `Client`；
- remote 分别使用 `StreamableHttpTransport` 或 `SSETransport`；
- FastMCP `Client` 固定 `timeout=None, init_timeout=0`，关闭 SDK 内层 request/
  initialize timer；所有阶段只由第 7.3、12.1 节的 OpenOctopus outer deadline
  裁决，避免同一操作出现两套 timeout 语义；
- Streamable HTTP/SSE 共用自有 HTTP client factory。factory 接受 `**kwargs`，
  丢弃 FastMCP 传入的 `follow_redirects=True`，并强制创建
  `httpx.AsyncClient(follow_redirects=False, trust_env=False, verify=True, ...)`；它还组合
  下述 capped `AsyncBaseTransport`；
- 不传 OAuth auth provider、custom verify 或 FastMCP multi-server dictionary；
- stdio 不传整个 `os.environ`。有效 child environment 是 MCP SDK 的跨平台 safe
  baseline 加配置 `env` overlay，并再次拒绝/删除所有 `OPENOCTOPUS_*`；Py6
  `env_allowlist` 仍只控制 exec/PTY；
- env overlay 在 POSIX 以 exact key、Windows 以 Unicode casefold key 匹配，配置值
  覆盖 baseline；两轮合并前后都删除 case-insensitive `OPENOCTOPUS_`
  前缀。Windows executable resolution 使用 candidate `PATH`/`PATHEXT`，保留
  `npx.cmd` 等常见 shim；`.cmd`/`.bat` 只通过固定 OS `ComSpec` launcher
  启动，不把用户 `command`/`args` 重新解析为任意 shell string；
- Client startup 已从自身环境删除 Device token；MCP SDK safe baseline 也不含它；
- runtime 自己打开并持有 cross-platform `os.devnull` sink 作为 child
  stderr，同时给 FastMCP `Client` 传 noop `log_handler`；stdio stderr/MCP logging
  notification 不上报
  Server、不进入普通日志；
- noop `log_handler` 只处理 MCP logging notification，不能代替 Python logger
  隔离。创建 transport 前，Client 必须为 `fastmcp.*`、`mcp.*`、`httpx`、
  `httpcore` 安装不向 application/root handler propagate 的丢弃边界；即使第三方
  logger 使用 DEBUG 或自定义 TRACE，也不能输出 raw JSON-RPC message、完整 URL 或
  response body；
- context exit 不是 cleanup contract。success、failure、timeout、cancellation、
  replacement 和 shutdown 路径都必须在 bounded shielded cleanup 中
  显式 `await client.close()`，再关闭自有 devnull handle。

stdio close 是幂等状态机：先标记 CLOSING/拒绝新 request，给 protocol
close + stdin EOF 2 s，再 terminate 受管 process tree 并等 3 s，最后 force-kill
并等 5 s；总上限 10 s，每阶段都 shield cancellation。若受管 root/tree 仍无法
确认收敛，runtime 进入 `CLEANUP_BLOCKED(cleanup_incomplete=true)`，保留
process handle 供 Client shutdown 再做一次同样 bounded cleanup；当前 Client 进程不再为
同 server config 启动 replacement/retry，防止累积 child。validation runtime 清理不完
使 candidate 失败并在该 Client 进程内阻止同 sink 再验证；Agent/Client
其它能力继续。故意 daemonize/脱离受管 tree 的 descendant 仍属于明确的
跨平台非保证边界。

所有 MCP 入站 JSON-RPC message 有 12 MiB raw 上限，检查必须发生在
UTF-8、SSE field accumulation、Pydantic 和 JSON decode 之前：

- stdio adapter 直接按 bytes 读 stdout，以 LF 分隔的单条 record 计数；
  未出现 LF 前超限也立即拒绝；
- Streamable HTTP JSON 按单个 response entity 计数；先可用
  `Content-Length` 快速拒绝，chunked body 仍按实际 bytes 计数；
- HTTP/SSE request 强制 `Accept-Encoding: identity`，response 的
  `Content-Encoding` 只允许 absent/`identity`，其它值在读 body 前拒绝；
- SSE 按单个完整 event 计数，包括 `event`/`data`/`id`/comment 与
  delimiter；正确处理 LF、CRLF、bare CR 及跨 chunk delimiter，长连接总
  bytes 不累计。

stock MCP transport 在这些位置会先无界缓冲，所以不得用“raw API
返回后再计数”替代上述 adapter。超限必须关闭 response/session/child，不留
task/process。validation 阶段超限使 candidate 以 `mcp_message_too_large`
失败且不保存；运行时超限关闭当前 MCP runtime，当前调用返回
`tool_mcp_message_too_large` 并按普通 backoff 重连。因为调用可能已在
MCP 中产生副作用，错误 message 仍提示 Agent 不要盲目重放。

### 7.3 明确 timeout

FastMCP 默认值不能成为产品契约。Py7 固定：

| 阶段 | deadline |
|---|---:|
| transport connect + MCP initialize | 30 s / server |
| 四 surface 完整 discovery | 额外 30 s / server |
| 单次 tool/resource/prompt invocation | 60 s |
| 整个 PATCH candidate validation | 300 s |

Client 最多并行验证 4 个独立 MCP server。达到 candidate 总 deadline，所有未完成
候选均清理，PATCH 整体失败。300 s 覆盖 16 个 server 在并行度 4 下的
`ceil(16/4) * (30 + 30) = 240 s` 最坏阶段预算、四批每批最多 10 s
cleanup，外加 20 s 收敛空间。
Py7 不暴露 per-server timeout 配置；真实需求出现后再加。

### 7.4 Telemetry-safe session 边界

FastMCP 3.4.7 没有只关闭 Client OpenTelemetry spans 的公开开关；其
`*_mcp` mixins 会创建包含 session/resource/tool/prompt identity 和 exception text
的 spans，tool/resource/prompt 路径还可注入 trace context。Py7 因此只用 FastMCP
`Client` 管理 transport/context/initialize/close，通过公开 `client.session` 直接
调用 MCP 1.26 `ClientSession`：

```text
list_tools / list_resources / list_resource_templates / list_prompts
send_request(ClientRequest(CallToolRequest), CallToolResult) / read_resource / get_prompt
```

不调用 FastMCP `list_*_mcp`/`call_tool_mcp`/`read_resource_mcp`/
`get_prompt_mcp`，不传 `_meta`，不使用进程级 OTel disable/monkeypatch。这保留
FastMCP transport/lifecycle，同时不创建 FastMCP client spans 或向 MCP 注入
`traceparent`。tool invocation 也不调用 `ClientSession.call_tool()`：该 convenience
method 会依据 discovery 的 `outputSchema` 隐式校验结果并在进入 OpenOctopus 映射前
抛异常；Py7 使用公开 raw `send_request`，由第 12 节唯一决定稳定结果语义。

## 8. Discovery、catalog 与 wrapping

### 8.1 四 surface 与分页

Client 通过 `client.session` 使用 MCP SDK raw result API，并手动遍历
opaque cursor：

- `list_tools`；
- `list_resources`；
- `list_resource_templates`；
- `list_prompts`。

只有 initialize capability 宣告存在的 surface 才请求；缺失 capability 视为空。
resources capability 下仍分别读取 static resources 与 templates。每次 validation/recovery
使用 fresh responses，不使用 FastMCP response cache。

限制如下：

| 范围 | 上限 |
|---|---:|
| 每 surface / server pages | 16 |
| 每 server 四 surface capability 总数 | 256 |
| 每 Device capability 总数 | 512 |
| 单 capability canonical JSON | 256 KiB |
| 每 Device 完整 discovered catalog | 2 MiB |
| 单个入站 JSON-RPC stdio record / HTTP entity / SSE event | 12 MiB raw, pre-decode |
| opaque cursor | 4096 bytes |
| description | 4096 Unicode chars |
| raw name / URI | 256 / 4096 Unicode chars |
| JSON/schema nesting depth | 32 |
| 每 owner 合并后 enabled logical final names | 256 |
| 每 owner 最终 MCP Provider tools canonical JSON | 256 KiB |

cursor 不解析、不跨 MCP session 持久化。重复 cursor、空页继续返回相同 cursor、
页数/item/decoded-byte/depth 超限、raw transport frame 超限、advertised surface
list 失败，均使该 config validation 失败；
任何一个 config 失败使整个 REST candidate 失败，不发布 partial catalog。
最后两个 owner 级限制在合并 install sites、注入完整
`openoctopus_device` enum 并用与 Provider request 相同的 canonical JSON 编码后
计算，但不包括 fixed built-ins；disabled entries 仍可持久，但不计入。
它们是硬资源上限，不保证
每个便宜 Provider 都有足够 context。Server 在 owner advisory lock 内基于
所有 Device candidate 重验，不允许多个单独合法 Device 聚合后超限。

### 8.2 名称规范化

每个 capability 保留明确 source identity；另生成 Provider-safe alias：

1. 对 raw capability name 做 Unicode NFKC；
2. 转 lowercase；
3. 每段非 `[a-z0-9_-]` 字符折叠成单个 `_`；
4. 去掉首尾 `_`/`-`；
5. 得到 `mcp_<server>_<alias>`。

alias 为空、最终名称不匹配 `^[a-zA-Z0-9_-]{1,64}$` 或超过 64 chars 时整个 config
validation 失败。不截断、不加 hash、不自动 suffix，也不提供手工 alias map。

四个 surface 共用一个最终名称 namespace。filter 应用后统一检查：

- 同一 server 内 intra-surface 和 cross-surface collision；
- static resource 与 resource template collision；
- 同一用户所有 Device install site 的 collision/schema drift；
- 所有 built-in/non-MCP tools 的名称。

`mcp_` 前缀由 MCP 保留，built-in tool 不得使用。filter 在 collision/merge 前应用，
因此 disabled capability 不阻止安装；若一个 enabled final name 同时选择两个冲突
identity，则 validation 失败。

### 8.3 `enabled_capabilities`

discovery 总是收集并保存所有四类能力，然后解释 allowlist：

- `null` 或字段 omitted：选择 catalog 中全部 final names；
- `[]`：选择零项，但仍真实 initialize/discover/validate bounded catalog；
- 非空 list：唯一、最多 512 项，精确选择列出的 final names；
- 任一 unknown final name：整个 candidate 失败；
- 不支持 raw names、surface object、glob、prefix 或 negative filter。

REST `mcp_discovered` 显示 full catalog 与每项 enabled 状态，供用户构造下一次 PATCH。
首次接入不熟悉的 MCP 时，推荐先用 `enabled_capabilities=[]` 完成安全 discovery，读取
返回的 final names，再进行第二次精确 allowlist PATCH；这样 discovery 期间不会短暂
向 Agent 发布未知能力。

### 8.4 各 surface schema

**Tool**：保留 MCP `inputSchema`，要求顶层 JSON object schema；`properties` 必须为
object，`required` 必须为唯一 string list 且是 properties 子集。source schema 若已
定义 `openoctopus_device`，validation 失败。其余有界 JSON Schema keyword 原样保留。
可选 `outputSchema` 作为有界 canonical JSON object 保存在 source/persisted catalog，
参与 digest、drift 和跨 Device equal-merge 比较，但不进入 Provider tool schema。
OpenOctopus 不把它当作额外的运行时结果 validator；结果是否可接受只按第 12.2 节
的确定性映射判断。

**Static resource**：生成无业务参数的 object schema。MCP SDK 1.26 会用
Pydantic `AnyUrl` 解析 Resource URI；内部 route 和 logical identity 因此固定为
SDK 返回的 normalized `str(AnyUrl)`，不声称保留 wire 上的原始大小写、
dot segments 或 Unicode host 形式。

**Resource template**：先用单次有界 scanner 验证 RFC 6570 expression，再交给
`uritemplate==4.2.0` 变量提取/展开。scanner 要求所有 brace 完整配对，
expression 非空且精确为 `{operator? varspec (, varspec)*}`，operator 只能是
RFC 6570 的 `+ # . / ; ? &`，varname 只由 ASCII alnum/underscore、dot-separated
segments 和完整 `%HH` 组成，modifier 只能是单个 `*` 或 `:1..9999`。
任何 unmatched/nested brace、空 varspec、whitespace、非法 operator/modifier 均在
validation 失败；`uritemplate` 的宽松接受不能覆盖该结果。

每个唯一 template variable 生成 required string property，保留首次出现
顺序。变量名与 `openoctopus_device` 冲突，或展开后 URI 超 4096 chars
时 validation/调用失败。展开结果再经 `AnyUrl` 规范化后交给
`client.session.read_resource`；Client 不自行字符串替换。持久 template identity 是 MCP
SDK 解码后、通过上述严格验证的 template string。

**Prompt**：每个 MCP PromptArgument 生成 string property；`required` 按 MCP flag；
重复 argument 或 `openoctopus_device` 冲突使 validation 失败。

Server 只在最终 schema build 时注入 required `openoctopus_device` enum。每类 description
明确写出 `MCP tool/static resource/resource template/prompt from '<server>'`，让模型
理解行为；不把 secret、raw URI query 或 transport config 放进 description。

### 8.5 Source catalog 与 persisted canonical catalog

Client 在 validate/register frame 中返回 `source_catalog`。它按 server 分组，
每项只含 surface、MCP SDK 解码/规范化后的 source name 与 invocation
identity、description、input schema/prompt arguments 等 discovery 事实；不含
`entry_id`、final name、enabled 或 digest。Server 对它做 bounds、strict schema、
naming、allowlist 与 collision validation，再生成要 commit 的 persisted catalog。

persisted catalog 每项至少包含：

```text
entry_id (Server-assigned UUIDv7，只在 commit candidate 时生成)
server name
surface
raw name
raw invocation identity (tool/prompt name or resource URI/template)
final name
provider description
canonical input schema
canonical output schema（tool 可选，Provider-hidden）
enabled
```

Server commit 后在 authoritative `config_update`/`hello_ack` 中下发 persisted catalog；
Client 用 source identity 把已验证的 candidate runtime 映射到 Server-assigned
`entry_id`。`entry_id`、Device UUID、config revision、catalog digest 都是
Provider-hidden。

canonical digest projection 使用 UTF-8、sorted object keys、compact separators、
`ensure_ascii=false`、禁止 NaN/Infinity；array 顺序在构造前按
`(server, surface, source identity)` 排序。projection 包含 `version`、source route、
provider-visible shape 与 enabled 状态，但排除 envelope 自身的 `digest`、随机
`entry_id` 和所有 secret。catalog `digest` 是该 projection 的 lowercase SHA-256。
上面的空 catalog default 对 canonical `{"servers":[],"version":1}` 求得。Client 和
Server 使用相同 contract fixtures 验证 digest。

同一 final name 跨 Device merge 还要求 surface、raw invocation identity、description、
input schema 与 enabled provider shape 完全一致；只差 install site 时合并并扩展
`openoctopus_device` enum。

## 9. Protocol v3

### 9.1 版本与首次 ready

`hello.version` 固定为 `"3"`；v3 peer 收到其它版本关闭 4409。官方 Py7 Client 在
MCP、PTY 或平台支持上不协商降级 capability。

首次启动：

1. 使用 `OPENOCTOPUS_SERVER_URL` 直接发起一次 WS connection 和真实 hello；不做
   `/health` preflight；
2. DNS/TCP/TLS/HTTP 429/5xx、pre-ack EOF、1000/1001/1006/1013、hello timeout 在
   首次 ready 前都视为 startup unreachable，清理后 exit 1；
3. 401/403/4401 exit 1；URL/local config/protocol/4409 exit 78；
4. Client 收到 matching `hello_ack`，成功安装 non-MCP config，发出 matching
   `config_applied`，并收到 Server 的 matching `config_applied_ack` 后，进程级
   `ever_ready=true`；
5. `ever_ready=true` 后，普通网络断线/1000/1001/1006/1013/4408 使用 Py6 backoff
   永久重连；4000 replacement 和 4401 仍永久退出。

MCP runtime readiness 不参与 Client 首次 ready。坏掉的 MCP 不能阻止 file、web、
transfer、exec 或 PTY 工作。

### 9.2 Config frames

`hello_ack` 与 authoritative `config_update` 都保留 Py6 的 `type`/UUIDv7 `id`。
`hello_ack.id` echo `hello.id`；一次 validated candidate 对应的
`config_update.id` 等于前面 `config_validate.id`，使 Client 可以 promote 精确
candidate；其它 update 使用 fresh id。两种 frame 的 authoritative payload 包含：

```jsonc
{
  "type": "config_update",
  "id": "<uuid-v7>",
  "device_name": "laptop",
  "config_revision": 7,
  "config": {
    "workspace_path": "/home/me/work",
    "restrict_to_workspace": true,
    "ssrf_denylist": ["10.0.0.0/8"],
    "shell_timeout_max": 600,
    "env_allowlist": ["PATH", "HOME"],
    "mcp_servers": ["raw secret-bearing configs"]
  },
  "mcp_catalog": {"version": 1, "digest": "...", "servers": []}
}
```

Client 完成 local activation 后返回：

```json
{"type":"config_applied","id":"<matching-frame-id>","config_revision":7}
```

Server 验证 frame id/revision 与当前 generation 后，在 generation 仍被 fence
时通过同一 serialized writer 成功发出：

```json
{"type":"config_applied_ack","id":"<matching-frame-id>","config_revision":7}
```

只有 `_send_text(config_applied_ack)` 成功返回后，Server 才在不发生 await 的
原子步骤中清 fence/发布 non-MCP generation 为 routable。因此并发
`tool_call` 不可能在 writer 中超过 ack；ack send 失败则 generation 从未
routable，直接 retire。

初始与更新两条路径的 `config_applied`/ack 都各有 10 s deadline。Server 在
初始 `config_applied` 前不把 generation 标记 routable；更新期间从 precommit
fence 到 matching `config_applied` 都拒绝新 policy-bound calls。Client 在
matching ack 前不启动/重注册 MCP runtime。ack timeout 或 ambiguous send 使该 WS
generation retire，DB 中已 commit config 不回滚；下次 hello 读取它。

### 9.3 Candidate validation frames

Server→Client：

```jsonc
{
  "type": "config_validate",
  "id": "<uuid-v7>",
  "base_config_revision": 7,
  "candidate_config": {"...full config including MCP secrets...": "..."},
  "validate_servers": ["corp"],
  "deadline_ms": 300000
}
```

Client→Server：

```jsonc
{
  "type": "config_validate_result",
  "id": "<same uuid-v7>",
  "ok": true,
  "source_catalog": {"version": 1, "servers": []},
  "failures": []
}
```

失败 result 只含 server name、阶段和稳定 code/bounded safe message；不含 raw exception、
command、args、URL query、env/header value 或 MCP stderr。`ok=true` 时 failures 必为空；
`ok=false` 时 `source_catalog` 不存在。`validate_servers` 是 Server 计算的唯一、
非空、有界 changed/filter-only server 名列表；成功 `source_catalog.servers`
必须与它精确一一对应，不回传 unchanged/deleted server。Client 不在这个
pre-commit DTO 中伪造 Server-assigned entry id/final catalog digest。

validation-only runtime 与 active runtime 分离，不发送 `register_mcp`。Server commit
成功后的 `config_update` 携带同一 validation id，Client 才可 promote 对应 candidate；
Server rejection/cancellation 时 best-effort `config_validate_cancel`，Client 自身也在
result 后给未 promote candidate 60 秒 lease。Server 的 precommit lock/DB transition
额外受 30 秒 deadline，不得在 candidate lease 过期后才 commit；matching
`config_update` 在 lease 内到达后由 Client 接管 cleanup。

每个 Device 同时最多一个 validation。expired/cancelled validation id 转为
WS-generation-scoped tombstone，每 generation 最多 64 个；它不按固定 TTL
删除，只在收到精确 late result 或该 generation retire 时移除。精确
late result 被忽略，真正未知/冲突 id 才是协议错。若容量将迫使未过期
tombstone 被驱逐，先 retire 该 WS generation，不使未知 late frame 污染健康
generation。

### 9.4 Runtime catalog registration

Client 在以下时机发送 `register_mcp`：

- 每次 fresh `hello_ack` 后，已有 runtime 的 snapshot；
- config candidate promote 后；
- 任意 authoritative config revision 改变后，即使 MCP config/catalog 本身
  未改变，也要把保留的 runtime 重新绑定/注册到新 revision；
- MCP background recovery/重新 discovery 后；
- STARTING/READY 进入 unavailable/drifted 后；
- `list_changed` bounded refresh 得到结果后。

`runtime_generation` 在每次 runtime attempt 进入 `STARTING` 前生成 UUIDv7，
因此 missing executable/initialize failure 也有可定位的 generation。OO WS
reconnect 保留原值，MCP retry/restart 生成新值；READY 转为
UNAVAILABLE/DRIFTED 使用当前 attempt generation 报告状态。aggregate DTO 固定为：

```jsonc
{
  "type": "register_mcp",
  "id": "<uuid-v7>",
  "config_revision": 7,
  "catalog_digest": "<expected persisted sha256>",
  "servers": [{
    "name": "corp",
    "runtime_generation": "<uuid-v7>",
    "state": "ready",
    "code": null,
    "source_catalog": {"tools": [], "resources": [], "resource_templates": [], "prompts": []}
  }]
}
```

`state` 只能是 `ready | unavailable | drifted`；`ready` 必须带 fresh full
`source_catalog` 且 `code=null`，其余状态禁止 catalog 并必须带 bounded
stable `code`。Server 规范化 source catalog、与
persisted catalog 比较并返回：

```jsonc
{
  "type": "register_mcp_ack",
  "id": "<matching uuid-v7>",
  "config_revision": 7,
  "catalog_digest": "<matching sha256>",
  "results": [{
    "name": "corp",
    "runtime_generation": "<matching uuid-v7>",
    "accepted": true,
    "code": null
  }]
}
```

Client 为每个 request 保留 immutable desired snapshot；每个 server 的 snapshot 至少
包含 state、stable code、runtime generation 和 canonical source catalog。只有 frame id、
revision、digest、server name、runtime generation 都匹配，且该 request snapshot 仍与
当前 latest desired snapshot 完全相同，Client 才应用每项 ack。任何 runtime state 或
catalog 变化必须先同步清除旧 accepted marker，再更新 latest snapshot/enqueue
registration；因此 stale ACK 不会重新开放已失效 runtime，而只触发下面的 coalesced
最新 exchange。Client 仅在 active route 仍绑定 latest accepted snapshot 时接受对应
MCP entry 的 tool call；Server 也不把该 runtime binding 标为 available。ack
`results` 与 request servers 一一对应；只有 `state=ready` 且 catalog 完全匹配
才能 `accepted=true`，`unavailable`/`drifted` 均为 `accepted=false` 并带稳定
code。一次
fresh hello 发现 authoritative revision 改变时，必须先安装 config；MCP config
未变可保留 session 但仍在新 revision 下重注册，已变则替换 runtime。

digest/schema 不同不会自动改 DB 或工具面。Server ack `accepted=false`，该 server
进入 drifted/unavailable，last-good schema 继续可见，直到用户重新 PATCH 完成真实
validation。

每个 WS generation 同时最多一个 aggregate registration exchange；运行时
状态/recovery/`list_changed` 在 ACK 前变化只合并为一份 latest desired
snapshot，不堆积 frame/task。每个 request `servers` 名称唯一，且必须精确
覆盖当前 authoritative `mcp_servers`；未就绪的 attempt 用
`state=unavailable` 与稳定 code 占位。Server 成功处理 aggregate 后原子
构造该 revision 的整个 candidate binding set，不保留 request 中缺失的旧
binding。它在新 binding 仍 unroutable 时经 serialized writer 发出
`register_mcp_ack`；只有 send 成功后才在无 await 步骤中原子替换/发布
binding set，避免 `tool_call` 超过 ACK。ACK send 失败不发布 candidate。
ACK deadline 为 10 s；timeout/ambiguous send 使当前 OO WS generation retire，MCP
sessions 按普通 WS 断线语义保留，新 connection 重注册。matching ACK 后若
latest snapshot 已变，立即发下一个 single-flight exchange。

### 9.5 MCP tool call route

MCP invocation 复用现有 `tool_call`/`tool_result`，但 `tool_call` 增加 Provider-hidden：

```json
{
  "mcp_route": {
    "entry_id": "<uuid-v7>",
    "config_revision": 7,
    "catalog_digest": "<sha256>",
    "runtime_generation": "<uuid-v7>"
  }
}
```

built-in call 不带 `mcp_route`；带 route 的 call 必须对应一个 MCP final name，反之亦然。
Client 用 entry id 查 active route，再精确验证 revision/digest/runtime generation/
final name，不从名称拆分。

`openoctopus_device` 只是 Server 注入的 install-site selector。Server 在完成
exact device/ownership/route 选择后必须删除该字段，只把剩余 source args
发给 Client/MCP；Client 和 MCP source schema 都不得看到它。

Agent turn 构建 schema 时冻结
`(device_id, device_name, entry_id, config_revision, catalog_digest)`；dispatch 时再获取
当前 accepted `runtime_generation`，并在发送前再次检查
ownership/name/config。明确尚未发送的 stale route 返回普通 `tool_mcp_unavailable`；
已发送/可能发送的结果丢失返回 `tool_execution_outcome_unknown`。

每个已 issue 的 invocation task 同时绑定发起它的 OO WS writer generation
和 MCP runtime generation。旧 WS 结束/被替换后，对应 MCP 调用可在本地继续
收敛，但结果只会被丢弃，绝不经新 writer 发送。Server 在 call timeout/
cancel/disconnect 时立即释放 pending byte admission，并把 call id 转为该 WS
generation 下最多 256 个的 tombstone。tombstone 不按固定 TTL 删除，
只在 matching late result 到达或 generation retire 时移除；matching late result 被
忽略；如容量将驱逐仍有效的 tombstone，先 retire 该 generation。当前
generation 上不匹配任何 pending/tombstone 的 result 仍是协议错。

所有 text frames 继续受 12 MiB 上限与 strict `extra=forbid` DTO 约束。

## 10. Validate-before-save transaction

同一 Device 的 PATCH 由 per-device config mutation lock 串行；handler 在读取 candidate
前取得它，exact no-op、validation/commit 失败等所有非移交路径在 `finally` 释放。
该 lock 可跨 Client validation await，但不是 DB lock。另有一个 per-device registry publication gate；
handshake 的 authoritative row read + handle publication、replacement、unregister、token
rotation/delete 和 config precommit 公平共用它。它只可跨短 DB transaction，
不跨 Client network await。整体状态机为：

```text
OLD_ACTIVE
  -> BUILD_CANDIDATE
  -> [REMOTE_VALIDATE when required (old runtime continues)]
  -> SERVER_VALIDATE
  -> CANCELLATION_SAFE_TRANSITION
  -> PUBLICATION_GATE / OPTIONAL_OR_REQUIRED_FENCE
  -> COMMIT(config + catalog + revision)
  -> PUSH/PROMOTE
  -> NEW_ACTIVE or RETIRE_CONNECTION
```

详细顺序：

1. 短 DB read 捕获 row、owner、revision、current config/catalog；先比较必填的
   `base_config_revision`，不持锁等待 Client。
2. resolve redaction markers，构造完整 strict candidate；识别 exact no-op、无需
   remote validation 的 mutation（包括 pure MCP removal）与 add/modify。exact no-op
   直接返回当前 envelope，不增 revision。
3. add/modify 捕获当前 `(device_id, WS handle, WS generation)`，发送
   `config_validate`；旧工具和
   active MCP 继续服务，heartbeat/binary frames 不排在 validation 后。
4. Client 对 changed MCP configs 建 temporary runtimes，真实 initialize、四 surface
   discovery；unchanged configs 可复用持久 catalog，但 filter-only change仍执行一次
   fresh discovery。
5. Server 把 changed servers 的 fresh source catalog、unchanged servers 的 persisted
   canonical entries 和 candidate deletions 合并成 full candidate catalog，再做 naming、
   enabled、schema、bounds、built-in 和 owner 其它 Device last-good collision 检查；
   不改 active cache。
6. remote validation 成功后，或无需 remote validation 的 candidate 完成 Server
   validation 后，启动一个独立的 cancellation-safe transition task。在该 task
   启动前的 HTTP disconnect/cancel 会放弃 candidate；启动后 handler cancel 只
   停止等待 response，不传播给 transition。task 必须先存入强引用 task registry，并在
   不经过 await 的同一临界步骤接管 mutation lock 的释放责任；handler 的 `finally`
   只在尚未移交时释放，transition 的 `finally` 在成功、失败或自身取消时最终释放。
   因而 handler cancellation 与下一次 PATCH 之间不存在无锁 transition 窗口。
7. transition 获取 publication gate 并生成 immutable `fence_token`。remote-validated
   分支必须原子确认 captured WS handle 仍是 current/ready 且 generation
   未变，再安装 fence；否则不 commit。无 remote-validation 分支在此时
   捕获可选 current handle：在线则 fence 它，离线也可继续 commit。gate
   在短 DB commit 结束前不允许 handshake/replacement 发布 handle。
8. 开启短 DB transaction：锁 Device row，获取以 owner UUID 稳定映射的
   PostgreSQL transaction-level advisory lock，重读 revision/current config 与该 owner
   全部 Device catalogs，重做 cross-install collision 与 owner 级 schema limits。任一竞态使整个 candidate
   失败。Device create/delete/rename 与所有 MCP catalog commit 也使用同一
   owner advisory key。
9. Server 为新 logical entries 生成 UUIDv7，未变 entry 保留原 immutable id，
   并原子写 config/catalog/revision后 commit。owner advisory lock 使多 worker 上两个
   Device 不能同时提交相冲突的 final name/schema。
10. DB commit 结果明确后才释放 publication gate。若 commit 返回边界
    不明，transition 仅在 CAS 匹配 captured `(handle, generation, fence_token)` 时
    退役它，再用全新 DB connection 重读 durable
    revision/config/catalog 判定结果；在结果被定义且 registry 按最新 durable
    row 收敛前，不接受同 Device 的下一次 mutation。
11. commit 成功后先关 DB session，再仅对 CAS 仍匹配的 captured handle push
    full config/persisted catalog；Client promote candidate 并返回 matching
    `config_applied`，Server 验证后发出 `config_applied_ack`。若 handle 已被
    successor 替换，不向 successor 发旧 transition frame；successor 的 hello 只读最新
    durable row。这些后续不占 DB/publication gate。
12. push/ack ambiguous 时也只 CAS retire captured `(handle, generation, fence_token)`/
    清其 fence，绝不按 device id 退役健康 successor；不回滚 durable
    commit，不在新 connection 自动重发旧 tool call。validation 失败/commit
    race/rollback 则清除 fence并关闭 temporary runtimes，旧 config/runtime/catalog/
    cache 原样保留。

DB session、row lock 或 advisory lock 永远不跨 Client network await。

离线 pure removal commit 时没有可通知的 Client。若旧 Client 进程仅与 OO Server
断线但仍存活，已删 MCP runtime 可继续在宿主上运行，但 Agent 无法再路由
到它；Client 重连收到新 config、token 失效或 Client shutdown 时才关闭它。

## 11. Client MCP runtime 生命周期

### 11.1 状态

每个 configured server 的 runtime 状态为：

```text
ABSENT -> STARTING -> DISCOVERING -> AWAITING_ACK -> READY
              |            |              |          |
              +------------+--------------+----------+
                                   -> UNAVAILABLE -> BACKOFF -> STARTING
                                   -> DRIFTED
READY/config remove -> CLOSING -> ABSENT
                       -> CLEANUP_BLOCKED
```

持久 config/catalog 与 runtime state 分离。`UNAVAILABLE`/`DRIFTED` 不删除 DB config
或 last-good catalog。

### 11.2 Client 启动与 OO WS reconnect

- Client 先完成 OO hello/config `config_applied_ack`，再在后台最多并行
  4 个 MCP startup；
- 调度前先为 authoritative config 中每个 server 创建 attempt record/
  `runtime_generation`，因此等待并行 semaphore 的项也能在 full registration
  snapshot 中以 `unavailable, code=mcp_starting` 占位；
- 启动时读取 Server 下发的 authoritative config/catalog；每个 MCP 必须重新 initialize
  和 full discovery 后才可 READY；
- 无 runtime 时其能力仍来自 Server last-good catalog，调用返回
  `tool_mcp_unavailable`；
- 普通 OO WS 断线不关闭 MCP sessions。fresh hello 先安装 authoritative
  revision/等 `config_applied_ack`，再重报 current full snapshot 并等 register ack；
- Client 进程重启不恢复旧 MCP session state，只按 config 新建；
- Server 重启从 PostgreSQL 恢复 last-good schemas，不依赖旧 registry memory。

### 11.3 Transport failure 与 recovery

- stdio root exit、未恢复的 HTTP/SSE stream failure、retryable timeout/5xx 使
  server alias `UNAVAILABLE`；完整非 stream response 的正常 EOF 不是失败。
  OO runtime 后台用 1/2/4/8/.../60 秒 capped exponential backoff + jitter 重连；
- MCP SDK 1.26 在 Streamable HTTP 内使用 `Last-Event-ID` 的同 session stream
  resumption 是允许的 transport delivery recovery；它不重发 JSON-RPC request，不是
  tool invocation replay，`runtime_generation` 不变，也不同时启动第二套
  OO backoff。invocation/candidate 的 60/300 秒 outer deadline 仍覆盖所有 resume；
  只有 SDK session 最终退出/被关闭后才进入新 attempt/generation。legacy SSE
  若没有对等的 protocol resumption，unexpected EOF 直接交给 OO backoff；
- 401/403、missing executable、invalid config/protocol 等 permanent failure 进入
  `UNAVAILABLE` suspended，等待 config_update/Client restart，不 hot-loop；
- recovery 每次都是新 MCP session，重新 initialize 和四 surface discovery；
- fresh catalog 与 persisted digest 相同，经 register/ack 后 READY；
- 不同则 DRIFTED，不发布新 schema、不用旧 schema 调新 route，等待用户重新保存；
- config delete、Device delete/token rotation、connection replacement、Client normal
  shutdown 关闭对应/all runtime；普通 WS network loss除外；
- 所有退出都执行第 7.2 节的 explicit shielded FastMCP close；故意 daemonize/
  脱离的 descendant 与 exec 一样
  不承诺跨平台找回或清除。

### 11.4 Config change 与在途调用

validation 阶段完全不影响旧 runtime。commit 后 Server fence 新 MCP call；Client
按 revision 串行应用 config：

- promoted candidate 用 Server 下发的 persisted entries 完成 route 映射，先成为
  新 generation，register/ack 后开放；
- 即使 MCP config 未变，每次 revision 改变也会把保留 runtime 短暂置为
  `AWAITING_ACK`，按新 revision 重注册；
- 被删除或替换的旧 runtime 不再接新 call；
- 已发出的旧 call 可在其固定 60 秒 deadline 内结束；结果仍只匹配旧 pending id；
- deadline 后关闭旧 runtime，ambiguous call 返回 outcome unknown，late result tombstone；
- 一个 Device 最多保留 active + 一个 draining MCP generation；前一代未在 deadline
  内收敛时新的 MCP config PATCH 返回 busy，而不是无限累积 generation。

调用、validation、recovery 和 close 都不得阻塞 WS reader/ping/pong。现有 per-Device
bounded tool FIFO 继续串行 normal tool execution；candidate validation 与 runtime
supervisor 是独立 bounded tasks。

### 11.5 `list_changed`

Py7 不自动接受动态 schema，但不会忽略 server 的变化通知：

- tools/resources/prompts `list_changed` callback 只 enqueue signal；
- per-server debounce/coalesce 后做一次 bounded full four-surface rediscovery；resources
  通知同时刷新 static resources 与 templates；
- catalog 相同则保持/恢复 READY；不同则进入 DRIFTED 并报告 Server，必须由用户
  PATCH 才能替换持久 catalog；
- refresh 失败保留 last-good schema并标记 unavailable；
- `resources/updated` 只表示内容变化，不改 schema；不订阅、不轮询。

## 12. Dispatch、结果与错误

### 12.1 Invocation API

- tool：构造 MCP `ClientRequest(CallToolRequest(...))` root wrapper，通过
  `client.session.send_request(request, CallToolResult)` 取得 raw typed result，保留
  MCP `isError`、content、structuredContent；不调用会依据 `outputSchema` 隐式校验的
  `client.session.call_tool`；
- static/template resource：展开 URI 后 `client.session.read_resource`；
- prompt：`client.session.get_prompt`；
- 所有 invocation 使用 60 秒 OpenOctopus outer timeout；FastMCP/SDK 内层 timeout
  已关闭；outer timeout 时请求可能已经发出，返回
  `tool_execution_outcome_unknown`；
- OpenOctopus 从不因 timeout、disconnect 或 Agent Stop 自动调用第二次。

### 12.2 Result mapping

Client 把 MCP 内容确定性映射为现有 text/image safe blocks。所有 JSON
都使用第 8.5 节的 canonical 编码：

- `TextContent` → 一个内容不变的 text block；
- `ImageContent` 只接受精确 `image/jpeg | image/png | image/gif |
  image/webp`，base64 必须 strict-valid，然后转 image block；
- `ResourceLink` → text block
  `"[mcp_resource_link]\n" + canonical_json(model_dump(by_alias=true,
  exclude_none=true, mode="json"))`，不自动读取目标；
- 顶层或 `EmbeddedResource` 的 `TextResourceContents` → text block
  `"[mcp_resource]\n" + canonical_json({"uri": ..., "mimeType": ...}) + "\n" + text`；
  `mimeType` 为 null 时从 descriptor 省略；
- 顶层或 embedded `BlobResourceContents` 使用同样 URI/MIME descriptor，但先
  输出 `"[mcp_resource_image]\n" + canonical_json(descriptor)` text block，再输出
  image block；只接受上述四种 MIME 与 strict-valid base64；
- `AudioContent`、其它已知 binary/media 整体返回 `tool_unsupported_media`；
  未知 block、invalid base64、非 finite JSON number 或其它无法 canonicalize 的内容
  整体返回 `tool_mcp_invalid_result`，不用 Python repr/`Display` stringify；
- tool `structuredContent` 只要存在就始终在 protocol `content` 之后追加 text
  block `"[mcp_structured_content]\n" + canonical_json(value)`；Py7 不猜测双表示
  是否重复；
- prompt 保持 message/content 顺序；每条先输出精确 text block
  `[mcp_prompt_message role=user]` 或 `[mcp_prompt_message role=assistant]`，再按上述
  规则映射其 content，不做裸文本 join；
- 空成功结果返回唯一 text block `(no output)`；
- MCP `isError=true` 在内容成功完整映射后成为
  `is_error=true, code=tool_mcp_error` 的正常 tool result。
- deadline 内收到 JSON-RPC error response 时，SDK 抛出的 `McpError` 映射为
  `tool_mcp_error`；不得把 third-party `message`/`data` 原样返回或写日志。若
  `CallToolResult` 或其它 invocation result 的 Pydantic typed parse 失败，则映射为
  `tool_mcp_invalid_result`，不得 stringify/log validation input。

映射为 all-or-nothing；任何 block 失败都不返回前面的 partial blocks。

Client 在构造完整结果期间受 `max_result_bytes` 约束；MCP call 使用现有 12 MiB
control-frame credit，Server 再统一加 untrusted warning，并把 provider-visible text
限制为默认 16,000 chars。图片仍只接受现有四种 MIME 和 base64/总 frame 上限。
大小以包含所有 label/descriptor/base64 的最终 encoded `ToolResult` frame 计算；
超 `max_result_bytes` 复用现有 all-or-nothing `tool_result_too_large`，不截断 MCP
block。
这些是 decode 后 semantic/result limits；第 7.2 节的 raw cap 是独立的
pre-decode memory boundary。

### 12.3 稳定错误语义

| 场景 | code |
|---|---|
| Device offline/WS generation 不可达 | `tool_device_unreachable` |
| Device online但 MCP starting/down/drifted/not acked | `tool_mcp_unavailable` |
| MCP server 返回 `isError` | `tool_mcp_error` |
| MCP server 返回 JSON-RPC error response | `tool_mcp_error` |
| MCP runtime 入站 message 超 raw cap | `tool_mcp_message_too_large` |
| 结果含不支持 media/blob | `tool_unsupported_media` |
| MCP result 无效 base64/JSON/unknown block | `tool_mcp_invalid_result` |
| 最终 encoded MCP result 超 response credit | `tool_result_too_large` |
| 调用明确未发送且 config 已变 | `tool_mcp_unavailable` |
| 调用已发送或发送边界不明确后丢结果 | `tool_execution_outcome_unknown` |
| REST MCP syntax/discovery/limit failure | `config_validation_failed` / `mcp_spawn_failed` / `mcp_message_too_large` |
| wrapped-name collision | `mcp_within_server_collision` |
| owner install sites schema drift | `mcp_schema_collision` |
| owner 合并后 capability/schema 超限 | `mcp_owner_schema_limit` |

错误 message 告诉 Agent MCP/Device 当前状态和“不要盲目重放可能有副作用的调用”；
Agent loop 继续。未知 third-party exception 只映射到稳定 code 和 bounded generic message。

## 13. Tool registry、cache 与 Agent turn

Server tool registry 对当前 user 查询所有 paired Device 的持久 last-good catalogs，
不按 online 过滤。构建顺序：

1. fixed built-ins；
2. 每个 Device catalog 的 enabled entries；
3. 按 logical identity/schema 合并 install sites；
4. 注入 `openoctopus_device` exact enum；
5. 生成或复用 Provider shape cache，并从本次 DB/catalog snapshot 单独生成 immutable
   route table。

Provider shape cache 只含 Provider-visible schema，不含 config revision、entry 的运行时
binding 或隐藏 route。它只在 Device create/delete/rename、MCP config/catalog commit
或 enabled shape 改变时失效；WS connect/disconnect、MCP crash/recovery、register
availability 和不改变 tool shape 的普通 config revision 不使其失效。隐藏 route table
不得跨 Provider iteration 缓存：每个 iteration 都从同一份最新 DB/catalog snapshot
重建并绑定该时刻各 Device 的 revision/digest。这样复用 schema shape 不会携带旧
revision。

每个 Provider iteration 使用同一 DB/catalog snapshot。Provider 返回 tool_use 后，
runner 使用该 iteration 捕获的 route；如果当前 revision/digest 不再相同，明确 pre-send
失败，不把旧 args 交给新 schema。

Provider 永远只看到 final name、description、input schema 和
`openoctopus_device`；看不到 transport、URL、command、env/header、entry id、revision、
catalog digest、runtime state 或 validation metadata。

## 14. 安全与可观测性

Py7 的真实信任模型：

- Server admin 信任 Server 进程和 PostgreSQL；
- Device owner 配置 MCP 等价于允许该 MCP 使用 Device 用户权限；
- LLM/Agent、MCP output、web content 和文件内容仍是不可信输入；
- `restrict_to_workspace` 只降低结构化工具误操作，不防恶意 shell/MCP/local process；
- remote/stdio MCP 可以访问内网并绕过 `web_fetch` denylist；这是明确接受的用户安装
  capability；
- HTTP MCP static headers 可能授予第三方高权限；Py7 无 per-call approval。

允许的 lifecycle 日志字段：Device/server name、transport、state transition、attempt、
elapsed、capability counts、catalog digest 前 12 chars、稳定 code。禁止 command、args、
cwd、URL query、headers、env、MCP stderr、tool input/output、raw protocol frame 和 secret。
Client 显式丢弃 `fastmcp`、`mcp`、`httpx`、`httpcore` 的 third-party log
records，包括 TRACE；只由 OpenOctopus wrapper 产生上述 normalized lifecycle log。
测试进程安装 recording OTel exporter 后，discovery/invocation 仍不得产生
FastMCP client span 或向 fake MCP 传 `traceparent`/`tracestate`。

不新增 REST diagnostics、doctor command 或 Client 本地持久状态。用户通过 Device
config detail 看 last-good discovery，通过正常 tool error 看 runtime availability。

## 15. TDD 实现顺序

### Slice A：字段改名与网络策略

先写失败测试覆盖：旧 `sandbox_mode` strict reject、默认/显式 restriction、路径矩阵、
exec 在 true Device 的 schema/working_dir guard、SSRF 与 restriction 解耦、Server admin
denylist canonical/invalid/no-save/hot-read/redirect snapshot。然后最小改 DB/DTO/API/
protocol fixtures/path resolver/tool registry/prompt/docs。

### Slice B：FastMCP transport spike

先 pin 依赖并写三个真实 fake MCP：stdio、Streamable HTTP、SSE。验证 initialize
timeout、explicit transport、no redirect、headers、safe stdio env、stderr、shielded close，
以及通过 `client.session` 的全部 SDK list/raw-call API/OTel recording exporter 边界。
把 secret sentinel 同时放入 URL query、JSON-RPC tool input/output 和 response body，
开启 Python 最低可用日志级别并捕获 root/application handlers，断言
`fastmcp.*`/`mcp.*`/`httpx`/`httpcore` 没有任何 sentinel 泄漏。另用声明
`outputSchema` 但返回不匹配结果及包含无效 schema keyword 的 fixtures，断言 raw
`send_request` 仍进入第 12.2 节映射，不出现 SDK convenience validation exception；
另测 JSON-RPC error 与 typed result `ValidationError` 的稳定 code 和脱敏。
还要在 decode 前测 stdio 单/多 chunk/无 LF
超限，HTTP Content-Length/chunked 边界，SSE 多 data/巨大 id/comment/LF、
CRLF、bare CR 及跨 chunk delimiter，以及 gzip/deflate 在读 body 前拒绝。
三平台还要测 bidirectional stdin、Windows `PATHEXT`/`npx.cmd`、close 的
2/3/5 s 阶段、幂等性、`cleanup_incomplete` 后禁止 replacement。
三平台 source test 后再接入 runtime，不在 spike 中造通用 framework。

### Slice C：Catalog 与 wrapping

先测四 surface、多页/重复 cursor/limits、RFC6570、prompt args、64-char naming、所有
cross-surface collision、built-in reserve、enabled 三态、unknown selector、跨 Device
equal merge/schema drift、canonical digest fixtures。再实现纯 catalog/wrapper 模块。

### Slice D：REST validation 与 Protocol v3

先测 v3-only/4409、首次失败退出/ever-ready重连、secret redaction/retain/delete、offline
pure removal、offline modification reject、candidate success/failure/no-write、HTTP cancel、
replacement、late validation tombstone、commit-push ambiguity、config_applied fence、
register/ack 和 stale route。使用 deterministic barriers 覆盖每个 await boundary，并测
ACK 前 READY→UNAVAILABLE/DRIFTED 且 generation 不变时旧 snapshot 不会重新 accepted，
以及 handler cancellation 后 transition 仍独占 mutation lock 直至 `finally`；并测
direct WS spoofed forwarded proto 被拒绝、trusted TLS reverse proxy 被识别为 WSS。

### Slice E：Runtime、dispatch 与 result

先测 startup 不阻塞 fixed tools、stdio crash、HTTP/SSE drop、backoff、permanent suspend、
WS reconnect keeps runtime、Client shutdown/removal cleanup、schema drift、list_changed
coalesce、old generation drain、no replay、late result，以及所有 content mapping/size/
error cases。result 使用精确 canonical fixtures 断言 block 顺序/label/descriptor、
structuredContent、invalid base64/non-finite JSON、unsupported mixed media 的 all-or-nothing，
以及最终 encoded frame credit。Agent 测试断言错误后继续下一 iteration。

### Slice F：Packaging、文档与 E2E

更新 PyInstaller hidden imports/data，三平台构建 frozen Client 并运行 stdio MCP child；
更新 `DECISIONS.md`、`SCHEMA.md`、`API.yaml`、`PROTOCOL.md`、`TOOLS.md`、
`SYSTEM_PROMPT.md`、Client README 和 contract snapshots。同步更新
`docs/reference/adr-audit-python-main.md` 的 translation 结论，并在 Py5/Py6 历史
design 顶部增加被 Py7 Protocol v3 supersede 的简短指针；不改写其历史内容。
最后运行 Docker PostgreSQL +
RustFS + Server、native/frozen Client、fake provider 与真实 MCP 的完整 E2E。

## 16. 测试矩阵与合并门禁

### 16.1 自动测试

Linux Server CI：

- server 全套 pytest、Ruff、strict mypy；
- PostgreSQL-backed admin denylist 与 Device MCP config/catalog transaction；
- Protocol v3 registry/fence/cancellation/late-result tests；
- 真实 TLS reverse-proxy WSS + explicit `FORWARDED_ALLOW_IPS` secret-config E2E，以及
  不受信 direct peer 伪造 `X-Forwarded-Proto` 的 fail-closed 测试；
- offline catalog schema build、collision/cache/turn snapshot；
- Docker fake provider + Client stdio/HTTP/SSE integration。

Linux/macOS/Windows Client CI：

- client 全套 pytest、Ruff、strict mypy；
- source-mode stdio MCP real child；HTTP/SSE transport integration；
- path restriction outside sentinel、symlink/reparse/junction cases；
- exec/PTY `restrict=true` 初始 cwd 与 command 可逃逸边界的诚实测试；
- PyInstaller build、frozen existing smoke、frozen stdio MCP smoke；
- Device token/`OPENOCTOPUS_*` sentinel 不出现在 child env；配置的 MCP env
  sentinel 必须到达 child，但不出现在 OpenOctopus log/error/catalog。fake MCP
  另外主动 echo 该值时，结果允许包含它，以验收 trusted-boundary 说明。

macOS 至少 arm64，现有 x64 runner 保留；Windows 使用 NTFS junction/reparse cases，
Linux 使用 symlink swap harness。平台缺少权限时，关键 boundary test 不得静默 skip；
应使用不需要管理员权限的等价 fixture或使该 job失败。

### 16.2 容量与资源

- 16 configured MCP、512 capabilities、2 MiB catalog 边界；
- 多个单独合法 Device 聚合后的 owner 256 enabled names/256 KiB Provider
  schema 边界，并发跨 Device commit 只能一个成功；
- validation parallelism=4、tool queue、pending byte credit 与 reconnect tasks high-water；
- repeated crash/recovery/config replace 后无 root MCP child、HTTP connection、task、future
  或 candidate runtime 泄漏；
- 500 Device harness 继续作为手动 merge gate，加入 bounded offline catalog variant，
  上传 JSON、RSS、FD/handle、task、queue 和 MCP runtime high-water artifact。

### 16.3 真实 smoke

合并前执行一次：

1. Docker 启动 PostgreSQL、RustFS、OpenOctopus Server；
2. API 注册 admin/user/device；
3. admin 热改 Server web denylist并验证 invalid no-save；
4. native或 frozen Client 连接；
5. 在线 PATCH 添加 stdio、Streamable HTTP、SSE MCP，验证四 surface discovery；
6. Agent 调用 tool、static resource、template、prompt、Client internal web_fetch、exec；
7. 断开 Client，确认 schema 保留且返回 device unreachable；
8. 恢复 Client/MCP，确认不 replay旧调用并恢复 READY；
9. 离线删除 MCP成功、离线修改失败；
10. shutdown 后无 Client/MCP/child/container 残留。

真实付费 LLM smoke 使用临时 API key，key 只经现有 admin provider config 写入，不写
fixture、commit 或日志；确定性 CI 使用本地 Anthropic-compatible fake provider。

## 17. Acceptance gate

Py7 只有同时满足以下条件才完成：

- 三平台 Client 与 Linux Server 自动矩阵全绿；
- Protocol v3 无 v2 compatibility path；
- 旧 `sandbox_mode`、device `enabled_tools`、typed MCP infix 和离线乐观新增逻辑从
  Python-main canonical docs/code/fixtures 删除；
- secret redaction、validate-before-save、last-good offline visibility、no replay、
  config cancellation 和 schema drift 均有 deterministic regression tests；
- source 与 frozen Client 都可运行真实 stdio MCP；HTTP/SSE 不依赖 transport inference；
- Server web denylist 热更新在多个请求间生效且每次调用只取一个 immutable snapshot；
- 500 Device artifact 和 Docker Server + native Client E2E 通过；
- implementation diff 只实现本 spec，不提前实现 Server MCP、OAuth 或 OS jail。

## 18. 决策状态

本 draft 已把开始实现前的产品决策闭合；没有要求实现者暗选的开放分支。审阅若接受，
先单独 commit 本 spec，再按第 15 节以 TDD slices 实现。canonical ADR/API/PROTOCOL
在 Slice A/D 随实现更新，不能仅让本 design 文档长期覆盖已过时契约。

参考的一手契约：

- [FastMCP 3.4.7 release](https://github.com/PrefectHQ/fastmcp/releases/tag/v3.4.7)
- [FastMCP Client](https://gofastmcp.com/clients/client)
- [FastMCP transports](https://gofastmcp.com/clients/transports)
- [MCP cursor pagination](https://modelcontextprotocol.io/specification/2025-11-25/server/utilities/pagination)
- [MCP schema reference](https://modelcontextprotocol.io/specification/2025-11-25/schema)
- [uritemplate 4.2.0](https://pypi.org/project/uritemplate/4.2.0/)
