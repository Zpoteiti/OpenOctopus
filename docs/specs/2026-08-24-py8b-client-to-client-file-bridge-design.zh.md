# Py8b 不同 Client 单文件桥接设计

**状态：** accepted，implemented
**Milestone：** Py8b Client-to-client single-file bridge
**依赖：** 已完成的 Py5 Python Client/device files、Py6 cross-platform exec 与
Py7 Protocol v3
**目标协议：** Protocol v3，不升级版本
**后继：** Py8c recursive directory transfer 复用本设计的逐文件 bridge；Py8b
本身不实现目录语义

## 1. 结果与边界

Py8b 允许同一个 authenticated owner 的两个不同已配对 Device 之间复制或移动一
个普通文件。例如：

```text
alice-laptop:reports/a.pdf -> alice-phone:archive/a.pdf
```

Server 是唯一的协调者和有界字节 relay。两个 Client 不互相连接，不交换 Device
token，也不发现对方的网络地址。Server 不把文件写入 RustFS、本地临时文件或其它
durable staging；它只在两个既有 Device WebSocket generation 之间转发 Protocol v3
控制帧与有界 binary chunks。

Py8b 只解除当前 `file_transfer` 对“两个不同 Client”组合的限制。以下既有路径不改
语义：

- `server -> server`：继续由 `WorkspaceService` 执行；
- `server -> client` 与 `client -> server`：继续使用现有单端 transfer slot；
- 同一个 Client -> 同一个 Client：继续调用私有 `transfer_local`，不经过 bridge；
- 不同 Client -> 不同 Client：新增本设计的一个端到端 bridge slot。

工具参数和 REST body 不增加字段。Agent 工具的 `mode` 继续默认 `copy`；REST
继续要求显式提供 `mode`。返回值继续是 `bytes_transferred`、`sha256`、`warnings`。
没有 DB schema 变更、migration 或 compatibility shim。

## 2. 已确定的决策

1. **仅同 owner。** Source 与 destination 必须是当前 authenticated user 拥有的两个
   不同 Device row。管理员身份不提供跨用户代传能力。
2. **一个逻辑 bridge。** 一个 distinct-client 调用只有一个 UUID v7、一个结果、一个
   Server global transfer permit 和一个该 owner 的 per-user transfer permit。它不是
   两次相互独立的 transfer。
3. **两个 Client 各占一个本地 slot。** Source Client 在收到 `transfer_request` 后占
   一个 sender slot；destination Client 在收到 `transfer_begin` 后占一个 receiver
   slot。既有每 Client 最多两个 active transfer slots 的限制不变。
4. **Protocol v3 不变。** Bridge 复用现有 `transfer_request`、`transfer_begin`、
   `transfer_ready`、`transfer_progress`、`transfer_end` 和 binary frame。没有新 frame、
   field、capability 或 fallback，因此不升级到 v4。
5. **同一 UUID 跨两个 route。** 同一个 slot UUID 同时绑定 source 与 destination 的
   `(device_id, WS generation, config_epoch, device_name)` snapshot。UUID 不脱离 route
   单独作为授权或查找依据。
6. **Destination ready 后 source 才能发送。** Server 只有在 destination 已保留目标
   临时文件并返回 `transfer_ready` 后，才把 ready 转给 source。此之前 source 不得发送
   binary bytes。
7. **纯 relay。** Server 使用固定大小 queue 和现有 per-slot WS bulk lane 做
   backpressure，不持久化、不整文件缓存、不为了重试保留字节。
8. **永不覆盖。** `file_transfer` 继续没有 overwrite flag。Destination 已存在或在
   commit 前被外部创建时失败；不能退化为 `os.replace`。
9. **目标原子发布。** Destination 写入同目录随机临时普通文件，增量计算 SHA-256，
   校验 byte count/digest，flush/fsync 后用 atomic no-replace primitive 发布。失败或
   取消不暴露 partial destination。
10. **Move 是 copy-then-conditional-delete。** 只有 destination 的成功最终 ACK 已被
    Server 验证后，Server 才以 source `transfer_begin.etag` 为 `if_match` 请求删除
    source。删除失败不回滚 destination；调用成功并返回 `source_delete_failed`
    warning。
11. **不自动重放。** queue/reconnect/timeout 只改变可用性，不会重新发送一个已发出
    或可能发出的 bridge。失败重试由 Agent 或用户在检查两端状态后决定。
12. **单 worker 边界不变。** Registry、bridge slots、admission、queues 与 tombstones
    都是当前单 ASGI worker 的进程内状态；Py8b 不宣称 multi-worker 可路由。

## 3. 范围

### 3.1 包含

- Agent `file_transfer` 对两个不同 paired Client install sites 的 schema 与 runtime
  支持；
- `POST /api/workspace/transfer` 对相同组合的支持；
- 同 owner 双 Device identity、online route 与 generation/config snapshot 校验；
- 一个 UUID v7、两个 generation-scoped endpoint bindings 的 bridge state machine；
- destination-ready gating、64 KiB binary chunks、有界 queue 与端到端 backpressure；
- Server 增量 byte-count/SHA-256 校验，但不持久化 file bytes；
- Client 现有 no-overwrite、same-directory temp、fsync、atomic no-replace commit；
- `copy` 与 `move`，包括 fingerprint 条件删源和 warning；
- admission、idle timeout、cancellation、disconnect、connection replacement、late
  frame 与 tombstone 生命周期；
- Server/Client unit、contract、integration、真实双 Client E2E 与容量/内存测试；
- 实现 PR 中同步更新 canonical ADR/API/PROTOCOL/TOOLS 与 contract fixtures。

### 3.2 不包含

- 文件夹、递归 walk、manifest 或空目录传输；
- HTTP Range、offset、resume、checkpoint 或断点续传；
- content-addressed deduplication、compression、archive 打包或解包；
- Server/RustFS staging、跨重启恢复、持久 transfer queue；
- Client 直连、WebRTC、P2P 地址交换或 Device token 转发；
- 跨用户分享、管理员代传、shared Device ownership；
- overwrite、merge、rename-on-conflict 或 destination rollback；
- 多文件事务、move 的分布式原子性；
- Protocol v4、版本协商或旧 frame alias；
- 前端传输 UI、进度页面或历史记录。

## 4. 不可破坏的不变量

1. 每个 REST、Agent schema snapshot、DB identity lookup、route capture 和初始 send 都
   检查 authenticated owner；另一个用户同名 Device 永远不能被选中。
2. Source 和 destination 必须解析为两个不同 immutable Device IDs。名称相同的请求
   进入既有 same-client local 路径；名称复用或 rename race 失败关闭，不改绑到新 row。
3. 在等待 admission、Client、WebSocket I/O、文件 I/O 或 source delete 时不持有 DB
   session、registry global lock 或两个 connection send locks。
4. 一个 bridge 在 Server admission 中只计一次，且从 admission 成功直到 terminal
   cleanup 完成始终持有同一 lease。任何路径都必须恰好释放一次。
5. Source request 与 destination begin 使用同一个 UUID；每个入站 frame 还必须匹配其
   endpoint role、Device ID、WS generation 和状态。UUID 相同不能跨 generation 投递。
6. Destination `transfer_ready` 是 source 收到 ready 的唯一依据。Server 不凭 route
   online、临时文件创建任务已启动或 queue 存在而提前放行 source。
7. Server 从不保留整个文件。每个 bridge 最多保留四个 64 KiB source chunks 加一个
   relay worker 当前 chunk；现有 destination WS writer 的 per-slot lane 仍保持有界。
8. Source 声明的 `total_bytes` 必须是非负整数。Server 拒绝超过声明长度的 chunk，
   并在转发成功终端前独立核对实际 byte count 与 SHA-256。
9. Destination 只有在 source 成功 terminal、全部已转发 chunks、byte count 和 digest
   全部匹配后才能 commit。最终 ACK 丢失时可能已经 commit，因此属于明确的
   outcome-unknown 边界。
10. 失败、取消、idle timeout 或断线不得暴露 destination partial；destination Client
    拥有临时文件与 path lock 的清理责任。Server 永远不能用 delete destination 去
    猜测一个不确定 commit。
11. `move` 永远先确认 destination commit，再条件删除 source。Source 删除失败最多
    留下两份，不允许出现“删除 source 后 destination 未提交”。
12. Connection replacement 不迁移 active bridge。旧 generation 的 frame 只能由旧
    endpoint binding/tombstone 消费；新 generation 不继承 slot、queue 或 terminal。
13. 精确 late terminal 和已知失败后的有界在途 chunks 不关闭健康连接。未知、冲突、
    过期或 normally-completed slot 的 binary frame 仍是发送方 generation 的协议错误。
14. 任何自动 reconnect 都只恢复 Device 在线状态，不恢复、不继续、不 replay bridge。
15. Source 与 destination 各自使用 slot 开始时捕获的 immutable
    `restrict_to_workspace`/workspace snapshot。配置更新只影响后来开始的 transfer。
16. Server 不向任一 Client、Provider、日志或错误暴露另一个 Device 的 token、私有
    config、Server 解析出的 canonical path 或原始协议 payload。调用方提供的
    `src_path`/`dst_path` 只按既有 v3 transfer metadata 传递。

## 5. 授权、install sites 与 schema

### 5.1 同 owner identity

Agent 每次 Provider iteration 已捕获 owner 的 `device_targets: name -> immutable id`。
Distinct-client dispatch 必须：

1. 从该 snapshot 取得 source 与 destination 的预期 Device ID；
2. 在一个短 DB 操作中按 `(user_id, name, expected_id)` 重新解析两行；
3. 要求两行均存在且 ID 不同；
4. 关闭 DB session；
5. 在 registry 同一临界区捕获两个 current ready route snapshots；
6. 在 admission 后、首次 send 前再次验证两个 snapshots 仍为 current。

REST 使用 `get_current_user` 的 `user.id` 做相同解析，只是不带 Provider turn snapshot。
missing、deleted、renamed、另一个用户拥有、offline、config update in flight 和 stale
generation 对调用方统一表现为 `tool_device_unreachable`，不得形成跨用户 Device
existence oracle。

Registry 新增 `get_bridge_route_pair(...)`，在一次 registry critical section 中取得 pair
snapshot，而不是依次取得两个 handle 后假装它们来自同一个时点：

```python
async def get_bridge_route_pair(
    *,
    user_id: UUID,
    source_device_id: UUID,
    source_device_name: str,
    destination_device_id: UUID,
    destination_device_name: str,
) -> BridgeRoutePair | None: ...
```

返回值为：

```text
BridgeRoutePair(
  source=DeviceRouteSnapshot(handle, config_epoch, device_name),
  destination=DeviceRouteSnapshot(handle, config_epoch, device_name),
)
```

返回后不持有 registry lock。初始 source `transfer_request` 的 send 使用 source route
fence；destination `transfer_begin` 的 send 使用 destination route fence。

### 5.2 Agent schema

`openoctopus_src_device` 与 `openoctopus_dst_device` 保留
`x-openoctopus-device: true`，merge 后 enum 都是：

```text
["server", *owner_device_names]
```

Py8b 删除 `file_transfer.input_schema.anyOf` 与
`x-openoctopus-same-device`。两个 enum 的全部笛卡尔组合都成为合法 schema：

| Source | Destination | 执行路径 |
|---|---|---|
| `server` | `server` | existing WorkspaceService local transfer |
| `server` | Client A | existing server-to-client slot |
| Client A | `server` | existing client-to-server slot |
| Client A | Client A | existing private `transfer_local` |
| Client A | Client B | new Py8b bridge |

Paired-but-offline Device 继续出现在 enum；runtime 返回
`tool_device_unreachable`。Provider schema 不随 online/offline 抖动。

### 5.3 Runtime request

现有五个字段保持：

```json
{
  "openoctopus_src_device": "alice-laptop",
  "src_path": "reports/a.pdf",
  "openoctopus_dst_device": "alice-phone",
  "dst_path": "archive/a.pdf",
  "mode": "copy"
}
```

- device names、paths、`mode` 与 unknown-field validation 不变；
- `src_path` 与 `dst_path` 分别只由对应 Client 按自己的 policy 解析；
- Server 不把一个 Client 的 canonical absolute path 发给另一个 Client；wire 只携带调用
  中的 display/request paths；
- source 必须是一个普通文件；directory/symlink/special file 按既有稳定错误拒绝；
- destination 必须能创建一个普通文件，且最终路径必须不存在。

## 6. Server runtime 结构

### 6.1 一个专用 bridge slot

`TransferManager` 新增专用 `_BridgeSlot`，不通过同时调用
`start_client_to_server()` 与 `start_server_to_client()` 拼接两次 transfer。后者会产生
两个 leases、两个 caller outcomes 和难以收敛的 ACK 语义，不符合本设计。

唯一公开编排入口为：

```python
async def start_client_to_client(
    *,
    source_route: DeviceRouteSnapshot,
    destination_route: DeviceRouteSnapshot,
    user_id: UUID,
    src_path: str,
    dst_path: str,
    mode: Literal["copy", "move"],
    delete_source: Callable[[str], Awaitable[None]] | None,
    on_issued: Callable[[], None] | None,
) -> TransferResult: ...
```

`delete_source` 的参数是 source begin 中的 opaque fingerprint。`FileTransferTool` 新增
`_client_to_distinct_client()`，负责同 owner DB resolution、route pair、构造条件删除
callback，并只调用该入口一次。

逻辑结构至少包含：

```text
slot_id: UUIDv7
user_id: UUID
source_route: DeviceRouteSnapshot
destination_route: DeviceRouteSnapshot
src_path / dst_path
mode: copy | move
state: BridgeState
lease: TransferLease
queue: Queue[bytes | EOF]  # maxsize=4
relay_task / completion
source_begin / source_end / destination_end
source_fingerprint
bytes_received / bytes_forwarded / sha256
last_progress
source_issued / destination_issued / destination_terminal_issued
destination_committed / source_ack_delivered / source_ack_impossible
source_resolution: open | destination_ack | timeout_ack
source_timeout_ack_sent
```

`source_issued` 与 `destination_issued` 是两个独立的 generation-fenced send boundary，
分别在向 A 发送 `transfer_request`、向 B 发送 `transfer_begin` 的同步 issued callback 中
设置。只有 source callback 触发 public caller `on_issued`，且恰好一次；destination flag
只决定 B route 的 cleanup/tombstone。`destination_terminal_issued` 在向 B 发送 source
success terminal 的同步 send boundary 设置，用于禁止之后再发送竞争 failure terminal。
`source_resolution`只在bridge lock内从`open`原子选择为`destination_ack`或`timeout_ack`；
前者由B的validated ACK选择，后者由A的exact sender-timeout或“B结果无法再确认”的terminal
选择。它决定S最终向A发送哪一种ACK语义，不能被随后frame改写。

Manager 使用 endpoint index 将两端 frame 路由到同一逻辑 slot：

```text
(source_device_id, source_generation, slot_id)      -> (bridge, SOURCE)
(destination_device_id, destination_generation, slot_id) -> (bridge, DESTINATION)
```

同一 UUID 在不同 route key 下合法。一个 key 不得同时属于普通 slot 和 bridge；随机
UUID collision 或重复 start 在任何 frame 发出前失败。`active_slots`/admission metrics
按逻辑 bridge 计一次，endpoint index 可单独暴露调试计数但不是容量单位。

### 6.2 为什么不 staging

Bridge 不创建 Server `TransferSink`、RustFS multipart upload、Server temp file 或完整
bytes object。Server 只保存协议 metadata、hash state 和有界 chunks。结果是：

- destination 必须与 source 同时在线；
- 慢 destination 通过 queue/WS writer 反压 source；
- Server restart 或任一 generation replacement 终止 transfer；
- 已转发 bytes 不可用于 resume 或自动 retry；
- Client-to-client 不消耗 Server workspace quota或 object-storage connection。

这不是“先 Client A -> server，再 server -> Client B”的两步用户可见操作，也不会在
Server workspace 留下中间文件。

## 7. Protocol v3 端到端流程

设 source Client 为 A，Server 为 S，destination Client 为 B，slot ID 为 X。

```text
S -> A  transfer_request(X, purpose=file_transfer, src_path, dst_path)
A -> S  transfer_begin(X, direction=client_to_server, metadata + source etag)
S -> B  transfer_begin(X, direction=server_to_client, rewritten route metadata)
B -> S  transfer_ready(X)
S -> A  transfer_ready(X)
A -> S  binary(X, chunk) ...
S -> B  binary(X, same chunk) ...
A -> S  transfer_end(X, ack=false, ok=true, bytes_sent, sha256)
S -> B  transfer_end(X, ack=false, ok=true, bytes_sent, sha256)
B -> S  transfer_end(X, ack=true, ok=true, bytes_sent, sha256)
S -> A  transfer_end(X, ack=true, ok=true, bytes_sent, sha256)
S -> A  tool_call(__workspace_rest__.delete_file, if_match=etag)  # move only
```

最后一行是既有私有 tool dispatch，不是 transfer frame。`copy` 没有该步骤。

### 7.1 Source request 与 begin

首次可产生远端可见状态的动作是 S 向 A 发送 `transfer_request`。`on_issued` 必须在
source route 的 fenced send boundary 恰好触发一次。在此之前的失败是 pre-issue；
此后整个调用是 issued，即使 B 尚未收到 begin。

A 使用 slot 开始时的 config snapshot：

1. 解析并锁定 `src_path`；
2. no-follow 打开并 `fstat` 普通文件；
3. 捕获 size 与 opaque source fingerprint；
4. 返回现有 v3 `transfer_begin(direction="client_to_server")`。

S 验证 frame 来自 source route，ID/purpose/direction/path 与请求一致，且
`total_bytes` 非负。`src_device` 若存在必须匹配 captured source name。当前 Client 对
普通 `file_transfer` 会把 `dst_device` 写成 `server`；S 不把该值当作最终 destination
授权，也不原样转发。

### 7.2 Destination begin 与 ready gate

S 根据已验证 source begin 构造一份新的 v3 begin 发给 B：

```json
{
  "type": "transfer_begin",
  "id": "<same X>",
  "direction": "server_to_client",
  "purpose": "file_transfer",
  "src_device": "alice-laptop",
  "src_path": "reports/a.pdf",
  "dst_device": "alice-phone",
  "dst_path": "archive/a.pdf",
  "total_bytes": 2457600,
  "etag": "<opaque-source-fingerprint>"
}
```

可选 `sha256`/`mime` 只有在 source begin 合法提供时才复制。S 不添加 v3 DTO 未定义
的 bridge 字段。

B 必须先解析 destination policy、取得 path lock、确认 destination 不存在并创建同目录
临时文件，才发送 `transfer_ready(X)`。S 验证 ready 来自 destination route 后才转发
给 A。B busy、路径失败或 destination exists 时发送既有失败 terminal；A 永远不会
收到 ready，也不会发送文件 bytes。

### 7.3 Binary relay 与 backpressure

A 收到 ready 后才能发送 binary frames。每帧保持既有布局：

```text
16-byte UUIDv7 X | 0..65536 payload bytes
```

Source inbound handler：

1. 以 `(A device_id, A generation, X)` 查 bridge；
2. 验证 role/state，拒绝 ready 前、terminal 后或超过 declared length 的 bytes；
3. 更新 `bytes_received` 和 Server SHA-256；
4. 在 idle deadline 内写入 maxsize=4 的 bridge queue。

单独 relay worker 从 queue 取一帧，使用 captured destination route 将同一 X 与相同
payload 写入 B 的既有 per-slot bulk lane；send 完成后才取下一帧。queue 满会反压 A
的 WS reader，不扩容、不丢 chunk。critical control lane、normal control lane 与其它
slot 的 round-robin 行为保持现有合同。

`transfer_progress` 是可选提示。只接受 source route 上单调不减且不超过已声明大小的
progress；bridge active 时可转发给 B。它不刷新一个没有真实 byte/control progress 的
无限 deadline，也不参与最终正确性判断。

### 7.4 成功 terminal 与 commit

A 完成读取、再次验证 source identity 后发送
`transfer_end(ack=false, ok=true, bytes_sent, sha256)`。S 必须：

1. 验证 source terminal 与实际接收 byte count/digest 一致；
2. 等待 bridge queue 与 destination writer 中该 slot 的 chunks 全部完成；
3. 将相同成功 terminal 发送给 B；
4. 等待 B 校验 temp、fsync 并 atomic no-replace commit；
5. 验证 B 的 `ack=true` response：`ok=true` 时核对 byte count/digest并记录 commit；
   `ok=false` 时按下面的失败 ACK 合同收敛；
6. 在bridge lock内尝试把`source_resolution`从`open`置为`destination_ack`。成功才把B的
   ACK转发给A；若`timeout_ack`已先胜出，则不再发送late B ACK，直接按§7.6收敛；
7. 只有成功 ACK 已确认送达 A 时，才对 `move` 执行第 9 节的 source delete；
8. 完成 caller result、cleanup、tombstone 与 lease release。

B 的成功 ACK 是 destination 已经可见的 commit point。此后不能因为 caller cancel、A
断线或 source delete 失败而删除 B 的文件。S 在记录
`destination_committed=true` 前必须对 commit result 使用 cancellation-safe transition；
调用任务取消不能让“B 已提交但 Server 当作普通 aborted cleanup”的状态出现。

B 在收到 A 的 `ack=false, ok=true` terminal 后，仍可因 digest、fsync、no-replace commit
或其它 destination validation 失败而返回
`transfer_end(ack=true, ok=false, code=...)`。这是 Protocol v3 已有的合法 receiver response，
不是 conflicting terminal。S 验证并原样转发给 A；A 的 sender 必须接受“success sender
terminal 对应 failure ACK”，释放 source slot/path lock并以该 failure 收敛，不能因
`ok` 不相等抛 ProtocolError 或关闭健康 WebSocket。该路径 destination 未 commit、source
未删除，也不触发自动 retry；无需新 frame、field 或 Protocol v4。

若A的本地idle timer已发出sender-timeout、但S的`destination_ack` resolution先胜出，A仍
必须接受随后到达的这一个chosen B success/failure ACK并释放slot；“已经发过timeout”不能把
该ACK判成terminal mismatch。之后S只消费late exact timeout，不再返回第二个ACK。

### 7.5 失败 terminal 的桥接

- **A 在 B begin 前失败：** S 直接向 A 返回 matching failure ACK；B 从未创建 slot。
- **A 在 B active 后失败：** S 将 failure terminal 发给 B，B 删除 temp 并 ACK；S 将
  matching ACK 发给 A。
- **B 在 ready 前拒绝：** S 将 B 的 failure terminal 发给 A；A 停止 sender、释放
  source lock 并 ACK；S 将 matching ACK 发给 B。
- **B 在 streaming 中失败：** 同上；S 停止接受/转发后续普通 bytes，只按 known-failed
  tombstone 的窄例外 drain 已在途的有界 chunks。
- **Server 自身在 destination terminal issue 前检测 length/digest/state 错误：** 向仍
  在线的两端发送 stable failure terminal，建立两端 tombstones；协议违规只关闭实际
  违规的 generation。若 destination success terminal 已进入 send boundary，则适用
  `destination_terminal_issued` fence，不再向 B 发第二个 terminal。

Failure ACK 不产生自动重试。任何 failure propagation send 自身失败都进入第 11 节的
disconnect/outcome rules。

### 7.6 Source sender timeout 与 B ACK 的唯一 resolution

现有 Client sender 在发出 `ack=false, ok=true` success terminal 后仍受 idle timer约束；
若迟迟收不到ACK，它会发送第二个
`ack=false, ok=false, code=workspace_transfer_timeout`。当
`destination_terminal_issued=true`时，S在bridge lock内让A timeout与B validated ACK原子
竞争`source_resolution`：

1. S先验证frame来自A的exact route/slot，且code/payload精确匹配sender-timeout形状；
2. timeout先把`open -> timeout_ack`：S只向A返回matching failure ACK并设置
   `source_ack_impossible=true`；matching ACK的send boundary确认后才置
   `source_timeout_ack_sent=true`。S绝不向B转发timeout、发送第二个destination terminal或
   取消B commit，继续在原bounded deadline内等B真实ACK；
3. 此后B success时destination成功：copy返回success + `transfer_ack_failed`，move禁止source
   delete并返回success + `transfer_ack_failed, source_delete_failed`；B failure按确定failure
   收敛且source保持；B未知时caller outcome unknown；
4. B ACK先把`open -> destination_ack`：S只把该chosen success/failure ACK发送给A；A即使
   已在本地发出timeout也必须接受它。随后到达S的exact timeout只被active slot/tombstone
   幂等消费，不回复matching timeout ACK、不改变结果；
5. 两条路径都不能向A发送两个ACK，也不能在resolution选定后被另一端frame改写。

其它第二terminal、wrong code/route/payload仍是protocol error。这个active-slot例外与
§13 source-resolution tombstone例外使用同一validator，不能写成两个逐渐漂移的规则。

## 8. 状态机

一个 `_BridgeSlot` 使用以下单调状态：

```text
PREFLIGHT
  -> ADMITTED
  -> SOURCE_REQUESTED                 # issue boundary crossed
  -> SOURCE_BEGUN                     # source metadata/fingerprint known
  -> DESTINATION_BEGUN
  -> READY                            # destination ready forwarded to source
  -> STREAMING
  -> SOURCE_ENDED                     # verified source terminal; queue draining
       +-> DESTINATION_FAILED -> FAILED       # verified failure ACK
       +-> DESTINATION_COMMITTED              # verified success ACK
            -> SOURCE_ACK_RESOLVED             # delivered or warning
                 +-> COMPLETED                 # copy
                 +-> SOURCE_DELETE_RESOLVED -> COMPLETED  # move

ADMITTED..STREAMING -> ABORTING -> ABORTED
SOURCE_ENDED + destination_terminal_issued=false -> ABORTING -> ABORTED
SOURCE_ENDED + destination_terminal_issued=true + B ACK deadline -> OUTCOME_UNKNOWN
```

规则：

- `PREFLIGHT` 不进入 endpoint index，也不拥有 permit；
- `ADMITTED` 已拥有唯一 Server lease，但尚未 issued；
- `SOURCE_REQUESTED` 起不得把 disconnect 描述为“未执行”；
- `READY` 只能由 B 的 ready 驱动；
- 第一个有效 non-empty chunk 把 `READY` 变成 `STREAMING`；空文件可从 `READY`
  直接到 `SOURCE_ENDED`；
- `DESTINATION_COMMITTED` 是不可回滚终点，后续只能成功或成功加 warnings；
- `DESTINATION_FAILED` 表示 B 已明确拒绝 commit/已清理 temp；S尝试把failure ACK交给A，
  无论delivery是否成功都以该确定failure收敛，不执行source delete；
- `SOURCE_ENDED` 后若 `destination_terminal_issued=false`，仍可发一个 failure terminal并
  abort；该 flag 一旦为 true，任何取消/timeout 都不得再向 B 发送第二个 terminal，只能
  bounded 等待 B 的 success/failure ACK；
- 上述B ACK deadline到达且仍无verified ACK时，在同一bridge lock内原子选择
  `source_resolution=timeout_ack`、设置`source_ack_impossible=true`、发布两端provisional
  expectations并进入`OUTCOME_UNKNOWN`；随后才完成caller、cleanup和lease release。Late B
  ACK或A sender-timeout只按§11.1/§13消费，不能改写terminal result或触发move delete；
- `source_resolution`只能从open单向选为destination_ack或timeout_ack；后续另一端frame
  只能由exact active exception/tombstone消费，不能再次向A发ACK；
- `source_ack_impossible=true`等价于timeout_ack胜出；此后不得再向A发送B的ACK，也不得
  执行move delete；
- `copy` 从 `SOURCE_ACK_RESOLVED` 直接完成；`move` 再经过
  `SOURCE_DELETE_RESOLVED`；
- warnings 是 terminal result metadata，不是另一套可回滚状态；
- cleanup/tombstone/lease release 是 terminal transition 的一部分，必须幂等；
- 任一 endpoint 上的 out-of-order、wrong-role、duplicate-active 或 cross-generation
  frame 不能推进状态。

## 9. Destination 原子性与 move

### 9.1 Destination

B 继续复用现有 Client transfer receiver：

1. 依据 captured `restrict_to_workspace` snapshot 解析 `dst_path`；
2. 拒绝 NUL、path escape、symlink/reparse traversal、directory/special file；
3. 取得 canonical destination path lock；
4. 确认 destination 不存在并记录 parent/path identity；
5. 在 destination 同目录创建随机隐藏 temp；
6. 增量写入、hash、flush、fsync；
7. 重查 parent、temp identity 与 destination absence；
8. 使用 atomic no-replace primitive 发布并 fsync parent；
9. 成功后发送 final ACK。

平台/filesystem 若不能证明 atomic no-replace，返回已有 conflict/unsupported storage
错误，不允许 fallback 到 overwrite。OpenOctopus path lock 只协调同一 Client 进程内
操作；对不合作的宿主进程不宣称分布式或 OS 级事务。

### 9.2 Move source deletion

`mode="move"` 时，S 在验证 B 成功 ACK、记录destination commit后：

1. 只有§7.6的`source_resolution=destination_ack`时才把chosen ACK发送给A，并且只有send
   boundary明确确认成功时设置`source_ack_delivered=true`，使sender slot释放source path
   lock；
2. `source_resolution=timeout_ack`时绝不转发B ACK；ACK delivery未确认或根本不允许发送时，
   禁止发出任何source delete，直接返回success与稳定warnings
   `transfer_ack_failed, source_delete_failed`；
3. 只有 `source_ack_delivered=true` 且§11.4 new-call fence仍匹配时，才使用captured source
   identity与当前exact handle调用私有
   `__workspace_rest__`：

   ```json
   {
     "operation": "delete_file",
     "path": "reports/a.pdf",
     "if_match": "<source transfer_begin.etag>"
   }
   ```

4. 等待既有 30 秒 bounded private call result；
5. 条件删除成功则无 warning；missing fingerprint、source changed、route replaced、
   timeout、disconnect、ambiguous result 或普通删除失败都返回
   `source_delete_failed` warning。

Source delete 不得换到重连后的新 generation 重试，不得去掉 `if_match` 做无条件删除。
Destination commit 已确定时，source ACK 发送失败增加 `transfer_ack_failed`；`move` 同时
增加 `source_delete_failed`。Warnings 去重并保持稳定顺序：

```text
transfer_ack_failed, source_delete_failed
```

调用仍返回 destination 的 `bytes_transferred` 与 `sha256`。这不是原子跨设备 move；
它是保证不会丢失最后一份的 copy-then-delete。

## 10. Admission、queue 与锁顺序

### 10.1 Server permit

Distinct-client bridge 复用现有 `FairTransferAdmission`：

- 一个 bridge 占一个 `device_transfer_max_concurrency` global permit；
- 同时占一个 owner 的 `device_transfer_max_concurrency_per_user` permit；
- queue timeout 继续使用 `device_transfer_queue_timeout_seconds`；
- 每用户 FIFO、用户间 round-robin、公平与 bounded waiter 规则不变；
- capacity exhaustion/queue timeout 不创建 endpoint slot，不发送 frame；
- admission在任何frame前为source/destination各预留一个tombstone credit；这样post-issue
  provisional publication不能因store已满失败。未issued endpoint的credit直接释放；已issued
  endpoint在需要离开active handling时，先用该credit原子materialize pinned provisional，
  terminal cleanup完成后才转为final TTL entry并从此刻开始计TTL；不能跳过provisional；
- permit 从 `ADMITTED` 持有到 terminal cleanup、move delete attempt 和所有 owned task
  收敛后释放。

不得为 source leg 与 destination leg 各 acquire 一次。默认 per-user limit 为 2 时，同一
用户应能同时运行两个 bridge，而不是因为内部实现被减半为一个。

### 10.2 Client local slots

A 和 B 各自执行已有 `MAX_ACTIVE_TRANSFER_SLOTS=2` 检查。任一 Client 无本地 capacity
时使用 `tool_device_busy` failure terminal；Server 释放唯一 bridge permit。另一个 Client
若已创建 slot，必须收到失败并清理。

### 10.3 Byte 与 storage accounting

- Bridge queue 固定四个 payload chunks；chunk 仍最多 64 KiB；
- Server 只保留 hash/counters/frames/tasks 等 O(active bridges) metadata；
- 不新增用户可调 buffer、bridge pool 或 staging directory；
- distinct-client destination 不使用 Server workspace quota、REST transfer admission
  或 RustFS connection；
- Py8b 不增加 Client disk quota。空间不足由 B 返回
  `workspace_storage_unavailable`；
- 不增加 file-size cap。任意大小的普通文件必须保持恒定内存，受 idle timeout、磁盘
  空间和既有整数/协议 bounds 约束。

### 10.4 获取顺序

必须遵循：

```text
short owner DB lookup
  -> close DB session
  -> capture route pair under registry lock
  -> release registry lock
  -> acquire one fair transfer lease
  -> revalidate both routes
  -> publish bridge endpoint index
  -> send source request
  -> source local path lock
  -> destination local path lock
```

两个 Client 的 path locks 位于不同进程，不建立跨设备 lock ordering。Server 发送任何
frame 时最多取得对应一个 connection send lock，绝不同时持有 A 与 B 的 send lock。
Cleanup 也不能在 registry global lock 内等待 transport、Client task 或 lease release。

## 11. Issue boundary、断线与 outcome

### 11.1 Caller-facing boundary

| 时点 | 结果语义 | Server 自动重试 |
|---|---|---|
| admission 前/排队中取消 | 未发送；取消并释放 waiter | 否 |
| capacity 满或 queue timeout | `tool_device_busy` | 否 |
| 两端 route 任一在 source request 前失效 | `tool_device_unreachable` | 否 |
| source request send 已开始或是否发送不明确 | issued/ambiguous | 否 |
| issued 后任一端 disconnect、replacement 或结果丢失 | `tool_execution_outcome_unknown` | 否 |
| 有确定 failure terminal/ACK，且 destination 未 commit | 对应稳定 transfer/path error | 否 |
| destination success ACK 已验证 | 成功；后续 transport/delete 问题转 warnings | 否 |

后两条“verified destination result”优先于泛化的issued-disconnect行；已验证failure/success
不能因随后transport事件降级为outcome unknown。

`on_issued` 由 source request 的 generation-fenced send 触发一次。Destination begin、
binary relay、terminal 与 source delete 不重复触发。

任何`destination_terminal_issued=true`、`source_resolution=open`但B结果在bounded wait后仍
无法确认的分支，在发布outcome unknown前必须在bridge lock内选择`timeout_ack`并设置
`source_ack_impossible=true`，但不主动向A发送ACK。Source provisional tombstone随后可对A的
第一个exact sender-timeout返回matching failure ACK以释放slot；destination provisional
tombstone只验证并吞掉B的第一个late success/failure ACK。Late frame只完成endpoint cleanup，
不得改写caller的outcome unknown、触发source delete或向A转发B ACK。

### 11.2 Disconnect 与 connection replacement

任一旧 handle retire 时，registry 同步 fence 包含该 endpoint 的 bridge，并启动幂等
cleanup：

- 若尚无authoritative verified terminal result且destination尚未收到source success terminal，
  停止relay，向已经issued的另一在线端发送正确role的failure terminal并清理destination temp；
  这是issued disconnect，caller始终返回outcome unknown；
- 若 `destination_terminal_issued=true` 但 ACK 尚未确认，不向 B 发第二个 terminal，也不
  向 A 合成一个虚假的 failure ACK；bounded 等真实 ACK，无法确认则先按§11.1选择
  `timeout_ack`再返回outcome unknown；
- 若 destination 已 commit，保留成功结果，source ACK/delete 的失败转 warnings；
- 任一已验证并能证明destination未commit的terminal failure都是authoritative，包括A source
  failure、B pre-ready/streaming rejection，以及Server preterminal failure的已确认双端abort
  handshake。随后A/B disconnect、Stop或failure ACK转发失败只影响endpoint cleanup，不得
  降级为outcome unknown，也不执行source delete；
- old source/destination generation 的 slot 永不迁移到 replacement connection；
- replacement generation 可接受新 transfer，但不能发送旧 X 的 frame；
- reconnect 后不发送旧 request/begin，也不继续旧 offset。

关闭 A 不得顺带关闭健康 B；关闭 B 也不得关闭健康 A。只有收到真正 malformed/unknown
frame 的 generation 按现有 Protocol v3 规则以 protocol error 关闭。

### 11.3 Idle timeout

`device_transfer_idle_timeout_seconds` 对每个等待阶段生效：

- source begin；
- destination ready/rejection；
- 下一次 source chunk 或 source terminal；
- queue/destination writer progress；
- destination final ACK 或 failure ACK；
- 失败 terminal 的 matching ACK。

真实 chunk forward 或合法状态推进重置对应 deadline；单纯任务仍存在、重复无进展
progress 或 heartbeat 不让 stalled bridge 无限存活。若
`destination_terminal_issued=false`，timeout 发送 `workspace_transfer_timeout` failure
terminal，清理两端并释放 permit；若该 flag 已为 true，则只 bounded 等 ACK、绝不发送
竞争 terminal。稳定`workspace_transfer_timeout`只允许在
`destination_terminal_issued=false`且所有已issued endpoints的abort/matching ACK已确认、
从而证明destination未commit时返回。若destination terminal已经issued而B结果未知，即使A
收到matching sender-timeout ACK也不能证明B未commit；必须先按§11.1选择source resolution并
返回`tool_execution_outcome_unknown`。

### 11.4 Config epoch

Source与destination各自独立执行config fence：

- 对某endpoint的initial request/begin尚未issued时，其config epoch改变使该endpoint
  pre-send fail；另一端若已issued，则按正常failure terminal/ACK收敛；
- initial frame已经generation-fenced issued后，该endpoint的transfer frames继续绑定captured
  Client config/path-policy snapshot与同一WebSocket generation；中途config update不能把slot
  迁到新policy，也不能让late frame绑定new generation；
- connection replacement仍终止旧slot，和单纯config epoch change不同；
- move的`delete_file(if_match=...)`是destination commit后的一个新private call，不属于已issued
  transfer frame。发送前必须重新验证source仍是同device、同WebSocket generation且同config
  epoch；任一变化都不向旧或新epoch发送delete，只返回`source_delete_failed`warning。

这组规则按endpoint分别测试；不能用“bridge整体已issued”跳过尚未发送B begin的destination
fence，也不能用“A transfer已issued”绕过后续delete的new-call fence。

## 12. Cancellation 与 commit race

### 12.1 Pre-issue

取消 queue waiter 或 `ADMITTED` 但尚未进入 source send 的 bridge：

- 从 fair queue/endpoint index 移除；
- 取消 owned tasks；
- 释放唯一 lease；
- 不创建 tombstone、不发送 frame、不标记 issued。

### 12.2 Post-issue、commit 前

Agent Stop、HTTP disconnect、handler cancellation 或 Server shutdown：

- 立即停止新的 source bytes admission；
- 若 `destination_terminal_issued=false`，先同步标记 slot aborting，并按独立的
  `source_issued`/`destination_issued` 为已经或可能收到 initial frame 的 endpoint 发布
  failure tombstone，再执行可能 await 的 failure terminal send；随后取消 relay worker、
  drain/drop 只属于 X 的有界 queue，并要求 B abort temp；
- 若 `destination_terminal_issued=true`，绝不发送第二个 failure terminal；
  cancellation-safe 地 bounded 等 B 的 success/failure ACK。ACK 胜出时按真实结果收敛，
  deadline/disconnect 前无法确认时先在lock内把仍open的source resolution选为
  `timeout_ack`，再返回outcome unknown；不再要求B abort，也绝不猜测删除destination；
- 等待 cleanup task cancellation-safely 收敛并最终释放 lease；
- 已 issued caller 不获得“安全重试”承诺，Server 不 replay。

若取消到达时B的success/failure ACK已在bridge lock内验证，真实destination result优先：
success按commit规则保留，failure保持确定failure；caller cancellation不能把任一结果改成
unknown或相反结果。

REST caller 已断开时不需要制造 HTTP response，但后台 cleanup 仍必须完成。

### 12.3 Commit transition

B 可能在 Server caller cancellation 的同一时刻完成 atomic publish。接收/验证成功 ACK
到记录 `destination_committed=true` 必须是一个不可被普通 caller cancellation 切断的
transition：

- ACK 未确认：按 outcome unknown 清理，绝不猜测并删除 destination；
- ACK 已确认：先在bridge lock选择source resolution，再按唯一chosen ACK收敛；只有
  destination_ack胜出且delivery确认时才继续bounded source delete；
- cancellation 只影响 caller wait，不撤销已提交 destination；
- shutdown grace 后仍未收敛的任务记录 sanitized diagnostic，但不自动重放。

## 13. Tombstones 与 late frames

Bridge terminal 后，按 `source_issued`/`destination_issued` 为每个已经或可能收到
initial frame 的 endpoint route 建立现有 transfer tombstone。通常两者都为 true，因此是：

```text
(A device_id, A generation, X) -> source terminal expectation
(B device_id, B generation, X) -> destination terminal expectation
```

它们复用既有 transfer TTL/entry bound；一个 bridge 计一或两个 endpoint tombstone
entries。若 A 在 destination begin 前结束，B 从未知道 X，不为 B 建 tombstone。
Tombstone 不持有 file bytes、path lock、Client slot 或 Server permit。

Failure/outcome expectation必须在任何await cleanup前先以`provisional`形态发布。Provisional
entry在对应active bridge cleanup期间不计TTL、不可被普通tombstone eviction驱逐；其数量由
active bridge permit硬界定。Bridge完成terminal cleanup时，原子写入最终resolution/
expectation、把expiry刷新为“从此刻起完整既有TTL”并解除pin。这样长cleanup不会让保护窗在
slot尚未收敛时提前过期；不能为同一endpoint并存provisional与final两条entry。

处理规则：

- exact duplicate terminal 或 matching failure ACK：幂等忽略；
- `source_resolution=destination_ack`后到达A的exact sender-timeout只幂等消费，不返回第二
  ACK；chosen B ACK即使与A后来发出的timeout形状不同，Client sender也必须接受；
- `source_resolution=timeout_ack`时source active slot/tombstone只允许matching timeout
  ACK语义：若`source_timeout_ack_sent=false`，第一个exact timeout获得matching failure ACK
  并原子置true；否则只幂等消费。Late B ACK由destination side验证/消费但绝不再转发A；
  destination/caller result不被改写，move不触发source delete/replay，也不关闭A；
- known failed slot 在 peer 尚未看到 failure 前已进入 WebSocket/OS buffer 的 non-empty
  binary chunks：只在累计 source bytes 不超过 begin 声明的 total 且 tombstone TTL 未过期时
  丢弃，不按 Client writer lane 猜测 frame 数量；payload 不保留、不转发、不写 destination；
- known-failed source provisional/tombstone还可丢弃最多Server contract constant
  `LATE_PROGRESS_MAX=64`个well-formed、monotonic、未超过declared bytes的late
  `transfer_progress`。这是critical failure ACK可能越过的Client `SerializedWriter` normal
  control lane backlog上界；Server与Client是独立package/process，不做运行时派生或协商，而由
  cross-runtime contract fixture锁定`LATE_PROGRESS_MAX == Client _NORMAL_MAX == 64`。每帧消耗
  一个credit且不刷新TTL。额度耗尽、wrong-role或malformed progress仍是protocol error；
  normally completed slot没有该例外；
- normally completed slot 的 late binary、过期 tombstone、wrong endpoint、wrong role、
  conflicting terminal 或完全未知 X：`protocol_transfer_unknown_id`/malformed；
- 一个 endpoint 的 late frame 不能查找或消费另一个 endpoint 的 tombstone；
- bridge 已关闭后任何 late frame 都不能重新创建 task、queue、lease 或 source delete。

上述provisional expectation必须在await abort send/worker cancellation/cleanup前可见，
避免已在途的合法ACK/chunks/progress被误判并关闭健康Device。

## 14. 稳定错误与结果

Agent 继续得到普通 `ToolResult(is_error=true)`；REST 继续使用 canonical
`WorkspaceError` HTTP mapping。

| 条件 | 稳定 code | REST |
|---|---|---:|
| malformed args/path 或非普通 source | `tool_invalid_args` / 既有 path code | 400/既有 mapping |
| source 是目录 | `tool_is_directory` | 409 |
| missing/not-owned/offline/pre-issue stale route | `tool_device_unreachable` | 409 |
| Server admission 或任一 Client local slot busy | `tool_device_busy` | 429 + `Retry-After` |
| destination exists、commit race 或 source fingerprint changed | `workspace_file_changed` | 409 |
| destination permission/policy failure | 既有 `workspace_*` / `tool_path_outside_workspace` | 既有 mapping |
| no progress idle timeout，且 matching failure 已确认 | `workspace_transfer_timeout` | 408 |
| byte count/SHA-256 mismatch | `workspace_transfer_integrity_failed` | 502 |
| Client disk/I/O unavailable | `workspace_storage_unavailable` | 503 |
| issued/ambiguous 后 disconnect、replacement、lost terminal | `tool_execution_outcome_unknown` | 409 |

REST 的 `Retry-After` 使用
`ceil(device_transfer_queue_timeout_seconds)`，不是 REST upload/download admission 的
queue timeout。

成功 response 不改变：

```json
{
  "bytes_transferred": 2457600,
  "sha256": "<64-lowercase-hex>",
  "warnings": []
}
```

允许的 bridge warning 只有既有稳定值：

- `transfer_ack_failed`；
- `source_delete_failed`。

错误文本、日志和 metrics 不包含 raw paths、frame payload、tokens、Device config 或
source fingerprint。

## 15. Protocol 与 Client compatibility

Py8b 不修改以下 wire DTO：

- `TransferPurpose = file_transfer | workspace_upload | http_relay`；
- `TransferDirection = client_to_server | server_to_client`；
- transfer control frame fields；
- `16-byte UUID + <=64 KiB payload` binary layout；
- hello `file_transfer=[send, receive]` capability；
- Protocol v3 strict unknown-field rejection与 close codes。

Source Client 只看到一个普通 client-to-server request/slot；destination Client 只看到一个
普通 server-to-client begin/slot。Server 改写 direction/install-site metadata并转发控制
语义，所以 Client 不需要知道“bridge”这一新 Server 编排概念。

因此：

- `PROTOCOL_VERSION` 在 Server 与 Client 都保持字符串 `"3"`；
- 不添加 `bridge=true`、第二个 slot ID 或新的 purpose；
- 不做 feature negotiation；合法 Protocol v3 Client 已具备两个角色；
- contract fixtures 应证明旧 v3 frame JSON 完全不变，并新增双 route 流程测试。

任何实现若需要新增/改变 wire field，必须停止 Py8b 实现并另行设计 protocol bump，不能
在 v3 中依赖 unknown-field tolerance。

## 16. 实现切片

实现按下列 TDD slices 依次提交；每个 slice 都保持现有 transfer tests 通过。

### Slice A：schema、route pair 与 preflight

- 删除 `FILE_TRANSFER_SCHEMA` 的 `anyOf` 和 `x-openoctopus-same-device`；
- 简化 registry merge 中只为该 marker 服务的 equality branch 逻辑；
- 删除 distinct-client runtime reject；
- 增加同 owner 双 Device DB resolution 与 atomic route-pair snapshot；
- 保持 server/same-client/one-client 路径不变。

**Proof：** Provider schema 接受 Client A -> Client B；跨用户、rename/name reuse、任一
offline/config-fenced route 在任何 frame 前失败。

### Slice B：bridge happy path 与 bounded relay

- 新增 `_BridgeSlot`、两个 endpoint indexes 和一个 logical lease；
- source request/begin、destination begin/ready gating；
- maxsize=4 queue、relay worker、Server hash/count；
- source terminal drain、destination commit ACK、source ACK；
- copy result 与 cleanup。

**Proof：** 两个真实 protocol transports 传输空文件、binary 文件和大于 queue 的文件；
source 在 destination ready 前零 bytes；active/permit/task/queue 回到 baseline。

### Slice C：failures、move 与 lifecycle races

- 两向 failure terminal propagation；
- destination 对 source success terminal 返回 `ack=true, ok=false`，以及 Client sender
  对该合法 failure ACK 的匹配/释放；
- no-overwrite/digest/length/state errors；
- move conditional delete 与 warnings；
- cancellation-safe commit、disconnect、replacement、idle timeout；
- 两 endpoint issued fences、destination-terminal fence、ACK-vs-timeout single resolution、
  tombstones 与 late-frame drain。

**Proof：** 在每个 state 注入 source/destination disconnect、cancel 与 delayed frames；
destination partial 永不暴露，committed destination 永不回滚，新 generation 不接旧 slot。

### Slice D：REST、Agent、真实 E2E 与 canonical docs

- REST error/response 与 Agent tool normalization；
- 双真实 Client source-mode E2E 和 Linux frozen-client smoke；
- capacity/RSS/fairness harness；
- 同步 canonical docs、OpenAPI、protocol/schema fixtures。

**Proof：** Agent 与 REST 都能完成 A -> B copy/move；CI 全绿；docs 与 runtime schema
一致。

## 17. 必需测试

### 17.1 Unit 与 contract

- merged schema 覆盖五种 site combinations，且不再含 equality-only constraint；
- Agent `mode` omitted -> `copy`，REST `mode` 仍 required；
- 双 Device 必须同 owner、不同 immutable ID，另一个用户同名不泄露；
- Provider turn captured ID 与 live DB name/ID 不匹配时 fail closed；
- pair snapshot 同时捕获两个 ready generation/config epochs；
- bridge 只 acquire/release 一个 global/per-user lease；queue cancel 不 issued；
- bridge pre-issue预留两endpoint tombstone credits，未issued endpoint释放、issued endpoint
  在任何cleanup await前按`credit -> pinned provisional -> final TTL entry`转换，store满时不会
  在post-issue cleanup才发现无法发布provisional；
- A/B 各占一个 local slot，任一 busy 正确传播 failure；
- 同一 X 在两个 endpoint keys 合法，在第三个 handle/generation 上非法；
- destination ready 前不转发 ready，source early binary 是协议错误；
- 0 B、1 B、64 KiB、64 KiB+1、多 chunk 与慢 destination backpressure；
- queue/writer 高水位严格有界，多个 slot round-robin 不饿死 control；
- short/long/extra bytes、source/destination/server digest mismatch；
- source/destination path mismatch、wrong direction/purpose/device metadata；
- destination exists before begin、ready 后 external create、temp replacement 与 ENOSPC；
- atomic no-replace，失败无 destination partial/temp/lock leak；
- copy 保留 source；move 仅在 destination ACK 后条件删除；
- source changed/missing fingerprint/delete timeout/ambiguous delete 返回 warning；
- source ACK send failure在 destination committed 后返回稳定 warnings；
- source ACK delivery 未确认时 move 绝不 dispatch delete，稳定返回
  `transfer_ack_failed, source_delete_failed`；
- destination commit/validation failure ACK 可与 source success terminal 配对，A 释放 slot
  且健康 WebSocket 不关闭；
- B terminal pending跨过A idle timer且sender-timeout先胜出时，A只获matching timeout ACK；
  B继续独立收敛，success/failure/unknown分别按§7.6处理且move不删源；
- 用barrier覆盖`B success/failure ACK`与`A sender-timeout`两种接收顺序：B ACK先胜出时A
  接受chosen ACK、late timeout无第二ACK；timeout先胜出时A只收matching timeout ACK、late
  B ACK不转发；四种组合都不关闭健康WebSocket或触发错误source delete；
- source fail before/after B begin、B reject before ready、B fail while streaming；
- cancellation 在 admission、source send、destination ready、queue full、source end、commit
  ACK、source delete 每个边界；
- `SOURCE_ENDED`且destination terminal尚未issued时timeout/cancel可进入ABORTING；terminal
  issued后只等B ACK，不发第二terminal；ACK deadline进入OUTCOME_UNKNOWN时在发布caller结果/
  cleanup前原子选择timeout_ack并安装两端provisional；
- source 与 destination 分别在每个 state disconnect/connection replaced；
- route config update按A/B各自initial-frame issued边界独立fence；issued endpoint继续captured
  Client config，同epoch以外的move delete只warning且不迁移；
- idle timeout 覆盖 begin/ready/chunk/end/ACK/failure ACK；
- exact late ACK、duplicate terminal、declared-byte/TTL bound 内的 known-failed late chunks、
  unknown/conflicting/expired frames；
- known-failed late progress在`LATE_PROGRESS_MAX=64`内drop，逐帧耗尽且不续TTL；第65帧/
  malformed仍protocol error；failure ACK越过全部64帧不关闭健康A；cross-runtime fixture
  断言它等于Client normal-lane capacity；
- A source failure、B pre-ready/streaming rejection和Server preterminal confirmed abort在随后
  disconnect/Stop后仍是稳定failure；只有尚无authoritative terminal result的issued路径才
  降级outcome unknown；
- provisional tombstone在cleanup超过普通TTL时仍不可过期/驱逐，terminal后从零刷新完整TTL；
- B failure ACK验证后在转发A前发生disconnect/Stop仍保持确定failure；
- B ACK始终未确认而bridge outcome unknown时先选择timeout_ack：late A timeout获matching ACK，
  late B success/failure只消费，caller result与source delete不改变；
- destination已commit时同样服从`source_resolution`选择；不能在committed tombstone里另写
  一套无条件matching-timeout ACK规则；
- cleanup/tombstone publication 对 cancellation 安全且 permit/task/index 回到 baseline；
- existing server/server、server/client、client/server、same-client tests 无行为漂移；
- Server 与 Client `PROTOCOL_VERSION == "3"`，现有 DTO JSON fixture 不变。

### 17.2 Integration

- 两个真实 FastAPI WebSocket connections，不使用共享 fake handle，完成 bridge copy；
- Agent `ToolRegistry` 从 owner snapshot 构建 schema 并调用 distinct Client；
- `POST /api/workspace/transfer` authentication、same-owner success 与 cross-user failure；
- source/destination 各自不同 `workspace_path`/`restrict_to_workspace` policy；
- destination slow writer 反压 source，同时两边 heartbeat/control 保持存活；
- 两个 bridge 在相同两 Client 间交错 binary frames，无 slot 串线；
- source 或 destination reconnect 后旧 X 不恢复，新 transfer 可成功；
- no RustFS get/put/multipart/delete 与无 Server temp file 的断言；
- move destination committed、source delete success；以及 delete failure后两份存在和
  warning；
- Agent/REST 的 pre-issue unreachable、busy、timeout、integrity、outcome unknown 映射
  一致。

### 17.3 真实双 Client E2E

启动真实 PostgreSQL/RustFS/FastAPI Server 和两个独立 source-mode
`openoctopus-client` 进程，分别使用不同 Device tokens 与工作目录：

1. 创建同一 user 的 `source-device` 与 `destination-device`；
2. 两个 Client 完成 Protocol v3 ready；
3. 在 source 创建包含 NUL/非 UTF-8 bytes 的普通文件；
4. 通过 REST copy 到 destination，逐字节与 SHA-256 验证；
5. 再通过 Agent tool 执行 move，验证 destination 存在、source 删除；
6. 预创建 destination，验证 no-overwrite 且原内容不变；
7. streaming 中终止 destination，验证无可见 partial、旧 transfer 不 resume；
8. destination 重连后新 transfer 成功；
9. 检查 Server workspace/RustFS 没有 bridge 中间对象；
10. 进程退出后 registry slots、permits、tasks、queues 与 temp files 回到 baseline。

Linux frozen E2E 至少让其中一个 Client 使用当前 PyInstaller one-folder artifact；另一端
可使用 source client，以证明 wire compatibility 而不把 CI 成本扩大为双 frozen build。

### 17.4 Capacity 与内存

可重复 harness 至少覆盖：

- 多 user 的并发 bridges 达到 configured global limit；
- 同一 user 最多 active `per_user` 个 bridge，每个 bridge 只计一个 permit；
- 额外 waiter 按每用户 FIFO、用户间 round-robin，满时稳定 busy；
- 慢 destination 不让一个 user 占满其它 user 的 admission；
- 每个 Client local slot limit=2 生效；
- parent RSS 随 active bridges 达到平台后保持稳定，不随已完成文件总字节或次数增长；
- queue high-water 不超过 4 chunks，endpoint/tombstone/task/FD 计数在完成后回落；
- transfer load 下 `/health`、heartbeat、其它 Device tool 与不同 bridge 仍可前进。

Harness 记录 wall time、p50/p95、peak RSS、task/FD、active/waiting、queue high-water 与
warning/error 分布，但 Py8b 不凭空制定 latency SLO。

## 18. CI 与验收门槛

实现 PR 必须通过：

- Server 完整 pytest；
- Client 完整 pytest；
- Server/Client Ruff；
- Server/Client strict mypy；
- runtime OpenAPI 与 `docs/API.yaml` consistency；
- Protocol v3 Server/Client contract fixtures；
- PostgreSQL/RustFS integration tests；
- 真实双 Client source E2E；
- Linux frozen-client bridge smoke；
- bridge capacity/memory harness，无 leaked permit/task/slot/temp。

Py8b 完成的产品验收是：

1. 同 owner 两个不同在线 Client 可 copy/move 一个普通文件；
2. destination ready 前 source 不发送 bytes；
3. Server 不 staging 且内存有界；
4. no-overwrite、digest 与 atomic destination commit 成立；
5. move 只在 destination ACK 后条件删源，失败返回 success warning；
6. destination failure ACK、success-terminal cancellation fence与A sender-timeout/B ACK race
   都在bridge lock内选择唯一source resolution，不关闭健康连接、不发竞争terminal/ACK，
   ACK未确认时move绝不删源；
7. 所有 disconnect/cancel/replacement/late-frame 路径不串 slot、不污染新 generation、
   不自动 replay；
8. 一个 bridge 的 Server admission weight 精确为一；
9. Protocol 仍为 v3，既有 Client roles 和所有其它 transfer combinations 无回归。

## 19. Canonical 文档与代码落点

本 spec 合入不提前宣称实现存在。实现 PR 完成时同步：

- `docs/DECISIONS.md`：追加 Py8b ADR，说明 ADR-087 的 future bridge 在本 milestone
  正式落地，并 supersede ADR-131/Py5 的 bridge deferral；不改写历史 ADR；
- `docs/PROTOCOL.md`：更新 §4.5 Device -> Device、slot lifecycle、disconnect/late-frame
  与 v3 no-bump 说明；
- `docs/TOOLS.md`：更新 `file_transfer` site matrix、schema、mechanism、errors/warnings，
  删除 distinct-client reject 与 same-device-only marker；
- `docs/API.yaml`：更新 `/api/workspace/transfer` 与 `TransferRequest` 描述，删除 bridge
  deferred/equality-only extension，保留 request/response shape；
- `docs/SCHEMA.md`：确认无 DB schema 变更，不增加空的兼容字段；
- Server：`tools/file_transfer.py`、`tools/registry.py`、`devices/registry.py`、
  `devices/transfer.py` 与对应 tests/fixtures；
- Client：优先复用现有 sender/receiver runtime；只有 contract/race test 证明必要时才改
  runtime，不增加 bridge-specific wire branch；
- E2E/capacity scripts：增加双 Device bridge 场景及 no-staging assertions。

Canonical docs、runtime OpenAPI、Provider schema snapshot、Server/Client protocol fixtures
必须在同一实现 PR 中收敛，不能留下“ADR 说已支持、API/TOOLS/code 仍拒绝”的混合状态。
