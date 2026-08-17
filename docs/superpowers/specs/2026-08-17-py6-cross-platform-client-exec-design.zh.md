# Py6 跨平台客户端 Exec v2 设计

**状态：** 提案 v2（2026-08-18）
**里程碑：** Py6 Client shell execution
**依赖：** 已接受并实现的 Py5 Python 客户端/设备文件切片

本文是 Py6 的中文目标契约。实现时必须同步更新 `docs/TOOLS.md`、
`docs/PROTOCOL.md`、`docs/API.yaml`、`docs/SCHEMA.md`、`docs/DECISIONS.md`
和契约 fixtures；本文本身不替代这些 canonical docs。

## 1. 目标与边界

Py6 为可信配对设备增加三个仅客户端的 Agent 工具：

```text
exec                 启动独立 shell 进程
write_stdin          查询或操作一个 exec session
list_exec_sessions   列出当前聊天拥有的 session
```

`exec` 默认使用普通 pipe；只有显式传 `tty=true` 才使用真实终端：

```text
tty=false（默认） -> stdin 关闭，stdout/stderr pipe
tty=true           -> POSIX PTY 或 Windows ConPTY
```

两种后端共享 session、所有权、容量、超时、重连和清理生命周期，但不强行把
两种不同的 I/O 伪装成相同的流。

PTY 只保证**行式交互**，用于 Python/Node REPL、TTY 检测、交互式 SSH shell 和
简单确认提示。`git commit` 的保证路径是 `-m`/`-F`；让它自动打开 Vim、nano 或
其他编辑器不属于验收范围。Py6 不承诺完整 Vim/nano/top/curses 等 TUI、screen
snapshot、视觉模型、终端 resize、鼠标、完整 job-control 或密码/2FA/私钥
passphrase 输入。

PTY 原始终端流由 Client 规范化为文本，不把 ANSI 控制字节直接交给 Provider。
官方 Client 启动时发现 PTY backend 缺失会永久失败；自检后某次 PTY 创建失败则
返回 `tool_pty_unavailable`。任何情况都不静默降级为 pipe。

这是官方发行包完整性检查，而不是运行时 capability negotiation。依赖、helper 或
原生库静态缺失表示安装包不满足 Protocol v2；支持 pipe-only degraded mode 需要新增
能力协商，不属于 Py6。运行期间因瞬时 OS/资源错误无法创建某一个 PTY，才是单次
`tool_pty_unavailable`。

Linux、macOS、Windows 是同等目标平台。Py6 不实现 MCP、exec REST、前端
Terminal、文件夹传输、installer/service-manager、OS 级安全 sandbox 或磁盘
session 恢复。

## 2. 已确定的决策

1. **双后端、pipe 默认。** `exec` 只有一个 `tty:boolean` 字段，默认 `false`；
   不增加 `interactive`、`mode` 或第二个 terminal 工具。
2. **每次 exec 独立进程。** 不共享 cwd、环境、alias、shell 状态或登录会话。
   `login` 只控制 profile 加载，默认 `false`。
3. **只保证行式交互。** 不做完整 TUI、截图、视觉处理或 resize。
4. **pipe stdin 默认关闭。** pipe 模式的 `write_stdin` 只能 poll、用唯一的
   `chars="\u0003"` 请求中断，或 terminate；需要发送其它文本必须选择 `tty=true`。
5. **PTY 输入复用 `chars`。** 支持普通文本和 `\u0003`、`\u0004` 等控制字符，
   不增加复杂按键枚举。
6. **没有 secret 输入通道。** Agent 不接收或转发密码、2FA、passphrase；需要
   凭据的操作留给未来的人类接管终端。
7. **trusted-only。** 只有 `sandbox_mode=false` 的设备展示 exec。Server 是
   可信控制平面，但 exec 仍使用用户 OS 权限，不是安全 sandbox。
8. **直接升级 Protocol v2。** 不兼容 Py5/v1，不做 v1 fallback、版本协商或
   动态 RegisterTools。
9. **Server 拥有 canonical schema。** Server 注入设备路由；Client 做第二道
   严格校验，不发布第二份 schema。
10. **session 按聊天隔离。** Provider 看不到 `chat_session_id`、设备 UUID、
    policy epoch 和 transport 元数据；另一个聊天即使属于同一用户也不能操作。
11. **每个 Client runtime 最多 8 个 session 记录。** 启动、运行、终止中和等待
    最终 poll 的终态记录都占槽位。
12. **输出有界。** pipe 的 stdout/stderr 各 50,000 Unicode 字符；PTY 的规范化
    output 50,000 字符；单次报告默认 10,000、最多 50,000。
13. **30 分钟 idle cleanup。** 只有访问被指向的 session 才刷新其 idle 时间。
14. **yield 与 timeout 分离。** `yield_time_ms` 是报告窗口；`timeout` 是从成功
    spawn 开始计算的进程硬终止时间。
15. **Chat Stop 只停止当前 Agent turn。** 不发送 process-cancel frame；已发出
    的调用继续到自然退出、terminate、hard timeout、idle cleanup 或 Client 清理。
16. **普通 WS 断线和 Server 重启保留进程。** token rotation、设备删除、
    connection replacement、正常 Client shutdown 和执行策略改变会有界终止
    session。Client 重启、SIGKILL 或机器断电后不做磁盘恢复，也不保证清除已逃逸
    的孤儿进程。
17. **删除 `command_denylist`。** Py6 不制造 shell denylist 的安全假象，真实
    isolation 推迟到后续 milestone。
18. **三平台都必须提供 PTY。** POSIX 通过一个无网络线程的内部 helper 调用标准库
    `pty.fork()`，建立真正的 controlling terminal，主 Client 不直接 fork。Windows
    固定使用 `pywinpty==3.0.5` 和 ConPTY。原生 PyInstaller smoke 必须验证 helper/
    原生扩展/DLL/ConPTY；缺失时明确报错。
19. **不新增 exec REST。** Shell 调用仍经 Server→Device WebSocket 路由。

## 3. 范围

### 3.1 包含

- Protocol v2 handshake、严格 DTO、固定 client-only routing；
- 三个 canonical tool schema、provider-hidden chat ownership；
- Linux/macOS/Windows shell discovery、argv 构造、pipe 和 PTY/ConPTY 后端；
- runtime-owned、内存内、可跨普通 WS 重连发现的 session manager；
- per-device admission、硬超时、idle cleanup、bounded output、终态保留；
- stdout/stderr 并发 drain、PTY 行式规范化、进程树终止和 reap；
- non-zero exit、signal、timeout、disconnect、Stop、late-result Agent 行为；
- PATCH pre-commit fence、统一 outcome-unknown、late tombstone；
- Linux 自动/冻结 E2E、三平台 native smoke、500-peer capacity gate。

### 3.2 不包含

- 全屏 TUI、screen emulator、screen snapshot、截图、视觉模型、resize、鼠标；
- 密码、2FA、私钥密码等 secret 输入；
- MCP（client-side 进入 Py7，server-side 进入 Py8）；
- 文件夹/续传/client-to-client 等高级传输扩展进入 Py8 的独立切片；
- Server 端 exec、REST exec、frontend terminal、doctor/status CLI；
- 磁盘 session、Client 重启恢复、自动重放已进入 transport 的命令；
- 任意用户 shell executable path、OS 级 process/filesystem/network sandbox；
- `command_denylist`。

## 4. 不可破坏的不变量

1. Provider 永远看不到 `chat_session_id`、设备 UUID、policy epoch、result
   credit 或其它内部路由元数据。`exec` 返回的 opaque `session_id` 是 Agent 后续
   poll/write/list 所需的业务句柄，必须对 Provider 可见；它不是 OS PID。
2. session 必须同时匹配 immutable owner chat UUID 和当前 Client runtime；缺失、
   过期、跨聊天访问返回相同的 not-found，不泄漏 session 存在。
3. `sandbox_mode=true` 设备不出现在三个工具的注入 enum；Client 还要检查
   stale/misrouted frame。
4. 8 槽 reservation 在 spawn 前原子完成；第 9 个请求不能先创建进程再失败。
5. pipe stdout/stderr 和 PTY 原始流全生命周期 drain，不能因 Agent 不在等待而堵塞子进程。
6. WS 断线、heartbeat、config 准备和 30 秒报告窗口不能阻塞 Client control
   reader、ping/pong 或 cleanup。
7. 明确未进 transport 的调用可返回 `tool_device_unreachable`；已进或可能进
   transport 但丢结果必须返回 `tool_execution_outcome_unknown`，不能说“未执行”。
8. issued/cancelled/timed-out call 的 late result 由 bounded、generation-scoped
   tombstone 消费；不能关闭健康的新 socket 或复活旧 Agent turn。
9. 每条终止路径通过一个 terminal future 收敛，只 release 一次，等待有界 reap
   和最终 drain；leader 已消失或 tree 脱离时报告 `cleanup_incomplete`。
10. 非零退出、signal、hard timeout、missing process、unexpected death 都只是
    bounded tool result，不能使 Client 崩溃、关闭 WS 或停止 Agent。
11. `OPENOCTOPUS_*` 永远不进入 child environment；命令、输入、输出、环境值、
    凭据不进入日志。
12. policy update 有序 fence：旧策略被 fence 后到旧 session 终止前，不能启动新进程。
13. 每次 exec 有新的 process/cwd/environment snapshot，不共享 shell state。
14. Server 永不执行 shell；exec 没有 `server` routing site。

## 5. Protocol v2

### 5.1 版本与重连

Device WebSocket `version` 改为严格的 `"2"`：

- v2 Server 收到 v1：关闭 `4409`，返回
  `{"code":"version_unsupported","protocol_version":"2"}`；
- v2 Client 收到不兼容 handshake：永久失败并退出；
- `4000 connection_replaced`：旧 Client 清理 session 后永久退出，不抢占重连；
- DNS/TCP/TLS、1000、1001、异常 1006、1013、heartbeat 4408：可重连，保留
  runtime-owned session；
- 4401：清理 session 后永久退出。

官方 v2 Client 必须具备 pipe 和 PTY，因此不增加 `caps.exec` 或 terminal
capability 协商。Client 在进入连接循环前自检当前平台的 PTY backend；依赖或原生
库缺失属于永久启动失败。自检后若某次 PTY 创建因 OS/资源错误失败，该调用返回
`tool_pty_unavailable`；两种情况都绝不降级为 pipe。

### 5.2 hello 与 config

```jsonc
{
  "type": "hello",
  "id": "<uuid-v7>",
  "version": "2",
  "client_version": "0.0.1",
  "os": "windows",
  "caps": {
    "shared_tools": true,
    "web_fetch": true,
    "file_transfer": ["send", "receive"],
    "http_relay": true
  },
  "shells": {
    "default": "pwsh",
    "available": ["pwsh", "powershell", "powershell_x86", "cmd"]
  }
}
```

`os` 是 `linux | darwin | windows`。`shells.available` 必须非空、无重复，且
default 为其成员；仅用于提示和诊断，不是授权。Server 不持久化该列表。

`hello_ack.config`、`config_update.config` 包含 `workspace_path`、
`sandbox_mode`、`ssrf_denylist`、`shell_timeout_max` 和 `env_allowlist`。Py6
新增的持久字段只有后两项；不新增 command denylist 或 mcp_servers。

### 5.3 Provider-hidden ownership

```jsonc
{
  "type": "tool_call",
  "id": "<uuid-v7>",
  "name": "exec",
  "args": {"command": "python", "tty": true},
  "chat_session_id": "<chat UUID>",
  "max_result_bytes": 320000
}
```

`chat_session_id` 由 Server `ToolContext` 添加，只存在于 Server→Client frame；
不进入 Provider schema/args、持久化 tool input 或 provider-visible result。设备
`openoctopus_device` 在发 frame 前剥离。Server 发布前必须重新验证 `(name, UUID,
owner, sandbox_mode=false)`，防止 rename/delete/name reuse 的 stale call。

共享 Protocol DTO 中该字段可空，以兼容不拥有持久 chat 资源的既有 Device 工具；
对 `exec`、`write_stdin`、`list_exec_sessions` 则必须存在且非空。`session_id` 由
Client 生成 UUIDv7，作为普通 tool result 返回给 Agent。

### 5.4 issued-call outcome

| 阶段 | 含义 | 结果 |
|---|---|---|
| preflight/admission rejected | 明确未进入 transport | `tool_device_unreachable` 或 `tool_device_busy` |
| `issued_or_ambiguous` | frame 可能已经写入 socket | `tool_execution_outcome_unknown` |
| matching result | 收到当前 generation 正确结果 | 使用 Client 结果 |

在进入 `transport.send_text()` 的临界区内，registry 同时 fence generation/route
并把 call 改为 `issued_or_ambiguous`。send await 未返回不能证明未执行。Server
不自动重试 exec 或 stdin。

`DeviceOutcomeUnknownError` 是统一的 Server 边界，不能由各调用方分别猜测：

- Agent 工具返回 `tool_execution_outcome_unknown`；
- 尚未开始响应的 Workspace REST 返回 HTTP 409 和同一稳定 code；已经开始流式
  响应时只能中止流，并在脱敏诊断中保留同一 code；
- Agent/REST file transfer 返回同一 code，不自动重试、不删除源、不声称回滚，
  也不发起补偿传输。

Server 重启遇到没有 transport proof 的 unpaired tool_use，插入 paired
`tool_execution_outcome_unknown`；Agent 可 list/check external state，但不能说
命令未执行。

### 5.5 late-result tombstone

每个 generation 的 issued ID 有 bounded tombstone：

- 不使用任意短 wall-clock TTL；到匹配 late result 或 generation retire 才回收；
- 边界受 Client FIFO、Server pending-call admission 和 byte limit 约束；
- 当前 generation 的合法 late result 消费 tombstone 后丢弃，不关闭 socket；
- 新 generation 不接受旧 generation 的 tool_result；unknown/conflicting ID 仍为
  protocol error；
- late result 不能改写 Stop 产生的 synthetic result 或启动旧 runner。

### 5.6 Server transport deadline

Server deadline 只等待 Device result，不等于 process hard timeout：

- exec：`yield_time_ms + 5s`，无 yield 时使用正常前台报告窗口加 5s；
- write_stdin：有 `wait_for` 时 `wait_timeout_ms + 5s`，否则 `yield_time_ms + 5s`；
- list：10s；chars write/flush 额外最多 5s；
- grace 是实现常量，不是用户配置。

shell call 不能排在已运行的普通 Py5 FIFO 项或 config activation 后无限等待；
exec manager admission/config fence 中立即返回 `tool_device_busy`。yielded
background process 不占 tool worker，但占 exec slot。

## 6. Agent 工具 schema 与行为

三个工具使用固定的 `CLIENT_ONLY` routing：source schema 不包含设备字段，Server
在 Provider schema 构建时注入必填 `openoctopus_device`。enum 只包含当前用户已配对
且持久配置为 `sandbox_mode=false` 的设备，永远不包含 `server`；没有可信设备时
省略三个 schema。可信但离线的设备仍保留在 enum 中，dispatch 时稳定返回
`tool_device_unreachable`。每次 Provider iteration 捕获 `(canonical name, immutable
device UUID)`，实际发布前再次核对 owner/name/UUID/policy，失败时不重绑同名新设备。
Server 不接收 Client 动态 schema advertisement 或 `RegisterTools`。

### 6.1 `exec`

设备注入前的 source schema：

```json
{
  "name": "exec",
  "description": "在可信配对设备执行命令。默认使用 pipe；需要 REPL、TTY 检测或行式交互时设置 tty=true，并为长时间交互显式设置足够大的 timeout。yield_time_ms 不延长 hard timeout。",
  "input_schema": {
    "type": "object",
    "properties": {
      "command": {"type": "string", "minLength": 1, "maxLength": 24000},
      "working_dir": {"type": "string", "minLength": 1, "maxLength": 4096},
      "timeout": {"type": "integer", "minimum": 0, "maximum": 86400},
      "shell": {"type": "string", "enum": ["bash", "sh", "zsh", "pwsh", "powershell", "powershell_x86", "cmd"]},
      "login": {"type": "boolean", "default": false},
      "tty": {"type": "boolean", "default": false},
      "yield_time_ms": {"type": "integer", "minimum": 0, "maximum": 30000},
      "max_output_chars": {"type": "integer", "minimum": 1000, "maximum": 50000, "default": 10000}
    },
    "required": ["command"],
    "additionalProperties": false
  }
}
```

不接受 `cmd`、`workdir`、`max_output_tokens`、`interactive`、`mode` 或任意用户
shell executable path。`command` 拒绝 NUL，作为选定 shell 的一个 command 参数
传入，不拼接进父 shell。`working_dir` 缺省为 workspace_path；相对路径相对
workspace root，trusted exec 允许 OS 可访问的绝对路径，目录须存在。

24,000 字符是 source schema 的跨平台总上限；Client 还必须在 spawn 前检查最终
argv 的平台限制。`cmd.exe` 的 command 最多 8,000 UTF-16 code units（为 `/D /S /C` 和系统
8,191 限制留余量）；Windows 所有 shell 的完整 wrapper 必须小于 CreateProcessW
32,767 UTF-16 code units。超出时返回 `tool_invalid_args`，不得截断或改用临时脚本。

`timeout` 从 spawn 成功开始计时。省略时为 `min(60, shell_timeout_max)`，但 cap
为 0 时仍为 60；显式值不能超过正 cap。`timeout=0` 只有 cap=0 且显式提供有界
`yield_time_ms` 时有效；`timeout>60` 必须显式 yield。yield 到期只返回
`status=running` 和 session_id，不终止进程。hard timeout 会终止进程树；若该
session ID 已暴露，Agent 可再 poll 读取保留的最终状态/输出，但不能恢复进程。
REPL、SSH shell 和其它长时间交互 session 必须显式传入足够大的 `timeout`；
`yield_time_ms` 只控制本次报告窗口，绝不延长 hard timeout。`exec` schema description
和 Provider prompt 都必须包含这条提示。

`login=false` 是无 profile 形式，`login=true` 只表示加载 profile，不表示 tty。
`tty=false` 使用 pipe，stdin 从 spawn 起关闭；`tty=true` 使用 PTY/ConPTY。启动
自检后的单次 PTY 创建失败返回 `tool_pty_unavailable`，不能启动 pipe 代替。

### 6.2 pipe 后端

- stdout/stderr 独立、持续并发 drain；结果不保证两者跨流相对顺序；
- stdin 默认关闭；唯一的 `chars="\u0003"` 转为 OS interrupt，其它非空 chars
  返回 `tool_exec_stdin_closed`；
- 适用于脚本、编译、测试、`git commit -m`、非交互包管理器，以及已配置 key、
  已存在匹配 known_hosts 且保证无 prompt 的 `ssh host command`；首次 host-key
  确认或其它非秘密 SSH prompt 必须使用 `tty=true`；
- 需要 REPL、TTY 检测、SSH shell 或行式交互 prompt 时用 `tty=true`；
- 命令自行 `&`、`start`、daemonize 或逃逸 process group 不在契约内，cleanup
  best effort 并设置 `cleanup_incomplete`。

### 6.3 PTY/ConPTY 行式后端

- Linux/macOS 的主 Client 不在已有 asyncio/HTTP/WebSocket 线程环境中调用
  `fork()`/`preexec_fn`。主 Client 先按 §11.2 构造唯一的过滤后 `child_env`，再以
  exec-style 子进程启动最小内部 helper；不得让 helper 先继承完整 parent environment
  再自行删除。helper 不读取 parent environment，也不接收 Device token、JWT 或其它
  credential，只通过私有本地通道接收 argv、cwd 和控制消息。helper 在创建任何线程
  或高层网络对象前调用标准库 `pty.fork()`，由 forkpty/login_tty 语义让 shell 成为
  session leader 并获得 controlling terminal；forkpty child 原样继承同一份
  `child_env`。helper 再通过该通道向主 Client relay 输入、输出、pid/exit 和控制；
  该通道不是 Device wire protocol，command/input/output 不写日志。macOS native
  spike 必须先证明 frozen helper re-entry、event-loop progress、reap 和 shutdown；
- Windows 使用条件依赖 `pywinpty==3.0.5; sys_platform == 'win32'` 和 ConPTY，
  最低支持 Windows 10 1809；不启用旧 WinPTY fallback。Client 为每个 ConPTY
  session 使用专用 reader thread 持续执行同步 read 并写入 thread-safe bounded ring，
  写入/等待通过该 session 的受限串行 adapter 执行，不能占用 event loop 或无界的
  默认 thread-pool queue。shutdown 必须唤醒并 join reader；
- PTY 只有一个合并输出流，没有 stdout/stderr 分离；固定为 80 列 × 24 行，
  不支持 resize；
- POSIX helper 在 exec shell 前显式初始化 cooked termios：`ICANON`、`ECHO` 和
  `ISIG` 开启，`VINTR` 为 ETX。输入可能由 terminal driver 回显到合并输出，Agent
  必须容忍。前台程序可以自行改变 termios；一旦它关闭 `ISIG` 或重映射 `VINTR`，
  后续 `\u0003` 可能作为普通数据而不产生 SIGINT，Client 不强行改回；
- Client 使用跨 read-chunk 的有界增量状态机规范化输出。UTF-8 与 CSI/OSC 可以跨
  chunk；单个未终止 escape 序列最多保留 256 bytes，超限后丢弃并记录
  `terminal_control_truncated`。`CRLF` 变为一个 LF，bare CR 也变为 LF，不模拟覆盖；
  退格只删除当前尚未投递文本片段中的前一个 Unicode scalar，不能回改已经由成功
  poll 返回的文本；tab 保留为 `\t`。颜色和其它非文本控制序列不进入 Provider，
  也不维护 row/column 或 screen canvas；
- 两个平台的 PTY adapter 都实现固定原点的最小 DSR compatibility responder：
  `CSI 5 n` 精确回复 `ESC [ 0 n`，`CSI 6 n` 精确回复 `ESC [ 1 ; 1 R`，
  `CSI ? 6 n` 精确回复 `ESC [ ? 1 ; 1 R`。它只避免行式程序等待 terminal response，
  不声称报告真实光标位置。response 通过 session 的串行 writer 直接写回 PTY，
  不进入 output ring。其它需要完整终端查询/屏幕状态的协议仍不支持；responder
  只在实际 response write/flush 失败或超时时终止 session 并返回脱敏的
  `tool_exec_failed`，不能让应用无限等待；未知或未支持的 terminal query 直接忽略；
- `chars` 发送文本或控制字符；`\u0003` 原样写入 PTY/ConPTY 输入通道，表示一次
  best-effort Ctrl-C 请求，但不保证前台程序产生中断或退出。POSIX PTY 通常把 `\u0004`
  解释为 Ctrl-D/EOT，但 Windows 不保证相同 EOF 语义，Agent 应发送所选 REPL/shell
  的正常退出命令；必须结束进程时使用 `terminate=true`。控制字符不写入日志；
- shell argv 仍把 command 作为一次 shell `-c`/`-Command` 参数启动；child
  直接绑定真实 PTY，不先启动裸 shell 再把 command 当输入写入。POSIX 使用
  `bash/zsh [-l] -c`；PowerShell tty=true 使用 `-Command`，login=false 保留
  `-NoProfile` 但去掉 `-NonInteractive`，login=true 去掉 `-NoProfile`；cmd 使用
  `/D /S /C`。PTY 使 command 的子程序看到 `isatty`。交互目标始终是放在
  `command` 中的 REPL 程序，例如 `command="python"`；若需要 PowerShell 自身 REPL，
  使用 `shell="pwsh", command="pwsh"` 启动嵌套交互进程。空 command/直接启动裸
  shell 不受支持；
- 简单 REPL、SSH shell、TTY 检测和行式确认是支持目标；自动打开 Vim/nano 等
  编辑器和其它全屏 TUI 不是 acceptance gate；
- `sudo -n`、SSH key、host-key 确认和非秘密 y/n prompt 属于支持边界。OO 无法可靠
  自动识别所有 password/2FA/passphrase prompt；Agent 一旦看到此类提示，必须停止
  输入、终止或放弃该 session，并让用户在 OO 外完成凭据配置，不能在聊天里索要秘密。

### 6.4 `write_stdin`

```json
{
  "name": "write_stdin",
  "description": "查询或操作当前聊天拥有的 exec session。pipe 的唯一非空 chars=\\u0003 是 OS interrupt 控制操作，不会写入 ETX；tty 将 chars 写入终端，其中 \\u0003 只是 best-effort Ctrl-C。必须结束进程时使用 terminate=true。",
  "input_schema": {
    "type": "object",
    "properties": {
      "session_id": {"type": "string", "format": "uuid"},
      "chars": {"type": "string", "maxLength": 65536, "description": "最多 65,536 个 Unicode 字符，且 UTF-8 编码最多 65,536 bytes"},
      "terminate": {"type": "boolean", "default": false},
      "yield_time_ms": {"type": "integer", "minimum": 0, "maximum": 30000},
      "wait_for": {"type": "string", "minLength": 1, "maxLength": 4096},
      "wait_timeout_ms": {"type": "integer", "minimum": 0, "maximum": 30000},
      "max_output_chars": {"type": "integer", "minimum": 1000, "maximum": 50000, "default": 10000}
    },
    "required": ["session_id"],
    "additionalProperties": false
  }
}
```

规则：chars 省略或空表示只 poll。pipe 把唯一的 `chars="\u0003"` 解释为当前
进程树的 OS interrupt 控制操作，不向 stdin 写入真实 ETX byte；其它非空 chars
立即返回 `tool_exec_stdin_closed`，并提示以 `tty=true` 重新执行。PTY 把 chars
原样写入终端；`\u0003` 只是 best-effort Ctrl-C 请求，程序可将其作为数据；
`\u0004` 只承诺传输 EOT 字符，不承诺所有平台都把它解释为 EOF。
`terminate=true` 是跨平台的强制结束入口，执行 bounded tree termination，不能与
非空 chars 或 wait_for 同时使用。不提供 `close_stdin`：pipe stdin 从 spawn 起关闭，
PTY 没有可靠的跨平台 half-close。

没有 wait_for 时使用 optional `yield_time_ms`，缺省 1000ms、上限 30000ms；有
wait_for 时使用 optional `wait_timeout_ms`，缺省 10000ms、上限 30000ms；两者不能
同时显式提供，验证必须基于字段是否出现，而不是 Pydantic 注入的默认值。
没有 `wait_for` 时显式传 `wait_timeout_ms` 返回 `tool_invalid_args`。
Server 和 Client 都拒绝 UTF-8 编码超过 65,536 bytes 的 chars。wait_for 是
substring，可跨 read chunk 匹配，不是 regex。每次调用先检查已有未读内容，再等待
新内容；pipe 分别在 stdout 与 stderr 中匹配，不能跨两个流拼接命中；PTY 在规范化的
单一合并 output 中匹配。deadline
到期只返回 running session。chars write/flush 最多 5s，部分写入后丢结果返回
`tool_execution_outcome_unknown`，不自动重发。成功 poll 原子消费 unread output；
终态首次 final poll 返回剩余内容并移除。`terminate=true` 本身等待有界终止、返回
这份最终报告并移除记录，不要求再调用一次 final poll。

poll/result 投递是 at-most-once：Client 在构造 result 时推进 unread cursor，并且
Protocol v2 不增加 `tool_result_ack` 或可重放 output cursor。如果随后 WS 丢失，
Server 返回 outcome unknown；已消费字符或已移除的终态记录不在新 generation 重放。

### 6.5 `list_exec_sessions`

```json
{
  "name": "list_exec_sessions",
  "description": "列出当前聊天在指定可信设备上拥有的 exec sessions。",
  "input_schema": {
    "type": "object",
    "properties": {},
    "additionalProperties": false
  }
}
```

只返回当前 chat、最多 8 条记录：`session_id`、status、tty、shell、login、cwd、
elapsed、idle、硬超时剩余时间和 200 字符 command preview。列表最多 16,000 字符；
list 不刷新 idle，也不泄漏其它 chat。

## 7. 结果与错误

### 7.1 报告格式

Device `tool_result.content` 仍是安全文本块，不新增 JSON result frame。固定标签：

```text
session_id / status / tty / shell / login / exit_code / signal / reason
elapsed_ms / cwd / stdout / stderr / output
stdout_truncated / stderr_truncated / output_truncated
stdout_dropped_chars / stderr_dropped_chars / output_dropped_chars
stdout_total_dropped_chars / stderr_total_dropped_chars / output_total_dropped_chars
response_truncated_chars / terminal_control_truncated / cleanup_incomplete
```

pipe 使用 stdout/stderr；PTY 使用合并 output。状态为 `running | exited | terminated`。
自然 zero exit 和 running/yield 为 `is_error=false`；nonzero、signal、hard timeout
和 unexpected failure 为 bounded `is_error=true`，但 Agent loop 继续。

Server 在发布调用前按 `max_output_chars` 预留最坏情况 JSON string escaping 加固定
frame envelope 的 `max_result_bytes`，不能按英文/ASCII 的常见字节数估算。中文、
emoji、引号、反斜杠和控制字符的 50,000 字符编码测试锁定该上界。

### 7.2 稳定错误码

| 错误码 | 含义 |
|---|---|
| `tool_invalid_args` | schema/跨字段校验失败 |
| `tool_device_busy` | 8 槽已满或 config activation 中 |
| `tool_device_unreachable` | 明确未发往当前设备 |
| `tool_execution_outcome_unknown` | 已发出调用或写入的结果/副作用不确定 |
| `tool_exec_timeout` | 进程达到 hard timeout |
| `tool_exec_failed` | spawn/runtime 管理失败 |
| `tool_exec_session_not_found` | 缺失、过期或 foreign-chat session |
| `tool_exec_stdin_closed` | pipe session 不接受普通文本输入；应以 tty=true 重启 |
| `tool_exec_interrupt_failed` | 进程仍在运行，但 OS interrupt 无法可靠投递 |
| `tool_shell_unavailable` | shell 不存在或不适用当前 OS |
| `tool_pty_unavailable` | tty backend 不可用且未降级 |
| `tool_shell_login_unsupported` | shell 不支持 login=true |
| `tool_client_shutting_down` | Client 永久关闭中 |

`tool_command_denied`、`tool_env_not_allowed`、`tool_cwd_outside_workspace` 不属于
Py6 exec 新契约；文件工具自身 path policy 仍保留。

## 8. Device config 与 PATCH fence

Py6 的持久字段：

```text
shell_timeout_max INTEGER NOT NULL DEFAULT 600 CHECK (0 <= value <= 86400)
env_allowlist     JSONB   NOT NULL DEFAULT
  ["PATH","HOME","LANG","TERM","SystemRoot","ComSpec","PATHEXT",
   "TEMP","TMP","USERPROFILE"]
```

不添加 command_denylist/mcp_servers；开发项目直接更新 model/bootstrap/schema。
REST `POST /api/devices`、`GET /api/devices`、`PATCH /api/devices/{name}/config`
同步包含字段。PATCH 是 partial top-level，空 PATCH 不发送 config_update；
env_allowlist 是 whole-list replacement。allowlist 最多 64 个唯一精确名称，禁止
空白/NUL/control/`=` 和 `OPENOCTOPUS_` 前缀；Windows 比较大小写不敏感。带旧
command_denylist 字段的请求作为 unknown field 拒绝。

修改 workspace_path、sandbox_mode、shell_timeout_max 或 env_allowlist 时使用
取消安全的 per-device 状态机：

```text
OLD_ACTIVE -> UPDATE_FENCED -> COMMITTED -> NEW_ACTIVE
```

1. DB commit 前发布 fence，阻止旧 policy 下的新 shell call 进入 transport；
2. 只有确认 rollback 才回 OLD_ACTIVE；HTTP cancellation 不能猜 commit 结果；
3. commit 成功后 fence 不能回滚：终止旧 policy session，push 新 config，再激活；
4. push/activation 失败则 retire generation、停止旧路由，等待下一次 hello_ack；
5. transition 独立拥有 commit、DB close、push 和最终 fence，不持有 DB session 等
   Client；
6. Server 离线时立即停止路由，但不能物理杀掉不可达 Client，Client 本地限制仍有效；
7. name/ssrf_denylist 单独改变不终止 process。

Server 是 trusted control plane；Client 重复检查只防 stale/misrouted frame，不能
防御被信任 Server 主动把设备切成 trusted 后发恶意命令。

## 9. Agent loop、Stop 与多 tool_use 配对

Provider iteration 从同一 DB snapshot 生成 trusted device tools。提示只说明：普通
读写优先 file tools；exec 使用宿主权限；REPL/TTY/行式 prompt 设置 `tty=true`；
长任务使用 yield 后 list/poll；outcome unknown 先检查 session/外部状态，不重跑；
password/2FA/passphrase 需要用户接管。

Stop 取消 Server runner 和当前等待，但不发 process cancel：

1. Stop 在 transport preflight 前获胜：当前及同 Provider batch 中所有未 dispatch
   tool ID 写入 paired `user_cancelled` synthetic result；
2. Device call 已成为 issued_or_ambiguous：先安装 late tombstone；该 ID 写入
   paired `tool_execution_outcome_unknown`，其它未开始 ID 写 `user_cancelled`；
3. 事务完成后才标 turn cancelled；
4. 后来的真实 result 只消费 tombstone，不能替换 synthetic result 或复活 runner；
5. 本地 process 继续到 natural/terminate/timeout/idle/Client cleanup，同 chat
   后续回合可 list/poll。

这保证每个 assistant tool-use 都有可接受的 tool-result，且 late result 不复活 Agent。

删除 chat 也不新增一条隐式 process-control frame。删除后其本地 record 不再能由
Agent 寻址，并在最后一次访问后的 30 分钟 idle deadline（或更早 hard timeout/
Client lifecycle cleanup）被终止和回收。Py6 明确接受这段有界占槽窗口，避免为
chat 删除单独引入另一套远程取消协议。

## 10. Client 架构与 session manager

```text
ClientRuntime
├── ExecSessionManager（跨 retryable reconnect 存活）
│   ├── PipeBackend
│   └── PtyBackend
├── reconnect loop
│   └── generation-scoped reader/writer/heartbeat/tool worker
└── periodic idle cleanup
```

推荐模块边界：`process.py` 负责 discovery/argv/env/cwd/pipe/pty spawn/tree terminate；
`exec_sessions.py` 负责 buffer、session、manager、terminal future；`tools/exec.py`
负责 strict args、adapter、formatter；`pty_worker.py` 仅实现 POSIX frozen helper；
`protocol.py` 负责 v2 DTO；`connection.py` 负责 generation、config ordering、late
result。共享高层生命周期，低级 pipe/POSIX PTY/ConPTY I/O 分别实现；不要引入
platform plugin registry 或通用 subprocess framework。

每个 admitted exec 在 spawn 前创建 UUIDv7：

```text
RESERVED -> STARTING -> RUNNING -> TERMINATING -> TERMINAL -> REMOVED
                         |             ^
                         +-------------+
```

记录包含 owner chat、policy epoch、pid/handle、tty/shell/login/cwd、脱敏 preview、
started/last_access/deadline、reader/buffer、terminal reason、exit code/signal、
per-session lock 和唯一 terminal future。spawn 后 cancellation 也不能产生 untracked
child。等待输出不得持有全局 admission lock。

8 槽覆盖 RESERVED/STARTING/RUNNING/TERMINATING/未 final poll 的 TERMINAL；初始
exec 已完成且未暴露 session ID 时立即移除；暴露过 ID 的终态到首次 final poll 或
idle cleanup 才移除；第 9 个原子失败为 `tool_device_busy`，只能显示 `8/8`。
Py6 明确不增加跨设备的 user-level exec quota：进程和主要内存都消耗在用户自己的
设备上。若以后有 Server 资源证据要求该限制，再单独设计按 user_id 的 admission。

pipe stdout/stderr 各使用 50,000 字符 head+tail ring（前 25,000 + 后 25,000）；
PTY 使用单一规范化 output ring。增量解码 UTF-8，非法序列 replacement。Client
每 30 秒用 monotonic clock 扫描：running 30 分钟无访问为 idle_timeout，terminal
再保留 30 分钟后 remove。output 和 list 不刷新 TTL。所有终止竞态通过 terminal future
收敛。

## 11. Shell 与三平台

| OS | 默认优先级 | canonical names |
|---|---|---|
| Linux | bash → sh → zsh | bash, sh, zsh |
| macOS | zsh → bash → sh | zsh, bash, sh |
| Windows | pwsh → powershell → powershell_x86 → cmd | pwsh, powershell, powershell_x86, cmd |

Discovery 使用 parent PATH 和 Windows known locations。无 shell 时连接前永久失败；
Windows 还要求绝对 System32 `taskkill.exe` 可用。schema 不接受绝对 shell path；
explicit unavailable 不 fallback。

### 11.1 shell argv 与 PTY

Client 使用显式 argv，不使用 `shell=True`。pipe 和 PTY 都把 command 作为一次
shell `-c`/`-Command` 参数启动；不要先启动裸 shell 再把 command 当输入写入，
避免 prompt/回显/引用竞态。

| Shell | login=false | login=true |
|---|---|---|
| bash | `bash --noprofile --norc -c <command>` | `bash -l -c <command>` |
| zsh | `zsh -f -c <command>` | `zsh -l -c <command>` |
| sh | `sh -c <command>` | `tool_shell_login_unsupported` |
| PowerShell pipe | `-NoLogo -NoProfile -NonInteractive -Command <command>` | `-NoLogo -NonInteractive -Command <command>` |
| PowerShell tty | `-NoLogo -NoProfile -Command <command>` | `-NoLogo -Command <command>` |
| cmd | `cmd.exe /D /S /C <command>` | `tool_shell_login_unsupported` |

PTY 使 command 的 child 看到 `isatty`；`login=true` 可加载 profile、改变环境、
产生副作用，不是安全模式。

`tty=true` 仍然执行一个非空 `command`，不把所选 shell 本身变成跨调用共享的裸
交互 shell。Agent 需要把目标 REPL 写进 command；PowerShell 自身交互使用嵌套的
`shell="pwsh", command="pwsh"`。不同 `exec` 调用之间不共享该 REPL 的 cwd、变量或
profile 状态。

### 11.2 environment、cwd、终止

Client 只构造一次 `child_env`：复制 allowlist 中的 parent 值，Windows 大小写不敏感，
再删除全部 `OPENOCTOPUS_*`。POSIX PTY helper、forkpty child、pipe child 与 Windows
ConPTY child 都只能使用这同一份环境，不得与 parent environment 再次 merge。
为减少 pager、颜色和 TUI 噪音，Client 固定覆盖
`TERM=dumb`、`NO_COLOR=1`、`PAGER=cat`、`GIT_PAGER=cat`、`GH_PAGER=cat`；
`login=true` 的 profile 仍可能再次改变它们，这是用户明确接受的行为。
`working_dir` 展开 `~`，相对 workspace root，要求已存在目录；trusted exec 允许
OS 可达绝对路径。

固定 root/final drain 1 秒、graceful terminate 后强杀前 2 秒，不作配置项。

- POSIX pipe 使用 `start_new_session=True`；PTY helper 的 forkpty child 自成 session/
  process group。两者的 interrupt 都用 `SIGINT`，terminate 用 `killpg(SIGTERM)` 后
  `SIGKILL`，最后 await/reap child 与 helper；
- Windows pipe 以 `CREATE_NEW_PROCESS_GROUP` 启动；pipe 的 `chars="\u0003"` 通过
  process-group-scoped `CTRL_BREAK_EVENT` 实现最接近的 Ctrl-C 语义，投递失败返回
  `tool_exec_interrupt_failed` 且不声称进程已停止。ConPTY 只把 `\u0003` 写入终端；
  console input mode 或 application handler 可使它成为普通输入，因此不承诺中断。
  pipe 与 ConPTY 都尽早放入带 `KILL_ON_JOB_CLOSE` 的 Job Object；创建到
  assign 之间的窄窗口无法消除时必须在风险与测试中显式保留。terminate 先走正常
  控制/Job 终止，`taskkill /PID <pid> /T /F` 仅作有界兜底；无法证明子树已消失时
  报告 `cleanup_incomplete`；
- 不追踪故意 setsid/daemon escape，属于后续 OS sandbox。

## 12. 生命周期表

| 事件 | session 行为 | Client 行为 |
|---|---|---|
| DNS/TCP/TLS、1000/1001/1006/1013/4408 | 继续 | backoff reconnect |
| Server restart | 继续，受本地 deadline/idle | reconnect、比较 config |
| Chat Stop | 不发 cancel，继续自然结束 | 无 process frame |
| Chat 删除 | 不再可寻址；最迟由 30 分钟 idle cleanup 终止 | 无 process frame |
| 相同 policy reconnect | 继续 | 同 chat list/poll |
| policy 改变 | `policy_changed` terminate | fence 后激活新 policy |
| token rotation/device delete/4401 | 全部 terminate | 永久退出 |
| 4000 connection_replaced | 全部 terminate | 永久退出、不抢占 |
| Client SIGINT/SIGTERM/正常退出 | 全部 bounded terminate | cleanup 后退出 |
| Client crash/SIGKILL/断电 | 无恢复保证 | 无磁盘恢复 |

generation detach 时，原 adapter 与 process record 分离；running record 仍可由同
chat list/poll。旧 generation result 不在新 generation 重放；Server restart 对未
配对 tool_use 仍修复为 outcome unknown。永久关闭时拒绝新 reservation、推进 epoch、
并发终止 tree、drain/reap、清理 records 和后台 task，外层 guard 15 秒。

## 13. 安全与控制平面

Py6 exec 是用户账户权限下的远程代码执行，可读 workspace 外文件、联网、启动
descendants。cwd、env allowlist 和 shell 选择只是人体工学控制。

- `sandbox_mode=true` 不暴露 exec；false 是明确 trusted 选择；
- Server 是 trusted control plane，Client 只防 stale/misrouted frame；
- 不用 command denylist 冒充 isolation；
- token、`OPENOCTOPUS_*` 不进入 child；
- lifecycle 日志只记录 session/state/shell/elapsed/error code，不记录 command、
  输入、输出、环境值、JWT 或 credential；
- Agent 没有 secret prompt 通道；password/2FA/passphrase 以后由人类接管；
- foreign-chat not-found 防 session probing。

## 14. TDD 实现计划

### Slice A：协议、schema、REST

先写失败测试：v2-only/4409、hello shell metadata、配置默认值、alias-free schema、
唯一 `tty`、trusted-only enum、immutable name/UUID fence、hidden chat ownership、
删除 command denylist、optional deadline presence、PATCH fence、outcome unknown、
late tombstone、Stop pairing 快照。

### Slice B：shell 与 process backend

先测三平台 discovery/default、explicit unavailable、login argv、参数长度、环境
allowlist、cwd、pipe stdin closed。然后做 macOS frozen-helper/forkpty spike，验证
controlling terminal、event-loop/reap/shutdown，再实现 POSIX helper 和 Windows
pywinpty 3.0.5/ConPTY。Windows PyInstaller spec 条件化收集 `winpty` 的原生扩展、
hidden imports、DLL 与包数据（例如 `collect_all("winpty")` 的等价显式结果），并在
构建时断言必需二进制存在。native smoke 从干净环境验证 import/DLL/ConPTY。测试
中文/emoji/非法字节、双流
大输出、PTY REPL 的默认/显式长 timeout、TTY detection、terminal echo、ISIG 开关、
best-effort Ctrl-C、POSIX Ctrl-D/EOT、Windows 正常 exit、helper/shell/descendant 环境
无 token sentinel、UTF-8/ANSI 跨 chunk、bare CR→LF、未投递片段退格、固定
`ESC[0n`/`ESC[1;1R`/`ESC[?1;1R` DSR 应答、Windows pipe
`CTRL_BREAK_EVENT` 成功/失败、nonzero/EOF、heartbeat 不阻塞。

### Slice C：ExecSessionManager

使用 injected monotonic clock 与 deterministic barrier 测试：8-slot admission、
foreign ID、终态保留/final poll、每流 50k ring、10k/50k report、wait_for 跨 chunk、
30 分钟 idle、natural/interrupt/terminate/timeout/policy/shutdown 单终态、root
exit + descendant pipe、spawn cancellation 无 orphan、process tree reap。

### Slice D：Server/Agent 竞态

测试 preflight 未发与 issued/ambiguous 分类、无自动 replay、旧 TTL 后 tombstone、
replacement generation late result、Stop 多 tool_use 配对、Server restart unknown
repair、PATCH fence/rollback/cancellation-safe push、revoke/delete/replacement/
shutdown tree cleanup 和所有调用方统一 outcome-unknown。

### Slice E：冻结包、三平台和容量

每个平台 native harness 覆盖 shell/login/cwd/Unicode、pipe/tty REPL、Ctrl-C、
yield/poll/list/terminate/final removal、reconnect、hard timeout、8th success/9th
busy、foreign-chat、4401/4000 cleanup、PTY unavailable 不降级和无 secret logs。
POSIX 另外验证 Ctrl-D/EOT；Windows 通过 shell/REPL 的正常 `exit` 命令验证退出。
500-peer harness 不启动 500 个真实 shell，只压测 registry/WebSocket/heartbeat/
pending/tombstone/queue，记录 RSS、FD、task、event-loop lag、high-water 和 cleanup
baseline。

## 15. Acceptance gate

合并前必须通过 Server/Client focused tests、Ruff、strict mypy、`git diff --check`；
所有 canonical docs/snapshots 与 v2 一致；Linux source/frozen E2E、macOS/Linux/
Windows native artifacts、真实 Server+PostgreSQL+RustFS+deterministic Provider+
Client Agent E2E 和 500-peer capacity gate 均通过。native PyInstaller smoke 必须
验证 pywinpty DLL/ConPTY；所有 artifact 无 token、命令、输入、输出和 secret sentinel，
不提交 generated bundle 或临时 tracker。

## 16. 决策状态

本文已锁定 pipe 默认、显式 `tty`、行式 PTY、无截图/视觉、无 secret channel、
timeout/yield、三平台 backend、生命周期、Stop 配对、PATCH fence、late tombstone、
统一 outcome-unknown，以及“仅每个 Client runtime 8 槽、不增加跨设备用户级配额”。
实现前没有剩余产品决策；native spike 若证明某个平台 backend 不可冻结或不能满足
进程树清理不变量，应停止该切片并回到设计评审，不能静默降级或缩小平台承诺。
