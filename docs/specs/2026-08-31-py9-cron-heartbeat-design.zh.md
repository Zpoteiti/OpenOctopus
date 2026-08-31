# Py9 Cron / Heartbeat 自动化设计

**状态：** approved（已批准）

**Milestone：** Py9 Cron / Heartbeat

**依赖：** 当前 `main` 上已经完成的 Browser Frontend、普通 Agent loop、Workspace 与消息持久化

**协议：** Device Protocol v3 不变；Py9 不修改 Client

本设计是 Python-main 中 Py9 的 implementation authority。它把
`DECISIONS.md`、`SCHEMA.md`、`API.yaml`、`TOOLS.md` 和
`SYSTEM_PROMPT.md` 中仍为 Rust-era、placeholder、deferred 或互相冲突的
Cron / Heartbeat 文字收敛为一个可实现、可测试的合同，并取代：

- `cron_jobs.session_id NOT NULL` 与 Session 外键绑定；
- 删除 Cron job 同时删除专属 Session 和历史；
- Cron 向创建它的聊天 Session 注入消息；
- Server 启动后每隔相对 30 分钟运行 Heartbeat；
- Heartbeat 只要文件非空就直接运行完整 Agent；
- naive 时间默认 UTC、但同时允许可选时区的含混契约；
- Py7 文档中 `cron` 只是 future placeholder 的状态。

Py9 保留 ADR-005 的核心原则：所有入口最终都发布普通 user-role message，
不增加 `EventKind`、`PromptMode::Cron` 或 `PromptMode::Heartbeat`。Cron 和
Heartbeat 的 Phase 2 都复用现有 PendingMessage、TurnRun、compaction、Agent
loop、tool registry 与 Provider limiter。

## 1. 结果与边界

Py9 交付两个互补的自动化入口。

### 1.1 Cron

- 用户、REST caller 或 Agent 可以创建一个明确时间表和任务说明；
- 每个 job 固定路由到 `cron:{job_id}`，不继承创建聊天；
- 每次实际 fire 都向同一只读 Session 注入一条 synthetic owner message；
- 同一 job 忙碌时跳过新的到期，不排队、不补跑；
- 删除 job 只停止未来 fire，已经存在的 Session 和历史保留；
- 删除 Session row及其全部历史，但不删除 job；下次 fire 以同一个 Session UUID 和
  `session_key` 重建空 Session；
- one-shot 在被可靠接纳后自动删除 job row，历史 Session 继续保留。

### 1.2 Heartbeat

- 每个用户天然拥有一个条件式巡检入口，不创建隐藏 Cron row；
- 全局 pulse 对齐 UTC 墙钟的每个 `xx:00` 与 `xx:30`；
- 缺失、过大、格式无效或没有有效 `## Active Tasks` 的 `HEARTBEAT.md`
  直接跳过，不调用 Provider；
- Phase 1 使用单一、强制的 `heartbeat_decision` tool 判断 `skip` 或
  `run + tasks`；任何无法严格验证的结果 fail closed 为 skip；
- 只有 `run` 才向每个用户固定的 `heartbeat:{user_id}` 只读 Session 注入
  Phase 2 synthetic owner message并运行正常 Agent；
- Phase 1 不进入聊天历史，不保存决策状态；Phase 2 的完整工作历史可在浏览器查看；
- Heartbeat Session 可删除；下一次真实 Phase 2 使用同一稳定 UUID 重建。

### 1.3 用户可见闭环

- 侧栏增加 `/automations`；
- Cron 支持创建、编辑、删除、查看上次/下次时间和进入历史；
- Heartbeat 支持编辑 `HEARTBEAT.md`、查看个人时区和进入历史；
- Account 页面支持保存用户级 IANA timezone；
- Cron / Heartbeat Session 在普通聊天历史页可读、可删除、可取消正在运行的
  turn，但没有消息输入框。

## 2. Review 时需要特别确认的实现级默认

以下不是新增产品分支，而是把已确定行为落成唯一可测语义：

1. **Cron fire 使用同一数据库事务。** 推进 `next_fire_at`（或删除
   one-shot）、JIT 创建 Session、写 PendingMessage 和预留 TurnRun 一起 commit。
   commit 前崩溃会重试；commit 后崩溃由 pending recovery 接手。不存在“时间已推进、
   消息尚未落库”的空窗。
2. **Cron expression 只支持标准五段式。** 不支持秒字段、年字段、Quartz 扩展、
   `@daily` alias 或自然语言。
3. **`every_seconds` 最小 60 秒。** Agent 工作不适合亚分钟调度；重叠跳过仍是
   必须的不变量，而不是依赖该下限防积压。
4. **用户 timezone 默认 `UTC`。** Cron 省略 `tz` 时使用用户 timezone；
   Heartbeat 的 Phase 1 同时接收 UTC、用户本地时间和 IANA 名称。
5. **自动化 Session UUID 稳定。** Cron Session UUID 等于 job UUID；Heartbeat
   Session UUID 等于 user UUID。删除历史后 URL 不变。Web session creation对全局 active
   job/user保留 UUID做 404 fence，避免其它用户抢占。
6. **Py9 不新增跨渠道通知。** 自动化结果首先进入只读历史。Discord / DingTalk
   delivery 与扩展后的 `message` routing 属于 Py10。
7. **Heartbeat fan-out 有固定 staging 上限。** User query每页 100，最多 32 条 user
   pipeline并发；该上限保护 DB/RustFS/内存，不替代既有 Provider limiter，也不新增配置项。
8. **时间依赖固定。** Py9 使用 `croniter==6.2.4` 解析五段 cron，并显式依赖
   `tzdata==2026.3`；所有 schedule/timezone行为只经过一个 wrapper。

## 3. 已确定的产品决策

1. Cron 和 Heartbeat 都是正常 Agent work 的触发器，不是独立 Agent 实现。
2. 每个 Cron job 一个独立 Session；每个用户一个 Heartbeat Session。
3. 自动化 Session 不继承创建聊天的 history、channel、chat_id、effort 或附件。
4. Cron job 不保存 Session 外键；稳定 route 由 job UUID 派生。
5. 删除 Cron job 只停止未来触发并保留历史。
6. 删除 Cron Session 不影响 job；下一次 fire JIT 重建。
7. one-shot 成功进入 durable runner 后删除 job row并保留历史。
8. 同一 Cron job running 或 pending 时，本次到期直接跳过；不创建 Message、
   PendingMessage 或 TurnRun。
9. 忙碌跳过只写 bounded lifecycle log；不增加 run-history 表、计数器或 API 字段。
10. `last_fired_at` 只在本次 fire 已原子写入 synthetic pending 并预留 TurnRun 时更新。
11. 不同 Cron job 可以并发；仍共享现有 Provider 和 context admission 上限。
12. Server restart 不补 recurring 的错过次数；直接推进到下一个未来 occurrence。
13. Server 离线期间错过的 one-shot 在启动恢复时删除，不迟到执行。
14. Heartbeat 不是 Cron row，也不保存 last pulse、last decision 或 enabled 状态。
15. Heartbeat pulse 对齐 UTC `:00` / `:30`，启动等待下一个严格未来边界。
16. 修改 `HEARTBEAT.md` 不立即触发；等待下一边界。
17. 全局 pulse scan 自身不得重入；上一个 scan 跨过下一边界时跳过该边界。
18. 某用户 Heartbeat Phase 2 running 或 pending 时，该用户本轮在 Phase 1 前跳过。
19. `HEARTBEAT.md` 必须有精确的 `## Active Tasks` section 才可能调用 Provider。
20. Phase 1 强制唯一 `heartbeat_decision`；invalid output、无调用、多调用、
    错误工具名、错误参数、空 run tasks 或 Provider 最终失败全部视为 skip。
21. Phase 1 不做格式修复重试；Provider 层既有 transient retry 合同保持不变。
22. Phase 2 使用标准 system prompt 与完整 owner tool list，不增加 PromptMode。
23. Heartbeat 不增加 notification evaluator，也不自动发送外部消息。
24. 用户级 timezone 是持久 profile 字段；浏览器可建议本机 zone，但不能静默保存。
25. Cron 仍可提供独立 per-job `tz`；省略时才继承用户 timezone。
26. 前端必须随 Py9 交付，否则里程碑不算闭环。
27. 不增加 pause、resume、run-now、batch operation、execution statistics、
    Heartbeat countdown 或 Heartbeat runtime-status API。

## 4. 范围

### 4.1 包含

- 用户 timezone 的 DB、`/api/me` 与 Account UI 合同；
- Cron schedule parser、canonical projection、REST CRUD 与 Agent tool；
- `cron_jobs` 最终 schema、due index、shared write service 和 process-local wake；
- single-process Cron ticker、busy skip、atomic fire、restart recovery 和 clean shutdown；
- Web / Cron / Heartbeat 共用的内部 normalized inbound publish transition；
- Cron / Heartbeat 稳定只读 Session、删除竞态和 JIT recreation；
- Heartbeat wall-clock pulse、Workspace read、deterministic preflight、Phase 1
  forced-tool call、Phase 2 publish；
- `/automations`、Account timezone、只读历史入口与 i18n；
- fake clock、fake Provider、PostgreSQL、RustFS、frontend 和 Docker acceptance。

### 4.2 不包含

- Py10 Discord / DingTalk、channel config、Bot identity 或外部 delivery；
- Py11 Dream、自动 MEMORY consolidation 或 Skill discovery；
- multi-worker leader election、distributed scheduler、Redis、PostgreSQL advisory
  leader lease 或外部 queue；
- durable heartbeat state、last-decision audit table、per-user pulse cursor；
- generic automation graph、workflow DAG、dependencies 或 retry policy；
- cron run-history table、skipped counter、metrics dashboard、billing 或 quota；
- natural-language schedule parsing、秒级 cron、calendar/RRULE、holiday calendar；
- timezone 自动定位、GeoIP、从浏览器隐式覆盖用户 profile；
- Heartbeat manual run、pause、per-user interval、webhook 或 push notification；
- Cron attachment snapshot、创建聊天 context capture 或跨 Session confirmation；
- Client 侧 cron、离线 Client scheduler 或 Device Protocol 变更；
- 多 worker / 多 replica 下的 exactly-once 承诺。

## 5. 不可破坏的不变量

1. 所有自动化都由 owner 创建或由 owner 的 Workspace 文件定义，按 owner 完整权限运行。
2. 浏览器不能向 `cron` / `heartbeat` Session POST human messages，即使该稳定
   Session row 当前不存在。
3. 自动化 synthetic message 必须先进入 PendingMessage，再按普通 runner 合同 promotion；
   不能直接插入 Message 绕过 queue、compaction 或 crash recovery。
4. runtime block 仍由唯一 codec 在 ingress 生成一次、持久化并 replay；它不是授权来源。
5. Cron schedule 推进与 synthetic pending/TurnRun reservation 同一 commit；不能一边成功、
   另一边失败。
6. 同一 Cron job 最多一个 running/pending fire；busy 时不得创建“以后补跑”的 durable row。
7. 跳过的 occurrence 不改变 `last_fired_at`，不污染 Session history。
8. 不同 Cron job 的 Session、pending、TurnRun、effort 和 history 绝不混用。
9. 删除 job 不删除或中断当前 Session turn；删除 Session 不删除 job。
10. active job 的 UUID 和任一现有 User UUID 不能被浏览器创建成 Web Session。
11. Cron / Heartbeat Session recreation 使用相同 UUID 和 session_key；不能生成新的历史 URL。
12. one-shot 的 durable acceptance 与 job row 删除同一 commit；commit 后绝不自动再 fire。
13. Scheduler 不自动 replay 一个已经可能产生 tool side effect 的 Agent turn；普通 runner
    的现有 outcome-unknown 与 recovery 合同继续适用。
14. Heartbeat Phase 1 不写 Session、Message、PendingMessage 或独立状态表。
15. Heartbeat Phase 1 只有严格 `run` 才能触发 Phase 2；任何不确定性 fail closed。
16. Phase 2 与 Web/Cron 使用相同 system-prompt builder、tool registry、Provider limiter、
    compaction 和 cancellation 语义。
17. 用户 timezone 与 Cron per-job timezone 都必须是有效 IANA 名称；内部 instant 统一 UTC。
18. 用户 timezone 变化只影响未来解析/判断，不重写已保存 one-shot instant或 Cron history。
19. shutdown 先停止新 tick/pulse，再关闭 ChatRuntime、Workspace、MCP、RustFS 与 DB；
    后台 automation task 不得访问已关闭依赖。
20. API、Agent tool 和前端必须共享同一 schedule validation/projection；不能各自解析。

## 6. 内部 normalized inbound

### 6.1 目的

当前 `accept_message()` 把 Web 身份、Web route、runtime block、PendingMessage 与
TurnRun reservation 写在同一函数。Py9 需要 Cron / Heartbeat 复用后半段，但不能伪造
HTTP request 或复制 queue 逻辑。

Py9 抽出一个仅供 Server 内部使用、无公共 JSON parser 的 immutable envelope：

```python
@dataclass(frozen=True, slots=True)
class InboundMessage:
    message_id: UUID
    owner_user_id: UUID
    session_id: UUID
    session_key: str
    channel: str
    chat_id: str
    content: tuple[dict[str, object], ...]
    attachment_refs: tuple[dict[str, object], ...] = ()
    effort: Effort | None = None
```

刻意没有：

- `EventKind` 或 `PromptMode`；
- generic `metadata` escape hatch；
- caller-provided timestamp；
- `session_key_override`；
- platform SDK object；
- tool list / permission profile；
- already-built runtime block。

事务内 publish transition 生成 authoritative `received_at`，通过唯一 codec prepend
runtime block，然后在稳定 id advisory lock之后先锁 owner User `FOR KEY SHARE`，再
创建/验证 Session、PendingMessage 和可选 TurnRun。owner不存在就不发布。Py9 三个来源
全部是 owner；Py10 的 sender authority 与 channel event idempotency 在 Py10 spec 中扩展，
不从 runtime 文本反推授权。

### 6.2 route constructors

调用方不能自由组合 route 字段。只有三个小 constructor：

```text
web:
  session_id = request path UUID
  session_key = web:{session_id}
  channel = web
  chat_id = session_id

cron:
  session_id = job_id
  session_key = cron:{job_id}
  channel = cron
  chat_id = job_id

heartbeat:
  session_id = user_id
  session_key = heartbeat:{user_id}
  channel = heartbeat
  chat_id = user_id
```

transition 必须验证已有 Session 的 `user_id/session_key/channel/chat_id` 全部匹配。
相同 UUID 下出现其它 route 是一致性错误，不能改写已有 Session。

### 6.3 public write fence

`POST /api/sessions/{id}/messages` 在创建缺失 Web Session 前检查：

- 若 `id` 等于任一用户 active Cron job UUID，返回 not found；
- 若 `id` 等于任一现有 User UUID（Heartbeat保留身份），返回 not found；
- 若已有 Session 不是 owned Web Session，同样拒绝。

这样用户删除自动化历史后，不能在下一 fire 前把保留 UUID 抢成 Web Session。
检查是全局存在性查询，但响应始终使用普通 404，不返回 owner、route 或保留原因。Job/User
ID 由 Server生成时同样不能与已有 Session/保留 ID冲突；caller不能提供这些 ID。

## 7. 持久化模型

本项目尚无真实用户和 migration framework。Py9 直接修改 SQLAlchemy authoritative
metadata 与 `SCHEMA.md`，开发环境 reset DB，不编写迁移或兼容读取。

### 7.1 `users.timezone`

```sql
ALTER TABLE users ADD COLUMN timezone TEXT NOT NULL DEFAULT 'UTC';
```

- canonical value 是 `zoneinfo.ZoneInfo` 可加载的 IANA name；
- 长度 `1..64`；不接受固定 offset、abbreviation、空串或 `null`；
- 注册不增加字段，默认 UTC；
- `GET /api/me`、login/register 的 User projection 和 admin user projection 都返回；
- `PATCH /api/me` 可独立或与 name/email/password 一起更新；无效值 400 且整次 patch
  不保存；
- Browser `Intl.DateTimeFormat().resolvedOptions().timeZone` 仅作为建议按钮，不自动提交。

### 7.2 最终 `cron_jobs`

```sql
CREATE TABLE cron_jobs (
    id              UUID        PRIMARY KEY,
    user_id         UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name            TEXT        NOT NULL,
    schedule_kind   TEXT        NOT NULL
                                CHECK (schedule_kind IN ('every','cron','at')),
    schedule_value  TEXT        NOT NULL,
    timezone        TEXT,
    message         TEXT        NOT NULL,
    last_fired_at   TIMESTAMPTZ,
    next_fire_at    TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_cron_jobs_user_id ON cron_jobs(user_id);
CREATE INDEX idx_cron_jobs_next_fire ON cron_jobs(next_fire_at, id);
```

与旧占位模型相比：

- 删除 `session_id` 外键；
- 删除可由 `schedule_kind='at'` 推导的 `one_shot`；
- 把无前缀歧义的 `schedule` 拆成 `schedule_kind + schedule_value`；
- `timezone` 保存 effective IANA zone；`every` 必须为 `NULL`；
- `id` 由 shared write service 先生成，以便保留同 UUID Session route；
- 不增加 enabled、updated_at、retry_count、last_error 或 run_history。

`schedule_value` canonical 规则：

- `every`：十进制正整数秒；
- `cron`：trim 后、空白规范化为单空格的五段 expression；
- `at`：UTC RFC3339 instant，使用 `Z`，微秒规范化。

### 7.3 Session 关系

Cron job 与 Session 没有 FK。若 Session 存在：

```text
sessions.id          = cron_jobs.id
sessions.user_id     = cron_jobs.user_id
sessions.session_key = cron:{cron_jobs.id}
sessions.channel     = cron
sessions.chat_id     = cron_jobs.id
```

Heartbeat 没有 job row；其 Session id直接复用同一用户 UUID。JIT create 的默认 title：

- Cron：`Cron · {job.name}`；
- Heartbeat：`Heartbeat`。

普通 `ON DELETE CASCADE` 继续删除 Session 的 Message、PendingMessage 和 TurnRun。
删除 Session 不留下额外 tombstone；active Cron row或 deterministic Heartbeat route本身就是
保留身份。

## 8. Schedule 合同

### 8.1 三种输入

Create 必须且只能提供一种：

```json
{"every_seconds": 3600}
{"cron_expr": "0 9 * * 1-5", "tz": "Asia/Shanghai"}
{"at": "2026-09-01T09:00:00+08:00", "tz": "Asia/Shanghai"}
```

约束：

- `every_seconds`: integer（bool 不算），`60..31536000`；
- `cron_expr`: `1..256` chars，标准五段式且能产生未来 occurrence；
- `at`: future ISO 8601 datetime；aware 值直接确定 instant，naive 值用 effective
  IANA timezone 解释；
- `tz`: `1..64` IANA name，只能和 `cron_expr` / `at` 一起提交；cron 和 naive `at`
  省略时使用 `users.timezone`；aware `at` 省略时 effective zone 为 `UTC`；
- name trim 后 `1..120`；省略时取 message 前 30 个 Unicode code points；
- message trim 后 `1..32000`；存储保留内部换行，不保留首尾空白；
- request models `extra='forbid'`。

五段顺序固定为 `minute hour day-of-month month day-of-week`。允许 `*`、十进制值、逗号、
闭区间、step以及不区分大小写的三字母 month/weekday name；Sunday接受 0 或 7。禁止
`? L W #`、随机值、hash、macro/alias和其它 croniter扩展。day-of-month 与
day-of-week同时受限时采用传统 Vixie cron 的 **OR** 语义（`day_or=True`），不能由 library
默认版本漂移。

PATCH：

- name/message 可独立更新；
- 不提交 schedule field 时保留原 schedule；
- 一旦提交任一 `every_seconds/cron_expr/at/tz`，必须在同一 PATCH 提交完整的一种
  schedule；`tz` 不能单独修改旧 schedule；
- empty patch、显式 null、混合 timing forms 全部 400；
- schedule update 以 commit 时 UTC now 重新计算 `next_fire_at`，不立即 fire。

### 8.2 时区与 DST

- DB instant 与 API timestamp projection 一律 UTC aware；
- Cron wall-clock evaluation 使用 effective IANA timezone；
- aware one-shot 同时提交 `tz` 时，输入 offset必须等于该 IANA zone在目标 instant的
  offset，否则拒绝；省略 `tz` 时保存 effective zone `UTC`；
- naive one-shot 落在 DST 不存在时间时拒绝；落在重复时间时也拒绝，要求 caller 提交
  explicit offset，避免猜 fold；
- cron expression 落在 DST spring-forward 缺失时间时跳过该 local occurrence；
- fall-back 重复 local time 只执行一次，选较早的 UTC occurrence；
- changing `users.timezone` 不重算已有 job；只有省略 tz 的下一次 create/update使用新值。

### 8.3 next-fire 计算

- `every` 以已保存的 scheduled boundary 为 anchor；fire 或 busy skip 后按整数倍推进到
  第一个严格晚于 now 的 boundary，长期不随 Agent duration 漂移；
- `cron` 每次取严格晚于 now 的下一个 local occurrence；
- `at` 必须严格晚于 write helper 的 now；
- create/update validation 和 ticker advancement 调用同一个纯函数模块；
- 所有比较使用 aware UTC datetime；sleep 只负责唤醒，不作为时间真相。

### 8.4 唯一时间实现

Server runtime dependencies新增并固定：

```text
croniter==6.2.4
tzdata==2026.3
```

`openctopus_server.automations.schedule` 是 REST、tool、ticker和测试唯一可调用的 wrapper：

- 在调用 croniter 前验证恰好五段并拒绝 alias、秒/年扩展和 library-specific syntax；
- croniter只负责 expression validation和候选 occurrence迭代；
- `zoneinfo.ZoneInfo` + pinned tzdata负责 IANA offset/gap/fold；
- wrapper应用 §8.2 的 missing/repeated local-time规则并投影 aware UTC；
- 不允许 REST、tool或前端直接 import croniter，也不手写另一套 cron状态机；
- Docker image必须包含 pinned tzdata，不能依赖 host `/usr/share/zoneinfo` 是否完整。

升级任一依赖必须先运行全部 timezone/DST golden tests并作为独立 review diff。

## 9. Cron REST 与 Agent tool

### 9.1 REST

保留现有路由并定稿：

```text
GET    /api/cron?limit=&offset= -> 200
POST   /api/cron             -> 201
GET    /api/cron/{job_id}    -> 200
PATCH  /api/cron/{job_id}    -> 200
DELETE /api/cron/{job_id}    -> 204
```

所有 route 只访问当前 JWT user 的 rows；跨用户 UUID 返回 404。Create/patch 使用 shared
write service，成功 commit 后唤醒 process-local ticker。DELETE：

- hard-delete job row；
- 不查、不删、不 cancel Session；
- 当前已经启动的 Agent turn继续；
- wake ticker；
- 重复/跨用户删除返回 404，不伪装成功。

List 使用 `limit` default 50、range `1..200` 和 `offset >= 0`，按
`(created_at DESC, id DESC)` 稳定排序，返回：

```json
{"items": [], "next_offset": null}
```

`items` 元素是 `CronJobSummary`，字段与下述 `CronJob` 相同但不含最多 32,000 chars 的
`message`；Server以 `limit + 1` 探测后续行，有更多时返回
`next_offset=offset+len(items)`，最后一页为 null。不返回昂贵的 total count。前端必须顺序
翻页，不能依赖截断列表。编辑前通过 `GET /api/cron/{job_id}` 取得完整 job/message，避免
list response随任务正文无界膨胀。

`CronJob` response 使用结构化 schedule，前端不得反解析 DB string：

```json
{
  "id": "<uuid>",
  "name": "weekday report",
  "message": "Prepare the weekday report.",
  "schedule": {
    "type": "cron",
    "cron_expr": "0 9 * * 1-5",
    "tz": "Asia/Shanghai"
  },
  "session_id": "<same uuid or null when history row is absent>",
  "last_fired_at": null,
  "next_fire_at": "2026-09-01T01:00:00Z",
  "created_at": "2026-08-31T12:00:00Z"
}
```

`user_id` 不需要回显：JWT 已限定 owner。`session_id` 只有实际 Session row 存在时才返回，
其非 null 值必等于 job id。用户删除历史后它变 null；下次 fire 恢复同一值。

`schedule` 是 discriminated union；另外两种 projection 为：

```json
{"type": "every", "every_seconds": 3600}
{"type": "at", "at": "2026-09-01T01:00:00Z", "tz": "Asia/Shanghai"}
```

`at` 中的 instant 永远回显 UTC，`tz` 保留创建/编辑时使用的 effective IANA zone，用于 UI
解释；它不改变已确定的 instant。

### 9.2 `cron` Agent tool

Py9 注册一个 Server built-in tool，保持 root schema 简单：

```text
action = add | list | remove
```

- add：message + exactly one timing form；可选 name/tz；
- list：只额外接受 `offset >= 0`，每页固定最多 20 jobs；
- remove：只接受 job_id；
- 不提供 update；编辑由 REST/UI 完成；
- 省略 tz 使用 `ToolContext.user_id` 对应的用户 timezone；
- 结果最多 16,000 chars；list不回显 job message，每项包含 job id、name、canonical
  schedule、next fire和可用 history session id，并返回 `next_offset` 或 null；
- remove result明确“future triggers stopped; existing history retained”；
- 运行在 Cron Session 内的 job 可以 remove 自己；当前 turn 不受影响。

稳定错误：

```text
tool_invalid_schedule
tool_missing_required_field
tool_cron_job_not_found
tool_db_error
```

REST 对同一原因返回既有 Error envelope 与 400/404/503 映射；tool 不泄露 DB exception。

## 10. Cron ticker 与 fire transaction

### 10.1 process lifecycle

每个 Server process 一个 `CronScheduler`：

- 当前 Py9/部署合同固定单 ASGI worker；
- scheduler 在 DB、RustFS、Server MCP 和 ChatRuntime ready、旧 running turns 被 abandon
  之后 start；
- scheduler stop 后才开始关闭 ChatRuntime 与其它依赖；
- loop 异常记录 bounded stderr log，以 1/2/4/.../60 秒 capped backoff重新 arm，成功 scan
  后重置；不能静默永久死亡或 DB outage busy-loop；
- `/health` 不增加 scheduler 子状态，DB/storage 的既有健康合同不变。

### 10.2 wake loop

1. 查询最早 `next_fire_at`；
2. sleep 到该 instant，最长 60 秒；
3. create/update/delete 后通过 process-local `asyncio.Event` 提前唤醒；
4. 醒来后重新读 DB，不相信旧内存 heap；
5. 按 `(next_fire_at, id)` 稳定处理 due jobs；
6. 每批最多 100 rows，批间 yield event loop；仍 due 时立即继续；
7. 正常 scan 不 await Agent completion，只完成 durable acceptance 后交给 ChatRuntime。

Event 的 clear/set 必须避免 lost wake；60 秒 rescan 是最后保险。

### 10.3 单 job fire

对一个 due row：

1. 进入 `ChatRuntime.session_operation(job_id)`，再获取同 UUID 对应 advisory transaction
   lock；DB transition和 commit后的 cancellation-safe runtime handoff完成后才退出 operation；
2. 先锁 owner User `FOR KEY SHARE`，再 `SELECT CronJob ... FOR UPDATE`，并重验 owner仍
   匹配、row仍存在、`next_fire_at <= now`；owner已删除则放弃本次 transition；
3. 检查稳定 Session 是否有 running TurnRun 或任意 PendingMessage；
4. 若 busy：
   - recurring 推进到第一个未来 occurrence；
   - one-shot 删除 row；
   - 不改变 `last_fired_at`；
   - 不创建任何聊天 row；
   - commit 并写一条不含 message 内容的 skip log；
5. 若 idle：
   - JIT create/validate stable Session；
   - 通过 normalized inbound transition 写 synthetic PendingMessage；
   - 在同事务创建 running TurnRun并捕获该 message id；
   - recurring 设置 `last_fired_at=now` 并推进 next；
   - one-shot 删除 job row；
   - commit；
6. commit 后在同一 session operation 内 cancellation-safe 地交给
   `ChatRuntime.schedule()`，再释放 operation。

Synthetic message runtime route：

```text
channel: cron
chat_id: <job UUID>
sender: partner:<owner UUID>
trust: partner
```

正文是 job `message`，前面增加一段短的 Server-authored说明，包含 job name、job id 和
scheduled occurrence。它不携带创建聊天内容、附件、reasoning effort 或 external delivery target。

### 10.4 commit、崩溃与恢复

- commit 失败：schedule 与 pending 都没改变，下次 scan重试；
- commit 成功、内存 schedule 前被 cancel：durable pending保留；
- process crash：startup 先 abandon旧 running TurnRun，再扫描 automation Session 的
  PendingMessage，使用普通 `reserve_pending_turn` 重建 runner并 schedule；
- recovery 必须在新 ticker/pulse start 前完成；
- 已 promotion 且可能执行过外部 tool 的普通 Agent recovery继续遵循现有
  `tool_execution_outcome_unknown` / no automatic replay 合同；
- scheduler 不另建 outbox，因为 schedule transition 与 PendingMessage 已在同一 PostgreSQL
  transaction。

### 10.5 restart 与 wall-clock jump

Startup snapshot time 之前：

- recurring row直接推进到第一个未来 occurrence，不生成 fire；
- `at` row直接删除，不生成 Session/message；
- 原有 Session/history保留。

运行中若一次 scan 看见多个 missed recurring boundaries，只 durable accept 最接近当前的
一个 due fire，然后推进到未来；其余不补。系统 clock 向后移动时不会提前 fire，最长
60 秒 rescan 后重新计算 sleep。

### 10.6 竞态

- fire vs PATCH：同 advisory/row lock 串行；若 fire 先 commit，PATCH只影响未来；若 PATCH
  先 commit，旧 occurrence 不执行；
- fire vs DELETE job：delete先则无 fire；fire先则已 accepted turn继续，delete/one-shot
  语义不回滚；
- fire vs DELETE Session：`ChatRuntime.session_operation(session_id)` 与相同 advisory key
  串行。结果只能是完整删除后 JIT 新建，或完整 fire 后再删除；不得留下 orphan pending；
- 两个并发 fire：只有一个能从同一 due row commit；
- browser POST vs missing reserved Session：public write fence拒绝。

### 10.7 全局锁顺序

所有使用同一稳定 UUID 的 transition遵循唯一顺序，禁止局部实现自行交换：

```text
ChatRuntime.session_operation(id)   # 仅会触碰/detach runtime Session 的路径
  -> pg_advisory_xact_lock(id)
    -> users row FOR KEY SHARE      # 创建 FK/inbound 的路径需要时
      -> cron_jobs row FOR UPDATE   # 路径需要时
        -> sessions row FOR UPDATE  # 路径需要时
          -> pending_messages / turn_runs rows
```

- Cron create：先生成 Server-owned UUID，取 advisory lock，锁 owner User `FOR KEY SHARE`，
  确认不与 User/Session/Job保留 identity冲突，再 insert；
- Cron PATCH/DELETE：advisory lock → CronJob row；不得先锁 row再取 advisory；
- Cron fire：session operation → advisory → owner User `FOR KEY SHARE` → CronJob row →
  Session/pending/turn；
- Session DELETE/cancel：session operation → advisory → Session/TurnRun rows；
- 缺失 Web Session create：session operation → advisory → owner User `FOR KEY SHARE` → 全局
  CronJob/User reservation fence → Session insert；
- User registration：Server-generated user UUID先取 advisory并确认没有同 UUID Session/Job，
  再 insert User，使 Heartbeat保留 identity从用户 commit起生效。

User deletion继续先锁 User `FOR UPDATE` 再 cascade；因为 fire/publish在取得任何
CronJob/Session row之前先拿 User `FOR KEY SHARE`，两者不会形成“User等 child、fire等
User”的 AB/BA。任何路径都不得在持有 DB row lock时再 await `session_operation` 或
advisory lock。并发测试用 barrier强制覆盖 AB/BA 时序，不能只靠正常执行“看起来没有
死锁”。

## 11. Heartbeat 文件与确定性预检

### 11.1 文件位置与读取

Heartbeat 唯一配置源是用户个人 Server Workspace 根目录：

```text
HEARTBEAT.md
```

- 不读取 Shared Workspace、Device Workspace 或 attachment；
- 通过现有 Workspace service 读取，不能绕过 owner、quota、object validation 或
  RustFS admission；
- 先 stat；文件不存在、不是 regular file、无法读取或 metadata size超过 128,000 bytes
  时直接跳过，不发起 object GET；
- size合格后使用新的 bounded Workspace read一次最多读取 128,001 bytes；发生并发增长、
  超界、非严格 UTF-8 或 decode 后超过 32,000 Unicode code points时跳过；
- 文件内容不复制到 DB，也不新增 heartbeat config row；
- 单个用户的读取失败不能取消本轮其它用户。

UTF-8 每个 Unicode code point最多 4 bytes，因此 128,000 bytes 是 32,000 code points 的
可证明读取上界；额外 1 byte只用于检测 stat 后的并发增长。32,000 是 Phase 1 的输入安全
上限，不是 Workspace 文件大小上限。过大文件仍可由用户在 Workspace 页面编辑或下载，
只是不会触发 Heartbeat。禁止先完整下载大 object再计数。

### 11.2 `## Active Tasks` 预检

Provider call 前使用普通代码扫描 Markdown：

1. 按行查找第一个 trim 后完全等于 `## Active Tasks` 的 heading；大小写敏感；HTML
   comment与 backtick/tilde fenced code block内的伪 heading忽略；
2. section 在下一个 fenced-code外的 level-1 或 level-2 ATX heading 前结束；若没有则到
   EOF；
3. 删除完整 HTML comment block 后 trim；
4. section 缺失、为空、只有 Markdown comment 或空白时直接 skip；
5. 不执行 Markdown、template、front matter、link 或 HTML 语义，不用正则从其它 heading
   猜测任务。

其它章节可给 Phase 1 提供背景，但不能单独通过预检。预检只判断“是否值得询问
Provider”，不自行理解日期、状态或任务条件。

前端创建缺失文件时建议以下最小模板；模板本身没有 active task，因此不会产生调用：

```markdown
# Heartbeat

## Active Tasks

<!-- Add recurring checks here. Use Cron for exact execution times. -->
```

## 12. Heartbeat Phase 1

### 12.1 Provider request

Phase 1 使用当前管理员配置的同一 Provider/model、既有 transient retry、共享 Provider
concurrency limiter 与 provider timeout。它不进入普通 Agent loop，不加载 SOUL、MEMORY、
Skills、MCP、聊天历史或 Workspace 其它文件，也不暴露普通 tools。

请求只包含：

- UTC RFC3339 current time；
- 按 `users.timezone` 转换的 local RFC3339 current time；
- IANA timezone 名称；
- bounded `HEARTBEAT.md` 全文；
- 固定的短 system instruction，说明精确时间任务应使用 Cron；
- 唯一 `heartbeat_decision` tool，并强制选择该 tool。

固定 instruction要求只返回“本轮现在应执行”的任务，最多 8 项，按文件优先级顺序，不得
把未来条件尚未满足的任务提前执行或自行发明任务。超过容量的复杂巡检应拆成 Cron/job或
精简 `HEARTBEAT.md`；Phase 1 不做多批补跑。

Anthropic-compatible lowering 的关键字段为：

```json
{
  "tools": [
    {
      "name": "heartbeat_decision",
      "input_schema": {
        "type": "object",
        "additionalProperties": false,
        "required": ["action", "tasks"],
        "properties": {
          "action": {"type": "string", "enum": ["skip", "run"]},
          "tasks": {
            "type": "array",
            "maxItems": 8,
            "items": {"type": "string", "minLength": 1, "maxLength": 500}
          }
        }
      }
    }
  ],
  "tool_choice": {"type": "tool", "name": "heartbeat_decision"}
}
```

Provider abstraction 增加可选 `tool_choice` 参数；普通 Agent call 继续传 `None`，不改变现有
请求。Phase 1 复用管理员配置的 `max_output_tokens`，不另加一个较小 hard cap；reasoning
model可能先生成较长 thinking，过小上限会把唯一 tool call截断并造成稳定误判。thinking/text
块允许 Provider返回但不参与解析。

Phase 1 通过 `ChatRuntime.evaluate_heartbeat_decision(...)` 这个 lifecycle-owned入口调用
Provider；该入口复用 ChatRuntime 的 provider cache、generation/fingerprint、shared limiter、
retry和 shutdown/close，不在每个 pulse新建 `AsyncAnthropic` client。它不是 Agent loop，
不得创建 runner state或 subscriber。Provider protocol只新增 optional `tool_choice`；
`max_output_tokens` 继续来自传入的 `ProviderConfig`。

该入口复用现有 request token estimator。若管理员配置了 `max_context_tokens`，且 Phase 1
system + message + tool schema + configured output headroom无法放入窗口，本轮直接 skip并记录
`context_limit` reason；不得截断 `HEARTBEAT.md` 后作出不同决定，也不得发送注定溢出的请求。

### 12.2 严格决策解析

成功结果必须同时满足：

- response 中恰好一个 `tool_use` block；
- 名称恰好是 `heartbeat_decision`；
- input 通过严格 schema/Pydantic validation，bool 不冒充 string/list；
- `action=skip` 时 `tasks=[]`；
- `action=run` 时 tasks 非空，逐项 trim 后仍非空且不重复；
- trim 后全部 tasks 合计不超过 2,000 Unicode code points。

以下统一视为 skip：Provider 最终错误、timeout、无 tool call、多个 tool call、错误名称、
JSON/schema 错误、额外字段、空 run tasks、超界或不支持 forced tool。不得从普通文本、
thinking 或 Markdown code fence 中正则提取 `run/skip`，也不得为格式错误立即再问一次。

Phase 1 只保留进程日志中的 outcome/reason code 和 latency；不保存原文件、tasks、Provider
原始响应或 token-level trace。

## 13. Heartbeat Phase 2

### 13.1 durable publish

严格 `run` 后，Server 再次取得该用户稳定 Heartbeat Session operation lock 并重验：

- 用户仍存在；
- Session 没有 running TurnRun 或 PendingMessage；
- Server 尚未进入 shutdown；
- Phase 1 使用的 user timezone 仍可解析。

若此时变 busy 或用户被删除，本轮 skip，不排队。否则 JIT create/validate稳定 Session，并
在一个 DB transaction 中写 synthetic PendingMessage、预留 running TurnRun 后 commit，
再在释放 session operation 前 cancellation-safe 地交给 `ChatRuntime.schedule()`。
commit/cancel/crash recovery 与 Cron 使用同一合同。

Phase 2 synthetic message只包含：

- Server-authored Heartbeat 标识；
- decision 时使用的 UTC/local time 与 IANA timezone；
- Phase 1 选择的 tasks，保持顺序。

它不复制整个 `HEARTBEAT.md`，也不携带附件、创建页面、reasoning effort 或 external
delivery target。Agent 如需核对最新规则，可以通过普通 Workspace tool重新读取文件。

### 13.2 Agent 行为与历史

- route 为 `channel=heartbeat`、`chat_id=<user UUID>`、`sender=partner:<user UUID>`、
  `trust=partner`；
- 使用与 owner Web turn 相同的 system prompt composition、完整 built-in/MCP/Device tools、
  compaction、context admission、timeout 与 cancellation；
- Phase 2 输出和 tool history进入 Heartbeat Session；Phase 1 不进入；
- 浏览器可查看、cancel 当前 turn、删除 Session，但不能向该 Session 发消息；
- 删除后只有下一次真实 Phase 2 才 JIT 重建，不因 list/history request创建空 Session；
- Py9 不自动把 final answer推到 Web、邮件或外部 channel。用户需要主动回看历史。

## 14. Heartbeat wall-clock pulse

### 14.1 边界计算

Pulse 使用 UTC wall clock，固定落在每小时 `:00:00` 与 `:30:00`：

- startup 计算严格晚于当前 instant 的下一个边界并等待；
- 恰好在边界之后启动也不补该边界；
- Server 离线或 event loop stall错过的边界不追赶；
- 每轮结束后从新的 UTC now 重算下一个严格未来边界，避免相对 sleep 漂移；
- wall clock 向前跳只会错过边界，向后跳不会重复已经开始的 pulse；
- 修改 `HEARTBEAT.md` 不唤醒 pulse。

实现注入 `Clock.now_utc()` 与可取消 wait abstraction；测试不得依赖接近 30 分钟边界的固定
sleep。

### 14.2 scan 与 fan-out

一个 process 只允许一个 pulse scan：

1. 边界到达时读取当时最大的 `(created_at, id)` 作为本轮非持久 upper bound，再按该 tuple
   keyset升序分页枚举用户，每页 100；新注册用户等待下一 pulse，删除不会造成 offset跳行；
   不一次把全部 User或 HEARTBEAT正文装入内存；
2. 上一 scan 仍存在则整个新边界 skip 并记录一条 aggregate log；
3. 用固定 32 个 worker运行完整 user pipeline：busy check → bounded stat/read → preflight
   → Phase 1 → 可选 durable Phase 2 publish；分页 producer和 worker之间使用 capacity 64 的
   bounded queue；
4. 对每个用户先检查 stable Heartbeat Session 是否 busy，busy 直接跳过 Phase 1；
5. Workspace读取继续经过既有 DB pool、object-storage admission和 RustFS connection pool；
6. eligible user 的 Phase 1 仍统一经过既有 Provider limiter。32-worker是 staging/memory/I/O
   上限，不是新的管理员 Provider quota，也不新增环境变量；
7. 每个用户单独捕获异常，不能 fail-fast 取消其它用户；
8. 成功 Phase 2 durable publish 后即结束该用户 scan，不 await Agent completion；
9. pulse shutdown 取消尚未开始的 Phase 1；已经 durable accepted 的 Phase 2 留给普通
   ChatRuntime shutdown/recovery。

单 worker 是 Py9 部署前提。若未来启用多个 ASGI worker，必须先增加 distributed leader
lease；Py9 不允许在多 worker 配置下声称 Heartbeat/Cron 只有一个 scheduler。

## 15. Agent、prompt 与 Session 合同

### 15.1 prompt 与 tools

- Web、Cron、Heartbeat 共用一个 system-prompt builder；route 事实从 Session 投影，不增加
  Cron/Heartbeat 专用 prompt 模板；
- runtime block准确说明当前 `channel/chat_id/session_id`，不能把自动化伪装成 Web；
- synthetic message 是 Server 代表 owner 生成的 trusted owner input，不做 untrusted
  external-sender wrapper；
- Cron/Heartbeat Phase 2 暴露 owner 正常可用的完整最终 tool list；
- 新 `cron` tool加入该完整列表，因此 owner 可以从 Web 或自动化 turn管理未来 job；
- tool authorization 继续来自 Server-side registry/context，不能由 runtime 文本决定；
- Heartbeat Phase 1 只有 `heartbeat_decision`，不复用普通 Agent tool registry。

Py9 的 `message` tool仍只有当前已实现的 Web/session delivery能力，不凭空获得 Discord、
DingTalk、email 或任意跨 Session 地址。自动化任务若要求尚不存在的外部通知，Agent 应在
历史中如实说明无法投递，不能声称已发送。该缺口由 Py10 channel adapter补齐。

### 15.2 自动化 Session

- Session list/API 返回真实 `channel`，前端以 Automation badge标识；
- 只读历史复用现有 `GET /api/sessions`、`GET /api/sessions/{id}/messages` pagination、
  cancel 与 delete；Py9 不新增单 Session GET或 SSE replay API；
- 打开正在执行的自动化历史时，前端沿用现有 messages status/pending轮询获取增量；
- `POST /api/sessions/{id}/messages` 对 Cron/Heartbeat 永远只读；
- read-only 只限制 human POST，不限制 Server synthetic publish 或 Agent tool result；
- cancel 只停止当前 turn，不暂停或删除 Cron job；删除运行中的 Session先按现有合同终止
  turn再删除历史，下一次正常 Cron/Heartbeat边界才可能重建；
- Cron job rename只更新 job name；已经存在的 Session title不被静默覆盖，JIT 新建时才使用
  最新 name作为默认 title；
- compaction只在各自 Session 内发生，删除历史后不保留旧 summary；
- 删除 user仍 cascade 删除其 Cron rows、自动化 Sessions和 Workspace。

## 16. Frontend 闭环

### 16.1 导航与 Automations 页面

登录用户侧栏增加 `Automations`，路由 `/automations`。页面只使用已定义 REST/Workspace
contract，不展示 Server 没有提供的 skipped count、running badge、倒计时或健康状态。

Cron 区域：

- list card显示 name、canonical schedule、timezone、`last_fired_at`、`next_fire_at`；
- 按 `next_offset` 显示 Load more；不在首屏偷偷截断旧 jobs；
- create/edit form提供 `Every / Cron / Once` 三种互斥输入；
- schedule preview由 Server response生成，Browser 不自行成为 authoritative parser；
- timezone默认预填 `/api/me.timezone`，用户仍可为 cron/once选择其它 IANA zone；
- delete confirmation明确“只停止未来触发，历史保留”；
- `session_id != null` 时显示 `View history`；为 null 时显示 `No runs yet`；
- 不提供 pause/resume、run now、duplicate、bulk delete、retry 或附件。

Heartbeat 区域：

- 通过个人 Server Workspace API读取/保存根目录 `HEARTBEAT.md`；
- 文件缺失时在编辑器中显示 §11.2 模板，但不因打开页面自动创建；
- 显示当前 account timezone 和前往 `/account` 的链接；
- 说明精确时间任务应使用 Cron，保存文件不会立即运行；
- `/api/me.id` 就是保留 Heartbeat Session ID；直接请求
  `GET /api/sessions/{me.id}/messages`，404时显示 `No heartbeat runs yet`，不得从分页
  Session list中碰运气查找；
- 不提供 enabled switch、run now、last decision、countdown 或 Provider raw output。

### 16.2 Account timezone

Account Preferences 增加 timezone字段：

- 显示已保存 IANA name；
- 浏览器可显示 `Use detected timezone: ...` 建议按钮；只有点击并保存才 PATCH；
- Browser 不支持 IANA list API 时仍可输入文本并由 Server验证；
- 保存失败保持用户输入并展示现有 Error envelope；
- 语言与主题的 browser-local偏好不受影响。

### 16.3 只读历史体验

进入 Cron/Heartbeat history复用 Chat 页面，但：

- 隐藏 composer、attachment、reasoning effort和 send shortcut；
- header显示 `Cron` 或 `Heartbeat` 来源及返回 Automations 的链接；
- 保留 refresh、rename、delete、work-details折叠和当前 turn cancel；
- 删除后回到 `/automations`，不存在的旧 URL显示正常 not-found state；
- 页面不能用 disabled textarea伪装为可发送，而应完全不渲染 composer。

新增所有 copy必须进入现有 i18n catalog，英文为第一版 source，简体中文完整覆盖。表单满足
现有 keyboard/focus/label 规则；时间同时提供机器可读 UTC `datetime` 与用户时区显示。

## 17. 错误、日志与资源边界

### 17.1 稳定错误

Cron REST 复用既有 Error envelope，并增加：

```text
cron_invalid_schedule          400
cron_job_not_found             404
timezone_invalid               400
```

向自动化 Session POST仍返回既有 `session_not_found` 404，不增加可枚举 route 类型的新错误。

同一 invalid timezone 在 `/api/me` 与 Cron使用 `timezone_invalid`。DB/Provider/Workspace
内部异常不把 stack、DSN、API key、文件内容或 job message返回给 caller。

Heartbeat 没有专用 REST execution endpoint，因此文件缺失、busy、Phase 1 invalid/provider
failure 都是一次 pulse 的 skip，不制造持久用户错误对象。编辑器保存仍展示 Workspace API 的
正常错误。

### 17.2 structured lifecycle logs

Cron 日志可包含：event、job_id、user_id、scheduled_at、accepted_at、next_fire_at、
skip_reason、session_id、latency_ms。Heartbeat 可包含：pulse boundary、eligible/user counts、
user_id、preflight/decision reason、phase latency和 durable publish结果。

日志不得包含：job message、`HEARTBEAT.md` 内容、Phase 1 tasks、Agent回复、附件、API key、
MCP env、SOUL/MEMORY 或完整 Provider body。重复 loop错误使用既有 bounded/rate-limited stderr
策略，避免 Provider outage 每半小时为每个用户刷无限 stack trace。

### 17.3 容量

- due query每批 100，稳定排序并在批间 yield；
- Cron Agent completion不阻塞 ticker；并发最终受 ChatRuntime、Provider limiter和既有
  per-user turn serialization约束；
- Pulse 不新增管理员并发配置；固定 32-worker保护 staging，shared Provider limiter仍是唯一
  可配置的 HTTP admission；
- `HEARTBEAT.md` 128,000 bytes/32,000 chars、tasks 8/2,000 chars、Cron message
  32,000 chars均在进入 Provider/DB transition前验证；
- 不新增环境变量。固定的 scan batch、rescan 上限和半小时 pulse作为代码常量并集中定义；
  Phase 1 output上限复用 Provider config。

## 18. TDD 与验收矩阵

实现必须先增加失败测试，再写生产代码。异步时间测试使用 injected fake clock、event和
monotonic deadline；不得用接近 TTL/边界的固定 sleep制造跨平台 flaky test。

### 18.1 schedule / timezone unit tests

- every min/max、bool、mixed forms、empty/null/extra fields；
- 五段 cron canonicalization、无未来 occurrence、禁止 alias/秒字段；
- aware/naive one-shot、user default tz、per-job override；
- DST nonexistent/ambiguous one-shot与 cron spring/fall行为；
- pinned croniter/tzdata版本、central wrapper与 Docker zone database golden test；
- recurring anchor在长任务、busy skip和 wall-clock jump后不漂移；
- `/api/me` timezone validation、default UTC与浏览器 suggestion不自动保存。

### 18.2 Cron API / tool / DB tests

- owner CRUD、跨用户 404、response projection、due index和 list/Agent tool pagination；
- create/update同一 shared parser，失败时无部分保存；
- delete job保留 Session/history/current turn；
- delete Session保留 job，下一 fire同 UUID JIT重建；
- one-shot acceptance删除 row并保留历史；missed startup one-shot不执行；
- `cron add/list/remove` schema、bounded output、self-remove和错误映射；
- user deletion cascade；不再存在 `cron_jobs.session_id` FK。

### 18.3 ticker / transaction / recovery tests

- commit前 crash不推进 schedule；commit后 schedule + PendingMessage + TurnRun同时可见；
- commit后、内存 schedule前 cancellation可由 startup recovery继续；
- running/pending busy skip不创建任何聊天 row且不更新 `last_fired_at`；
- 两 job并发、同 job双 fire、fire/PATCH、fire/delete job、fire/delete Session；
- barrier强制验证每条路径遵循 §10.7 中适用的锁序前缀；覆盖 fire/PATCH/DELETE、Cron
  fire vs self/admin user delete、Heartbeat Phase 2 vs user delete，确认无 AB/BA deadlock；
- create/update/delete wake不会 lost；批量超过 100 可继续且会 yield；
- restart不 catch up recurring；clock forward/backward与 shutdown顺序；
- normal Web runner回归、pending drain、compaction与 outcome-unknown测试保持通过。

### 18.4 Heartbeat tests

- missing/empty/comment-only/wrong heading/valid section预检；stat超界不得 GET，stat/read增长
  race、128,001-byte bounded read、invalid UTF-8、code-point超界和 read failure均 skip；
- Phase 1 exact provider body、forced tool、同 Provider limiter和 configured output cap；
- configured context不足时在 HTTP前 fail closed，不截断 heartbeat正文；
- valid skip/run；无调用、多调用、错误名称、额外字段、wrong types、重复/空/超界 tasks、
  Provider retry最终失败全部 fail closed；
- Phase 1 不写任何聊天/decision row，不加载 Agent prompt/tools；
- Heartbeat Session ID 等于 user ID，Session delete/recreate URL不变；
- Phase 1 后变 busy/user deleted/shutdown竞态不 publish；
- Phase 2 使用完整 owner Agent路径、只包含 selected tasks、历史隔离且可恢复；
- UTC `:00/:30` next-boundary、startup不立即跑、missed不补、scan不重入；
- user pagination、bounded queue和最多 32 条 pipeline；多用户单点失败不取消其它用户，
  busy用户在 Provider call前跳过。

### 18.5 Frontend tests

- route/nav、Cron paginated list/load-more/create/edit/delete、三类 schedule互斥和 API error；
- `View history`/`No runs yet`、删除 Session后的返回与只读 composer fence；
- Heartbeat missing-file template不自动 PUT、save/read失败、timezone link；
- Heartbeat history直接使用 `/api/me.id`，不依赖 paginated Session list；
- Account detected timezone必须显式点击并保存；
- 英文/中文、light/dark、keyboard focus与窄屏布局；
- Browser尝试直接 POST自动化 Session得到只读/not-found响应。

### 18.6 最终真实验收

在最终 Docker Server artifact + PostgreSQL + RustFS、真实 Browser frontend、真实本地
Anthropic-compatible Provider上至少验证：

1. 创建每分钟 Cron，按时运行并在只读历史产生 Agent结果；
2. 将同一 job任务保持超过下一 occurrence，确认该次被跳过且无 pending/message积压；
3. 删除历史后下一 fire同 URL重建；删除 job后历史仍可读且不再 fire；
4. recurring restart不补跑，one-shot restart不迟到运行；
5. Heartbeat有效任务在下一个 UTC 半小时边界进入 Phase 2，空任务不调用 Provider；
6. forced-tool malformed response fail closed且不产生 Session/message；
7. timezone/DST projection与 Account/Automations UI一致；
8. 普通 Web chat、attachment、Workspace、Device Client和 MCP smoke无回归。

CI 门禁：Server Python 3.12/3.13 suite、Ruff、strict mypy、frontend unit/build、Playwright、
Docker image build和现有 Linux Client tests。macOS/Windows Client不因 Py9改协议，无需新增
Py9-specific Client case，但既有矩阵继续运行。

## 19. 实现切片与提交顺序

Py9 后续编码按以下 TDD切片推进；每个切片独立测试并形成可 review commit：

1. schedule纯函数、User timezone、DB model与 API projection；
2. normalized inbound publish、reserved Session fence与 read-only Session；
3. Cron shared service、REST、Agent tool；
4. Cron ticker、atomic fire、startup recovery与 lifecycle；
5. Heartbeat preflight、Provider forced-tool支持与 strict parser；
6. Heartbeat pulse、Phase 2 publish与 stable Session；
7. Account + Automations + read-only history frontend；
8. OpenAPI/docs sync、Docker real acceptance与全量回归；
9. 静态审查、并发/取消审查和 against-main code review。

实现阶段允许切片分支/PR，但不得在 Py9未合并时并行实现依赖其 inbound contract的 Py10。

## 20. 文档收敛与完成定义

实现时同步修改：

- `docs/DECISIONS.md`：修订 ADR-005/053/054/092/112/113和 Python roadmap；
- `docs/SCHEMA.md`：最终 User/Cron schema、索引与删除语义；
- `docs/API.yaml` 与生成 OpenAPI：timezone、Cron CRUD/schedule/read-only Session；
- `docs/TOOLS.md`：`cron` 从 placeholder变为稳定 built-in schema；
- `docs/SYSTEM_PROMPT.md`：只写已实现的 Cron tool和自动化 route事实；
- frontend i18n/catalog与用户文档。

Py9 只有在以下条件全部满足时完成：

- Cron/Heartbeat 的稳定 Session、skip、delete、restart、timezone语义均有自动化测试；
- schedule 与 synthetic message可原子恢复，不存在已推进但未落消息的窗口；
- Heartbeat Phase 1 malformed/provider failure可靠 fail closed且没有副作用；
- Browser能完成 Cron CRUD、Heartbeat编辑、timezone设置和只读历史查看；
- 普通 Web/Workspace/Device/MCP行为无回归；
- final Docker artifact通过 §18.6真实验收；
- authoritative docs不再保留“Cron绑定创建聊天”“删除 job删除 history”“Heartbeat相对
  30 分钟”“直接运行完整 Agent”或 future placeholder等旧合同。

Py9 结束后再开始 Py10 channel adapter、Discord、DingTalk 与外部 `message` delivery设计。
本 spec 不预先固定 Py10 ingress/storage/auth schema。
