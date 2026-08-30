# OpenOctopus 浏览器前端设计

日期：2026-08-26  
状态：接受，进入实现

## 1. 目标

为现有 Python Server 提供可公开演示、可日常使用的浏览器前端。前端覆盖已经实现的浏览器 API：

- 注册、登录、退出与账户管理；
- 会话列表、改名、删除、聊天、停止当前 turn 与浏览器附件；
- 个人/共享 Workspace、成员管理、文件浏览、上传、下载与文本编辑；
- 设备配对、配置、网络策略与 Device MCP；
- 管理员 Provider、Workspace 配额、Server Web Fetch、用户与共享 MCP 配置。

界面沿用已确认的“清爽 AI SaaS”视觉：浅色使用淡蓝背景，同时提供深色和跟随系统主题。

## 2. 不在本次范围

- 尚未接入 Python router 的 Channel、Cron 等文档预留 API；
- 会话摘要、副标题、单一设备归属、分享链接等 Server 未提供的数据或功能；
- Workspace 变更推送、协同编辑、Range 下载、二进制文件在线编辑；
- 前端独立账户体系、JWT 本地存储、浏览器连接 Device WebSocket；
- 像素截图回归、SSR、PWA、离线缓存与独立前端部署服务。

## 3. 工程结构与依赖边界

新增顶层 `frontend/`，使用 React、TypeScript 与 Vite。依赖保持在以下职责内：

- React Router：浏览器路由和登录/管理员访问边界；
- TanStack Query：Server 状态缓存、失效和轮询；
- `i18next` + `react-i18next`：集中管理用户可见文案与语言切换；
- `openapi-typescript`：从 `docs/API.yaml` 生成只包含类型的 API 契约；
- `react-markdown` + GFM：安全展示 Agent Markdown；不启用原始 HTML；
- Vitest、Testing Library、MSW：组件、API 和流式状态测试；
- Playwright：真实浏览器核心流程测试。

不引入完整生成式 SDK、全局状态框架或组件库。前端保留一个小型同源 `fetch` 封装，负责 JSON/二进制/NDJSON、稳定错误码以及请求取消。主题等设备本地偏好写入 `localStorage`；身份、JWT 和业务数据不写入浏览器存储。

## 4. 部署与路由

### 4.1 开发

Vite 开发服务器代理 `/api` 和 `/health` 到 `127.0.0.1:8080`。浏览器仍只访问一个来源，因此 HttpOnly Cookie 行为与生产一致。

### 4.2 生产

Server Docker 镜像通过多阶段构建生成 `frontend/dist`。FastAPI 在所有 API、健康检查和文档路由之后托管 `/assets`。现有 404 handler 只在 `GET + Accept: text/html + 非保留路径 + index.html 存在` 时返回 SPA；其他请求继续使用稳定 JSON 404/405。这支持正常的 History API URL，同时避免全局 catch-all 改变未知请求的路由契约。

`/api/*`、`/assets/*`、`/health`、`/docs`、`/redoc`、`/openapi.json` 和 `/ws/*` 永远不进入 SPA fallback。缺失前端构建不会影响 API-only 测试和源代码开发；生产镜像构建门禁保证前端资源存在。

## 5. 身份与全局导航

应用启动先请求 `GET /api/me`：

- `200`：进入应用；`is_admin` 决定是否渲染管理入口；
- `401`：进入登录页；
- 其他错误：展示可重试的错误状态，不伪装成未登录。

注册和登录只依赖 Server 设置的 HttpOnly Cookie。注册页中的 Admin Token 是可选字段；Server 不会因 Token 不匹配而拒绝普通注册，因此前端始终以响应中的 `is_admin` 为准，并在用户填写 Token 但得到普通账户时明确提示。退出后清空内存缓存并回到登录页。账户删除成功后执行同样清理。

桌面端使用固定侧栏，窄屏使用顶部导航。主题支持 `auto`、`light`、`dark` 三态；`auto` 响应 `prefers-color-scheme` 的实时变化。第一版默认英文，同时提供英文/简体中文切换。显式选择保存在 `localStorage`，不存储身份或业务数据；所有用户可见文案通过集中资源表渲染，便于后续增加语言包。

## 6. 会话与聊天

### 6.1 会话

会话列表只展示 `GET /api/sessions` 实际返回的标题和时间。时间使用 `last_inbound_at` 表达“最近提问”；不显示设备名称。标题不会由 LLM 自动生成，初始值是 `New chat`，只通过用户操作改名。新会话在浏览器生成 UUID，直到首次发送消息时由 `POST /api/sessions/{session_id}/messages` 原子创建。

改名、已读时间和删除使用现有 session 元数据 API。非 Web 会话可以查看和管理元数据，但 Composer 必须禁用。删除当前会话后切换到下一条会话或新的本地草稿。

### 6.2 发送与恢复

`POST messages` 的 NDJSON 响应只作为当前连接的即时预览。前端逐行解析完整 JSON 记录，不能把分块边界当作记录边界；不依赖 keepalive，也不为安静的 Provider 设置短 idle timeout。`message_persisted` 按消息 ID upsert，`stream_replaced` 是排队订阅者被更新连接接管的正常结果，不等同于 Agent 失败。

连接中断不取消 Agent turn，也不自动重发 POST。若在收到 `message_accepted` 前断线，请求可能已经持久化；前端先 GET 恢复，并把结果显示为不确定状态，不能用自动重试制造重复消息。

持久消息以 `GET messages` 为准。前端在以下时机重新获取：

- POST 流结束；
- 页面重新获得可见性；
- 当前会话状态为 queued/running 时按有界间隔轮询；
- 用户显式刷新。

临时 token、thinking 和工具进度以 `turn_id`/消息 ID 与持久结果协调；一旦 canonical 消息出现即移除对应预览。停止按钮调用 cancel API，仅停止当前 Agent turn，不推断或终止 Client exec session。

### 6.3 附件

附件选择器提供三种来源：

- 当前浏览器所在电脑：浏览器文件选择器读取本机文件，先通过 Workspace Files `PUT` 写入个人 Server Workspace 的 `.attachments/uploads/{uuid}/{safe_filename}`，新建时发送 `If-None-Match: *`；只有所有文件上传成功后才调用消息 API；
- Server/共享 Workspace：使用 OpenOctopus 文件选择界面选择已有路径，直接发送 `{openoctopus_device: "server", path}`，不复制文件；
- 已连接 Client：通过 `GET /api/workspace/list/{path}` 浏览远程文件，每次请求同时发送设备名称与 `openoctopus_device_id`；选中后发送 `{openoctopus_device, device_id, path}`，不把 Client 文件上传到 Server Workspace。

Client 引用只表示当前在线文件。Server 在消息接收时验证用户、设备名称和不可变 UUID，但不读取 Client 字节；Agent 后续通过该设备的 `read_file` 操作文件。消息排队或 Server 重启后，持久化的引用继续以名称级 UUID fence 约束路由、Device MCP 与 System Prompt：名称被新设备复用时不允许读到或暴露替代设备；同名出现多个历史 UUID 时该名称暂时不可调用。Compaction 摘要不继承可操作附件引用。

每条消息遵守 Server 的 10 个附件和 Server 图片总量限制。浏览器本机文件上传成功但消息发送失败时保留文件，用户可以重试；前端不偷偷删除用户 Workspace 内容。`attachment_refs` 用于恢复附件 UI，不与 `message` 工具产生的 `delivery_refs` 混用。

## 7. Workspace

Workspace 页面由三个位置组构成：个人 Server Workspace、共享 Workspace、在线设备。Server/共享文件使用现有 REST 文件 API；设备文件通过相同 API 和明确的 `openoctopus_device` 路由。

- 目录列表分页加载；API 没有 MIME 和修改时间，扩展名类型必须标注为前端推断；
- 下载直接消费原始响应；
- 文本编辑只对可安全解码且在前端大小上限内的内容开放；保存使用读取到的 ETag 作为 `If-Match`，冲突要求重新加载；
- 上传是完整内容写入；大文件使用浏览器流式请求能力，不在 React state 中复制一份；
- `SOUL.md`、`MEMORY.md`、`skills/` 和 `.attachments/` 只做视觉强调，不改变后端权限或路径语义；
- 共享 Workspace 成员权限相同。成员页只提供添加、移除和退出，不创造 owner/editor 角色；
- Workspace 及成员变更后精确失效相关 Query；不增加 Server push。

## 8. 设备与 MCP

设备列表仅展示 API 返回的名称、在线状态、Token hint 和配置摘要。创建/轮换 Token 后使用一次性确认层展示完整 Token，离开后不可恢复。

设备配置按 Server 契约整体编辑：详情页先从设备列表取得设备字段，再从 config API 取得配置；保存时携带 `base_config_revision`，成功后同时重新获取两者。Workspace 路径提供 Linux/macOS 与 Windows 示例，`sandbox_mode` 明确是 Workspace 路径和初始工作目录限制，不是 OS jail；exec/PTY 网络默认开放。保存配置或 MCP 必须在设备在线并完成真实验证后报告成功。

共享 MCP 仅在管理员页面管理；Device MCP 仅在对应设备页面管理。两者的保存都是带 revision 的完整列表替换，不伪装成互相独立的单项 CRUD。能力选择使用“全部启用 / 全部禁用 / 精确选择”的产品语言，底层严格映射 `null = 全部禁用`、`[] = 全部启用`、非空数组 = 精确选择，不向用户暴露实现值。Server MCP 页面展示配置、最后成功 catalog 和进程内运行状态，不把它描述为长期远程健康监控；Device MCP 不伪造每个服务的 runtime 或队列状态。

## 9. 管理页面

管理入口及页面组件只对 `is_admin=true` 渲染；Server 仍是最终授权边界。配置页面按 Provider、Workspace 配额和 Server Web Fetch 分模块提交现有 PATCH 字段，不提供原始 JSON 编辑器。

API Key 等写入后不可读的 Secret 使用“保持现有值 / 输入新值”的控件，不回填伪造值；保留现有值时省略字段，不提交 `null` 或脱敏占位符。用户管理页使用真实用户字段，列表计数只能表示“本页”，不能伪装成全局总数；`workspace_locked` 解释为超配额写锁而不是账户封禁，并展示最后一名管理员不可删除的错误。

## 10. 错误、加载与可访问性

- API 错误统一展示 Server 的稳定 `code` 和面向用户的说明；字段验证错误贴近输入框；
- 首次加载、空状态、后台刷新和提交状态分开表达；后台刷新不清空现有页面；
- 所有写操作防止重复提交，删除/轮换 Token 使用明确确认；
- 导航、对话框、菜单、表格和文件列表支持键盘操作及可见焦点；
- 颜色不是唯一状态信号；浅色、深色均满足正文和交互控件对比度；
- `prefers-reduced-motion` 下关闭非必要动画。

## 11. 测试与合并门禁

### 11.1 前端 CI

Linux + 当前 Node LTS：

1. `npm ci`；
2. OpenAPI 类型重新生成后工作树无差异；
3. ESLint 与 TypeScript strict；
4. Vitest；
5. Vite production build；
6. Playwright Chromium 核心流程。

组件测试覆盖认证路由、管理员可见性、主题、停止当前 turn、NDJSON 跨 chunk 解析、流断开后的 canonical 恢复、文件 ETag 冲突、一次性 Token 和稳定错误码。Playwright 覆盖注册与登录、发送、附件、Workspace 文本编辑、设备创建和管理员配置；测试使用真实 FastAPI/PostgreSQL/RustFS，Provider 使用仓库内可控测试服务。

前端产物是平台无关浏览器资源，不重复在 Windows/macOS CI 跑相同浏览器矩阵，也不做像素快照。

### 11.2 Server 回归

Server 测试覆盖 SPA fallback 和保留路径：API/健康检查/OpenAPI 不被静态路由吞掉，已存在资源使用正确 MIME，未知浏览器路由返回 `index.html`，未知 `/api/*` 保持 JSON 404。

## 12. 实现顺序

1. 工程骨架、OpenAPI 类型、主题、API 错误和认证路由；
2. 全局导航、会话列表、聊天流与附件；
3. Workspace 浏览/成员/文本编辑；
4. 设备、Device MCP、管理员配置/用户/Server MCP；
5. FastAPI 静态托管、Docker、CI 和真实浏览器 smoke；
6. 静态审查、完整测试、PR。
