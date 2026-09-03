# Py10 Channels 验收记录

日期：2026-09-03
分支：`py10-impl`（未提交工作树）

本文只记录验收结果，不记录平台令牌、配对码、用户 ID、服务器 ID 或聊天 ID。

## 自动验收

- Server：`2268 passed, 17 skipped`；Ruff 与 strict mypy 通过。
- Frontend：17 个测试文件、171 项测试通过；TypeScript、ESLint 与生产构建通过。
- 500 与 1,000 idle Adapter 容量 harness 均通过；结果见
  `server/scripts/capacity-results/2026-09-02-500.json` 与
  `server/scripts/capacity-results/2026-09-02-1000.json`。
- replacement 回归确认旧 external Turn 的 omitted/explicit/cross-channel target 均在
  resolver 和每个 action 的 issue boundary 复验来源 binding，不能转投新 Bot；
- startup 回归确认 deleted/replaced binding 的旧 Pending 会收敛为 revoked terminal，
  当前有效 Pending 不会在 Adapter ready 前启动。
- Discord Gateway 使用 `discord.py==2.7.1` 的公共 ready 状态；timeout/cancel 会先取消并等待
  ready waiter，再关闭 Client。冷启动不再注册可能在关闭后晚到的 `on_ready` handler。

## Discord 真实平台验收

通过：

- 浏览器保存凭据、Gateway 连接、状态展示和主人 DM 精确配对；
- 主人 DM 文本触发、结构化群聊 mention 触发，以及未 mention 群消息忽略；
- 群聊 backfill 使用当时可取得的全部 43 条历史，并在浏览器历史中折叠展示；
- 4,271 字符回复由 Adapter 发送为 3 个有序文本 action，内容首尾完整；
- 主人入站附件写入 Server Workspace，Agent 可读取内容；
- `message` 从 Server Workspace 向 Discord 发送文件；
- `message` 从在线 Client `client-relay` 读取文件并直传 Discord；文本 action 与
  `file_message` action 均为 `sent`，Discord 附件内容与 Client 原文件一致，Server 对象存储
  仍只有 `SOUL.md` 与 `MEMORY.md`，没有生成该文件的 durable copy；
- 25 MiB + 1 byte 文件在平台 issue 前稳定失败：文本 action 已发送、文件 action 失败、
  同 Turn final 被阻止；Server 不自动重试，下一条用户消息开启的新 Turn 可正常发送；
- 后端重启后配置、配对和会话恢复，durable message/delivery/action 计数不增加，未重放；
- allow list 更新不改变 binding generation，更新后 Bot 仍可立即收发；非数字渠道用户 ID
  被前端和 Server 拒绝，旧配置不变；
- OpenOctopus 浏览器历史为只读，展示渠道、sender authority、平台用户 ID、群聊背景折叠、
  delivery 状态与失败后的新消息提示；
- 删除配置后 Bot 停止接收，历史仍保留；数据库 message/delivery 计数不再增加。

未完成的真实平台边界：

- 测试服务器当时只有 43 条可用历史，因此真实平台未达到 100 条 backfill；100 条上限、
  64 KiB 裁剪和去重由自动测试覆盖；
- 没有第二个人类 Discord 账号，因此真实平台未执行 allowed/unauthorized 用户消息和
  non-owner 附件拒绝；这些硬边界由 fake Adapter 与 ingress/dispatch 测试覆盖；
- Discord thread 与 token rotation 未做真实平台验收；
- 钉钉真实 E2E 仍为 `blocked by credentials`。

## 清理

- Discord 测试配置已删除，令牌不再保存在临时数据库中；Bot 仍安装在测试服务器中但离线；
- 临时数据库、临时账号对象存储前缀和本地测试附件已删除；
- 裸跑 Server、Vite 与打包 Client 已停止；本次创建的 `postgres:latest` 和
  `rustfs/rustfs:latest` 容器已删除，镜像保留。
