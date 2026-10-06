# Tray Client 独立审查与验收

审查日期：2026-10-06。分支：`refactor-tray-client`。原实现审查起点：`77e8dbf`。
本记录区分已验证行为与仍需实际桌面验收的部分。

## 审查修复

| 问题 | 修复与验证 |
| --- | --- |
| 停止清理失败后仍提交新配置、启动新核心 | 只有退出结果明确确认清理完成才替换；异常退出阻止本次会话再次启动。回归测试覆盖未完成清理和崩溃 |
| 凭据异步读取/保存完成后可能越过用户停止、退出或托盘消失 | 串行处理凭据操作，取消后丢弃迟到结果并删除未提交凭据；地址与 Token 使用同一配置快照 |
| 无托盘时保存会隐藏唯一窗口并继续后台连接 | 无托盘时保持可见入口、停止连接并阻止新连接，支持重新检测 |
| 核心尚未退出就显示停止成功；旧 generation 的退出消息可误确认清理 | 停止状态以进程退出及清理结果为准；核心控制器校验状态和退出消息的 generation |
| stdin 队列满时丢失消息和 EOF | 使用有界队列的背压，命令突发测试确认 EOF 仍触发完整停止 |
| GUI 启动预加载文档转换、MCP 和进程执行模块 | 内部模式按需导入；源码测试确认 GUI 不导入这些执行依赖 |
| 托盘强制覆盖 Server 的 Workspace 配置 | 恢复原有 `workspace_path` 配置和更新契约，移除私有管道中的覆盖项 |
| Windows 图形入口与需要标准流的核心共用 windowed 可执行文件 | 打包独立 GUI/core 入口并共用依赖；核心、转换和进程执行保留管道并隐藏控制台 |
| Windows 安装器默认开启自启，使用了与 GUI 不同的 Run 项名称，没有开始菜单入口 | 自启默认关闭，安装快捷方式；卸载清理 GUI 使用的同名自启项，运行中要求先退出 |
| macOS 的 `CFBundleExecutable` 指向目录，两个架构的 DMG 同名 | 交由 PyInstaller 创建 `.app`，DMG 保留应用结构并使用架构后缀 |
| 自启的路径转义和 macOS 关闭行为有误 | XDG 使用 Desktop Entry 参数编码，macOS 使用 plist 序列化并只更改下次登录登记，Windows 保存完整命令 |
| 自定义陈旧锁代码错误解析 Qt 锁格式，Windows 存活判断反向 | 使用 Qt 的长生命周期锁与内建进程存活检测，本地 socket 限制为当前用户 |
| Release 的 Bash `case` 缺少引号，Windows NSIS 安装命令依赖未定义函数 | 修复脚本，增加原生安装/卸载及 DMG 内程序烟测；所有 Bash 步骤通过语法检查 |

Windows 标准流行为参见 [PyInstaller 说明](https://pyinstaller.org/en/stable/common-issues-and-pitfalls.html#sys-stdin-sys-stdout-and-sys-stderr-in-noconsole-windowed-applications-windows-only)；
长期锁的行为参见 [Qt QLockFile](https://doc.qt.io/qt-6/qlockfile.html)。

## 本机已验证

- Client：720 passed、29 skipped；Ruff 和严格 mypy 通过。
- 冻结包：PDF/DOCX/XLSX/PPTX/HTML 转换、损坏/超限输入、exec/PTY、stdio MCP、核心管道和托盘单实例烟测通过。
- 真实 PostgreSQL + Server + 冻结核心：设备生命周期、聊天工具路由、文件中继/跨设备传输、exec/PTY、MCP 各入口、RustFS 目录传输，8 个 E2E 通过。
- Qwen 3.5 4B：实际模型普通聊天通过；另一个完整烟测经 Server 调用冻结客户端读取随机标记、转换 PDF、写回标记，通过。测试使用临时数据库和 Workspace。
- 原生 GNOME 托盘：使用当前源码 GUI 和冻结核心，实际点击设置窗口的保存按钮；系统凭据库往返、首次不可达重连、第二实例唤起、XDG 自启文件开关、停止并回收核心均通过。凭据条目和临时文件在结束后清理。
- `.deb` 构建成功。开发机上原先安装的旧构建未被覆盖。

原生 GNOME 检查使用临时自启目录，没有执行真实退出登录。CI 的 offscreen 检查也不替代桌面手动验收。

## 下一步原生桌面验收

Windows/macOS 的真实桌面验收仍待完成，即使 CI 安装器和冻结程序检查通过，也需检查：

1. 安装后由开始菜单或 Applications 启动；托盘/菜单栏可用，不闪控制台。
2. 保存 Server address 与 Token，重启应用后仍能连接；打开网页交给默认浏览器。
3. 首次不可达重连、Token 撤销、修改配置、重复启动、停止与退出；确认没有残留任务进程。
4. 本地 PDF 转换、文件读写、exec/PTY（Windows 为 ConPTY）、MCP、传输。
5. 自启默认关闭，开启后真实重新登录启动，关闭后重新登录不启动；升级和卸载。

用户审阅并决定合并；本分支不自动合并。
