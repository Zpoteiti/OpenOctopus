# Py8c 递归目录传输设计

**状态：** accepted；在 Py8b 合并后开始实现
**Milestone：** Py8c recursive directory transfer
**依赖：** 已完成的 Py5/Py6 单文件传输，以及 Py8b 不同 Client 之间的单文件
bridge
**目标协议：** Protocol v3，不升版

本文实现 ADR-087 的递归目录语义，并按 ADR-088 保持“空目录不是 Server
workspace 一等对象”的存储模型。实现完成时必须同步更新 `docs/TOOLS.md`、
`docs/PROTOCOL.md`、`docs/API.yaml`、`docs/DECISIONS.md` 和相关契约 fixtures；本文
本身不是这些 canonical contract 的替代品。

Py8c 显式依赖 Py8b。不同 Client 之间的目录传输不是第二套 relay：Server 先完成
一次有界目录 manifest/preflight，再按 manifest 顺序反复调用 Py8b 已验收的单文件
bridge。Py8b 未合并、不同 Client 单文件 bridge 尚未稳定前，不开始 Py8c 实现。

## 1. 结果与边界

Py8c 继续使用唯一的 Server-owned `file_transfer` 工具和
`POST /api/workspace/transfer`：

```text
source 是 regular file -> 现有单文件路径
source 是 directory    -> 本文递归目录路径
其它 source kind       -> 整次调用失败
```

Agent 不传 `recursive`，也不选择另一个工具。Source install site 在 no-follow
检查下识别 source kind；目录 source 的所有普通文件映射到 `dst_path` 下相同的相对
路径。四种方向都支持：

```text
server -> server
server -> client
client -> server
client A -> client B
```

同一 Client 同时作为 source/destination 时仍走一个私有、generation-bound local
directory job，不把字节绕经Server。目录 `move` 在同一 Client、同一filesystem/volume
且平台提供已证明的
exclusive no-replace primitive 时，是一次原子的目录项 rename。`server -> server`
底层是 RustFS/S3 object prefix，不存在目录项 rename；它即使两个路由字段都叫
`server`，也必须走 manifest、逐对象 copy、全部成功后 delete 的对象存储路径，不能
虚构 prefix rename 的原子性。

Manifest/copy 路径只传 regular files，不重建空子目录；完全不含 regular file 的 source
拒绝。Same-Client 原子 rename 是唯一结构保持例外：source 至少含一个 regular file 时，
rename 原样保留其中的空子目录，因为删除它们会破坏单次 rename 的原子语义。

所有非 rename 路径只保证：

- 完整、有界的 preflight 在第一个 destination commit 前完成；
- 每个普通文件独立临时写入、校验并原子 no-replace 发布；
- copy 阶段失败时精确、best-effort 清理本次调用已经创建的 destination entries；
- 跨 site 不提供整棵树的原子 snapshot、原子 publish 或事务 rollback。

目录传输不打 tar/ZIP，不引入 Server durable cache，也不把整棵树或单个大文件读入
内存。

## 2. 已确定的决策

1. **仍然只有 `file_transfer`。** 参数保持
   `openoctopus_src_device`、`src_path`、`openoctopus_dst_device`、`dst_path`、
   `mode`；不增加 `recursive`、`source_kind`、`overwrite` 或新工具。
2. **Source kind 自动识别。** regular file 使用 Py8b 完成后的单文件实现；directory
   使用 Py8c。Symlink、junction、其它 reparse point、FIFO、socket、device 和其它
   special file 全部拒绝。
3. **四方向统一。** 不同 Client 的每个文件复用 Py8b bridge；Server 不增加第二套
   client-to-client byte pump。
4. **目标根必须不存在。** 文件或目录、空或非空都算存在；没有 overwrite flag，也
   不 merge 到现有目录。Filesystem destination 在 copy 前用 exclusive mkdir 原子占有
   root并持有 subtree reservation；Server object prefix 在 root-absence recheck 前取得
   process-local subtree lease。每个最终文件仍用 no-replace commit 防御其它 race。
5. **映射的是 root contents。** 若 `src_path="pkg"` 中有 `lib/a.py`，且
   `dst_path="backup"`，最终路径是 `backup/lib/a.py`，不会再追加一层 `pkg`。
6. **包含全部普通文件。** dotfiles、`.git`、`node_modules`、`__pycache__` 和其它
   list/find 的 noise directory 都不忽略。递归传输是精确 tree operation，不应用
   discovery ignore rules。
7. **任何 link/special entry 使整次 preflight 失败。** 不 follow、不复制 link
   本身、不静默 skip。规则不随 Client `restrict_to_workspace` 改变。
8. **Manifest/copy 不保留空目录。** Manifest 用 scan-only `directories` 记录所有真实
   子目录（包括 empty dirs），只用于 bounds、tree digest、revalidation 和 move cleanup；
   它们不映射成 copy destination entries。完全不含 regular file 的 source directory 以
   `workspace_invalid_request` 拒绝。Same-Client 原子 move 是唯一例外：合格的非空
   source 用一次 rename 原样保留空子目录，不把 rename 拆成 copy/delete。
9. **Manifest 完整且有硬上限。** 最多扫描 10,000 个 source entries，canonical
   encoded manifest 最多 5 MiB。任一上限超过即整体返回
   `workspace_directory_too_large`，不截断、不分页后继续、不转成 partial transfer。
10. **Manifest 是 immutable plan。** 每个 file entry 包含 canonical relative path、
    byte size 和 opaque source fingerprint/ETag；filesystem source root 与每个 scan-only
    directory 也携带 opaque identity。Directory entries 与 file entries 共同形成确定顺序
    和 manifest digest。
11. **一致性是 manifest snapshot，不是 filesystem snapshot。** Manifest/copy 已列项在
    open/stream 时必须仍匹配 fingerprint；漂移使 copy 失败，manifest 完成后新加入的
    entry 不参与 copy。Same-Client atomic rename 在 syscall 前做一次 bounded no-follow
    full revalidation；OpenOctopus 自身写入由 subtree lock 排除，但不承诺阻止宿主外部
    进程在 revalidation 与 syscall 之间竞态。
12. **所有 destination preflight 先于写入。** 完整 path mapping、平台可表示性、
    destination root absence、内部 collision、Server quota/soft-lock/single-operation
    limit 和所有目标 SKILL.md validation 全部先完成；随后才原子占有 filesystem root或
    在 subtree lease 下重查 Server prefix。
13. **Server quota 按目录总字节。** Folder sum 是 ADR-078 single-operation size；
    目标 Server workspace 在第一个文件发布前完成一次原子 quota reservation，之后
    每个 child commit 消耗该 reservation。
14. **逐文件原子。** 文件先写临时 destination，核对 byte count/SHA-256、flush/fsync
    或完成 RustFS upload，再 no-replace publish。失败文件本身从不暴露 partial bytes。
15. **Copy failure 做精确 best-effort cleanup。** 只按本次调用记录的 destination
    path + committed fingerprint/ETag 条件删除，绝不递归删除一个可能已被其它写者
    修改的未知 tree。
16. **Cleanup 结果决定错误语义。** 若确认所有已创建 entry 和空 parent 已清除，返回
    原始 transfer error；若不能确认 destination 已恢复为 absent，返回
    `tool_execution_outcome_unknown`，禁止自动 retry。
17. **Move 是 copy-all-then-delete。** Cross-site、Server object-store 和同一 Client
    非 rename 路径都必须先完整复制整棵 manifest tree；copy 中途绝不删除 source。
18. **Source cleanup 是条件删除。** Copy 全部成功后，按 manifest fingerprint 删除
    source files，再 deepest-first 删除空目录。删除失败不删除完整 destination，调用
    返回成功并带聚合 warning；任意时刻都不会出现 source/destination 两边都没有该
    文件的状态。
19. **同一 Client folder move 使用原子 rename。** 必须持有 source/destination
    subtree locks、完成 manifest/security/bounds/destination preflight，再调用平台
    exclusive no-replace directory rename。跨 volume 或缺少可靠 primitive 时明确失败，
    不静默 fallback 为 copy/delete；成功 rename 保留 source 的全部目录结构，包括空
    子目录。
20. **结果统一且只聚合。** Py8c 把 regular-file 与 directory success 统一为
    `kind`、`files_transferred`、`bytes_transferred`、`sha256`、`warnings`；file 的 count
    固定为 1，directory 为 manifest file count。仍只返回总字节、digest 和最多 8 个
    symbolic warnings，不返回 10,000 项逐文件数组，也不把失败路径列表塞进 Agent result。
21. **Protocol v3 不变。** 不增加公开 frame、binary header 或 capability bit；目录
    control 使用现有 `tool_call/tool_result` 中的私有 workspace actions，文件字节使用
    现有单文件 slot。
22. **不增加新 deployment config。** 复用现有 device/server transfer admission、
    queue timeout、idle timeout、object IO limits 和 pending-call byte admission。

## 3. 范围

### 3.1 包含

- `file_transfer` 对 regular file/directory 的 no-follow 自动识别；
- Server workspace、同一 Client、两个不同 Client 的递归 copy/move；
- deterministic bounded manifest、tree digest 和 destination mapping；
- destination path representability/collision/root-absence preflight；
- Client symlink/junction/reparse/special-file 整体拒绝；
- dotfiles/noise directory 的完整遍历；
- Server destination aggregate quota reservation；
- personal Server workspace 下 `skills/*/SKILL.md` 的全量 prevalidation；
- 逐文件原子 commit、source drift 检查和 SHA-256 verification；
- copy failure 的 conditional destination cleanup；
- move copy-all-then-conditional-source-delete；
- same-Client directory move 的 native atomic rename；
- fair bounded admission、Stop/cancellation、disconnect/replacement 和 late-frame 语义；
- Agent tool 与 REST 的 bounded aggregate result；
- Linux、macOS、Windows unit/native tests，以及真实 RustFS + 两 Client E2E。

### 3.2 不包含

- overwrite、merge-into-existing-directory、rename-on-conflict；
- manifest/copy 的 preserve-empty-directory、directory marker objects 或公开 `mkdir`
  工具；same-Client atomic rename 原样保留 source tree，不属于重建 empty dirs；
- symlink/junction/reparse-point copy、dereference 或 allow-inside-workspace 例外；
- copy 路径的 ownership、mode bits、ACL、xattr、resource fork、creation time、mtime
  preservation；same-Client native rename 因目录项移动而附带保留这些 metadata，但该附带
  行为不形成 copy/cross-site preservation contract；
- copy 路径的 sparse-file、hardlink identity、reflink 或 clone preservation；same-Client
  native rename 的附带保留同样不形成跨路径 contract；
- archive/tar/ZIP packaging、compression；
- range、resume、checkpoint、断点续传；
- dedup、content-addressed cache、delta sync；
- directory watch、live mirror 或 manifest 后新增 entry 的追赶；
- 跨 site point-in-time filesystem snapshot、whole-tree atomic visibility 或 durable
  transaction log；
- same-Client rename 对宿主外部进程的 filesystem freeze；rename 前 revalidation 与
  syscall 之间仍是明确的 OS race boundary；
- 多 worker/multi-node transfer ownership、Server restart 后 resume；
- 公开/持久化 directory job、前端 job API 或 progress tree；实现可以使用仅Client
  进程内、generation-bound的私有source/destination/local jobs与有界polling/paging；
- Server object prefix 的虚假 atomic rename。

## 4. 不可破坏的不变量

1. Provider-visible surface 仍然只有 `file_transfer`；source 是文件还是目录由执行端
   检测，Provider 不提供或猜测 `recursive`。
2. Source/destination 必须属于当前 user 已冻结的 install-site routing snapshot；
   device rename/delete/reuse 不能把在途 tree 转到另一个 device identity。
3. 完整 manifest、全部 path/quota/skill validation 成功前，destination 不出现任何
   user-visible entry。Filesystem root reservation 是验证完成后的第一个可见 mutation；
   Server object path 的第一个 child publish 才是第一个可见 mutation。
4. Manifest 超过 entry/byte bound 时整体失败；Server/Client 都不得返回 partial
   manifest，Server 不得接受 `truncated=true` 的普通 list result 作为 transfer plan。
5. 递归 walk 检查每个 entry 的 no-follow kind。遇到 link/reparse/special file 必须
   整体失败；静默 skip 会把“复制目录”降级成不可见的数据丢失，因此禁止。
6. Manifest file/directory relative paths 各自唯一、合并后严格排序、无冲突，也没有
   absolute/empty/`.`/`..` component；
   Server 不信任 Client 返回的 path、size、fingerprint、count 或 digest，必须重新
   validate 并重算 digest。
7. Destination root 在 preflight 时必须 absent；每个 child publish 再做 no-replace
   检查。Filesystem copy 还必须在首个 child 前 exclusive mkdir root；Server prefix
   必须在覆盖该 subtree 的 operation lease 下完成最终 absence check。Preflight 不是
   overwrite authorization。
8. 一个 directory operation 同时最多有一个 child file slot；不能为 10,000 个文件
   创建 10,000 个 asyncio task、writer lane、临时文件或 transfer waiter。
9. 每个 child file 都沿用单文件 byte count/SHA-256 双端校验；tree digest 不能替代
   每文件完整性校验。
10. Copy 阶段任何失败、Stop 或 cancellation 都不能删除 source。Destination cleanup
    只能条件删除本操作已确认 commit 的 exact entries。
11. Move 只有在整棵 destination tree 完成并验证后才可开始 source delete。一旦 source
    cleanup 开始，绝不 rollback destination。
12. Source file fingerprint/directory identity 不匹配时不读、也不删该 source path。一个后来
    占用同名路径的文件或目录不能被当作 manifest 中的旧 entry。该保证覆盖 OpenOctopus
    协调写者与实际观察到的 identity mismatch；宿主外部进程仍受下述 check-to-use race
    boundary 约束。
13. Destination cleanup fingerprint 不匹配时不删。Cleanup 无法证明 complete 就必须
    outcome unknown，不能声称失败且 destination absent。Fingerprint check 与 unlink/rmdir
    在 portable filesystem API 上不是跨外部进程的原子条件删除；实现不得把它表述为 OS
    sandbox 或对恶意本地写者的强保证。
14. Server quota reservation 与普通写入看到同一 projected usage；两个并发目录
    preflight 不能分别通过后合计超配。
15. 不在 DB transaction 内等待 admission、Device、RustFS body、文件 hash、skill
    validation 或 cleanup。授权完成后关闭 DB session。
16. Blocking walk/stat/hash/copy/fsync/rmdir 和大规模 canonicalization 不运行在 Server
    或 Client event loop。
17. Disconnect、config replacement 和 late binary/control frames 继续由 Py8b/现有
    generation-bound slot 与 tombstone 处理；目录 coordinator 不建立第二套帧路由。
18. Server 不持久保存 Client tree manifest、source bytes 或目录 job。允许有界 temp
    staging，但 success/failure/cancellation/shutdown 后必须删除。
19. Empty directories 不属于 manifest/copy transferable data。完全空 tree 的失败发生
    在 issued boundary 前，source/destination 都不改变。Same-Client atomic rename 对
    非空 source 保留 empty directories；manifest 只把它们作为 scan-only metadata，
    不是 transferable file entry 或 destination mkdir 语义。
20. Aggregate result 和 warning 数量有界；内部 manifest/committed list 虽可达
    10,000 项，也不能直接进入 Provider transcript 或 REST error body。
21. Manifest/copy 完成扫描后新增的 source entry 不参与本次 operation。Same-Client
    atomic rename 必须在 syscall 前重扫并比较root/directory identity、merged entry set、
    每个file的kind/size/fingerprint与manifest digest；任一漂移都在mutation前失败。重扫后
    由宿主外部进程制造的race不属于filesystem snapshot保证，可能随整个directory entry
    一起被rename。

## 5. `file_transfer` 与 REST contract

### 5.1 Input schema

Py8b 合并后，两个 device enum 已允许任意两个属于当前 user 的 install sites。Py8c
只更新 description，不增加 property：

```jsonc
{
  "name": "file_transfer",
  "description": "Copy or move one regular file or directory tree between install sites. Directories are recursive. Destination must not already exist.",
  "input_schema": {
    "type": "object",
    "properties": {
      "openoctopus_src_device": {
        "type": "string",
        "enum": ["server"],
        "x-openoctopus-device": true
      },
      "src_path": { "type": "string", "minLength": 1, "maxLength": 4096 },
      "openoctopus_dst_device": {
        "type": "string",
        "enum": ["server"],
        "x-openoctopus-device": true
      },
      "dst_path": { "type": "string", "minLength": 1, "maxLength": 4096 },
      "mode": {
        "type": "string",
        "enum": ["copy", "move"],
        "default": "copy"
      }
    },
    "required": [
      "openoctopus_src_device",
      "src_path",
      "openoctopus_dst_device",
      "dst_path"
    ],
    "additionalProperties": false
  }
}
```

Agent tool 省略 `mode` 仍默认 `copy`。REST body 延续当前契约，要求显式提供
`mode`。REST 和 Agent tool 必须进入同一个 `FileTransferTool.transfer()` orchestration，
不能各实现一套 folder semantics。

### 5.2 Source kind 与 path mapping

Source root 检查顺序：

1. 通过对应 install site 的 path resolver 解析 caller path；
2. no-follow 检查每个已存在 component；
3. 对最终 source 做 exact stat/lstat；
4. regular file 进入单文件分支；
5. real directory 进入 directory branch；
6. link/reparse/special 返回稳定错误。

Directory manifest path 一律使用 `/` 分隔、相对 source root、非空。Destination path
由 `dst_root / relative_path` 得到，然后重新通过 destination resolver/policy 验证，
不能用字符串拼接后直接访问 OS 或 object storage。

例如：

```text
src_path = projects/demo
source entries:
  .env
  .git/config
  lib/a.py

dst_path = archive/demo-copy
final destinations:
  archive/demo-copy/.env
  archive/demo-copy/.git/config
  archive/demo-copy/lib/a.py
```

Source root 的 basename 不再追加。`dst_path` 的 trailing separator 只参与正常 path
canonicalization，不改变映射。

### 5.3 Same-site overlap 与 Device namespace

Overlap只在OpenOctopus能够证明两端属于同一个logical install-site namespace时判断：Server
shared/personal alias先解析成immutable workspace/storage target；Client使用immutable Device ID，
同一Device的不同名称或generation仍属于同一namespace。当两端命中同一namespace时：

- source 与 destination 相同：拒绝；
- destination 是 source 的 descendant：拒绝；
- source 是 destination 的 descendant：destination root 已存在，按 no-overwrite
  拒绝；
- 比较必须使用 destination platform 的 canonical/case behavior，而不是只比较原始
  JSON string；
- Server shared/personal virtual aliases 先解析成 immutable target ID + relative path
  再比较。

该规则避免 local copy 把自己新建的 destination 再扫描进 source，也避免 move 把目录
移动到自身后代。

不同Device ID按独立filesystem namespace处理。Py8c不新增host/volume/root identity协议，也不
尝试从Server判断两个Client workspace是否实际指向同一或重叠的宿主目录、bind mount、junction
或network share。把两个Device配置到相同或祖先/后代物理workspace属于unsupported topology；
两侧进程内subtree lock不能互相协调，OpenOctopus不为该配置提供overlap、move或cleanup正确性
保证。部署文档必须明确该约束，不能把“不同Device”宣传成已证明物理隔离。

### 5.4 Result shape

Py8c 将 Py8b 阶段的 regular-file success response 扩展成同一个 strict discriminated
aggregate union；这是 additive fields，不增加 compatibility shim：

```jsonc
// regular file
{
  "kind": "file",
  "files_transferred": 1,
  "bytes_transferred": 2457600,
  "sha256": "<file-content-sha256>",
  "warnings": []
}

// directory
{
  "kind": "directory",
  "files_transferred": 37,
  "bytes_transferred": 8712042,
  "sha256": "<canonical-content-tree-sha256>",
  "warnings": ["source_cleanup_incomplete"]
}
```

`kind` 只能是 `file | directory`。`files_transferred` 对 file 固定为 1，对 directory 为
`1..10000`；完全空 tree 不成功。Directory `sha256` 是按 §6.4 计算的 canonical
content-tree digest；它不是 tar hash，也不包含
manifest/copy 未保留的 empty dirs 或 metadata；same-Client rename 即使保留 empty dirs，
digest 仍只证明 regular-file path/content set。每个 warning 是短 symbolic value，总数
最多 8，去重后按固定优先级排序。

Agent tool 对两种 kind 都返回一段短文本，包含 source/destination、kind、file count、
total bytes、digest 和 warnings。REST 返回上述 strict union。
两者都不返回逐文件成功/失败数组。

## 6. Directory manifest

### 6.1 Internal models

Server 与 Client 各自独立实现相同 strict model；不建立 shared Python package：

```text
DirectoryManifestEntry
  relative_path: str       # 1..4096 chars, canonical '/' form
  size: int                # 0..2^63-1
  fingerprint: str         # opaque visible ASCII, 1..512 chars

DirectoryManifestDirectory
  relative_path: str       # 1..4096 chars, canonical '/' form
  identity: str | null     # opaque ASCII 1..512; filesystem required; object prefix null

DirectoryManifest
  version: Literal[1]
  root_identity: str | null     # opaque ASCII 1..512; filesystem required; object prefix null
  scanned_entries: int     # len(directories) + len(entries)
  total_bytes: int         # checked sum, 0..2^63-1
  directories: tuple[DirectoryManifestDirectory, ...]  # sorted scan-only metadata
  entries: tuple[DirectoryManifestEntry, ...]  # 1..10000 regular files
  manifest_sha256: str     # 64 lowercase hex
```

`scanned_entries` 不包括 source root 本身，但包括其下访问到的普通目录和普通文件。
`directories` 包括 empty dirs，只为精确 bounds、tree revalidation 和 move 时条件删除原
source 的已列空目录；copy destination 永远不按它创建目录。Client filesystem manifest
的 root 与每个 directory identity 必须存在；Server object storage 的 directory 只是
从 key prefix 合成，没有可条件删除的目录项，因此只能为 `null`。遇到 link/special entry
立即失败；它不会因“失败前尚未计数”而绕过安全规则。
`scanned_entries == len(directories) + len(entries) <= 10_000`，file count 也不得超过
10,000，且 `entries` 必须非空。

Client directory manifest在source-probe job内冻结并分页返回时，完整canonical UTF-8 JSON
表示不得超过：

```text
MAX_DIRECTORY_MANIFEST_BYTES = 5 * 1024 * 1024
```

“Canonical UTF-8 JSON”固定为以下唯一编码，Server 与三平台 Client 不得使用各自默认值：

- filename/path保留filesystem返回的Unicode scalar sequence，不做NFC/NFD/NFKC
  normalization；lone surrogate或不能UTF-8 round-trip的名称拒绝；
- manifest的`directories`、`entries`以及merged stream都按`relative_path.encode("utf-8")`
  的unsigned byte lexicographic order排序；同一path出现两个kind属于冲突，不以kind打破平局；
- JSON使用`ensure_ascii=false`、lexicographic key order、紧凑`,`/`:` separators、禁止NaN，
  string只执行JSON要求的quote、backslash与control-character escaping，不escape `/`；
- 编码结果是无BOM的UTF-8 bytes。5 MiB计算完整`DirectoryManifest` DTO（包括
  `manifest_sha256`）的最终bytes，边界值可接受，超过一个byte即拒绝；
- 256 KiB page cap计算完整page value
  `{offset,next_offset,items}`的同种canonical UTF-8 JSON bytes，边界值可接受；外层
  `tool_result` frame另行遵守Protocol v3 12 MiB text-frame上限。Page builder加入下一项会
  超限时在该项前结束；若单项自身无法放入空page，整体manifest在preflight失败。

该JSON只用于wire、retained-size与page-size一致性；`manifest_sha256`仍严格使用§6.4的
length-prefix encoding，不hash JSON，也不存在把digest字段hash进自身的循环。

Server 本地生成的 manifest 也必须用同一个 canonical encoder 测量同一上限，不能因
“不走 wire”获得更大 contract。这个 dedicated cap 低于 Protocol v3 12 MiB text
frame；不复用普通 `list_dir` 的 display result、offset 或 noise filter。

Source-probe directory job使用dedicated decoded-content cap
`MAX_DIRECTORY_MANIFEST_BYTES`，不复用现有5,000,000-byte generic workspace display cap。
每个page另有256 KiB encoded上限；Server按单页最坏JSON escaping预留pending-result bytes，
send/receive两端检查每个frame仍低于Protocol v3 12 MiB。分页只降低单call/frame压力，不能
绕过完整manifest的5 MiB retained/validation上限。

### 6.2 Client walk

Client 在 worker thread 中使用 no-follow `lstat/scandir` 和显式有界队列遍历：

- 不调用会 follow link 的 convenience copy/walk mode；
- 每个 directory entry 在入队前检查 symlink/reparse/special kind；
- 每层按 canonical name 排序，最终 `directories`/`entries` 分别排序，并验证合并后的
  path/kind set 无冲突；
- POSIX 无法无损表示为 JSON Unicode string 的 filename 整体拒绝；
- Windows reserved names、separator、drive-relative、UNC 和 reparse rules 在 source 与
  destination 各自 resolver 中验证；
- POSIX mount point/bind mount在path policy中是real directory，不作为link/special entry
  拒绝；`restrict_to_workspace`仍只约束解析后的路径层级，不建立filesystem/mount namespace
  边界；
- zero-byte regular file 是有效 entry；
- permission/stat error 整体失败，不当作“文件不存在所以 skip”；
- `_NOISE`、`.gitignore`、find/list filters 完全不参与。

Fingerprint 复用 Py8b/现有单文件 transfer 的 opaque stat identity，包括能够检测的
file identity、size 和 nanosecond modification state。Manifest 不把 inode、Windows
file ID 或其它平台内部字段暴露给 Server；Server 只做 opaque equality。

Directory/root identity同样来自no-follow filesystem identity，覆盖稳定的device/volume +
file ID/inode，但不包含会因本操作删除child而变化的mtime/ctime。Tree revalidation另用完整
entry set与file fingerprints检测内容变化。Client若无法取得非null stable identity，整次目录
operation在destination mutation前返回`workspace_storage_unavailable`；不能为copy或move
退化成path-only equality。

### 6.3 Server workspace scan

Server source 必须独立探测 exact object 与 `object_name/` prefix 后再分派：只有 exact
是 regular file，只有 prefix 是 directory，两者都不存在是 `workspace_not_found`，两者
同时存在是 `workspace_storage_error`。不能看到 exact 后短路，否则 unsupported external
bucket writer 制造的非法 shape 永远无法检测。

Directory manifest 直接分页列举完整 prefix，不使用 `scan_objects()` 的 noise/filter
semantics。RustFS workspace 中只有 regular objects，directory 从 key prefix 合成，因此
没有 empty-directory entry 或可用 directory identity；每个 object ETag 是 source
fingerprint。

### 6.4 Digest vectors

Manifest digest 与成功 content-tree digest 都采用 length-prefix encoding，避免
`a/b + c` 与 `a + b/c` 等串联歧义。

Manifest digest：

```text
SHA256(
  b"openoctopus-directory-manifest-v1\0" ||
  b"R" || encode_optional_identity(root_identity) ||
  for item in merged_path_order:
    if directory:
      b"D" || u32be(len(path_utf8)) || path_utf8 ||
      encode_optional_identity(directory_identity)
    if regular_file:
      b"F" || u32be(len(path_utf8)) || path_utf8 ||
      u64be(size) ||
      u16be(len(fingerprint_ascii)) || fingerprint_ascii
)
```

`encode_optional_identity(null) = u16be(0)`；非null identity编码为
`u16be(len(bytes) + 1) || bytes`，从而空值与任何合法非空identity无歧义。Client
filesystem manifest 的 optional 值不得为 null；null 只用于 Server object-prefix。

成功 content-tree digest：

```text
SHA256(
  b"openoctopus-directory-content-v1\0" ||
  for entry in sorted_entries:
    u32be(len(path_utf8)) || path_utf8 ||
    u64be(verified_size) ||
    raw_32_bytes(file_sha256)
)
```

Server 收到 Client manifest 后必须验证两个序列的严格排序、合并 path/kind 唯一性、
count/sum、每个字段和 encoded byte cap，再重算 `manifest_sha256`。成功 digest 只使用
file child slot 已由双方验证的 size/SHA-256，不包含 scan-only directories，也不重新
读取 destination tree。

Canonical tests 固定包含 empty file、中文名、combining Unicode、nested dotfile 和
不同 fingerprint 长度的跨 Server/Client digest vectors。

### 6.5 Client `DirectoryJobManager`

Client 增加一个由当前 WebSocket generation拥有、仅内存的`DirectoryJobManager`。它统一
管理source scan、destination reservation/finalize/cleanup和same-Client local transfer；
每个job捕获创建时的config/path-policy snapshot。固定边界为：

- `MAX_ACTIVE_DIRECTORY_JOBS = MAX_ACTIVE_TRANSFER_SLOTS = 2`，不新增配置；每个job最多
  持有一个5 MiB manifest、10,000条bounded records和一个terminal snapshot；
- job-owned asyncio task/thread绝不能注册到或被dispatcher现有`_blocking_tasks`等待；否则
  单FIFO tool worker会在长scan/copy后阻塞同一job的status/cancel/cleanup；
- exact directory control actions由Client reader按strict operation discriminator路由到独立的
  generation-bound `DirectoryControlWorker`，固定queue capacity 8；它与普通`_ToolWorker`
  并行但仍通过相同`tool_call/tool_result` frames与normal response lane。每job最多一个pending
  status/page/command；queue full稳定busy。普通长`grep/delete_folder/__workspace_rest__`不能
  阻塞directory status/cancel，且control worker不执行filesystem work；
- private control action只做bounded schema decode、O(1)状态转换或有界page/snapshot读取，
  不同步等待walk/hash/copy/fsync/final scan；所有filesystem work由manager-owned worker执行；
- directory worker使用两个job-owned cooperative events：`stop_forward_work`停止scan/copy/new
  commit，进入destination rollback或move source cleanup时使用独立、初始未置位的
  `stop_cleanup`。否则Agent Stop会让cleanup在第一个unlink前自行取消。Worker在每个scandir
  entry、64 KiB read/write、文件边界以及开始下一个对应phase syscall前检查正确event；Python
  thread一旦进入
  `scandir/read/write/fsync/unlink/rmdir/rename`等filesystem syscall不能被强制取消；cancel只禁止
  后续步骤，job保持`cancelling`，当前subtree reservation与shared local-slot lease继续持有，直到
  syscall返回并完成结果/cleanup收敛后才可进入terminal。不得先释放锁或报告local terminal，
  再让被放弃线程产生晚到mutation；
- Py8c不引入filesystem helper process。极端故障的syscall可以让一个job与一个shared slot持续
  到syscall返回或Client进程退出；control worker、WebSocket reader与另一个shared slot仍保持
  可用。Client crash、强制终止或断电继续按既有无disk recovery边界处理；
- ClientRuntime持有runtime-owned shared admission，并强引用所有current/retired
  `DirectoryJobManager`及其drain records直到quiescent。Connection finally不能只清空当前manager
  引用；Client shutdown向current/retired managers同时置对应stop event并等待同一grace，尚未返回
  的syscall继续由drain registry追踪到进程退出；
- worker真实执行walk/hash/copy/prepare/finalize/cleanup时，从§12.1同一个shared local-slot
  admission取得lease；等待slot不占tool worker，wire slot与job work合计始终不超过2；
- 每个active worker phase在转入`READY_RETRIEVAL/HELD/FINALIZED_HELD`或任一terminal snapshot
  前，必须在`finally`恰好一次释放shared local-slot lease；cooperative cancellation不取消thread
  wrapper future，`finally`只有真实thread/drain完成后才执行。Metadata、manifest retention和
  subtree reservation本身不占该capacity。后续active phase重新取得lease；
- `progress_seq`只因真实work推进而增加。Server polling、重复status或等待slot不算progress；
- SCANNING/PREFLIGHTING/PREPARING/FINALIZING/CLEANING等worker状态使用真实work no-progress
  deadline。Deadline触发cooperative cancel，不承诺杀掉正在运行的thread syscall；Server只在
  有限reconciliation window内等待，尚不能得到local terminal时按issued boundary与已有mutation
  证据投影`tool_execution_outcome_unknown`。该window固定复用当前
  `device_transfer_idle_timeout_seconds`，从Server首次发出cancel或确认route loss/no-progress
  deadline的时刻开始，只计一次且重复status不重置；不增加配置。Source `READY_RETRIEVAL`暂停
  work deadline，改用
  retrieval-idle lease：只有请求
  新的contiguous `next_offset`才刷新，同offset retry/status不刷新；最后一页后必须进入
  HELD或release；

- 只读source scan或destination preflight尚未发送prepare/local-start等mutation-capable command
  时，即使本地read-only syscall仍阻塞，Server也可在window结束后稳定返回原timeout/unreachable，
  且destination unchanged；Client job仍保持cancelling与shared slot直到thread真实返回。只有已经
  越过mutation-capable issued boundary、或cleanup本身无法确认结果时，才投影outcome unknown；
- source `HELD`期间，status可携Server coordinator的monotonic `outer_progress_seq`，但只有该
  值因真实其它端preflight/child/cleanup progress而增加才刷新idle lease；重复值不刷新；
- destination `READY`（same-Client status投影为`ready_not_started`）使用同一个existing
  transfer-idle duration作为prepare-idle lease；重复status不刷新，只有一次有效prepare、local
  start、cancel或release消费该状态。到期时因尚无destination mutation，job稳定转
  `failed(workspace_transfer_timeout)`，释放manifest/reservation metadata并进入terminal
  retained outcome，第三个job必须能够进入；
- destination已claim root后的`RESERVED/COPYING`也不能因Server coordinator消失而永久持锁。
  Child byte/commit进度由job本身刷新idle lease；child之间只有Server携带严格增加的
  `outer_progress_seq`才刷新，重复poll不刷新。Move进入source cleanup后，Server把真实、严格递增的
  source cleanup progress继续作为destination status的`outer_progress_seq`转发；只要cleanup推进，
  `FINALIZED_HELD`不得expiry。到期后pre-finalized job自动进入background conditional cleanup，
  cleanup complete投影原timeout，否则`outcome_unknown`。Stalled `FINALIZED_HELD`到期时绝不删除
  已验证destination：释放subtree reservation，把bounded finalized outcome压入tombstone；尚未
  开始或不再推进的move source cleanup视为放弃并带`source_cleanup_incomplete`；
- `READY_RETRIEVAL` lease到期时source job转
  `failed(workspace_transfer_timeout)`，释放manifest/active metadata并按下述terminal outcome
  tombstone保留结果；此时完整manifest尚未交付，destination mutation必为零。
  Source `HELD`在`COPY_COMPLETE`前到期时同样terminal timeout，拒绝新child authorization，
  Server按当前destination阶段执行§10 cleanup并据其结果投影原timeout或outcome unknown；若
  destination已经`FINALIZED_HELD`，copy直接release并保持success；move禁止启动新的source
  delete，release destination并返回success + `source_cleanup_incomplete`。已经进入
  `source_cleanup`的不再使用HELD lease，而按
  active-work no-progress deadline有界best-effort收敛；
- `succeeded | failed | outcome_unknown` transition在释放active-work lease、subtree lock与bulk
  manifest/records后，原子移出active registry并写入bounded retained-terminal map；retained
  terminal与tombstone均不计入`MAX_ACTIVE_DIRECTORY_JOBS=2`。因此READY expiry及任何正常terminal
  都立即让后续job获得active名额。Server显式release删除retained outcome并写exact `released`
  tombstone；未release则启动terminal-release TTL，TTL到期把同一bounded aggregate outcome移入
  generation-scoped outcome tombstone。即使Server从未观察原job terminal，后续same-key status仍
  返回完整outcome，不能只返回ID+digest；exact release对retained/outcome tombstone都返回
  `released`并转成released tombstone；wrong digest拒绝。所有tombstone再按existing transfer
  TTL回收。ClientRuntime另持有一个固定4096-credit的directory lifecycle pool（复用既有
  `TOMBSTONE_MAX_ENTRIES`数值，不新增配置），current/retired managers共享；每个job在start被接受
  前预留一个credit，并由active record、retained terminal、outcome tombstone、released tombstone
  依次转交，直到最终TTL purge才释放。Purge后仍满则新start在任何work/mutation前稳定busy；未被
  Server release且TTL未到的完整bounded outcome不得为腾空间提前驱逐。Terminal snapshot不含path、
  manifest或per-file records，因此总retained bytes由4096个aggregate snapshots硬约束；
  `READY_RETRIEVAL`、source `HELD`与destination `READY/RESERVED/FINALIZED_HELD`都不是terminal，
  不使用terminal TTL；它们只按上述各自idle lease收敛，且`FINALIZED_HELD` expiry绝不rollback；
- config epoch改变时按§13.3进入cancel/cleanup但仍允许同WS generation的exact owner控制；
  connection generation retire对pre-finalized jobs请求cancel/cleanup；没有阻塞syscall时在bounded
  grace内收敛，仍阻塞时按上述旧manager持锁/slot边界继续后台等待；
  `FINALIZED_HELD`只保留verified destination并释放进程内reservation，绝不rollback。Client
  process crash不恢复。

Job registry与tombstone key固定为`(directory_operation_id, job_role)`，其中role只有
`source | destination`；action family确定role，不新增wire字段。Same-Client可同时有source与
destination两条record，但local transfer是destination record的状态升级，不创建第三个role/job。
同一key + 相同action parameters的重复start只返回已有job状态，不重跑；同key不同parameters/
digest返回`workspace_transfer_integrity_failed`。Server在start result丢失时只status/cancel，
不主动重发start；这个幂等约束只防transport duplicate/late delivery。
Prepare/finish/cancel也是job state machine中的one-shot monotonic command：同一command的exact
duplicate只返回当前state，不重复mkdir/rename/cleanup；越阶段或参数不同的command拒绝。
Release删除job时留下不含path/manifest的bounded key+digest `released` tombstone，复用现有
transfer TTL/entry bound，使release result丢失后的exact retry返回`released`；未经显式release
而因terminal TTL压缩的tombstone则保留bounded terminal snapshot；wrong key/digest仍拒绝。

## 7. 完整 preflight

### 7.0 Client source probe

当 source 是 Client 时（包括 destination 是同一 Client），Server 必须先启动一个只读、
Provider-hidden、generation-bound source job；不能先启动 regular-file slot，等Client返回
`tool_is_directory`后再fallback，因为source request已跨outer issued boundary。控制actions为：

```text
transfer_source_probe_start
transfer_source_probe_status
transfer_source_probe_page
transfer_source_probe_hold
transfer_source_probe_cancel
transfer_source_probe_release
transfer_directory_authorize_source_child
transfer_source_cleanup
```

`start`只登记job并立即返回`running`；no-follow kind probe与directory walk在
`DirectoryJobManager`的`source` role worker中执行。`status`返回
`scanning | ready_retrieval | held |
source_cleanup | succeeded | failed | outcome_unknown`、monotonic`progress_seq`与有界
aggregate counts；file/ready-directory result是strict discriminated union：

```text
FileSourceProbe
  kind: Literal["file"]
  size: int
  fingerprint: str

DirectorySourceProbe
  kind: Literal["directory"]
  root_identity: str
  scanned_entries: int
  file_count: int
  total_bytes: int
  manifest_sha256: str
  page_count: int
```

Directory status只有完整scan、bounds与digest验证成功后才变为`ready_retrieval`；scanning时不暴露partial
manifest。Ready snapshot冻结后，Server用`page(offset)`顺序取回merged directory/file item
stream：每页最多256 items且encoded JSON最多256 KiB，返回`offset/next_offset/items`。
每个item是strict union：directory为
`{kind:"directory", relative_path, identity}`，file为
`{kind:"file", relative_path, size, fingerprint}`；顺序就是§6.4 manifest digest的merged
path order。
Page按operation ID + manifest digest + offset幂等读取；丢失可重取同offset，不重扫。Server
取得全部pages后验证无缺页/重复/乱序，重建§6.1 model并重算count/sum/digest，之后才进入
destination preflight。单个5 MiB result不进入普通tool-result lane。

File probe在exact stat完成后释放其active-worker shared local-slot lease并进入terminal
`succeeded`；`FileSourceProbe`作为immutable terminal snapshot保留，start/result丢失后仍用同
operation ID查询，重复status不重stat。它使用§6.5统一terminal-release TTL并最终压入包含完整
bounded probe outcome的tombstone；destination始终unchanged。Server取得snapshot后先用
`transfer_source_probe_release`收敛到`released` tombstone，再把同一个outer Server operation
lease交给already-admitted Py8b file入口；后者的Client wire slot才重新取得shared local-slot
lease，因此scan与file slot不会双占。正式source begin/open必须再次匹配probe size/fingerprint；
漂移在destination mutation前返回`workspace_file_changed`。Release结果丢失只重试exact release
或查询tombstone，不重启probe；release完成到file begin之间的source变化由上述fingerprint fence
处理。

missing、link/reparse/special成为job的稳定terminal path error。File variant选择Py8b单文件
路径，且source `transfer_begin`的size/fingerprint必须仍匹配probe；directory variant进入
本文coordinator。Probe不创建destination、不占binary slot，也不触发public`on_issued`。
每个新page offset推进§6.5 retrieval lease；取齐后Server必须立即选择：same-Client local
分支发送`release`并在后续local job重验manifest；cross-site分支发送one-shot`hold`，把同一
source job与manifest保留到copy terminal或move source cleanup。Result/page/hold/release丢失
按同ID查询/重取，绝不启动第二次scan。

Cross-site每个Client-source child开始前，Server用预分配UUID调用
`transfer_directory_authorize_source_child`，一次性绑定operation ID、exact source path、
fingerprint与UUID；成功后才发送同UUID的source `transfer_request`。TransferManager消费授权
并把真实open/read/chunk/terminal进度回报source job，刷新HELD lease；wrong/duplicate/expired
授权或result丢失都不发送request。Copy完成/失败后cancel或release source job；move在完整
destination `FINALIZED_HELD`后发送one-shot`transfer_source_cleanup`，manager worker按job
自有manifest identities条件删除、通过同一status收敛，最后release。Source cleanup不再是
同步256-entry dispatcher batches。

同一Client路径在source job release与只读destination preflight全部成功后才发送
mutation-capable`transfer_local_directory_start`；该action把现有`destination` role的READY
preflight job原子升级为`LocalDirectoryJob`，不创建第三个job或复制第二份manifest。Local
job携带expected source identity/manifest digest并在mutation前重验。Server source由
`WorkspaceService`直接probe，不走Device action。

Kind detection 在一个已经取得的 outer transfer lease 内完成。File variant 继续调用
already-admitted 单文件入口并复用该 lease；不得释放后递归进入会再次 acquire 的 Py8b
public entry。这样并发 probe 仍受现有 global/per-user admission 约束，一个普通 file call
也始终只计一个 operation credit。

### 7.1 顺序

Directory operation 在任何 destination-visible write 前依次完成：

1. 捕获 user、source/destination immutable install-site identities 和在线 generation；
2. 获取一次 fair directory-operation admission；
3. 解析并授权 source/destination roots，关闭 DB session；
4. 通过本地 probe 或上述 Client strict union 识别 source directory并取得完整 bounded
   manifest；regular-file probe 已在此之前分派到 Py8b 路径；
5. 验证 manifest schema、digest、entry/path/byte bounds；
6. 把每个 regular-file relative path 映射成 destination path；scan-only directories 不
   产生 destination entries；
7. 验证 destination root absent、parent kind、path policy、平台可表示性和内部
   collision；
8. 若 destination 是 Server，原子建立 aggregate quota reservation；
9. 若最终 destination 命中 personal Server `skills/*/SKILL.md`，完成 §7.5 全部内容
   validation；
10. 按 §7.3 原子占有 destination root/subtree，并立即重查 absence，然后进入
    `DESTINATION_RESERVED`，再开始 `COPYING`。

步骤 1..9 失败都释放 admission/quota/temp/resource，不触发 `on_issued`，也不创建
destination root。步骤 10 的 filesystem prepare send/atomic mkdir 是 issued boundary；
此后失败必须进入精确 destination cleanup。

### 7.2 Destination representability 与 collision

Preflight 不只检查每条 path 长度。Destination side 必须对完整 mapped set 验证：

- 最终 path 长度仍不超过 4096；
- 无 empty、`.`、`..` 或 absolute relative component；
- parent 不会与另一个 manifest regular file 冲突；
- destination filesystem normalization/case folding 后没有两个 source path 映射到
  同一路径；
- Windows reserved/device names、trailing dot/space 和不支持的 path form 被拒绝；
- destination root 当前不存在且其现有 ancestor 都是真实 directory；
- 任何 ancestor 是 link/reparse/special 都拒绝。

Collision不在preflight阶段创建probe文件，也不根据当前volume的可变挂载选项产生不同wire
contract。它使用确定、保守的destination-platform key：

```text
Linux/其它POSIX：每个component的原始UTF-8 bytes
macOS：           NFC(component).casefold()的UTF-8 bytes
Windows：         NFC(component).casefold()的UTF-8 bytes
```

Windows trailing dot/space与reserved-name先独立拒绝。macOS/Windows即使实际volume或目录开启
case-sensitive mode，也仍使用上述保守key；因此实现可以拒绝少量底层filesystem本可同时表示的
名称，但同一目标平台policy不会随volume配置产生不同结果。Collision set只包含
regular-file mapped paths及为它们实际创建的derived parent paths。纯scan-only directory若不是
任一file的derived parent，不参与copy destination collision，因为copy不会创建它；它仍参与
manifest digest、bounds、source revalidation与same-Client rename snapshot。由file导出的parent
与另一file path/key冲突仍整体拒绝。

Linux/其它POSIX的支持topology要求destination filesystem执行case-sensitive、byte-distinct名称
语义；启用ext4 casefold、case-insensitive network mount或其它不符合该语义的volume属于
unsupported topology。Py8c不通过创建probe文件猜测mount行为，也不能把raw UTF-8 key宣传为对
这些volume的collision防护；其commit仍由no-replace兜底并稳定失败，但不保证完整preflight零写入。

Client destination以同一个generation-bound job贯穿preflight、prepare、children、finish/
cleanup；它使用`destination` role。`transfer_directory_preflight`只登记manifest与job并立即返回running；worker完成
只读representability/absence检查后，Server从`transfer_directory_status`观察到ready。
Preflight不创建parent。Server对ready只视为一次检查结果；随后仍须命令§7.3的atomic root
reservation，真正child commit也继续通过destination resolver和no-replace primitive再验证。
Preflight start/result丢失只按operation ID status/cancel，不重建第二job。

Destination status是有界snapshot：`state=preflighting | ready | preparing | reserved |
copying | finalizing | finalized_held | cleaning | failed | outcome_unknown`、monotonic
`progress_seq`、aggregate processed counts与terminal result/error；不含path、fingerprint或
per-file array。Status/result丢失可重查，poll本身不推进state。

### 7.3 Destination root reservation

Filesystem copy 在最后一次 read-only validation 后、首个 child 前执行原子 root claim：

1. 对future canonical root取得一个`PREPARING`状态的ancestor-aware subtree reservation；
   lock publication与其它OpenOctopus path actions在同一critical section完成；
2. 在该reservation内重新no-follow验证ancestors/path set，创建缺失但仍安全的root
   ancestors，并记录本操作创建的parents；
3. 以destination platform的exclusive mkdir/no-replace primitive创建exact `dst_root`；
4. 创建失败若表示root已存在，记录`root_claimed=false`，只清理由本操作创建且identity
   仍匹配的空parents，然后返回`workspace_file_changed`；竞争者的root不属于cleanup目标，
   也不要求它消失。创建成功则记录`root_claimed=true`与root identity，并把reservation
   原子切到`READY`；
5. 以`directory_operation_id`持有该reservation；成功finalize后仍保持
   `FINALIZED_HELD`，copy由Server确认后release，move直到source cleanup完成/放弃后release；
   failure则持有到destination cleanup完成；
6. reservation保存 generation、canonical root、完整 expected file path set/digest、每个
   已 commit destination fingerprint，以及root/owned ancestors/child parents的filesystem
   identity。Authorized child创建缺失parent时必须在同一owner mutation boundary登记identity；
   记录集合是manifest-derived directory set的有界子集，仍受10,000 entries/5 MiB bound；
7. Client 上任何非owner OpenOctopus file/transfer action 命中该 subtree 时必须立即返回
   既有 busy error，不能在单个 FIFO tool worker 内等待；否则它会排在 owner 的
   finish/cleanup 前形成 HOL deadlock。只有携 exact operation ID 的 status、authorize、finish、
   cleanup、release 与 local-job action 可以 join/bypass 自己的 reservation。Server 侧异步
   coordinator 可按既有公平 admission 等待；Client exec/PTY 与宿主外部进程不受应用锁
   控制，仍属于明确 race boundary；
8. 每个cross-site child开始前，Server先用私有
   `transfer_directory_authorize_child` 把 `(directory_operation_id, transfer_uuid,
   exact_dst_path)` 一次性绑定到captured destination generation。Receiver只允许exact
   matching `transfer_begin.id/dst_path` 消费该authorization并以owner身份bypass subtree
   lock；普通单文件call、wrong/duplicate/expired UUID不能借expected path穿透；
9. success finalize在reservation仍持有时做一次bounded no-follow exact-root scan，验证
   全部expected files的identity、只存在由expected paths导出的parent directories、没有
   extra/link/special entry，然后转`FINALIZED_HELD`而不解锁；无法证明exact tree时不开始
   source delete，按destination outcome unknown处理；failure按§10条件删除committed files、
   exclusive root和owned empty parents后释放。

Cross-site Client destination的`transfer_directory_prepare`只是向现有job提交一次
mutation command并立即返回accepted；其generation-fenced send callback触发outer
`on_issued`，耗时revalidation/exclusive mkdir在manager worker完成。Server轮询status直到
reserved；prepare result丢失只能status/cancel，不能再次mutation。最后一个child完成后，
`transfer_directory_finish`同样只触发后台exact-root scan；status明确`finalized_held`后才算
`FINALIZED_HELD`，此时才算`COPY_COMPLETE`。Copy在Server确认后调用
`transfer_directory_release`作为成功路径唯一unlock；move等source cleanup收敛后再release，
防止OpenOctopus自身在删源前改写/删除已验证destination。Failure/Stop用
`transfer_directory_cancel`请求job后台条件cleanup，terminal cleanup已unlock后再release
metadata。Same-Client local job内部持有相同subtree lock并
执行exclusive mkdir；`transfer_local_directory_start` 的generation-fenced mutation send
同样触发outer `on_issued`。Client disconnect/shutdown 对 active reservation做
bounded best-effort cleanup并释放进程内lock；仅当本操作成功claim过root时，Server才要求
该root最终absent。未claim root且owned parents已收敛时保留确定conflict；不能确认owned
mutation已清除时才投影outcome unknown。Client process crash/断电不做磁盘 reservation
recovery，遗留root由caller检查，绝不自动replay。宿主外部进程不受该lock控制，是明确race
boundary。

Child authorization是有界、generation-scoped、single-use的in-memory record，只能为当前
operation剩余expected path创建；数量同时最多为1，因为directory children严格串行。授权
result丢失时不发送该child，直接进入outer cleanup；不得重新使用相同UUID或自动replay。
prepare/finish/cleanup/status/release actions携exact `directory_operation_id`，只有reservation
owner可join/bypass lock。Receiver commit通过已消费authorization直接把destination
fingerprint/ETag登记到同一个job，再生成ACK；Server不能伪造committed list覆盖Client记录。

Server/RustFS没有可见directory marker，不创建空对象。`WorkspaceFS` 增加lease-counted、
ancestor-aware、process-local subtree lock，所有受支持的OpenOctopus workspace mutations
都参与；directory operation在最终prefix-absence check前取得并持有到terminal cleanup。
外部bucket writer与multi-worker仍不在支持topology内。第一次child object publish才触发
outer `on_issued`。

### 7.4 Server quota reservation

当 destination 为 Server workspace 时，`WorkspaceFS` 在 workspace mutation lock 下：

1. 扫描 authoritative current usage；
2. 加上其它 active reservation 的 remaining bytes；
3. 检查 soft lock；
4. 检查 `total_bytes * 5 <= quota_bytes * 4`；
5. 检查 projected usage 不超过 quota；
6. 注册 process-local、operation-ID-bound reservation。

每个 child commit 在同一 mutation boundary 中把本次文件大小从
`reserved_remaining` 转为 committed usage；projected total 不变。其它 write、upload、
single-file transfer 和 directory reservation 都必须把 active reservations 计入 quota
判断。所有终态释放 unused reservation；entry 必须 lease-counted/evicted，不能泄漏。

Server 单 worker 是该 reservation 正确性的部署前提。外部 bucket writer 和多 worker
不在支持 topology 内；Py8c 不为此增加 PostgreSQL durable reservation。

### 7.5 SKILL.md validation

ADR-087 的 write-time validation 同时适用于 single-file 和 directory transfer。
最终 destination 解析到当前 user 的 personal Server workspace，并且 relative path
精确匹配 `skills/<name>/SKILL.md` 时：

- single file 在 destination publish 前验证 source bytes；
- directory 先找出 manifest 中全部匹配项，逐个读取 manifest 所指版本并验证；
- 任一个 malformed，整次 directory transfer 在第一个 destination commit 前返回
  `workspace_invalid_skill_format`；
- shared workspace 中的同名目录不是 personal Skills source，不套用此规则；
- validation 使用 source fingerprint，后续正式 child transfer 仍必须匹配同一
  fingerprint，避免 validate A、commit B；
- Client source bytes 可通过现有单文件 source/relay core 流入 Server temporary
  staging sink，但不能提前写最终 Server destination；
- staging sequential、有界、失败后删除，不把所有 manifests 同时留在内存或磁盘。

Validator 需要提供 streaming/staged-file adapter：buffer 有界 frontmatter prefix，
对 conditional body 做 incremental UTF-8 validation；`always_on=true` 继续执行现有
64 KiB 与 token bound。不能为了验证一个大 conditional SKILL.md 把完整内容读入
Server event-loop memory。

## 8. 方向矩阵与执行路径

| Source | Destination | Directory copy | Directory move |
|---|---|---|---|
| Server | Server | Workspace manifest；逐对象 stream/copy | copy 全部后条件删除 source objects |
| Server | Client | 每项复用 server→client 单文件 slot | copy 全部后条件删除 Server source |
| Client | Server | 私有 manifest；每项复用 client→server slot | copy 全部后条件删除 Client source |
| Client A | Client B | 每项复用 Py8b bridge | copy 全部后条件删除 Client A source |
| Client A | 同一 Client A | 一个私有 local directory job | job内native exclusive directory rename |

### 8.1 Cross-site child invocation

Directory coordinator 始终以 outer `mode="copy"` 执行每个 child；不能让单文件代码在
每个 ACK 后按 outer `move` 删除 source。每个 child invocation 额外携带
Provider-hidden expected metadata：

```text
expected_source_fingerprint
directory_operation_id
outer_admission_lease
```

这些不是 `file_transfer` input schema 或公开 transfer frame 字段。Source open 后的
实际 ETag/fingerprint 必须与 expected value 相同；否则该 child 返回
`workspace_file_changed`。

Py8b 应暴露一个内部“already admitted single-file bridge”入口，使 directory
operation 在持有一个 outer lease 时复用相同 slot state machine，而不是递归获取第二
个 transfer lease。该入口还接受Server预分配的UUIDv7：Client destination child先用
这个UUID完成`transfer_directory_authorize_child`，成功后Py8b/现有单文件manager才以同一
UUID发送`transfer_begin`。Authorization失败或result丢失时绝不发送begin。普通单文件
public call仍自行allocate ID并acquire/release，不能获得reservation-owner bypass。

每个 child 成功返回 coordinator 内部数据：

```text
relative_path
verified_size
verified_sha256
destination_fingerprint_or_etag
```

最后一项只用于 conditional cleanup，不进入 Provider result。Protocol v3 DTO 已有
`etag + created` 字段，但 canonical contract 目前只要求 `workspace_upload` 使用该组合。
Py8c 将这两个既有字段合法扩展到 directory child 的 `purpose="file_transfer"` success
receiver ACK，并要求二者同时存在、`created=true`。实现必须同步 Server/Client validators
与 fixtures；没有新增 frame/field/binary layout，因此 Protocol v3 不升版。
Server 捕获 `etag/created` 作为 coordinator-only cleanup metadata 后，发送给 source
endpoint 的 ACK 必须规范化为普通 `file_transfer` ACK：保留 verified bytes/SHA-256，
移除 destination `etag/created`。否则 Py8b source sender会把 receiver新增的destination
metadata误当成terminal mismatch。Failure ACK同样不携带destination metadata。
Server destination 直接从 object commit metadata 得到 ETag。

### 8.2 Same-Client local job 与 copy

Directory 不复用 regular-file `transfer_local` 的单个长 tool call。Client把
`DirectoryJobManager`中同一`(directory_operation_id, destination)`的READY preflight record
原子升级为仅内存、generation-bound `LocalDirectoryJob`，由四个短私有actions控制：

```text
transfer_local_directory_start
transfer_local_directory_status
transfer_local_directory_cancel
transfer_local_directory_release
```

`start` 接收Server生成的`directory_operation_id`、mode、source/destination roots、expected
manifest digest和route/config snapshot；它必须找到同key、同roots/digest且state=READY的
destination preflight record，随后原地升级并复用其validated manifest。缺失/mismatch拒绝，
不能新建job；exact duplicate start只返回当前state。generation-fenced start send是outer
issued boundary。Action只完成O(1)升级并立即返回`state=running`；manager worker再从
§12.1共享local-slot admission取得一个lease并取得source/destination subtree locks，等待不占
tool worker且受既有queue timeout。Busy/timeout发生在mutation前；walk/hash/copy在manager
worker thread/background task中运行。

Server同一时刻只保留一个pending status call，以固定1秒间隔轮询有界snapshot：

```text
state: ready_not_started | running | cancelling | succeeded | failed | outcome_unknown
phase: waiting | preparing | hashing | copying | revalidating | renaming | cleanup
progress_seq: monotonic integer
files_processed: 0..manifest_file_count
bytes_processed: non-negative integer
terminal_result: aggregate | stable error | null
```

snapshot不含paths/fingerprints/per-file arrays。只有真实walk/hash/copy/fsync/cleanup/rename
状态推进才增加`progress_seq`并刷新existing transfer no-progress deadline；重复poll本身不
算progress。每个start/status/cancel/release tool_call仍受existing bounded private-call
timeout，但job没有独立whole-operation hard deadline。no-progress timeout触发Client cancel/
cleanup；Server失去generation或Agent Stop时发送一次cancel，随后只在bounded window内poll；
未观察terminal时按§13.2返回outcome unknown，不能把`cancelling`伪装成terminal。Start
result丢失表示job可能已启动：只允许按operation ID cancel/inspect，绝不duplicate start。
`transfer_local_directory_status`命中尚未升级的同key READY destination record时固定返回
`ready_not_started`，证明start未执行且destination unchanged；Server结束/取消该preflight并
返回稳定失败，不重发start。命中running/cancelling/terminal则说明升级已发生，继续按job
状态收敛。因为start与status走同一generation-bound control FIFO，后发status不能越过已接收
但尚未处理的start而虚假返回`ready_not_started`。

terminal snapshot按§6.5保留到Server确认收到后发送`release`，或压缩进outcome tombstone；
active worker进入terminal前已释放shared local-slot lease。`release`只清理job metadata与仍需
保留的subtree lock/reservation，不撤销成功filesystem mutation，也不负责释放active-work
capacity。Client process crash不做disk recovery，Server投影outcome unknown。Job的每个active
phase在success/error/cancel/config-replacement与generation retire路径都必须恰好一次释放lease；
不新增用户配置或公开job API。

Same-Client `mode="copy"` job：

- 同时持有 source/destination subtree `PathLocks`；
- revalidate manifest并按§7.3 exclusive创建destination root；
- 逐文件写入 destination-sibling temp、hash/fsync、no-replace publish；
- 记录每个 committed destination fingerprint及本job创建的root/parent directory identity；
- 失败按 §10 精确 cleanup；
- 不使用 WS binary slot，不把 bytes 发给 Server；
- terminal snapshot返回同一 aggregate outcome JSON。

整个 folder copy 不承诺 atomic visibility；其它 OpenOctopus local file operations 因
subtree lock 被挡住，Client exec/PTY与宿主外部进程仍属于诚实的 race boundary。

### 8.3 Same-Client atomic move

Same-Client `mode="move"` 在 subtree locks 和完整 manifest/preflight 后：

1. 要求 manifest 至少有一个 regular file；完全空 source 在任何 mutation 前拒绝；
2. 用相同 10,000-entry/5-MiB bound 做一次 no-follow full revalidation，并要求source root/
   directory identity、merged path/kind set、file size/fingerprint与manifest digest都和初始
   local rename snapshot完全相同；
3. 创建缺失 destination parents，并记录本操作创建的 parents及其identity；
4. 最后一次检查 destination absence；
5. 调用该平台已证明的 exclusive no-replace directory rename；
6. fsync 可用的 destination/source parent directory；
7. 步骤3以后任何未确认commit的出口都条件清理owned empty parents；清理不能确认complete
   则结果为outcome unknown；
8. rename 成功即真实成功，Client 必须在释放 locks/返回前完成结果收敛。

`LocalDirectoryJob` 在第一次 walk 时额外保留一个不出 Client 的 bounded
`LocalRenameSnapshot`：source root与每个directory的opaque identity、`scanned_entries`、
§6.4 `manifest_sha256`，以及每个 regular file 的 verified SHA-256。第一次 scan 在 worker
thread 中逐文件有界流式 hash，并在读取前后核对同一 fingerprint；这些 hashes 只用于
计算成功 response 的 content-tree digest。Manifest digest 已覆盖 scan-only directories
（包括 empty directory）以及每个 regular file 的 path/size/fingerprint。Revalidation
重算并比较 root identity、entry set、kind、size、fingerprint 与 manifest digest；全部相同
时可复用第一次 scan 的 hashes，不必在 irreversible syscall 前再次读取全树。因此同名
增删、file/directory kind 改变、空目录变化和 listed file 漂移都能在 syscall 前失败。
Snapshot 计入同一 10,000/5-MiB operation bound；hash record 另受同一 10,000-entry 固定
记录上限，不加入 REST result 或 Provider transcript。跨 Device manifest wire 仍只通过
§14.1 的私有 action 传输。

Linux/macOS/Windows 使用 Py5 单文件 exclusive move 已采用的 native family，并增加
真实 directory native tests。跨 volume、filesystem 不支持或无法证明 no-replace 时
返回 `workspace_storage_unavailable`，source/destination root 不改变；不 fallback。

Atomic 只表示 destination root 的 directory-entry rename。Preflight read 和 parent
创建不是事务，外部恶意进程仍可能竞态；OpenOctopus 不能把它宣传为 OS sandbox。
Rename 原样携带 source 中已扫描的 empty subdirectories；不能在 rename 前删除它们或在
rename 后异步修剪，否则都会破坏“单次原子目录项移动”的产品语义。Result digest 仍按
§6.4 只覆盖 regular files。

Subtree lock 排除所有经 OpenOctopus dispatcher 进入的重叠写入。它不能冻结宿主
filesystem：full revalidation 与 rename syscall 之间若外部进程新增 entry，该 entry 可能
随 source directory 一起移动；若在 revalidation 中已观察到变化，则返回
`workspace_file_changed` 且不 rename。该窄窗口不承诺 point-in-time snapshot，也不能
被表述为对外部恶意进程的 symlink/special-file sandbox。

### 8.4 Server→Server

`WorkspaceService` 为 source/destination 各保留 immutable authorization ticket，关闭
DB 后进入 `WorkspaceFS`。同一或不同 workspace target 都使用 manifest path：

- 按 entry 顺序 open immutable object stream；
- 比较 source ETag；
- stream 到 private temporary object；
- 验证 size/SHA-256；
- 在 quota reservation + mutation lock 下 promote-if-absent；
- 记录 destination ETag；
- outer move 只在全部 destination commits 成功后条件删除 source ETags。

S3/MinIO copy-object 即使可用也必须满足相同 checksum、no-overwrite、quota、cancellation
和 temp cleanup contract；不能因优化绕过 `WorkspaceFS`。

## 9. 一致性模型

### 9.1 Manifest-defined versions

Py8c 不承诺跨文件同一时点 snapshot。它承诺：

- manifest 记录 scan 时观察到的 path set 和每个文件 version fingerprint；
- listed file 在被打开时必须仍为同一个 regular file/version；
- stream 过程中沿用单文件 identity/fingerprint checks；
- destination bytes 与该 child 已验证的 SHA-256 完全一致；
- manifest 完成后的新增 entry 不自动加入本次 copy；
- listed file 在成功复制后又被外部修改，不改变已经验证的 destination snapshot。

因此一个活跃 source tree 可能在后续尚未传输的 file 漂移时整体失败，也可能在 copy
完成后获得一个由 manifest 各 file version 构成的稳定结果；它不是 filesystem
snapshot API。

### 9.2 Copy drift

以下都在 copy phase 失败并进入 destination cleanup：

- listed file missing/renamed；
- fingerprint/size 不匹配；
- file 变成 directory、link 或 special file；
- transfer size/digest mismatch；
- destination race/no-replace conflict；
- source/destination Device disconnect 或 route generation replacement；
- quota reservation/commit invariant 被破坏。

新增但未列 entry 不属于 manifest/copy，不返回 warning，也不触发第二次 scan。这样
行为有界且可解释，不会在持续写入的目录上永远追赶。

### 9.3 Move drift

Move copy phase 与 copy 完全相同。全部 destination files 完成后，source cleanup 对
manifest 中每个 path 使用原 fingerprint/ETag 条件删除：

- 未变的 listed file 被删除；
- 已missing的listed file视为已达到删除目的，不产生warning；已变、已替换或不可达的path
  保留并产生对应warning；
- manifest 后新增的 file/directory 从未在 delete list 中，始终保留；
- 只对 manifest 中 identity 仍匹配的 scan-only directory entries deepest-first
  `rmdir-if-empty`；无法取得或不匹配 identity 时保留，绝不 recursive delete 未知内容；
- root 因新增/变化 entry 不能删除时，返回 `source_cleanup_incomplete` warning。

Destination 此时是完整、已验证的 manifest snapshot，所以 source cleanup failure 是
成功加 warning，不是 copy failure，也不触发 destination rollback。

上述“保留mismatch path”保证覆盖OpenOctopus协调写者与cleanup实际观察到的identity；宿主外部
进程在最后check与unlink/rmdir syscall之间完成替换时，仍适用§10.2 check-to-use race boundary。

Same-Client atomic rename 不逐项复制或删除：它按 §8.3 在 syscall 前 full revalidate。
重扫比较root/directory identity、merged entry set、file kind/size/fingerprint与manifest
digest；任一漂移即失败。重扫后、syscall 前的宿主外部 race 可能随整个 directory entry
一起移动，是显式的非 snapshot 风险。

## 10. Destination cleanup

### 10.1 精确记录

Coordinator 对每个成功 child 只保留 bounded `CommittedDestination`：

```text
relative_path
destination_fingerprint_or_etag
verified_size
verified_sha256
```

记录数最多等于 manifest file count。Filesystem destination 在
`transfer_directory_prepare` 的 generation-fenced send boundary 调用 outer `on_issued`，
因为 exclusive root mkdir 可能已经发生；Server object destination 在第一次 child
publish 时调用。只读 preflight、skill staging 和 private temporary file creation不算
user-visible issued boundary。

### 10.2 Cleanup algorithm

Copy phase 失败/Stop/cancellation 后：

1. abort 当前 child slot，等待其单文件 temp cleanup/terminal ACK 有界收敛；
2. 按 reverse manifest order 条件删除每个 committed regular destination；
3. missing 视为已清理；fingerprint mismatch 不删除并标记 incomplete；
4. 从 deepest 到 root 只对reservation/job登记为本operation创建且identity仍匹配的
   directory做`rmdir-if-empty`；未登记或identity mismatch的derived parent必须保留并标记
   incomplete，不能因它当前为空就删除。最后同样只处理identity匹配的root与owned ancestors；
5. Server object storage 无 empty directory，删除 exact objects 后检查 prefix absent；
6. `root_claimed=true` 时重新检查 destination root，absent 才是 cleanup complete；若
   exclusive mkdir 已确定 EEXIST、`root_claimed=false` 且 owned parents 已收敛，竞争者的
   root 保持存在仍是 cleanup complete，保留原始 `workspace_file_changed`。

Filesystem条件删除是identity check后立即调用no-follow unlink/rmdir的best-effort序列；Py8c
不新增完整的跨平台handle-relative filesystem层。OpenOctopus subtree lock能排除本进程协调
写者，但宿主外部进程仍可在最后一次check与syscall之间替换component。实现检测到mismatch时
必须保留并投影incomplete；无法观察到的
check-to-unlink race属于Py5/Py7既有诚实边界，文档与测试不得宣称恶意本地写者隔离。

Client destination cleanup不由Server回传一份可伪造的committed path batch；job按自身登记的
commit/root/parent identities在后台串行清理，status只返回aggregate counts/complete boolean。
Client move source cleanup同样不回传delete list：Server只发一次O(1)
`transfer_source_cleanup` command，retained source job在后台按自有manifest串行处理并通过
`transfer_source_probe_status`返回aggregate progress/terminal。Worker可每处理最多256条主动
yield/checkpoint cancellation，但这不是wire batch、payload或多个tool calls。Server source则
由`WorkspaceService`在同一coordinator cleanup phase直接执行有界串行条件删除。两者都不返回
Provider-visible path list。

### 10.3 Error projection

- cleanup complete：保留原始 failure code；
- cleanup 因 fingerprint conflict、Device disconnect、timeout、storage ambiguity 或
  cancellation 无法证明 complete：`tool_execution_outcome_unknown`；
- caller cancellation不能中断已经启动的Server-owned bounded cleanup；cleanup task通过shield
  收敛后再投影最终结果。Client filesystem cleanup按§6.5 cooperative thread边界执行；Server
  reconciliation window到期仍未terminal时投影outcome unknown；
- graceful Server shutdown在进程仍持有coordinator state时shield当前child停止与有界cleanup；若
  原HTTP/Agent transport仍可交付结果，按cleanup证据投影原错误或
  `tool_execution_outcome_unknown`；
- Server crash、SIGKILL或进程丢失没有durable directory journal，原HTTP/Agent调用只表现为
  transport loss，重启后的新进程无法也不得伪造一条结构化`outcome_unknown`旧响应。Caller必须
  把该transport loss视为outcome unknown并检查destination；新进程绝不replay directory child；
- 启动恢复只复用既有`_openoctopus-transfers/`私有temporary-object清扫；没有operation journal时
  不自动删除任何user-visible destination object/root，即使其看起来像不完整directory。

`tool_execution_outcome_unknown` 明确表示 caller 必须检查 destination；Agent/Server
不得自动用相同 `dst_path` 重试，因为 destination 可能已经部分存在。

## 11. Move source cleanup

Source cleanup 只在 `COPY_COMPLETE` 后开始，并与 destination cleanup 完全分离：

1. 置状态 `SOURCE_CLEANUP`；
2. 条件删除 manifest regular files；
3. 继续 best effort 处理后续 entries，不因一个失败停止；
4. 对identity仍匹配的manifest scan-only directories deepest-first
   `rmdir-if-empty`，最后按root identity处理source root；identity unavailable/mismatch
   时保留并警告，manifest 后新增目录不在 delete plan；
5. 汇总 warning，不返回 path 数组；
6. 永远保留完整 destination。

Client source复用§7.0 retained `source` job：one-shot cleanup command只做状态转换并立即返回
accepted，后台worker取得shared local-slot lease后执行，Server用同一source status/release
actions收敛。Server source不创建job，直接在coordinator-owned bounded worker中执行。整个阶段
destination必须保持`FINALIZED_HELD`；只有cleanup完成或明确放弃后才发送destination release。

允许的稳定 warnings：

| Warning | 含义 |
|---|---|
| `source_cleanup_incomplete` | 至少一个 source file/directory 未能按 manifest 条件删除 |
| `source_changed_after_copy` | 至少一个 listed source version 在 copy 后变化或被替换，因此冲突项被保留 |
| `transfer_ack_failed` | Destination 已 commit，但最终 ACK/cleanup communication 未完整；沿用单文件既有警告语义 |

实现可把多个底层相同原因折叠成一个 warning，最多 8 个，固定顺序。Warning 不包含
绝对路径、device secret 或 traceback。

一旦第一个 source delete 开始，Stop/cancellation 不再把 operation 改判为普通失败。
Coordinator 必须有界完成/放弃剩余 source cleanup，然后返回 destination success +
warnings。Client thread仍阻塞在当前delete syscall时，Server可放弃等待并返回
`source_cleanup_incomplete`，但Client job保持cancelling并且只允许继续原先已启动的cleanup
收敛，不重新开始新plan。这样不会因取消把已经删掉的 source 与随后 rollback 的 destination
同时移除。

## 12. Admission、并发与公平

### 12.1 Operation lease

一个 directory transfer 从 source manifest 开始前取得一个 outer operation lease，
一直持有到 destination cleanup 或 move source cleanup 完成：

- 含任一 Client endpoint 时复用 `FairTransferAdmission` 的 global/per-user、per-user
  FIFO + cross-user round-robin 规则；
- 纯 Server transfer 复用现有 Server workspace transfer admission；
- 不增加 directory 专属 settings/semaphore；
- queue 满/超时发生在任何 side effect 前，映射现有 busy error；
- lease release 对 success/error/cancel 必须 cancellation-safe 且恰好一次。

Directory operation 计为一个 active transfer credit，不按 file count 计 10,000 个
credits。内部 child 调用使用 already-admitted API，不递归 acquire；同一 operation
同时最多一个 child slot。

Client 的 `MAX_ACTIVE_TRANSFER_SLOTS=2` 必须由一个 Client-runtime-owned shared local-slot
admission实际执行，而不是分别统计。Wire source/receiver slot、same-Client regular-file
`transfer_local`与任一directory job的active worker phase都从同一admission取得一个lease；
job纯metadata/status/reserved-idle阶段不占lease。Job等待lease发生在manager worker且受既有
queue timeout，绝不占tool worker；不能出现“2个wire slots + 1个active job worker”。底层
`TransferManager`与job worker接受already-acquired lease，不能双重acquire；每个phase的
terminal/cancel/config replacement/generation retire路径恰好释放一次。

该规则也修正既有wire/local-file thread abandonment：coroutine或slot cleanup在blocking thread
仍运行时，lease必须原子转交给ClientRuntime-owned drain record，直到真实thread task以及其
`on_abandoned` cleanup全部完成才释放。Receiver `fsync/commit`、sender `read`、regular
`transfer_local`与directory worker使用同一所有权规则；connection replacement不能靠0.1秒grace
提前归还slot。Runtime同时强引用draining manager/task，防止connection finally后失去watchdog与
shutdown ownership。

### 12.2 Fairness

Fairness 层次固定为：

1. 等待 directory/single-file operations 按现有 per-user FIFO、users round-robin admission；
2. admitted directory 只占一个 credit；
3. 它的 child files 按 manifest 顺序串行；
4. 与其它 admitted transfer 同时向一个 Client 写 binary 时，继续使用现有 writer
   lane round-robin；
5. manifest/preflight/cleanup 私有 calls 仍受 Device pending-call count/byte admission；
6. 不在 event loop 中 blocking scan/hash/copy。

一个大目录会较长时间占一个 active credit，这是有意的 operation-level backpressure；
它不能占用该 user 的全部 global capacity，现有 per-user limit 继续适用。将每个 child
重新排队会让一个逻辑 move 在中途无限期失去资源并制造 10,000 个 waiters，因此不采用。

### 12.3 Timeout

- admission queue 使用现有 queue timeout；
- 每个Client directory start/status/page/one-shot command/release使用现有bounded tool-call
  timeout；source cleanup本体不在该call内运行。Job control call超时后按operation ID+role
  reconcile，绝不重放mutation command；
- 每个 file slot 使用现有 no-progress idle timeout；
- source scan、destination preflight/prepare/finalize/cleanup与`LocalDirectoryJob`本体都用
  `progress_seq`驱动现有no-progress deadline；连续status polling不能续期。Deadline只请求
  cooperative cancel；若当前thread syscall尚未返回，Client job保持cancelling并继续持有其
  shared slot/lock，Server有界reconcile后返回outcome unknown而不伪造local terminal；
- 整个directory operation没有额外wall-clock hard deadline；Client scan/preflight、
  cross-site child bytes与same-Client job的真实progress持续时都可运行超过60秒；
- source status/page与destination preflight/status在mutation command前丢失，允许按同ID
  重查并最终安全失败，destination unchanged；prepare/local-start之后无法reconcile才可能
  outcome unknown。任何mutation command都不因call timeout自动重发；
- Agent 不获得 timeout 参数，保持 `file_transfer` schema 不变。

Idle timeout 以当前 child/cleanup phase 的真实 progress 为准，不能因 Server 在内存中
循环或重复发相同 progress event 无限续期。

## 13. 状态机与生命周期

### 13.1 状态

```text
NEW
  -> ADMITTED
  -> MANIFESTING
  -> PREFLIGHTING
  -> READY
  -> DESTINATION_RESERVING            # prepare/claim send may already have mutated filesystem
  -> DESTINATION_RESERVED             # root claimed or Server subtree lease held
  -> COPYING
  -> FINALIZING                       # verify exact destination tree; finish may be in flight
  -> FINALIZED_HELD                   # COPY_COMPLETE; destination reservation still held
       -> DESTINATION_RELEASE -> SUCCEEDED                         # copy
       -> SOURCE_CLEANUP -> DESTINATION_RELEASE -> SUCCEEDED_WARN  # move

MANIFESTING/PREFLIGHTING
  -> FAILED                                # no destination side effect

DESTINATION_RESERVING/DESTINATION_RESERVED/COPYING/FINALIZING
  -> DESTINATION_CLEANUP
       -> FAILED                           # cleanup proved complete
       -> OUTCOME_UNKNOWN                  # cleanup not provably complete
```

Filesystem prepare 在 mutation-capable send 后即进入 `DESTINATION_RESERVING`；result 丢失也
必须按“root可能已claim”清理。只有 prepare 明确返回operation-owned reservation才进入
`DESTINATION_RESERVED`。最后一个child成功只进入`FINALIZING`；只有
`transfer_directory_finish`（或Server本地exact-prefix scan）确认expected tree后，才能进入
`FINALIZED_HELD/COPY_COMPLETE`并允许source cleanup；成功reservation此时仍不释放。Copy
确认后release，move在source cleanup完成/放弃后release。Server object destination
在首个publish前若仅持有无副作用lease，可直接release并稳定失败；首个publish后遵守同一
cleanup状态。

Same-Client atomic move uses：

```text
NEW -> ADMITTED -> MANIFESTING -> PREFLIGHTING
    -> LOCAL_JOB_RUNNING -> RENAMING -> SUCCEEDED
                         -> FAILED          # no mutation or cleanup proved complete
                         -> OUTCOME_UNKNOWN # commit/cleanup outcome not provable
```

每个 transition 只由 coordinator lock 下的一个 owner 执行。Terminal future、outer
admission、quota reservation、temp staging 和 route refs 全部只能 release 一次。

### 13.2 Cancellation/Stop

| 时点 | 行为 |
|---|---|
| admission/manifest/preflight | 取消，释放资源；destination unchanged |
| prepare send 已发出、result 未知 | 视为可能已claim root；按operation ID shield cleanup，不能重发prepare |
| filesystem root 已 reserve、尚无 child commit | shield exact root/owned-parent cleanup；complete 后原错，否则 outcome unknown |
| child temp 尚未 publish | abort child，清理 temp，再清理 owned root/parents；全部 owned mutation 已确认清除才是普通取消 |
| 已有 destination commit | shield destination cleanup；complete 后原错，否则 outcome unknown |
| finish 已发出、result 未知 | 不进入COPY_COMPLETE；按operation ID查询/cleanup，不能开始source delete |
| `FINALIZED_HELD`、尚未删 source | copy先release destination再success；move可决定不启动delete，release后返回success + `source_cleanup_incomplete` |
| source cleanup 已开始 | 有界完成best effort，再release destination；success + warnings |
| atomic rename 已进入 irreversible syscall | 等待 syscall true result；不把线程取消误报为未执行 |

Agent Stop 不发送 10,000 个 cancel，也不 replay child。单文件 slot 的 late chunks/ACK
仍由原 generation tombstone 消费。Same-Client job只发送一次`cancel`并在Server侧bounded poll；
若cooperative worker已收敛则返回真实terminal，若thread syscall仍在运行、generation已失去或
无法确认cleanup则返回outcome unknown。后者不会把Client job标成terminal：旧generation manager
继续持有runtime-shared slot与subtree reservation，直到worker返回并收敛，或Client进程退出。

### 13.3 Device disconnect/replacement

- read-only preflight 前/中 source 或 destination offline：`tool_device_unreachable`，无写入；
- filesystem prepare 已发出但 result 丢失：root 可能已创建；执行 generation-bound cleanup，
  无法确认 absent 则 outcome unknown；
- copy 中 source disconnect：abort current child，尝试 destination cleanup；
- copy 中 destination disconnect：当前/已写结果可能未知，cleanup 无法完成即 outcome
  unknown；
- `FINALIZED_HELD`后destination generation retire：Client保留已验证destination tree并释放
  进程内reservation，绝不把它当failure cleanup删除；Server已观察finalized snapshot时保持
  destination success，未观察时caller outcome unknown。Move不得在失去destination generation
  后开始新的source delete；已开始cleanup则有界停止/完成best effort并返回warning；
- move copy complete 后 source disconnect：停止新的source delete，保留完整destination，
  destination release后返回source cleanup warning；
- config epoch 改变后不再开始新child，operation转入abort/cleanup；只要immutable device与
  WebSocket generation未变，携exact operation ID的owner cleanup/status/cancel/release仍可
  按该reservation/job捕获的旧path-policy snapshot执行，避免已创建root/job泄漏。它们不得
  扩展path set或重新开始copy；
- connection replacement 后不能再向旧 generation 发action。旧Client的retire hook负责本地
  按状态请求收敛：pre-finalized job触发cooperative cancel/conditional cleanup，
  `FINALIZED_HELD`按上文保留destination并只解锁；没有阻塞syscall的active phase正常释放shared
  slot。同一ClientRuntime内普通reconnect时，仍在syscall中的旧manager继续持有runtime-owned
  shared slot/lock直到真实退出，防止新generation额外获得两个slot；Server不能确认时投影outcome
  unknown。Cleanup不迁移到新generation，也不重放可能已进入旧transport的action；
- 若另一个Client进程使用同一Device token触发`connection_replaced`，两进程没有共享semaphore或
  subtree lock。旧Client收到replacement后永久停止重连、请求job收敛并在grace后退出；但新进程
  可能已开始工作，旧阻塞syscall到真实进程退出前仍属于明确的external-process race boundary。
  Replacement路径必须复用现有ClientRuntime shutdown watchdog：立即arm，只有所有retired
  manager/drain quiescent才cancel；否则现有15秒`_SHUTDOWN_WATCHDOG_SECONDS`到期调用hard exit，
  不能让default-executor thread无限阻止进程退出。同一Device token同时运行多个Client进程仍是
  unsupported topology，不能声称runtime-owned lock能跨进程隔离；
- late result/chunk 不能推进已 terminal 的 directory state。

Route identity 使用 immutable device ID + captured name/config generation。Device 被删除
后同名重建不能接收旧 operation 的 file、delete 或 cleanup。

## 14. 私有 Device actions 与 Protocol v3

### 14.1 Actions

`__workspace_rest__` strict action union 增加/扩展：

| Operation | 方向 | 作用 |
|---|---|---|
| `transfer_source_probe_start` | Server→source Client | 创建只读kind/manifest scan job并立即返回running |
| `transfer_source_probe_status` | Server→source Client | 返回source job progress/state或bounded terminal header |
| `transfer_source_probe_page` | Server→source Client | 读取READY immutable manifest的有界page |
| `transfer_source_probe_hold` | Server→source Client | 分页完成后保留source job供cross-site child与move cleanup使用 |
| `transfer_source_probe_cancel` | Server→source Client | 请求停止任一未terminal source job并后台收敛 |
| `transfer_source_probe_release` | Server→source Client | 释放scan job与retained manifest |
| `transfer_directory_authorize_source_child` | Server→source Client | 一次性绑定operation ID、source path/fingerprint与transfer UUID |
| `transfer_directory_preflight` | Server→destination Client | 创建destination job，后台验证root/path set/collision |
| `transfer_directory_status` | Server→destination Client | 返回preflight/prepare/copy/finalize/cleanup有界snapshot |
| `transfer_directory_prepare` | Server→destination Client | 请求后台exclusive root claim/reservation |
| `transfer_directory_authorize_child` | Server→destination Client | 一次性绑定operation ID、transfer UUID与exact child path |
| `transfer_directory_finish` | Server→destination Client | 请求后台验证expected tree并成功finalize |
| `transfer_directory_cancel` | Server→destination Client | 请求停止并按job-owned identities后台cleanup |
| `transfer_directory_release` | Server→destination Client | Server确认成功/terminal后释放job metadata与subtree reservation |
| `transfer_source_cleanup` | Server→source Client | one-shot启动retained source job后台条件删除并立即返回accepted |
| `transfer_local` | Server→same Client | 保留既有regular-file local copy/move |
| `transfer_local_directory_start` | Server→same Client | 原子升级READY destination job并立即返回running |
| `transfer_local_directory_status` | Server→same Client | 返回有界progress/terminal snapshot |
| `transfer_local_directory_cancel` | Server→same Client | 请求job停止并执行必要cleanup，不等待整棵目录 |
| `transfer_local_directory_release` | Server→same Client | Server确认terminal后释放job metadata与残留subtree reservation |

所有 model `extra="forbid"`，path/list/count/byte bound 与 Server model 对称。Source
manifest page最多256 entries/256 KiB，完整manifest/destination-preflight payload最多5 MiB；
source cleanup command/status只有O(1) parameters与aggregate counters，不携entry batch。
Client返回malformed status/page/result时，该
directory call映射`workspace_transfer_integrity_failed`，不把任意JSON当成plan/delete list。

Directory manifest只在`transfer_source_probe_status`进入ready后通过immutable page actions
取回；page不是第二次discovery，也不能在scan中途作为partial plan消费。

这些 actions 是 OpenOctopus 私有 tool calls，不进入 Provider registry。它们复用现有
`tool_call/tool_result` frame，所以不增加公开 WS frame。

### 14.2 Transfer frames

Binary slot sequence、UUID-v7 header、64 KiB chunk、ready/end ACK、idle timeout、SHA-256
和 tombstone 全部沿用 Py8b。Py8c 只更新两点 canonical semantics：

1. Directory child 仍是 `purpose="file_transfer"` 的一个普通 regular-file slot；
2. 成功 receiver ACK 对 `file_transfer` 返回 `etag` + `created=true`，供精确 cleanup。

没有 `transfer_source_probe` frame、directory chunk type、archive frame、range offset 或
resume token。Manifest 绝不能塞进 binary data lane冒充文件内容。

### 14.3 Protocol version

Py8c 不改变 frame discriminator、字段集合、binary layout 或 hello capability shape，
因此保持严格 `version="3"`。官方 Server/Client 同仓协调升级；不加入旧 Client
compatibility shim。契约 fixtures 增加现有 frame 字段的新合法组合和 private action
payload，不创建 Protocol v4 fixture directory。

## 15. Server 与 Client 模块边界

### 15.1 Server

推荐最小边界：

- `tools/file_transfer.py`
  - 保留 public request/schema/error projection；
  - 根据 prepared source kind 分派 file/directory；
  - 统一 Agent/REST result。
- `tools/directory_transfer.py`
  - manifest model/digest validation；
  - preflight/state machine/child sequencing；
  - committed tracking、destination/source cleanup；
  - 不直接访问 RustFS 或 WebSocket transport。
- `workspace/service.py`
  - 授权 directory tickets；
  - Server manifest/preflight/quota reservation/skill validation facade；
  - cache invalidation。
- `workspace/fs.py`
  - paged exact-prefix scan；
  - quota reservation；
  - ancestor-aware Server subtree operation lease，且所有 supported mutations参与；
  - per-object temp/commit/conditional delete/prefix absence checks。
- `devices/workspace.py`
  - 私有 probe/preflight/prepare/finish/cleanup action/result strict DTO 与 dispatch。
- `devices/transfer.py` 及 Py8b relay
  - already-admitted child API；
  - externally allocated child UUID与authorize-before-begin ordering；
  - committed destination metadata；
  - 继续拥有 file slot，不拥有 directory policy。

`directory_transfer.py` 是多阶段 orchestration 的独立模块，不把第二套 byte pump、path
resolver 或 quota logic复制进去。

### 15.2 Client

- `tools/workspace_rest.py`
  - 私有 source/destination/local job action、page、snapshot、cleanup DTO；
- `tools/dispatcher.py`
  - 只做strict action dispatch和O(1)/bounded page response；
  - directory worker task绝不登记进现有`_blocking_tasks`；
- Client connection runtime / transfer admission
  - 持有独立generation-bound `DirectoryControlWorker`与固定queue capacity 8，并在普通
    `_ToolWorker`之前按strict directory action discriminator路由；
  - 持有共享 `MAX_ACTIVE_TRANSFER_SLOTS=2` lease counter；
  - wire slots与directory job active work、regular `transfer_local`共用；
- `tools/directory_jobs.py`
  - connection-generation-owned `DirectoryJobManager`与bounded registry；
  - source scan/immutable pages、destination preflight/reservation/finalize/cleanup；
  - local recursive copy/atomic move、progress、terminal retention/release；
- `tools/locks.py`
  - 继续使用已有 ancestor-aware subtree overlap，不能退化成 exact-path-only locks；
- `transfer.py`
  - 单文件 source fingerprint check、destination commit metadata；
  - 不增加 directory state machine；
- `writer.py`
  - 继续 binary lane round-robin，无 directory 专用 lane。

Server/Client 可各自实现 digest encoder，但必须共享固定 vectors 测试，而不是导入对方
package。

## 16. 错误与 caller 行为

不新增目录专属 error code；复用现有稳定语义：

| 条件 | Code / result |
|---|---|
| source 不存在 | `workspace_not_found` |
| source 是 link/junction/reparse | `workspace_symlink_escape` |
| source/child 是 special file | `workspace_blocked_path` |
| source 完全空或 src/dst overlap | `workspace_invalid_request` |
| manifest >10,000 entries 或 >5 MiB | `workspace_directory_too_large` |
| destination root/path 已存在 | `workspace_file_changed` |
| destination parent 是 file | `tool_not_a_directory` |
| destination platform 无法表示/collision | `workspace_invalid_request` |
| source listed version 漂移 | `workspace_file_changed` |
| malformed target SKILL.md | `workspace_invalid_skill_format` |
| Server soft lock/quota/single-op cap | 现有 workspace quota errors |
| queue full/timeout | `workspace_transfer_busy` 或 `tool_device_busy`，沿现有 route projection |
| Device pre-issue offline | `tool_device_unreachable` |
| no-progress | `workspace_transfer_timeout` |
| child size/digest/manifest result invalid | `workspace_transfer_integrity_failed` |
| destination cleanup complete | 原始 copy failure code |
| destination cleanup 不能确认 complete | `tool_execution_outcome_unknown` |
| move source cleanup 不完整 | success + bounded warnings |
| same-Client cross-volume/无 exclusive rename | `workspace_storage_unavailable` |

REST 延续现有 HTTP mapping；`workspace_directory_too_large` 为 413，busy 为 429，timeout
为 408，integrity 为 502，outcome unknown/conflict 为 409。HTTP error body 仍只有
`code/message`，不增加可能超大或泄漏本地路径的 `items`。

Tool error 是普通 `tool_result(is_error=true)`，不停止 Agent loop。Outcome unknown 的
message 必须明确“检查 destination 后再决定”，不能建议自动 retry。

## 17. 安全、资源与可观测性

### 17.1 Path 与内容安全

- 不 follow source/destination 的 symlink、junction 或 reparse component；
- Client `restrict_to_workspace` 分别应用于该端 source/destination；值为 false 仍不允许
  link/special transfer；
- Server manifest path 经过独立 strict validation 后才映射 destination；
- 不解包 archive，因此没有 archive traversal/zip bomb contract；
- 临时文件/objects 使用随机私有名字，不进入 user-visible tree；
- no-overwrite 由 commit primitive 保证，不靠 preflight check alone；
- cleanup 使用 fingerprint/ETag；实际观察到后来替换的path不删除，最后check之后的宿主外部
  race按§10.2诚实边界处理；
- source/destination bytes、manifest paths、headers/token 不写日志。

### 17.2 Memory、task 与 temp bounds

- 每Client generation最多2个directory jobs；每job manifest/committed records最多10,000、
  encoded manifest最多5 MiB，terminal/release/TTL后回收；
- source manifest page最多256 entries/256 KiB；
- 一个 directory operation 一个 child task/slot；
- chunks 继续最多 64 KiB，沿用 bounded queues；
- SKILL validation sequential staging，不同时保留全部 content；
- Client cleanup worker每处理最多256 records主动yield/checkpoint，但不创建wire batch/call；
- Server object scan 分页，不一次读取 bucket 全部 metadata；
- Client walk/hash/copy在manager-owned thread/background boundary，且不进入dispatcher
  `_blocking_tasks`；
- temp staging 在每个 file/validator 完成后尽快删除；
- operation terminal 后没有 lingering timer/task/lock/admission/reservation。

### 17.3 Observability

允许的聚合 metrics/log fields：

```text
direction
mode
source_kind
manifest_entry_bucket
total_bytes_bucket
duration
terminal_state
cleanup_complete
warning_codes
```

不得记录 raw source/destination paths、file names、fingerprints、digests（除 debug test
fixture）、Device token 或文件内容。Metrics 必须能观察 active/waiting directory
operations、active child slots、quota reservations、cleanup failures 和 temp cleanup
backlog，且 label cardinality 有界。

## 18. TDD 实现顺序

每个 slice 先写失败 contract/lifecycle test，再写最小实现；每个 slice 独立保持
Server/Client lint、typecheck 和相关 suite 通过。

### Slice A：contract、manifest 与 digest

1. 更新测试中的 `file_transfer` schema expectation：不新增字段，description 支持
   file/directory，Py8b 四方向约束保留。
2. 添加 Server/Client strict manifest models、canonical encoder/digest vectors。
3. 测试 10,000/10,001 entry、5 MiB exact/+1、overflow、duplicate、unsorted、非法 path、
   malformed fingerprint/digest。
4. 添加source job status/page与destination/local job control strict DTO tests；
5. 添加统一 file/directory aggregate result union 和 REST DTO tests。

**Proof：** 两端固定 vectors 一致；所有 bound 在分配/dispatch 前生效；普通单文件
tests 保持通过。

### Slice B：Client recursive primitives

1. 实现 no-follow bounded walk，先覆盖 Linux fixture；
2. 加入 hidden/noise、zero-byte、empty-dir、empty-tree、permission 和 special-file tests；
3. 实现`DirectoryJobManager`、source start/status/page/cancel/release和完整后才可读的immutable
   manifest pages，并实现hold/source-child authorization/one-shot background source cleanup；
4. 实现destination job的preflight/status/prepare/authorize/finish/cancel/release与exclusive
   root reservation；
5. 实现`LocalDirectoryJob`的start/status/cancel/release；
6. 把wire slots、regular`transfer_local`与job active work接到同一个capacity=2的Client-runtime
   admission；
7. 实现job内directory copy + conditional cleanup与exclusive atomic directory move；
8. 增加cancellation-safe subtree locks/temp cleanup、terminal retention/release，并证明job
   worker不进入dispatcher `_blocking_tasks`；实现独立capacity=8的`DirectoryControlWorker`。

**Proof：** local copy 逐文件原子；fault injection 后 destination absent 或明确 unknown；
local move 用一次 native rename，cross-volume 不 fallback。

### Slice C：Server workspace primitives

1. exact prefix manifest scan，不使用 noise filter；
2. Server destination mapping/root absence；
3. ancestor-aware prefix lease与所有 supported mutations integration；
4. aggregate quota reservation 与普通 writes 的 projected usage integration；
5. per-object admitted copy/commit metadata/conditional delete；
6. personal Skills streaming validation；
7. cancellation/restart temp recovery tests。

**Proof：** 两个并发 reservations 不能超 quota；每个 child commit 不重复计算目录
single-op cap；RustFS 没有 partial object/root marker 泄漏。

### Slice D：Cross-site coordinator

1. directory operation admission/state machine；
2. pre-issue Client source job strict union/paged manifest与private destination job；
3. destination prepare与per-child UUID one-shot authorization；
4. sequential child calls接入 existing single-file paths；
5. client A→client B 接入 Py8b already-admitted bridge；
6. content-tree digest 与 aggregate result；
7. destination job exact cleanup与retained source job one-shot background cleanup/status。

**Proof：** 四方向 copy 在同一 coordinator contract 下通过；同一 operation 任意时刻
最多一个 child slot；manifest/preflight failure 零 destination commit。

### Slice E：Move、race 与 lifecycle

1. copy-all-then-source-cleanup；
2. conditional delete、new/changed source warning；
3. Stop/cancellation at every state；
4. source/destination disconnect、connection replacement、late result/chunk；
5. cleanup complete vs outcome unknown projection；
6. shutdown/admission/quota/temp/tombstone leak tests。

**Proof：** fault injection 证明 copy phase 从不删 source；source cleanup failure 永远保留
完整 destination；无法确认 destination cleanup 时从不报告普通可重试失败。

### Slice F：跨平台、真实 E2E 与 canonical docs

1. Windows junction/reparse/reserved-name/case collision native tests；
2. macOS symlink/Unicode/exclusive directory rename native tests；
3. Linux FIFO/socket/symlink/cross-volume tests；
4. Docker PostgreSQL + RustFS + 两个真实 Client 全方向 E2E；
5. capacity/event-loop/slow peer/large manifest gates；
6. 更新全部 canonical docs/fixtures，清除旧“folder deferred”表述。

**Proof：** CI matrix、真实 E2E、静态审查和 canonical documentation audit 全通过。

## 19. 测试矩阵

### 19.1 Unit 与 contract

- schema 仍无 `recursive`/`overwrite`/新工具；
- REST/Agent 进入同一 orchestration；
- Client source probe 对 file/directory 返回 strict union，且 file probe 后的 begin
  fingerprint必须匹配；绝不先 issue单文件再fallback；
- file probe覆盖terminal`succeeded`、start/result loss后的status、terminal TTL、exact release
  tombstone，以及release后already-admitted file slot不与scan worker双占Client capacity；
- source scan超过60秒但持续progress时status仍响应；manifest只在READY后分页，覆盖page
  256/257与256 KiB边界、丢失重取、重复/缺失/乱序、digest/offset mismatch、retrieval lease、
  terminal outcome tombstone与release TTL；
- READY retrieval lease只由新contiguous offset刷新；HELD lease只由严格增加的
  `outer_progress_seq`刷新，重复/倒退值不刷新。分别覆盖READY expiry零destination副作用、
  HELD在partial copy前/中触发destination cleanup，以及`FINALIZED_HELD`后expiry不删source、
  copy保持success、move返回source-cleanup warning并release destination；source_cleanup active
  仍按no-progress规则收敛；
- destination `READY/ready_not_started` abandon不被重复status续期，到期回收后第三个job可进入；
  已claim root的stalled job到期执行conditional cleanup，`FINALIZED_HELD`到期保留destination、
  release reservation并留下bounded outcome tombstone；
- destination preflight/prepare/final scan超过60秒但status/cancel仍响应，且manager-owned
  worker不被dispatcher `_wait_for_dispatcher_blocking`等待；
- fault-injected filesystem syscall阻塞时，cancel后job保持cancelling、锁与shared slot不释放且
  不出现late-terminal mutation；解除阻塞后才收敛并回baseline。Server reconciliation window
  到期时，纯source scan/read-only preflight稳定返回timeout且destination unchanged；
  mutation-capable phase返回outcome unknown。另一个shared slot与directory control lane仍可用；
- Server reconciliation window精确复用一次`device_transfer_idle_timeout_seconds`，重复status或
  第二次cancel不重置deadline；
- Stop在partial copy时只置`stop_forward_work`，随后destination rollback仍能执行unlink/rmdir；
  cleanup自身stall时才置独立`stop_cleanup`并按证据投影outcome unknown；
- 无关普通`delete_folder`、`grep`或长`__workspace_rest__`占用`_ToolWorker`时，独立directory
  control lane的status/cancel仍在bounded时间返回；第9个pending control稳定busy；
- source exact file、directory、missing、link、junction/reparse、FIFO/socket/device；
- hidden/noise directories 全包含；
- manifest/copy 的 nested empty dirs 不进入 destination/result，完全空 source 失败；
- same-Client atomic move 对非空 source 原样保留 nested empty dirs，且只执行一次
  exclusive rename；
- same-Client atomic move 在 rename 前 full revalidation；新增、删除、fingerprint 或 kind
  以及root/directory identity或manifest digest漂移失败且不mutation，OpenOctopus并发写由
  subtree lock阻塞；
- same-Client atomic move 第一次 scan 对每个 regular file 做 bounded streaming SHA-256，
  读前/读后 identity 一致；成功 aggregate digest 与相同 tree 的 copy path fixed vector一致；
- native fault test 在 revalidation 后由外部进程竞态新增 entry，验证实现不宣称
  snapshot，并记录该 entry 可能随 directory rename 的允许结果；
- manifest entry/byte exact boundaries、integer overflow、path length；canonical JSON覆盖中文、
  combining Unicode、quote、backslash、control escaping、5 MiB与256 KiB exact/+1 bytes，且
  Server/Client encoder与page split完全一致；
- manifest/content digest fixed vectors；
- local rename manifest-digest revalidation vectors，覆盖 empty directory 与
  file/directory kind；
- Client filesystem manifest root/directory identity、同名删除重建，以及identity unavailable
  时在destination mutation前整体拒绝；
- duplicate、ancestor-file conflict、case/Unicode collision；覆盖macOS/Windows保守key在
  case-sensitive volume上仍拒绝，以及scan-only empty dir不参与copy collision；
- destination root file/dir/link/special/absent races；
- filesystem preflight-to-exclusive-mkdir race、owned root/parent cleanup、expected child set、
  EEXIST时不删除/等待竞争者root、prepare result loss、finish exact-root
  rescan/extra entry/fingerprint drift与release；
- destination进入`FINALIZED_HELD`后，copy由显式release解锁；move在source cleanup完成/放弃前
  reservation持续阻止其它OpenOctopus mutation；严格递增的source-cleanup progress刷新其idle
  lease，重复值不刷新，stall expiry才release并产生warning；generation retire保留destination
  不做rollback；
- source/destination job start/status/page/command result loss与same-ID idempotency；同ID不同
  digest拒绝、generation replacement、active/retained job count与5 MiB memory回baseline；terminal
  transition立即移出active count，retained/outcome tombstone的status/release都返回exact完整结果，
  两个terminal记录不能阻止第三个active job；
- directory lifecycle credits在current/retired managers之间固定为4096；覆盖4096/4097 boundary、
  start cancellation、active→retained→outcome/released tombstone所有权转移、TTL purge回收，以及
  满载时新start在work前稳定busy；
- same-Client同一operation的`source`与`destination` role key互不冲突；source release后local
  start只原子升级既有READY destination job，缺失/mismatched preflight不能创建第三个job，
  role-scoped tombstone不会吞掉另一role的result；
- local start未执行而result丢失时，后续local status返回`ready_not_started`且destination
  unchanged；start已升级但result丢失时返回running/terminal，两个分支都不重发start；
- non-owner overlap在Client立即busy，owner finish/cleanup不会被FIFO worker中的等待者HOL
  deadlock；
- child authorization exact operation/generation/UUID/path、single-use、wrong ordinary call、
  duplicate/expired与lost-result-no-begin；
- Server object exact-vs-prefix invalid shape；
- Server subtree lease阻止其它OpenOctopus writes merge到destination prefix，成功release或failure
  cleanup后释放；
- aggregate quota/single-op/soft-lock/reservation race/release；
- malformed/valid single-file和folder SKILL.md，validate-version drift；
- per-file temp/size/hash/fsync/no-replace；
- directory child destination ACK要求etag/created=true，Server捕获后向source转发的ACK移除
  destination metadata且不触发sender terminal mismatch；
- cleanup fingerprint match/missing/mismatch；
- 宿主外部进程替换/新建空derived parent时cleanup不做path-only rmdir，保留该目录并返回
  outcome unknown；
- Client source cleanup只发送一次command，内部256-record yield/checkpoint前后都通过status
  推进且不产生第二个wire cleanup call；
- move delete 顺序与 warnings；listed source missing算删除目的已达到且不产生warning，
  fingerprint mismatch产生`source_changed_after_copy`；
- no Provider-visible per-item arrays；
- file/directory success 都返回 kind/count/bytes/digest/warnings；file count固定为1。
- same-Client local job持续真实progress超过60秒仍成功；stalled job触发no-progress timeout，
  Agent Stop/config update/connection retire分别cancel并收敛，start-result-lost不重复start，
  terminal后无ghost job/lock/shared-slot lease；
- config epoch在root claimed、child active、local job running三处变化时停止新child，但同一
  WS generation的exact owner cleanup/status/cancel/release可用旧snapshot收敛；connection
  replacement只靠旧Client retire hook，Server不向新generation迁移或重放；
- `connection_replaced`碰到blocked directory/wire syscall时arm既有shutdown watchdog；quiescent
  时取消watchdog，未quiescent时15秒后hard-exit callback恰好调用一次；
- shared local-slot capacity同时覆盖wire source/receiver、regular local file与directory job，
  任意组合都不超过2且无double-acquire/release。
- blocked wire sender-read/receiver-fsync/regular-local worker在connection retire后把shared lease
  转交runtime drain owner；真实thread与abandoned-result cleanup完成前，新generation不能重用该
  capacity，ClientRuntime始终强引用retired manager/drain record；
- 相同Device ID的source/destination执行overlap拒绝；两个不同Device ID不交换或比较物理
  workspace identity，相关部署约束在canonical docs中明确为unsupported topology。
- graceful Server shutdown收敛当前coordinator；SIGKILL/restart只产生transport loss、不会伪造
  old-call result或replay child，启动恢复只清理private transfer temporaries而不删除partial
  user-visible destination。

### 19.2 Direction E2E

以下每一行都覆盖 `copy`、`move`、nested paths、dotfiles、zero-byte file、至少一个
multi-chunk file、destination conflict、source drift 和 cancellation：

| Source | Destination | Required E2E |
|---|---|---|
| Server personal | Server personal | real RustFS |
| Server personal | Server shared | real RustFS + membership/quota |
| Server | Client A | real WS + native filesystem |
| Client A | Server | real WS + RustFS |
| Client A | Client B | two real WS clients + Py8b bridge |
| Client A | Client A | private local copy + atomic move |

Move E2E 额外 fault points：最后一个 child ACK 前、copy complete/source cleanup 前、
source cleanup 第一个/中间/最后一个 delete、Client disconnect、Server cancellation。

### 19.3 平台 native tests

- Linux：symlink、FIFO、Unix socket、`renameat2(RENAME_NOREPLACE)` directory、不同
  mount/volume；case-sensitive raw UTF-8 collision vectors，并记录casefold/case-insensitive
  volume为unsupported topology；
- macOS：symlink、NFD/NFC filename collision、`renameatx_np(RENAME_EXCL)` directory；
- Windows：file/directory junction、generic reparse point、reserved names、case-insensitive
  collisions、directory no-replace rename、locked file cleanup；
- 三平台：external writer 在 manifest 后新增、替换 listed source、替换 committed
  destination。

Same-Client move 的 external-writer case 分两类断言：revalidation 观察到变化时必须在
rename 前失败；测试钩子把变化精确放在 revalidation 后、syscall 前时，允许 whole-tree
rename 携带该变化，但不得误报“manifest snapshot”或再做破坏原子性的事后修剪。

不能只 monkeypatch `os.name` 声称 native proof。缺少权限创建某类 special entry 的 CI
runner可显式 skip，但至少一个受控 native lane 必须覆盖每个平台的 link/reparse 和
directory rename。

### 19.4 并发、容量与 event-loop

- 500 个并发 tool calls 到 admission boundary，active/global/per-user 不超过配置；
- 同一 user FIFO、users round-robin；取消 queued waiter 后无 ghost lease；
- 10,000-entry manifest 不产生 10,000 tasks/slots/writer lanes；
- 多个大目录各自最多一个 child，binary writer lanes 公平轮转；
- slow source/destination 不阻塞 ping/pong、chat、health 或其它 device control frames；
- Server RSS/task/temp/manifest memory 有明确上界；
- quota reservations、path locks、transfer slots、pending calls、tombstones、temp cleanup
  tasks 在终态回到 baseline；
- no-progress timeout 与持续 progress 的长目录分别正确；
- Server shutdown/restart 不 replay directory child。

### 19.5 CI commands

从 `server/` 使用项目 Conda 环境运行：

```bash
conda run --no-capture-output -n oo pytest
conda run --no-capture-output -n oo ruff check .
conda run --no-capture-output -n oo mypy src tests
```

从 `client/` 运行完整 pytest、Ruff、mypy、PyInstaller/frozen smoke 和各平台 native
jobs。Focused directory tests 可先跑，但合并 gate 不能只报告 focused suite。

真实 E2E 必须启动 PostgreSQL、RustFS、Server 和两个独立 Client process；in-process
fake transport 只算 unit/integration helper，不算 Client A→Client B 验收。

## 20. Canonical docs 更新

实现 PR 必须同步：

- `docs/TOOLS.md`
  - `file_transfer` 改为 regular file 或 recursive directory；
  - 四方向、empty/link/special/no-overwrite、result/error/warning；
  - 移除“folder/client-to-client rejected”。
- `docs/PROTOCOL.md`
  - 说明 directory orchestration 复用单文件 slots/Py8b bridge；
  - 更新成功 `file_transfer` ACK 的 ETag metadata；
  - 增加 private probe/preflight/prepare/finish/cleanup actions、state/cancellation；
  - 从 out-of-scope 删除 folder/client bridge，保留 range/resume。
- `docs/API.yaml`
  - transfer endpoint 支持 directory/all directions；
  - strict aggregate response、413/busy/outcome-unknown；
  - 无 `recursive`/`overwrite` field。
- `docs/DECISIONS.md`
  - ADR-087 标记 Py8b/Py8c 已实现；
  - 用 Py8c 终态语义覆盖 ADR-087 的旧 cleanup warning：destination cleanup 无法证明
    absent 时是 `tool_execution_outcome_unknown` error；只有完整 destination 已确认后的
    move source cleanup 失败才是 success warning；
  - 修正 ADR-087 的 input 文字：Agent 可省略 `mode` 并默认 `copy`，REST 仍要求显式
    `mode`，其它四个字段都 required；
  - 明确同一 Client atomic rename 与 Server object-prefix 例外；
  - ADR-088 明确 manifest/copy 不保留 empty dirs、empty source 拒绝，并记录
    same-Client atomic rename 的结构保持例外；
  - milestone 表更新 Py8 三切片结果。
- `docs/SYSTEM_PROMPT.md`
  - skill folder install 描述变为真实可用；
  - “atomic reject”只指全部 SKILL validation 发生在 destination commit 前，不宣称
    cross-site whole-tree atomicity。
- protocol/tool/API fixtures 与 error-code snapshot
  - 更新既有 DTO/ACK combinations；若没有新 error enum，不制造新 snapshot entry。

`docs/SCHEMA.md` 不需要新表/column。若实现发现必须持久化 manifest/job/reservation，
说明设计已经越过本 spec 边界，应停止并重新评审，而不是静默加 schema。

## 21. Acceptance gate

Py8c 只有同时满足以下条件才完成：

1. 同一 `file_transfer` 自动处理 file/directory，Provider schema 没有 recursive/new tool；
   两种 success 都返回统一 kind/count/bytes/digest/warnings aggregate。
2. Server↔Server、Server↔Client、不同 Client、同一 Client 全方向 copy/move E2E 通过。
3. 同一 Client folder move 真实使用 exclusive atomic directory rename；Server
   object-prefix 路径没有虚假 atomic 声明。
4. Destination root exists 始终拒绝；filesystem root由exclusive mkdir原子占有并持有
   operation reservation，Server prefix由subtree lease保护；每个file publish是atomic
   no-replace，不能退化成merge。
5. Hidden/noise regular files 全包含；任意 symlink/junction/reparse/special 使 preflight
   整体失败。
6. Manifest/copy 不保留 empty subdirectories；完全空 source 在任何 destination write
   前失败；same-Client atomic rename 对非空 source 原样保留 empty subdirectories。
7. 10,000 entries/5 MiB bounds、canonical digest 和跨端 vectors 全通过。
8. Listed source drift 失败；manifest/copy 扫描后新增 entry 不被传输；same-Client
   rename 前用root/directory identity与file fingerprint做full revalidation并捕获已可见漂移；move cleanup
   不删除已观察到identity mismatch的同名重建目录，且文档明确最后check后的外部race不属于
   point-in-time snapshot或OS sandbox保证。
9. Server destination 在写入前按 total bytes 完成 quota/single-operation reservation，
   并发写不超配。
10. 所有目标 personal Server SKILL.md 在第一笔 destination commit 前验证；任一 invalid
    时零 destination entry。
11. 非 rename path 同时最多一个 child slot，文件逐项 size/SHA-256 验证且不 materialize
    整个 file/tree。
12. Copy fault/cancel 后只条件清理本操作创建项；清理不完整一律 outcome unknown。
13. Move copy phase 绝不删 source；完整 copy 后才条件 source cleanup，失败保留完整
    destination 并返回 bounded warning；destination reservation在source cleanup完成/放弃前
    一直保持`FINALIZED_HELD`，显式release是在线成功路径唯一unlock。
14. Admission 对 user 公平、队列/内存/task/temp/manifest/cleanup 全部有界，ping/health
    在压力下保持响应。
15. Client wire slots、regular local file与directory job共用capacity=2 admission；local job
    可超过60秒持续progress、可被Stop/config/retire请求收敛；filesystem syscall返回后没有ghost
    job/lock/lease，syscall尚未返回时保持cancelling与原lease而不虚假释放。
16. 没有公开新 WS frame、没有 Protocol v4、没有 replay/resume/range/dedup/compression。
17. Server/Client 完整 tests、Ruff、mypy、frozen/native CI、真实双 Client E2E 和
    canonical docs audit 全通过。

## 22. 决策状态

本设计接受后：

- Py8b 是 Py8c 的硬依赖；
- ADR-087 的 client-to-client 与 recursive directory deferred 项分别由 Py8b/Py8c
  落地；
- ADR-088 的 implicit-directory 存储模型保持不变；
- range/resume/dedup/compression 与跨 site atomic snapshot 继续 deferred；
- 任何要求 manifest/copy preserve empty directories、merge/overwrite 或 durable
  directory jobs 的后续需求都必须单独设计，不能作为 Py8c implementation detail
  偷渡；same-Client atomic rename 的结构保持例外不扩展到其它路径。
