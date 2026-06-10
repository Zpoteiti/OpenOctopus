# Plexus M1d Workspace Files and Attachments Sub-Spec

**Status:** Verified
**Parent:** [Plexus M1 Living Design Spec](2026-05-12-plexus-m1-living-design.md)
**Branch:** `rebuild-m1-M1d`
**Base:** `rebuild-m1`
**Authors:** brainstormed in collaborative session 2026-05-18
**Supersedes:** M1c browser inline-image exception as the only browser image path

---

## 1. Goal

M1d turns the M1c browser chat spine into a server workspace-aware chat path.
It implements the server-side workspace file APIs, server file tools, a shared
quota-aware `workspace_fs` service, strict message ingress, and attachment
expansion from workspace files into provider-ready content blocks.

The success proof is intentionally server-scoped:

- browser callers can read, write, edit, list, search, and delete files in the
  authenticated user's server workspace through REST;
- server-side file tools use the same `workspace_fs` service as REST;
- mutating workspace operations enforce quota at the `workspace_fs` layer;
- message writes use one strict request shape with required `content` and
  `attachments` arrays;
- message attachments are references to already-existing workspace files;
- image attachments are expanded into durable base64 `image_url` blocks for the
  provider and database;
- non-image attachments are represented by path-text markers only;
- direct inline base64 `image_url` blocks remain supported for callers that do
  not want a workspace file;
- duplicate direct image blocks and attachment image refs are deduplicated by
  decoded bytes;
- the agent-visible file tool schemas require an explicit `plexus_device`, even
  though M1d only accepts `server`.

M1d should leave the API shape ready for later device routing without
implementing client device reads or writes in this milestone.

---

## 2. Non-Goals

M1d does not include:

- React/frontend implementation;
- device WebSocket implementation;
- client-side file routing;
- dynamic device discovery in the tool schema merger;
- non-server `plexus_device` execution;
- client-device attachment reads;
- base64 encoding of images that live on client devices;
- file transfer between devices;
- remote URL download or ingestion;
- moving, copying, renaming, or garbage-collecting files during message send;
- server-side `.attachments/` sweeper;
- full ReAct tool-loop implementation beyond the server file tool surface needed
  for this milestone;
- MCP execution;
- Discord or Telegram attachment ingestion changes.

M1d may update docs and ADR wording that conflicts with the chosen strict
server-workspace contract. Those updates are part of the milestone, not
incidental cleanup.

---

## 3. Contract Corrections

Earlier M1 docs contain several assumptions that M1d intentionally corrects:

- Workspace REST APIs no longer default `plexus_device` to `server`. The caller
  must pass it explicitly.
- Browser message `content` no longer accepts string shorthand. It must be an
  array of content blocks.
- Browser message writes require both `content` and `attachments` arrays.
- Browser message attachments are workspace references, not upload payloads.
- Message send does not place files in `.attachments/{message_id}`. Files stay
  wherever the upload/write API already placed them.
- Message send does not enforce quota for attachments because it only reads
  existing files. Quota is enforced by mutating workspace operations.
- M1d does not implement external URL ingestion. Direct image URLs must be inline
  base64 data URLs.
- M1d implements a `tools_registry` merge v0, not the complete dynamic ADR-071
  merger.

ADR-080's graceful degradation remains useful for future channel adapters that
receive bytes and must write attachments during ingress. It does not apply to
M1d browser message attachment refs, where the upload/write has already
completed before `POST /api/sessions/{id}/messages`.

---

## 4. Explicit Device Contract

The literal `server` remains the reserved built-in Plexus install-site name for
the server workspace.

M1d requires `plexus_device` everywhere a file target is named:

- REST file APIs require `?plexus_device=server`;
- message attachments require `"plexus_device": "server"`;
- agent-visible shared file tools require `plexus_device`, injected by the tool
  schema builder.

There is no default. Missing `plexus_device` is a validation error. In M1d, any
value other than `server` is rejected with a clear unsupported-device error.

This is intentionally stricter than older docs. The long-term contract is
explicit target selection; M1d simply has one valid target.

---

## 5. Message API

M1d keeps the existing browser message route:

```text
POST /api/sessions/{id}/messages
```

The request body has one strict base shape:

```json
{
  "reasoning_effort": null,
  "content": [],
  "attachments": []
}
```

Rules:

- `content` is required and must be an array.
- `attachments` is required and must be an array.
- `reasoning_effort` is optional and nullable.
- Missing or `null` `reasoning_effort` means Plexus sends neither
  `reasoning_effort` nor `chat_template_kwargs.enable_thinking` to the provider.
- Explicit `reasoning_effort="none"` sends `reasoning_effort="none"` and
  `chat_template_kwargs.enable_thinking=false`.
- Any other explicit reasoning effort sends that value and
  `chat_template_kwargs.enable_thinking=true`.
- Reject the request if both `content` and `attachments` are empty.
- Reject unknown top-level fields.
- Do not persist a user message, pending row, or SSE event until the whole
  request has validated.

### 5.1 Content Blocks

Request `content[]` accepts only these block shapes:

```json
{ "type": "text", "text": "Describe this image." }
```

```json
{
  "type": "image_url",
  "image_url": {
    "url": "data:image/png;base64,..."
  }
}
```

Rules:

- no string shorthand;
- no omitted `content`;
- no object shape other than `text` or `image_url`;
- no external `http://` or `https://` image URLs in M1d;
- direct `image_url` blocks must be inline `data:image/...;base64,...` URLs;
- direct image blocks are persisted in `messages.content` and sent to the
  provider;
- direct image blocks are not written to the workspace;
- direct image blocks do not create path-text markers by themselves.

### 5.2 Attachments

Request `attachments[]` accepts only this block shape:

```json
{
  "plexus_device": "server",
  "path": ".attachments/uploads/018f/cat.png"
}
```

Rules:

- `plexus_device` is required;
- `path` is required;
- unknown attachment fields are rejected;
- M1d accepts only `plexus_device="server"`;
- missing or malformed attachment fields return `400`;
- non-server `plexus_device` returns `400` with an unsupported-device error;
- valid-looking server paths that do not exist return `404`;
- forbidden paths, traversal attempts, and paths outside the allowed workspace
  return `400` or `403` according to the existing API error mapping;
- any invalid attachment rejects the whole message and persists nothing.

Attachment paths are not restricted to `.attachments/uploads/...`. They may point
to any file the authenticated user may read from the server workspace, including
existing workspace files outside `.attachments/`.

---

## 6. Attachment Flow

Attachments are references to existing files.

For a local browser upload, the frontend first writes the file through the
workspace REST API:

```text
PUT /api/workspace/files/{path}?plexus_device=server
```

The frontend chooses the destination path, for example:

```text
.attachments/uploads/{uuid}/{filename}
```

After a successful upload, the frontend sends a message containing an attachment
ref to that path. If the user selects a file that already exists in the Plexus
workspace, the frontend can send the existing workspace path directly.

The message API:

- validates that every attachment target exists and is readable;
- reads attachment bytes only as needed for image detection and encoding;
- injects path-text markers;
- generates base64 `image_url` blocks for non-duplicate image attachments;
- never moves, copies, renames, deletes, or garbage-collects attachment files;
- never changes the path used in the marker.

Orphan uploads are normal workspace files. They count toward quota until the
user or agent deletes them through workspace operations.

---

## 7. Image Detection and Dedupe

M1d should detect attachment images server-side before generating provider
content. Magic-byte sniffing should be the primary signal. Extension-derived
MIME may be used as a fallback where appropriate, but a fake `.png` containing
non-image bytes must not be treated as an image.

Direct image URLs are validated by decoding their inline base64 payload. Invalid
data URLs reject the whole request before persistence.

Deduplication compares decoded bytes, not raw base64 strings:

```text
sha256(decoded_image_bytes)
```

M1d dedupes attachment-generated image blocks against direct
`content[].image_url` blocks.

Rules:

- decode and hash every direct `content[].image_url`;
- read and hash every image attachment;
- if an attachment image hash matches a direct image hash, keep the direct image
  block and skip the duplicate generated image block;
- insert the attachment marker immediately before the first matching direct
  image block;
- if multiple attachments match the same direct image block, insert all of
  their markers immediately before that block, in attachment order;
- if the attachment image hash does not match any direct image hash, generate a
  new base64 `image_url` block for that attachment;
- M1d does not perform fuzzy or perceptual image dedupe.

M1d does not dedupe direct images against other direct images. The caller's
direct content order is preserved except for inserted markers.

---

## 8. Message Assembly Order

The persisted/provider-visible user content is assembled deterministically.

If the ADR-094 runtime block is active, it remains first. The rest of this
section describes the user-authored and attachment-derived blocks after that
runtime block.

For non-duplicate attachment images, expand attachments first in attachment
array order, with each marker immediately followed by its generated image block:

```json
[
  { "type": "text", "text": "User uploaded file to device='server', path=\".attachments/uploads/a/image1.png\"" },
  { "type": "image_url", "image_url": { "url": "data:image/png;base64,..." } },
  { "type": "text", "text": "User uploaded file to device='server', path=\".attachments/uploads/b/image2.png\"" },
  { "type": "image_url", "image_url": { "url": "data:image/png;base64,..." } },
  { "type": "text", "text": "Compare the first image and second image." }
]
```

For non-image attachments, insert only the marker in the same attachment-order
position.

Do not assemble as all markers followed by all images. The marker belongs next
to the image it describes.

For duplicate direct-image cases, the marker moves to the matching direct image
block instead of the attachment prefix:

Input:

```json
{
  "reasoning_effort": null,
  "content": [
    { "type": "text", "text": "Describe this image." },
    {
      "type": "image_url",
      "image_url": {
        "url": "data:image/png;base64,..."
      }
    }
  ],
  "attachments": [
    {
      "plexus_device": "server",
      "path": ".attachments/uploads/018f/cat.png"
    }
  ]
}
```

Persisted/provider-visible content, excluding the optional runtime block:

```json
[
  { "type": "text", "text": "Describe this image." },
  {
    "type": "text",
    "text": "User uploaded file to device='server', path=\".attachments/uploads/018f/cat.png\""
  },
  {
    "type": "image_url",
    "image_url": {
      "url": "data:image/png;base64,..."
    }
  }
]
```

This keeps file provenance attached to the surviving image block while avoiding
double-sending identical image bytes.

---

## 9. Workspace REST APIs

M1d implements the server side of the workspace file APIs already described in
`docs/API.yaml`, with one correction: every file route requires an explicit
`plexus_device` query parameter.

M1d server-only behavior:

- missing `plexus_device` returns `400`;
- `plexus_device=server` routes to server `workspace_fs`;
- any other value returns `400` with unsupported-device error;
- no route defaults to `server`.

The file REST surface mirrors the shared file tools:

```text
GET    /api/workspace/files/{path}?plexus_device=server
PUT    /api/workspace/files/{path}?plexus_device=server
PATCH  /api/workspace/files/{path}?plexus_device=server
DELETE /api/workspace/files/{path}?plexus_device=server
DELETE /api/workspace/folders/{path}?plexus_device=server
GET    /api/workspace/list/{path}?plexus_device=server
GET    /api/workspace/glob?plexus_device=server&pattern=...
GET    /api/workspace/grep?plexus_device=server&pattern=...
GET    /api/workspace/quota
```

`GET /api/workspace/quota` does not need `plexus_device`; it reports the
authenticated user's server workspace quota state.

---

## 10. Workspace FS

`workspace_fs` is the single server-side file abstraction shared by REST and
server file tools.

It owns:

- personal workspace path resolution;
- allowed shared-workspace path resolution where already defined by docs;
- workspace boundary checks;
- symlink escape protection;
- file reads;
- full-file writes;
- fuzzy edits;
- file and folder deletes;
- directory listing;
- glob;
- grep;
- quota checks for mutating operations;
- usage accounting or on-demand usage calculation;
- SKILL.md validation and skills-cache invalidation where applicable.

Quota belongs at this layer. No REST handler, message handler, or tool
implementation may calculate quota independently.

Mutating operations check quota through `workspace_fs`:

- `PUT /api/workspace/files/{path}`;
- `PATCH /api/workspace/files/{path}`;
- future copy/move/write operations;
- server file tools that write or edit.

Reads and message attachment expansion do not enforce quota. They only require
the file to exist and be readable.

Deletes remain allowed while over quota so users can recover.

---

## 11. Server File Tools

M1d implements the server-side shared file tools against `workspace_fs`:

- `read_file`;
- `write_file`;
- `edit_file`;
- `delete_file`;
- `delete_folder`;
- `list_dir`;
- `glob`;
- `grep`.

Tool behavior should match `docs/TOOLS.md` unless this spec explicitly corrects
device/schema behavior for M1d.

The server is still not a general code execution environment. `exec` remains
out of scope for M1d server execution.

---

## 12. Tools Registry Merge V0

M1d implements a deliberately incomplete tool schema merger.

For shared file tools:

- source schemas do not contain `plexus_device`;
- the registry injects a top-level `plexus_device` property;
- the injected property has enum `["server"]`;
- the injected property is appended to `required`;
- missing `plexus_device` in a tool call is invalid;
- any value other than `server` is rejected in M1d.

This is **merge v0**, not the complete ADR-071 merger.

M1d does not implement:

- automatic install-site discovery;
- connected/offline paired device enum construction;
- client tool advertisements;
- grouping schemas by canonical schema across install sites;
- extending intrinsic `x-plexus-device` fields with real device names;
- multi-site schema collision rejection;
- non-server dispatch over device WebSocket.

The important M1d boundary is:

```text
source tool schemas -> tools_registry builds agent-visible schemas
```

M1f will complete the dynamic behavior behind this boundary without adding
`plexus_device` fields back into individual shared file tool source schemas.

---

## 13. Future M1f Delta

M1f completes device routing.

The same explicit `plexus_device` contract stays in place, but enum values are
built by automatically detecting eligible install sites and devices according to
the device-state ADRs.

M1f adds:

- device WebSocket registration and availability;
- dynamic `plexus_device` enum construction;
- client-side file tool dispatch;
- non-server REST routing;
- non-server message attachment refs;
- device file reads for attachment expansion;
- image detection and base64 encoding for images that live on devices;
- collision handling for same-name schemas advertised by multiple install
  sites.

For example, if a future message references:

```json
{
  "plexus_device": "some-detected-device",
  "path": "/home/user/Pictures/screen.png"
}
```

and that path points to an image, Plexus should read bytes from that device,
encode them as a base64 data URL, persist the image block in `messages.content`,
and send that same block to the provider.

---

## 14. Error Handling

Message writes reject before persistence when validation fails.

Expected mappings:

- malformed JSON or wrong request shape: `400`;
- missing `content` or `attachments`: `400`;
- both arrays empty: `400`;
- invalid content block: `400`;
- invalid direct image data URL: `400`;
- missing attachment `plexus_device` or `path`: `400`;
- unsupported non-server attachment device in M1d: `400`;
- path traversal or path outside workspace: `400` or `403`, following existing
  `ApiError` mapping;
- attachment path not found: `404`;
- unreadable attachment path: `403`;
- quota soft lock or upload too large during workspace write/edit: `409`;
- invalid SKILL.md after write/edit: `422`.

Provider incompatibility with explicit reasoning controls remains the M1c
review decision: persist a synthetic assistant error and flow it through SSE so
the user can see that the configured model/provider rejected the requested
reasoning level.

---

## 15. Docs Updated During Implementation

Implementation updated these docs to match the M1d contract:

- `docs/API.yaml`
  - remove string shorthand from browser message writes;
  - require `content` and `attachments`;
  - add attachment ref schema;
  - keep direct inline base64 `image_url`;
  - require REST `plexus_device` on file routes;
  - document M1d server-only unsupported-device behavior.
- `docs/TOOLS.md`
  - mark M1d registry behavior as merge v0;
  - clarify that full dynamic merger lands in M1f;
  - ensure shared file tool source schemas remain device-free.
- `docs/DECISIONS.md`
  - corrected ADR-044 for M1d browser attachments staying at their existing
    workspace path instead of being moved into adapter-owned `.attachments/`;
  - scope ADR-080 graceful degradation to ingress adapters that write bytes
    during message receive, not M1d browser attachment refs;
  - record explicit `plexus_device` for REST and message attachments.
- `docs/reference/superpowers/specs/2026-05-12-plexus-m1-living-design.md`
  - align milestone scope and M1d/M1f split.

---

## 16. Verification

M1d automated verification covers:

- message request rejects missing `content`;
- message request rejects missing `attachments`;
- message request rejects both arrays empty;
- message request rejects string shorthand;
- message request accepts direct inline base64 image blocks;
- message request rejects external image URLs;
- message request rejects invalid direct base64 image data;
- message request rejects missing attachment `plexus_device`;
- message request rejects non-server attachment `plexus_device`;
- message request rejects missing attachment file with `404`;
- message request rejects forbidden/outside-workspace paths;
- valid non-image attachment persists a path-text marker only;
- valid image attachment persists marker plus generated `image_url`;
- duplicate direct image plus attachment inserts the marker before the matching
  direct image and does not add another image block;
- non-duplicate image attachments are assembled marker/image pairs before user
  content;
- REST file routes reject missing `plexus_device`;
- REST file routes reject non-server `plexus_device`;
- REST file writes enforce quota through `workspace_fs`;
- deletes are allowed while over quota;
- server file tools and REST routes share the same `workspace_fs` behavior;
- shared file tool source schemas contain no `plexus_device`;
- agent-visible shared file schemas include required
  `plexus_device: enum ["server"]`;
- non-server tool calls fail clearly in M1d.

Manual smoke should prove:

- upload a local image through `PUT /api/workspace/files/{path}?plexus_device=server`;
- send a message with that attachment ref;
- observe a path-text marker and image answer from a live provider;
- send the same image as direct base64 plus attachment ref and verify the image
  is not duplicated in the persisted message content;
- upload and reference a non-image file, then ask the agent to read it through
  the server file tool path once tool use is available in the milestone.

Verification completed on 2026-05-18 using the existing local PostgreSQL test
container `plexus`:

- `rtk cargo fmt --check`
- `rtk cargo clippy -p plexus-common -p plexus-server --all-targets -- -D warnings`
- `rtk env PLEXUS_TEST_DATABASE_URL=postgres://plexus:plexus@127.0.0.1:5432/plexus cargo test --workspace --all-targets`
- `rtk conda run -n Plexus python -c "import yaml, pathlib; yaml.safe_load(pathlib.Path('docs/API.yaml').read_text()); print('API.yaml ok')"`
- targeted stale M1d wording scan over `docs/API.yaml`, `docs/TOOLS.md`,
  `docs/DECISIONS.md`, and the M1/M1d specs
- `rtk git diff --check`
