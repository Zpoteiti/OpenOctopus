# Py4 Merge Hardening and MarkItDown Integration Design

**Status:** accepted
**Milestone:** Py4 merge gate
**Depends on:** implemented Py4a RustFS workspace foundation and accepted Py4
workspace/files/message design
**Supersedes:** the fixed shared four-slot REST materialization behavior, direct
Office/PDF text extractors, aggregate always-on skill truncation, and unbounded
workspace-deletion shutdown described by the earlier Py4/Py4a designs

## Outcome

Before `py4-impl` merges into `main`, OpenOctopus closes the five findings in
`issue.md` and replaces its hand-written document and HTML-to-Markdown conversion
with a narrowly configured MarkItDown integration.

MarkItDown is a conversion component, not a trust boundary. OpenOctopus continues
to own authorization, RustFS access, SSRF protection, byte and page limits,
fair admission, process isolation, resource limits, timeouts, output caps, and
error normalization. No untrusted URL or local path is ever passed to MarkItDown.

The merge gate remains intentionally narrower than MarkItDown's complete feature
set. It improves existing behavior without adding VLM, OCR, audio, video, Azure,
YouTube, archive traversal, remote-document fetching, or a frontend.

## Scope

### Included

- Fair, separately bounded REST upload and download admission with per-user
  limits, queue timeouts, idle timeouts, and guaranteed permit release.
- Conditional-skill frontmatter-only loading, complete valid always-on skills,
  deterministic local token estimation, single-flight skill snapshots, and
  bounded prompt/provider admission.
- A memory- and CPU-limited document conversion subprocess for PDF, DOCX, XLSX,
  and PPTX.
- MarkItDown conversion for those four document formats.
- PDF default/explicit page ranges of at most 20 pages, total-page reporting, and
  continuation guidance.
- MarkItDown HTML-to-Markdown conversion after the existing `web_fetch` network
  boundary has downloaded and validated the response.
- Bounded workspace-deletion worker shutdown.
- Complete live personal-workspace quota fields in administrator user listings.
- Regression fixtures for Chinese/English content, document structure, malformed
  and hostile inputs, concurrency, cancellation, and memory limits.
- Full test, Ruff, strict mypy, real PostgreSQL/RustFS, 500-session, and peak-memory
  verification.

### Deferred

- MarkItDown plugins and default built-in converter discovery.
- Image captioning or an OpenAI-compatible MarkItDown VLM configuration.
- OCR for scanned documents or embedded images.
- Audio transcription, video understanding, FFmpeg, Google Speech Recognition,
  Azure Document Intelligence, Azure Content Understanding, and YouTube.
- ZIP-as-content recursive conversion, EPUB, MSG, XLS, IPYNB, and other formats
  not already supported by the Py4 `read_file` contract.
- Fetching remote PDF or Office content through `web_fetch`.
- Provider-specific token-count APIs.
- Durable usage counters or new PostgreSQL tables/columns.
- Making startup cleanup of an existing deletion outbox bounded. This design
  fixes runtime-worker shutdown only; startup recovery remains synchronous.

## Non-negotiable invariants

1. Every newly introduced `OPENOCTOPUS_*` environment variable is required,
   has no code default, is range-validated at startup, and appears in
   `server/.env.example`.
2. REST transfer limits never apply to Agent workspace reads/writes. The RustFS
   pool remains shared, so REST download capacity must reserve at least one
   connection for non-REST work.
3. A document-conversion permit is acquired before downloading the document from
   RustFS. Queued requests therefore do not retain an 8 MiB document in the main
   process.
4. The conversion child sets operating-system resource limits before importing
   MarkItDown, Pandas, pypdf, pdfplumber, Mammoth, or Office libraries.
5. The selected MarkItDown converter receives only an in-memory byte stream plus
   explicit trusted format metadata. OO never calls the MarkItDown orchestrator's
   path, URL, URI, or local-file conversion APIs.
6. Plugins and default built-ins are not loaded. OO instantiates only the one
   converter matching the already-allowed request format.
7. PDF conversion never processes more than 20 pages in one call.
8. Every discovered valid always-on skill is rendered completely or rejected
   when written. It is never silently truncated or changed into a conditional
   skill.
9. Provider context rejection is reported safely to the user. API keys, request
   headers, endpoint credentials, SDK traces, and raw response objects are never
   exposed.
10. Cancellation, timeout, and failure release every per-user/global permit and
    do not leave conversion subprocesses or keyed limiter entries behind.
11. Py4 retains the current single-ASGI-worker deployment contract. These
    process-local admission controls are not presented as cross-worker limits;
    multi-worker server deployment remains unsupported.
12. No PostgreSQL connection is held while waiting for transfer, context, or
    conversion admission, reading RustFS, running a conversion child, or calling
    a Provider.

## Required deployment configuration

The following settings are added to `Settings`. Numeric relations are validated
after individual field validation.

| Environment variable | Example | Validation and meaning |
|---|---:|---|
| `OPENOCTOPUS_REST_UPLOAD_MAX_CONCURRENCY` | `8` | Integer `2..256`; active REST uploads, held from body admission through the RustFS write |
| `OPENOCTOPUS_REST_DOWNLOAD_MAX_CONCURRENCY` | `16` | Integer `2..255`; active REST responses, held until the response/body closes |
| `OPENOCTOPUS_REST_TRANSFER_MAX_CONCURRENCY_PER_USER` | `2` | Integer `1..255`; shared per-user cap across upload and download; strictly less than both direction-global limits |
| `OPENOCTOPUS_REST_TRANSFER_QUEUE_TIMEOUT_SECONDS` | `5` | Number `0.1..300`; maximum wait for transfer admission |
| `OPENOCTOPUS_REST_TRANSFER_IDLE_TIMEOUT_SECONDS` | `30` | Number `1..600`; maximum time without the next upload chunk or successful download-body send |
| `OPENOCTOPUS_CONTENT_CONVERSION_MEMORY_MB` | `1024` | Integer `256..4096`; child `RLIMIT_AS`; the Py4 acceptance run uses 1024 MiB |
| `OPENOCTOPUS_CONTENT_CONVERSION_TIMEOUT_SECONDS` | `20` | Integer `1..120`; wall-clock conversion timeout and child CPU soft limit |
| `OPENOCTOPUS_CONTENT_CONVERSION_MAX_CONCURRENCY` | `2` | Integer `1..32`; process-wide admitted document/HTML conversions |
| `OPENOCTOPUS_CONTENT_CONVERSION_QUEUE_TIMEOUT_SECONDS` | `5` | Number `0.1..60`; maximum fair-admission wait; per-user conversion concurrency is fixed at one |
| `OPENOCTOPUS_WEB_FETCH_MAX_CONCURRENCY` | `16` | Integer `2..256`; complete `web_fetch` operations, including bounded response bytes retained in the parent |
| `OPENOCTOPUS_WEB_FETCH_MAX_CONCURRENCY_PER_USER` | `2` | Integer `1..255`; strictly smaller than the global web-fetch limit |
| `OPENOCTOPUS_WEB_FETCH_QUEUE_TIMEOUT_SECONDS` | `5` | Number `0.1..30`; fair-admission wait inside the existing 30-second total deadline |
| `OPENOCTOPUS_CHAT_CONTEXT_MAX_CONCURRENCY` | `32` | Integer `2..256`; contexts simultaneously being built or retained for a Provider request |
| `OPENOCTOPUS_CHAT_CONTEXT_MAX_CONCURRENCY_PER_USER` | `2` | Integer `1..255`; per-user context cap, strictly smaller than the global context limit |
| `OPENOCTOPUS_CHAT_CONTEXT_QUEUE_TIMEOUT_SECONDS` | `30` | Number `0.1..300`; maximum wait before a queued turn fails with a retryable busy response |
| `OPENOCTOPUS_WORKSPACE_DELETION_PURGE_TIMEOUT_SECONDS` | `300` | Number `1..3600`; wall-clock limit for one runtime outbox target purge child |
| `OPENOCTOPUS_WORKSPACE_DELETION_SHUTDOWN_GRACE_SECONDS` | `5` | Number `0.1..60`; graceful wait before cancelling the active retry pass |

Additionally:

- The existing required `OPENOCTOPUS_OBJECT_STORAGE_MAX_CONNECTIONS` setting now
  validates as integer `5..256` rather than `1..256`; its example remains `32`.
  The higher minimum is the smallest value compatible with two upload slots, two
  download slots, and one reserved workspace/internal connection. Runtime health
  uses one additional fixed connection, so the process ceiling is the configured
  value plus one.
- `REST_UPLOAD_MAX_CONCURRENCY + REST_DOWNLOAD_MAX_CONCURRENCY` must be strictly
  smaller than `OBJECT_STORAGE_MAX_CONNECTIONS`. This leaves at least one RustFS
  connection for Agent and internal operations even when admitted uploads reach
  their final PUT together. With the examples, 8 + 16 leaves 8 of 32 connections.
- The worst-case conversion address-space allowance is approximately
  `CONTENT_CONVERSION_MAX_CONCURRENCY * CONTENT_CONVERSION_MEMORY_MB`, plus the server,
  input/output copies, and non-parser workers. Deployment documentation states
  this explicitly. The merge gate is fixed at 1024 MiB per child. If the accepted
  XLSX corpus cannot run within that limit, implementation stops and this design
  is revised and reviewed; the implementation may not silently raise it.
- Web-fetch parent-body memory is bounded at approximately
  `WEB_FETCH_MAX_CONCURRENCY * 5,000,000` bytes plus decoded/output copies. Web
  permits are independent of child conversion permits, so slow network bodies do
  not occupy the document conversion pool.
- The context limits protect OO memory independently of Provider throughput.
  `llm_max_concurrent_requests` may be lower or higher and does not replace these
  deployment limits. With the examples, 32 simultaneous maximum 200-skill
  prompts can retain roughly 400 MiB of skill text before message/tool-schema
  overhead and Provider SDK copies; operators size this separately from child
  conversion memory.
- Document authorization has a fixed five-second short-transaction deadline.
  After admission, stat/open/download share one 30-second materialization
  deadline. Streaming object reads move from a synchronous MinIO response in a
  thread to the cancellable async presigned-GET path defined below, so stage
  cancellation closes the response without waiting for an unkillable worker.
  Document `read_file` paths derive their outer timeout as `5 +
  CONTENT_CONVERSION_QUEUE_TIMEOUT_SECONDS + 30 +
  CONTENT_CONVERSION_TIMEOUT_SECONDS + 5`, rounded up. The last five seconds
  reserve child termination/reap and error conversion. A materialization-stage
  expiry returns `tool_exec_timeout` naming the 30-second stage; the outer
  wrapper cannot fire first while legal inner cleanup is running. Non-document
  `read_file` keeps its existing timeout.
- `web_fetch` retains its existing 30-second total deadline. DNS, download,
  conversion admission, and HTML conversion all remain inside that deadline.
- Existing environment variables remain required. No fallback value is added for
  tests; fixtures must set the complete environment.

## 1. Fair REST transfer admission

### Problem

Slow REST uploads currently hold the same four materialization slots used by
Agent reads and prompt construction. Slow downloads hold RustFS responses and
connections without a REST admission policy. A single user can therefore delay
unrelated users and Agent work.

### Design

A process-wide `RestTransferAdmission` is injected into the workspace REST
routes. It owns:

- one keyed per-user capacity limiter shared by uploads and downloads;
- one upload-global limiter;
- one download-global limiter; and
- automatic cleanup of unused keyed entries.

Acquisition order is always per-user first, then direction-global. A user can
therefore place at most the per-user limit into the global queue. If either step
times out, already-acquired capacity is released.

Upload behavior:

1. authorize and compute the maximum body size;
2. commit/close the authorization transaction;
3. acquire user then upload-global capacity;
4. reject an oversized `Content-Length` before consuming the body;
5. read each next request chunk under the idle timeout;
6. keep the permit while the collected bytes are validated and written;
7. release on success, oversize, disconnect, cancellation, idle timeout, or
   storage failure.

`WorkspaceFS.collect_upload()` no longer acquires the generic materialization
semaphore. REST admission bounds slow request bodies; the existing
materialization semaphore continues protecting Agent reads and in-memory file
transforms.

Download behavior:

1. authorize and create the immutable download ticket;
2. commit/close the authorization transaction;
3. acquire user then download-global capacity;
4. open the RustFS stream;
5. transfer ownership of both permits to the closing streaming response;
6. apply the same per-chunk idle timeout separately to `ObjectStream.read()` and
   the following ASGI `send(http.response.body)` operation;
7. close the object response and release both permits in `finally`.

`ObjectStorage.open_stream()` is changed for every Agent/REST read to use one
shared `httpx.AsyncClient` rather than running urllib3/MinIO response reads in the
thread pool. With the already-required fixed RustFS region, the MinIO client
creates a short-lived presigned GET URL locally without network I/O. The async
client uses `trust_env=False`, redirects disabled, `Accept-Encoding: identity`,
the existing connect/read settings and retryable-status policy, and connection
limits equal to `OBJECT_STORAGE_MAX_CONNECTIONS`. Its requests acquire the same
`ObjectStorage` semaphore as synchronous stat/list/write/delete operations, so
active operations across the two underlying HTTP pools cannot exceed the one
logical workspace/internal storage budget. Runtime health is the only exception:
it is single-flight on a dedicated one-connection client and pool, preventing
user traffic and health probes from blocking each other.

The presigned URL is an internal bearer credential: it is never returned, logged,
included in an exception, or persisted. Response size/ETag come from validated
headers. Async cancellation closes the response/socket and releases the shared
slot promptly even when a server continuously trickles bytes; no detached read
thread survives the timeout. Startup and integration tests prove presigning does
not perform I/O, streaming works against RustFS, and normalized errors redact the
query string.

A queue timeout returns HTTP 429 with the normal `{code, message}` body, stable
code `workspace_transfer_busy`, and `Retry-After: ceil(queue_timeout_seconds)`.
An upload idle timeout before headers are sent returns HTTP 408 with
`workspace_transfer_timeout`. A stalled download may already have sent 200
headers, so it terminates the body stream and closes resources; it cannot change
the status after the fact. Cancelling a timed-out async RustFS read closes its
response/socket and never continues with subsequent chunks.

This provides fair admission, not perfect bandwidth isolation. Upload and
download admission queues are independent, but both ultimately share RustFS
connections, network bandwidth, and host resources. Different users can still
collectively saturate configured shared capacity; the strict connection reserve
keeps at least one connection available for non-REST work.

### Tests

- One user cannot consume every upload or download slot.
- Saturating one direction does not consume the other direction's admission
  permits; both still obey the shared RustFS connection budget.
- A slow REST upload does not block `SOUL.md`, `MEMORY.md`, or Agent `read_file`.
- A saturated download pool still leaves the configured RustFS reserve usable.
- Queue timeout returns 429 plus `Retry-After`.
- Upload inactivity returns 408 without committing a file.
- A blocked ASGI send closes the RustFS response after the idle timeout.
- Completion, disconnect, cancellation, body error, storage error, and timeout
  release all permits and remove unused keyed entries.
- No database connection is held while waiting for transfer admission or client
  I/O.

## 2. Skill loading, token estimation, and prompt admission

### Skill discovery and validation

Discovery uses `MAX_SKILL_CANDIDATES = 200` and
`MAX_SKILL_DISCOVERY_OBJECTS = 1_000`. It scans no more than 1,000 raw listing
records and examines at most the first 200 resulting direct child candidates
under `skills/`, ordered by normalized POSIX-relative path using case-sensitive
Python string lexicographic order. A directory without `SKILL.md` consumes a
candidate position but renders nothing. If an examined `SKILL.md` has invalid
UTF-8 in the bytes the loader must read, invalid frontmatter, bad name/path
agreement, or invalid always-on byte/token limits, the complete snapshot fails with
`workspace_invalid_skill_format`; malformed manifests are never silently skipped.
Candidates after position 200 are not read. Thus 1,000 valid conditional skills
may exist under workspace quota, but only the deterministic first 200 are
advertised in Py4.

One shared `MAX_SKILL_FRONTMATTER_PREFIX_BYTES = 16 * 1024 + 9` constant includes
the four-byte opening delimiter, at most 16 KiB of YAML, and the five-byte closing
delimiter. The loader range-reads that prefix plus one look-ahead byte; the parser
and write validator use the same boundary. A delimiter ending on the last allowed
byte is valid and a longer frontmatter block is rejected.

- Conditional skills retain only `name`, `description`, `always_on=false`, and
  their `read_file` path. Their body is not downloaded, decoded, tokenized, or
  cached during prompt construction.
- Always-on skills receive a second bounded full read and retain their complete
  body.

An always-on `SKILL.md` is valid only when the entire UTF-8 file is at most
64 KiB and its body is at most 16,000 tokens under
`tiktoken.get_encoding("o200k_base")`. Both checks run on every Agent/REST write
path before the object is committed. The loader repeats the check defensively for
objects written outside the supported OO APIs. Oversized always-on manifests
return `workspace_invalid_skill_format`; they are not truncated.

The aggregate `ALWAYS_ON_MAX_CHARS` budget and silent always-on-to-conditional
downgrade are removed. All valid discovered always-on bodies enter the system
prompt completely. If the combined request exceeds the actual model context,
the Provider remains authoritative and may reject it.

### Cache and single-flight

`SkillsCache` remains a 64 MiB weighted global LRU. The separate 1 MiB per-user
admission is removed: any user snapshot whose existing calculated weight fits the
64 MiB global cache can enter it. Under the existing conservative weight formula,
the maximum legal 200-by-64-KiB snapshot still fits once and naturally evicts
older entries.

One per-user single-flight guards cache misses:

1. check cache;
2. join or own that user's in-flight load;
3. owner checks cache again, builds one immutable snapshot, and publishes it;
4. waiters receive the same snapshot;
5. generation invalidation prevents a load started before a skill mutation from
   repopulating stale data.

Users do not block other users. Failed or cancelled loads wake waiters and remove
the in-flight entry.

### Local context estimation

`tiktoken==0.13.0` is used as the deterministic, Provider-independent estimator.
The Provider protocol and Anthropic implementation remove
`messages.count_tokens`; OO no longer requires third-party Anthropic-compatible
services to implement `/v1/messages/count_tokens`.

`get_encoding("o200k_base")` is not allowed to perform a first-request network
download. OO commits the asset currently identified by
`https://openaipublic.blob.core.windows.net/encodings/o200k_base.tiktoken` under
the package's read-only assets directory using tiktoken's URL-derived cache filename
`fb374d419588a4632f3f557e76b4b70aebbca790`. Startup verifies its SHA-256 as
`446a9538cb6c348e3516120d7c08b09f57c36495e2acfffe59a5bf8b0cfb1a2d`, points
`TIKTOKEN_CACHE_DIR` at that packaged directory, locally imports tiktoken, calls
`get_encoding("o200k_base")` once, and stores the resulting `Encoding` in a
process-global immutable estimator. The estimator module does not import
tiktoken before this initializer. A missing or mismatched asset fails startup;
runtime never falls back to the public network. This is internal packaging, not
a new deployment environment variable. Tokenizer initialization runs before the
MarkItDown capability child or any request-serving task starts.

The estimator covers:

- the complete system prompt;
- all textual and thinking message fields;
- tool names, descriptions, and JSON schemas;
- tool inputs/results and structural JSON overhead; and
- 2,000 estimated tokens per binary image block, plus tokens for its textual
  metadata, without tokenizing base64 as ordinary prose.

The estimate drives existing compaction thresholds and headroom decisions. It is
not advertised as the Provider's exact tokenizer, and it does not preemptively
truncate valid always-on skills. Provider rejection remains the final context
limit check.

Local over-threshold handling is finite and advisory:

1. estimate the prepared request;
2. when eligible historical messages exist, run at most one applicable existing
   compaction stage and rebuild/re-estimate once;
3. when no history is eligible, or the rebuilt request is still over threshold
   because of system/tools/current content or an insufficient summary, do not
   reject locally and do not compact again; and
4. send exactly one final request to the Provider and let it accept or reject the
   real context.

The optional compaction-summary Provider call is separate from that one final
request. A summary failure follows the normal sanitized Provider-error path.

### Prompt/provider memory admission

A process-wide context limiter and keyed per-user limiter use the required
`CHAT_CONTEXT_*` deployment settings. They are acquired before building a
provider context and held through the corresponding Provider request. This bounds
simultaneously retained large prompts independently of the Provider's own
throughput setting. Acquisition is per-user first and global second, with both
released in one `finally` scope.

Before waiting for those permits, a short database preflight reads only the
session owner ID and closes its transaction. Once admitted, each database phase
materializes plain immutable snapshots of Provider configuration, session/user,
active and captured-pending messages, channels, devices, and shared-workspace
memberships, then commits/closes before prompt workspace I/O. `SOUL.md`,
`MEMORY.md`, and skill discovery use DB-free authorized personal-workspace
operations derived from that snapshot. Compaction commits and post-compaction
message reloads use new short transactions; every summary/final Provider call is
made after those transactions close.

One Agent iteration owns one context lease. Context construction, any compaction
summary request, context rebuild after compaction, and the final Provider request
reuse that lease rather than recursively acquiring it. The context limiter is
therefore non-reentrant without deadlocking when either global or per-user
capacity is one. The lease is released before a tool batch and a later iteration
acquires a fresh lease.

The existing administrator setting `llm_max_concurrent_requests` remains the
Provider-capacity control and keeps its current semantics. The Provider limiter is
acquired inside the bounded context scope, so waiting Provider calls cannot cause
unbounded prepared contexts. A context-admission timeout fails the accepted turn
with the existing `provider_unavailable` code and a retryable server-busy message;
it does not silently wait forever.

The prepared system/messages/tools objects are released before executing a tool
batch; they are rebuilt for the next Provider turn rather than retained while a
tool runs.

If the Provider rejects a request, OO persists a failed assistant response that
contains a sanitized stable code and useful upstream message when one is safely
available, for example a context-window rejection. Only structured SDK status and
message fields are eligible, capped at 1,000 characters. Endpoint URLs, headers,
keys, raw request/response bodies, reprs, and tracebacks are excluded. Unknown
errors retain the existing generic message.

### Tests

- Conditional skills read only the bounded frontmatter range.
- Discovery scans at most 1,000 raw listing records and examines at most 200
  candidate directories; missing manifests consume the candidate cap, while a
  malformed examined manifest fails the snapshot with
  `workspace_invalid_skill_format`.
- All 200 valid always-on skills render completely with no aggregate downgrade.
- An always-on skill over either byte or token limit is rejected before write.
- Concurrent same-user cache misses perform one RustFS snapshot load.
- A mutation during a load cannot repopulate stale content.
- Weighted global LRU eviction is deterministic and a maximum legal snapshot can
  be cached.
- Local estimates include system, messages, and tools and never call the Provider
  count-tokens route.
- Startup initializes the vendored `o200k_base` asset successfully with all
  outbound networking disabled; a missing or hash-mismatched asset fails startup.
- Image blocks do not tokenize their full base64 text as prose.
- Prompt admission is global and per-user, begins before construction, and is
  held through the Provider call.
- A blocked context queue, blocked prompt RustFS read, compaction summary, and
  blocked final Provider call each hold no checked-out PostgreSQL connection.
- Context queue timeout fails the turn cleanly, wakes its subscriber, and releases
  every keyed/global entry.
- A safe context-window error reaches the user; credentials, headers, endpoint,
  traceback, and unrelated response-body data do not.
- An end-to-end fixture with 200 valid always-on skills deliberately exceeds the
  local compaction threshold: OO neither truncates nor demotes them, performs no
  infinite compaction loop, sends exactly one final oversized request to the
  Provider, and persists the Provider's sanitized context-window rejection for
  the user.

## 3. Isolated MarkItDown document conversion

### Dependency and adapter

Production pins the beta package exactly:

```toml
dependencies = [
    "markitdown[pdf,docx,xlsx,pptx]==0.1.7",
    "pypdf>=5.0",
    "tiktoken==0.13.0",
]
```

The exact MarkItDown pin fixes the adapter API and top-level package version used
by OO. Its extras still declare version ranges for transitive libraries, so the
pin does not claim to freeze Pandas, pdfplumber, Mammoth, or their dependencies;
the acceptance corpus and memory gate run against the actually resolved
environment, and any MarkItDown upgrade requires another reviewed corpus run.
`pypdf` remains a direct dependency only for PDF page inspection/slicing.
Direct production dependencies on `python-docx`, `openpyxl`, and `python-pptx`
are removed; fixture-generation-only dependencies may remain under `dev` when
tests import them directly. The stale `types-openpyxl` dependency is removed if
no checked code imports `openpyxl`.

No `openai`, audio, OCR, YouTube, Azure, ExifTool, or FFmpeg dependency is added.
MarkItDown's unavoidable base dependencies are accepted, including
BeautifulSoup, markdownify, requests, Magika, charset-normalizer, and defusedxml.
XLSX brings Pandas and is the primary memory benchmark target.

A single worker-only OO adapter is the only file allowed to import untyped
MarkItDown APIs; any strict-mypy suppression is local to that import. The parent
process never imports this adapter. After resource limits are installed, the
worker instantiates exactly one of `PdfConverter`, `DocxConverter`,
`XlsxConverter`, `PptxConverter`, or `HtmlConverter` and calls that MarkItDown
converter's stream-based `convert(BytesIO(data), StreamInfo(...))` method. It
then applies the same trailing-whitespace and repeated-blank-line normalization
as the pinned MarkItDown orchestrator. It supplies no `llm_client`, Azure
endpoint, plugin, path, or URL.

The adapter supplies trusted metadata for converter acceptance and does not rely
on Magika's guess to choose the converter:

| Allowed suffix | Converter | `StreamInfo.extension` | `StreamInfo.mimetype` |
|---|---|---|---|
| `.pdf` | `PdfConverter` | `.pdf` | `application/pdf` |
| `.docx` | `DocxConverter` | `.docx` | `application/vnd.openxmlformats-officedocument.wordprocessingml.document` |
| `.xlsx` | `XlsxConverter` | `.xlsx` | `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet` |
| `.pptx` | `PptxConverter` | `.pptx` | `application/vnd.openxmlformats-officedocument.presentationml.presentation` |
| validated HTML | `HtmlConverter` | `.html` | `text/html` |

HTML additionally passes `charset="utf-8"` after the child preprocessor has
decoded the source charset and serialized UTF-8.

MarkItDown 0.1.7's orchestrator unconditionally initializes Magika/ONNX even when
complete trusted metadata is present. Under the fixed 1 GiB child limit this
created CPU-core-dependent virtual-address reservations and non-deterministic
`std::bad_alloc` failures in the acceptance corpus. OO therefore uses the pinned
MarkItDown converter classes directly and does not initialize Magika. Fragmentary
HTML and all four explicit document mappings have regression tests proving the
selected converter accepts the trusted mapping.

### Fair admission and download order

One process-wide conversion admission object is shared by the workspace tool
registry and `WebFetchTool`; neither component constructs a private semaphore.
The limiter allows one active conversion per user and the configured global
total. Acquisition is per-user first and global second, so only one request from
one user can wait for or hold global capacity. On release, an already-waiting
request from another user precedes that user's next queued request.

All `read_file` paths first use a short transaction to authorize and resolve an
immutable read ticket containing the target, relative path, display path, and
trusted suffix. The dispatcher commits/closes that session before any admission
wait or RustFS call. The ticket does not claim the object still exists; storage
remains authoritative when it is opened.

For a workspace document, the DB-free `read_for_tool(ticket, ...)` acquires
conversion admission before any ETag/stat/open operation. Once admitted, one
30-second materialization stage opens the RustFS stream, obtains its size/ETag,
performs the unchanged-read cache check, and, when needed, collects the body. A
queue timeout therefore performs no object-store operation. The request keeps its
conversion permit through materialization, subprocess conversion, output receipt,
and child reap. The generic workspace materialization slot wraps only
stat/open/body collection and is released immediately after the bytes are
assembled; it is never held while the child converts. Slow parsing therefore
cannot consume every slot needed by `SOUL.md` or `MEMORY.md`.

### Child lifecycle and operating-system limits

The spawn target lives in a lightweight worker module containing only standard
library imports at module import time. Its order is mandatory:

1. set `RLIMIT_AS` from `CONTENT_CONVERSION_MEMORY_MB`;
2. set soft/hard `RLIMIT_CPU` from the configured parse timeout, with a one-second
   hard-limit margin;
3. for OOXML, use only standard-library `zipfile` to preflight the container
   before importing MarkItDown/Pandas;
4. for PDF, import only pypdf, validate/slice the selected pages, and retain the
   resulting small byte stream;
5. only after preflight succeeds, locally import the worker adapter and
   MarkItDown converter modules;
6. convert and cap output inside the child;
7. send one small versioned result tuple through the pipe and exit.

Production startup runs a small conversion-capability child that sets both limits
and imports the pinned adapter. Because MarkItDown defers some missing-extra
errors, the probe also explicitly imports `mammoth`, `pandas`, `openpyxl`, `pptx`,
`pdfminer.high_level`, and `pdfplumber`, then constructs all five configured
converters. Startup fails if Linux cannot enforce the required address-space/CPU
limits or any required converter dependency cannot import. Py4 server document
conversion is Linux-only; silently running without limits is forbidden.

The parent enforces the wall-clock timeout, handles cancellation, terminates then
kills when necessary, reaps the child, closes both pipe ends, and releases
admission in `finally`. It distinguishes:

- `tool_content_conversion_busy` — fair conversion queue timed out; retryable;
- `tool_exec_timeout` — parent wall timeout or child `SIGXCPU`;
- `tool_content_conversion_resource_exceeded` — direct `MemoryError` or
  `OSError(errno.ENOMEM)`, including either exception preserved inside
  MarkItDown conversion attempts;
- `tool_content_conversion_failed` — malformed/encrypted/unsupported content or
  an unexpected converter failure, including an unattributable `SIGKILL`,
  `SIGSEGV`, or `SIGABRT`, with no library traceback.

Caller cancellation is not relabeled as a conversion failure: it propagates only
after child reap and permit release. Invalid PDF range syntax or an end page past
the actual document returns the existing `tool_invalid_args`, not a conversion
failure.

These are ordinary error `tool_result`s. The LLM sees `[code] message`, may retry
with a smaller PDF page range, and normally explains the problem to the user.
No separate document REST error response is added in Py4.

### OOXML preflight

Before MarkItDown sees DOCX/XLSX/PPTX, the child examines ZIP metadata without
extracting files. It rejects:

- encrypted members;
- absolute paths, `..`, NULs, or path traversal;
- more than 10,000 members;
- a member declaring more than 128 MiB uncompressed;
- more than 256 MiB total declared uncompressed content; or
- a member of at least 1 MiB declared uncompressed size whose ratio to
  `max(1, compressed_size)` exceeds 100:1.

These are input-rejection limits in addition to, not substitutes for, `RLIMIT_AS`.

### PDF paging

The child opens the PDF with pypdf, rejects encrypted documents, and determines
the total page count. It preserves both existing `pages` forms:

- omitted: pages `1..min(20, total)`;
- explicit `N`: page `N` only;
- explicit `start-end`: one inclusive range, with both ends inside the document;
- maximum 20 pages per call.

Only selected pages are written into a small in-memory PDF and passed to
MarkItDown. MarkItDown never receives the original many-page PDF. The header and
optional continuation footer are rendered first; only the Markdown body is
truncated to the remaining part of the 128,000-character budget, so paging
markers are never lost. Output begins
with:

```text
[PDF pages 1-20 of 386]
```

and, when pages remain after the selected end, ends with a computed next range
`end + 1` through `min(total, end + 20)`, for example:

```text
[More pages available. Call read_file with pages="21-40".]
```

An empty text result states that no extractable text was found and that OCR is
not enabled; it does not pretend the PDF is empty. The complete rendered result,
including paging markers, remains capped at 128,000 characters.

### Document tests

- DOCX: Chinese/English headings, paragraphs, lists, hyperlinks, and tables.
- XLSX: multiple sheets, Chinese/English cells, empty cells, formulas/cached
  values, and tables.
- PPTX: title, ordered shapes, tables, charts, notes, and embedded-image alt text
  without VLM calls.
- PDF: default first 20, explicit ranges, invalid ranges, continuation markers,
  prose, tables, Chinese text with embedded fonts, encrypted input, and empty
  scanned pages.
- All formats: malformed bytes, 8 MiB boundary, output over 128,000 characters,
  cancellation, wall timeout, and child crash/reap behavior.
- OOXML: excess members, excess declared size, excess ratio, encrypted member,
  and traversal names.
- Admission: queue timeout performs no object-store operation; one user cannot
  starve a second, global concurrency is respected, and permits survive no
  failure path.
- Database lifecycle: conversion queueing, materialization, a blocked child, and
  child reap each hold no checked-out PostgreSQL connection.
- Timeout boundary: an async fake RustFS response that trickles without finishing
  reaches the 30-second materialization deadline, closes promptly with no worker
  thread, and is reported by the inner stage as `tool_exec_timeout`; the outer
  wrapper never preempts cleanup or reports its longer aggregate duration.
- Resource limit: a test-only spawn target applies the production limit helper
  and performs a deterministic over-limit allocation. A separate parent-protocol
  test maps its `MemoryError`/`ENOMEM` result to
  `tool_content_conversion_resource_exceeded`, while the parent and a second
  session remain healthy. No production “allocate memory” operation is exposed.

## 4. `web_fetch` HTML conversion boundary

The existing `httpx` network path remains authoritative and unchanged for:

- `http`/`https`-only validation;
- credential and malformed-host rejection;
- DNS resolution and public-address enforcement;
- connection pinning and SNI/Host handling;
- repeated SSRF validation on every redirect;
- redirect, connect, and total deadlines;
- rejection of compressed bodies;
- the exact 5,000,000-byte raw response cap; and
- the caller's 50,000-character result cap.

Before DNS/network work, `web_fetch` acquires its independent keyed per-user and
global permits, in that order, within `WEB_FETCH_QUEUE_TIMEOUT_SECONDS`. The
permit is held through response conversion and output truncation. This bounds
simultaneous sockets and retained 5 MiB response bodies without allowing a slow
network peer to occupy a document-conversion child slot. Web-admission timeout
returns the existing `network_timeout` tool result.

After network checks and the bounded download, HTML waits for the separate shared
content-conversion permit while still holding its bounded web permit:

- `text/html` and `application/xhtml+xml` in `extractMode=markdown` use the
  isolated MarkItDown HTML converter through the shared conversion admission;
- a small BeautifulSoup preprocessing pass resolves relative `href`/`src` values
  against the final validated URL so replacing the current parser does not
  regress link behavior;
- the preprocessor removes `script`, `style`, `noscript`, `template`, and `svg`
  elements, matching the current skip set, and removes image elements whose
  source is a data URI rather than retaining a shortened payload marker;
- preprocessing decodes according to the declared/validated charset, rewrites the
  DOM, serializes UTF-8, and gives MarkItDown
  `StreamInfo(mimetype="text/html", extension=".html", charset="utf-8")`;
  GB18030 bytes are decoded and re-encoded, never merely relabeled;
- `extractMode=text` uses bounded BeautifulSoup text extraction because
  MarkItDown exposes Markdown, not a plain-text output mode; and
- non-HTML responses retain the existing bounded charset decode behavior.

Relative-link preprocessing, skip-element removal, Markdown conversion, and
`extractMode=text` BeautifulSoup parsing all execute inside the resource-limited
child, never synchronously in the event loop or parent. The HTML adapter never
calls a MarkItDown URL/local-path entrypoint. It receives
the already-downloaded bytes, declared charset, final public URL used only for
relative-link normalization, output mode, and output cap. It registers only
`HtmlConverter`; ZIP, RSS, Wikipedia, YouTube, image, audio, EPUB, Outlook, and
cloud converters are unavailable.

HTML conversion participates in the 30-second `web_fetch` total timeout. A
content-conversion queue timeout maps to `tool_content_conversion_busy`; child
wall/CPU timeout maps to `tool_exec_timeout`; an explicit `MemoryError`/`ENOMEM`
maps to `tool_content_conversion_resource_exceeded`; and other converter failure
maps to `tool_content_conversion_failed`. If the encompassing 30-second deadline
wins first, the existing `network_timeout` remains authoritative. Every path
reaps the child. This design deliberately accepts per-request child startup cost
for isolation and records concurrent HTML latency. A persistent worker pool is
outside Py4 scope.

Regression tests cover headings, lists, tables, links, relative links, script and
style removal, deeply nested HTML, UTF-8, GB18030 with a declared charset,
malformed bytes, output truncation, conversion timeout, and proof that no
MarkItDown network/path method is called. Existing SSRF, redirect, compressed
bomb, byte-cap, DNS, and timeout tests remain unchanged.

## 5. Bounded deletion-worker shutdown

The runtime retry coordinator no longer executes a potentially unbounded purge in
the main process's thread pool. It loads pending targets in a short transaction,
closes that transaction, and handles targets sequentially. For each target it
spawns one dedicated purge child that constructs a one-connection MinIO client
from an immutable storage configuration, lists/deletes only the already-authorized
workspace prefix, and returns a small success/error result. Credentials stay in
process memory and never enter the result pipe or logs.

The parent kills and reaps a purge child when
`WORKSPACE_DELETION_PURGE_TIMEOUT_SECONDS` expires; the durable row remains for a
later retry. `WorkspaceDeletionWorker.close()` is idempotent:

1. set the stop event and wait the configured shutdown grace;
2. if a purge child remains, send terminate, wait a fixed five seconds, then send
   kill if necessary and reap it;
3. cancel/await the database coordinator after the child is gone;
4. close pipe/process handles, clear task/child references, and return.

This process boundary is required because Python cannot forcibly stop a running
MinIO thread. Under normal Linux process semantics, a stalled or trickling RustFS
operation therefore cannot keep OO shutdown waiting on that operation. An
uninterruptible kernel-level process state is an operating-system failure outside
the application timeout contract; no application can promise otherwise. Startup
recovery retains the explicitly deferred synchronous behavior.

The cleanup outbox row is deleted only after a complete idempotent purge. If
cancellation occurs before that commit, the row remains and restart recovery
purges the remaining objects. If object deletion completed but the row commit did
not, replay observes an empty prefix and finishes safely.

Tests cover close while sleeping, graceful child completion, terminate then kill
of a child blocked past grace, runtime purge timeout, cancellation after a partial
prefix, durable-row retention, successful restart replay, repeated close, no DB
connection held during purge, no remaining normal child/pipe handle, credential
redaction, and ensuring `CancelledError` is not logged as an ordinary retry
failure.

## 6. Administrator live quota rendering

`AdminUserResponse.quota_bytes`, `bytes_used`, and `locked` become required
`int`, `int`, and `bool` fields. Nullable defaults are removed so future missing
population fails tests/serialization instead of returning silent `null`s.

`GET /api/admin/users` performs:

1. one paginated user query;
2. one personal-quota read under the existing quota read lock;
3. an immutable in-memory snapshot of users and quota;
4. transaction commit before RustFS work;
5. one `WorkspaceService.personal_usages(user_ids)` call that constructs personal
   targets inside the workspace boundary and runs live scans under the existing
   process-wide heavy-operation limit of four; and
6. explicit response construction with `locked = bytes_used > quota_bytes`.

The REST route does not construct `WorkspaceTarget` or call `WorkspaceFS`
directly. The batch service method is database-free, preserves input order, and
fails atomically if any usage scan fails.

There is no PostgreSQL usage cache and no per-user quota column. A storage failure
makes the complete request return the existing 503 storage error; partially
truthful rows with `null` usage are not returned.

Tests cover different usage values, `used > quota`, `used == quota`, exactly one
quota read, at most four concurrent usage scans, no checked-out DB connection
during a blocked scan, whole-page 503 on one storage failure, pagination, and an
OpenAPI/DTO contract in which the three fields are always required.

## API and error contract changes

The implementation adds these stable codes:

| Code | Surface | Meaning |
|---|---|---|
| `workspace_transfer_busy` | REST, HTTP 429 | Fair transfer admission timed out |
| `workspace_transfer_timeout` | REST upload, HTTP 408 | Request body made no progress before idle timeout |
| `tool_content_conversion_busy` | `read_file`/`web_fetch` tool result | Fair conversion admission timed out |
| `tool_content_conversion_resource_exceeded` | `read_file`/`web_fetch` tool result | Child reported an address-space allocation failure |
| `tool_content_conversion_failed` | `read_file`/`web_fetch` tool result | Input was malformed/unsupported or conversion failed |

`tool_exec_timeout` remains the document wall/CPU timeout code. Tool errors remain
inside normal persisted tool-result messages, so the LLM can react and explain
them. No new SSE event type is required.

`docs/API.yaml`, `docs/TOOLS.md`, `docs/SYSTEM_PROMPT.md`, and
`docs/DECISIONS.md` are updated in the same implementation. The accepted Py4/Py4a
specs receive short historical notes pointing to this superseding design. No
`docs/SCHEMA.md` or database migration is required.

## TDD implementation slices

Each slice begins with a failing focused test, implements only that behavior,
runs its focused suite, and forms a reviewable commit. `issue.md` remains a local
tracker and is not included in commits unless explicitly requested.

1. **REST transfer fairness**
   - config validation and examples;
   - keyed/global admission and error codes;
   - upload/download lifecycle and idle timeout tests;
   - API and decision documentation.
2. **Skills and local token estimation**
   - frontmatter-only reads and always-on validation;
   - single-flight weighted cache;
   - vendored tokenizer asset, startup initialization, local estimator, and
     removal of Provider count-token calls;
   - prompt/provider admission and safe Provider errors;
   - prompt/tool/decision documentation.
3. **Document isolation and MarkItDown**
   - dependency change and narrow adapter;
   - child resource protocol and fair admission;
   - OOXML preflight, PDF slicing, output/error contract;
   - document corpus and resource tests;
   - tool documentation.
4. **`web_fetch` conversion**
   - MarkItDown HTML markdown mode and bounded text mode;
   - relative-link and charset compatibility;
   - preservation of every network-security regression test.
5. **Deletion shutdown**
   - killable purge child, runtime deadline, bounded/idempotent close, and durable
     retry tests;
   - lifecycle decision documentation.
6. **Administrator live usage**
   - required DTO fields and bounded rendering;
   - storage failure and DB-release tests;
   - OpenAPI documentation.
7. **Merge-gate integration**
   - cross-feature cancellation/fairness tests;
   - real-service, 500-session, memory, formatting, typing, and diff hygiene;
   - tracker resolution only after all evidence passes.

## Verification and exit criteria

### Focused and full gates

Run from `server/` through the existing Conda environment:

```bash
conda run --no-capture-output -n oo pytest
RUN_RUSTFS_INTEGRATION=1 conda run --no-capture-output -n oo pytest tests/test_rustfs_integration.py
conda run --no-capture-output -n oo ruff check .
conda run --no-capture-output -n oo ruff format --check .
conda run --no-capture-output -n oo mypy src
git diff --check
```

The full suite uses real PostgreSQL where configured, and the second command
explicitly enables the otherwise skipped real RustFS integration module. Mocked
unit tests do not replace the startup probe, object-store, workspace, and
agent-loop integration coverage.

### Capacity and fairness

- Existing 500-independent-session coverage remains green.
- A dedicated scenario starts 500 sessions while transfer, prompt, Provider, and
  conversion limits are intentionally smaller; work is bounded, unrelated users
  make progress, expected queue failures are explicit, and no semaphore/keyed
  entry grows without bound.
- Slow REST upload/download tests prove Agent identity/memory reads still progress.
- Cancellation storms leave no parser processes, checked-out RustFS responses,
  transfer permits, prompt permits, or skill single-flight entries.

### Peak-memory evidence

The implementation adds a Linux-only measurement runner and executes it on the
target host/container from `server/`:

```bash
conda run --no-capture-output -n oo python scripts/check_content_conversion_memory.py \
  --fixtures tests/fixtures/documents \
  --memory-mb 1024 \
  --concurrency 2 \
  --repeat 20 \
  --output .pytest_cache/py4-content-conversion-memory.json
```

The runner invokes the production child protocol for PDF, DOCX, XLSX, PPTX, and
HTML, then the test-only over-limit target. Its JSON records the command and
resolved package versions, configured `RLIMIT_AS`, per-child `ru_maxrss` and
`/proc/<pid>/status` high-water RSS, parent RSS samples, wall time, exit/error
classification, and cgroup `memory.current`/`memory.peak` when available.
Cgroup values are diagnostic only because a normal process cannot reset its
inherited cgroup's historical peak. During the concurrent case, the runner polls
parent and both child RSS values at least every 10 ms until both children are
reaped.

The command exits nonzero unless all of these hold:

- every normal fixture succeeds with the fixed 1024 MiB address-space limit and
  finishes within `CONTENT_CONVERSION_TIMEOUT_SECONDS + 5` seconds;
- the deterministic over-limit target returns
  `tool_content_conversion_resource_exceeded`, the parent survives, and a
  subsequent normal conversion succeeds;
- no normal child's recorded high-water RSS exceeds 1024 MiB;
- two copies of the worst-RSS normal fixture reach a shared start barrier before
  release, complete simultaneously, and the maximum same-sample sum of parent
  RSS plus both child RSS values is no greater than settled parent baseline plus
  `2 * 1024 MiB + 256 MiB` protocol overhead;
- after garbage collection and event-loop idle, parent RSS after 20 mixed
  success/malformed/timeout/cancel rounds is no more than 64 MiB above its
  settled baseline; and
- no child PID, pipe endpoint, or conversion permit remains after any round.

Absence of cgroup v2 metrics does not skip any RSS hard gate. The JSON output is
attached to the implementation handoff. A failure at 1024 MiB requires a revised,
re-reviewed spec rather than an ad-hoc higher value.

### MarkItDown acceptance corpus

For every golden fixture, tests compare semantic Markdown properties rather than
the complete upstream formatting byte-for-byte. Required assertions include the
presence/order of headings, Chinese and English text, table rows/cells, slide
notes, sheet names, PDF page boundaries, and links. This permits an explicitly
reviewed MarkItDown upgrade later without hiding semantic regressions.

The Py4 branch is merge-ready only when all five tracker entries have focused
regression tests, the MarkItDown corpus passes, required documentation and example
configuration agree with code, every gate above is green, and peak-memory results
are recorded in the implementation handoff.
