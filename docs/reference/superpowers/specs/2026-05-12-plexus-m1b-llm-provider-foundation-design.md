# Plexus M1b OpenAI-Compatible LLM Foundation Sub-Spec

**Status:** Verified on 2026-05-13 from branch `rebuild-m1-M1b`
**Parent:** [Plexus M1 Living Design Spec](2026-05-12-plexus-m1-living-design.md)
**Branch:** `rebuild-m1-M1b`
**Base:** `rebuild-m1`
**Authors:** brainstormed in collaborative session 2026-05-12
**Supersedes:** none

---

## 1. Goal

M1b adds the OpenAI-compatible LLM foundation needed by later chat and agent
milestones.

The success proof is deliberately narrow:

- admins can set `llm_endpoint`, `llm_api_key`, and `llm_model`;
- provider identity changes are validated with `GET {llm_endpoint}/models`
  before any database write;
- invalid provider config leaves the existing `system_config` rows unchanged;
- a valid OpenAI-compatible fake provider can complete a non-streaming chat
  completion call;
- configured API keys are never returned in clear text by
  `GET /api/admin/config`;
- runtime chat-completion calls can be protected by the optional
  `llm_max_concurrent_requests` in-process semaphore.

M1b also adds a separate FastAPI mock LLM service in a sibling repository for
deterministic local development and manual smoke tests.

---

## 2. Non-Goals

M1b does not include:

- browser chat REST ingress or SSE delivery;
- persisted chat sessions or message storage behavior beyond what M1a already
  bootstrapped in the schema;
- the ReAct agent loop;
- streaming chat completions;
- tool calls or tool-call repair;
- context compaction;
- compaction decision-making inside `openai.rs`;
- database reads or writes inside `openai.rs`;
- image content handling or vision-retry behavior;
- real provider credentials in automated tests;
- native Anthropic, Gemini, or other provider protocols;
- a provider abstraction trait;
- production fake-model branches inside Plexus.

Those features belong to later M1 slices. M1b is only the provider foundation
and deterministic testing surface.

---

## 3. Architecture

Plexus speaks one LLM wire protocol in M1: OpenAI-compatible chat completions.
Non-OpenAI providers must be adapted outside Plexus by an OpenAI-compatible
gateway such as LiteLLM.

Production Plexus code uses a single server-only module:

```text
plexus-server/src/openai.rs
```

Every outbound external LLM request from Plexus must go through this module.
There is no `providers/` folder and no `llm/openai.rs` directory in M1b because
those names imply a native multi-provider framework that ADR-101 deliberately
avoids.

`openai.rs` owns:

- endpoint URL joining;
- bearer auth construction from `plexus-common::LlmApiKey`;
- `GET /models` validation;
- `POST /chat/completions` calls;
- request timeout behavior;
- transient provider retry/backoff;
- OpenAI-compatible request and response parsing;
- provider error mapping;
- chat-completion concurrency permit acquisition.

OpenAI request/response structs should remain private to `openai.rs` unless
later milestones prove they need to be shared. Shared base types, shared secret
wrappers, and shared error enums remain in `plexus-common`.

`openai.rs` is an external-call boundary, not an orchestration layer. It must
not load messages from the database or write LLM responses back to the
database. Later session/agent code owns DB reads, `context::build_context`,
compaction decisions, and persistence of assistant/tool rows.

M1b implements only string chat content through this provider path. Image
content blocks, `image_url` stripping, and VLM retry behavior are deferred to a
later chat/VLM slice. When ADR-026 vision retry is implemented, it belongs at
this external-call boundary so the database can keep full-fidelity messages
while any provider-specific retry transformation stays in memory.

---

## 4. Admin Config Behavior

M1b expands `PATCH /api/admin/config` to accept these provider identity keys:

- `llm_endpoint`
- `llm_api_key`
- `llm_model`

The M1a numeric/config keys remain accepted:

- `quota_bytes`
- `shared_workspace_quota_bytes`
- `llm_max_context_tokens`
- `llm_compaction_threshold_tokens`
- `llm_max_concurrent_requests`

`server_mcp` remains out of scope for M1b.

### 4.1 Validation Before DB Write

When a patch changes any provider identity key, the server builds the effective
provider identity by merging the incoming patch over the currently stored
values.

Validation then runs before committing any config write:

1. `llm_endpoint`, `llm_api_key`, and `llm_model` must all be present in the
   effective config.
2. Each identity value must be a non-empty string after trimming whitespace.
3. `llm_endpoint` must be an absolute `http` or `https` URL.
4. Plexus calls `GET {llm_endpoint}/models` with
   `Authorization: Bearer {llm_api_key}`.
5. The response must be a well-formed OpenAI-compatible models response.
6. The response `data` array must contain an object whose `id` equals
   `llm_model`.

If validation fails, the request returns `400` with `invalid_args` and the
database remains unchanged.

On first setup, the admin must provide all three identity keys in one patch
because there are no existing stored values to complete the effective config.
After a valid identity exists, the admin may patch one identity key at a time;
Plexus validates the merged result before writing.

### 4.2 Secret Read Redaction

`PATCH /api/admin/config` accepts a new `llm_api_key` value for initial setup or
rotation.

`GET /api/admin/config` must never return the raw key. When a key is configured,
the response returns:

```json
"llm_api_key": "<redacted>"
```

M1b should not seed a placeholder `llm_api_key` row. Before a key is configured,
`GET /api/admin/config` omits `llm_api_key`.

To keep an existing key unchanged, callers omit `llm_api_key` from the patch.
If a caller sends the literal redaction marker as a new key, Plexus rejects the
patch with `invalid_args` rather than saving the marker.

Error messages and logs must not include the submitted API key.

---

## 5. Chat Completion API

M1b exposes an internal Rust function for non-streaming chat completions. It is
not a public REST endpoint.

This is the internal equivalent of calling the OpenAI-compatible
`/chat/completions` API with `stream=false`. M1b should send `stream: false`
explicitly so the contract is obvious even when providers default to
non-streaming responses.

The minimal M1b request surface is:

- configured model;
- ordered messages;
- text content;
- roles needed for basic OpenAI-compatible chat: `system`, `user`, and
  `assistant`.

The minimal response surface is the first assistant message content from
`choices[0].message.content`, plus enough internal error information to produce
stable server errors.

M1b does not expose temporary dev REST routes for chat completion. Later
milestones call this internal function from the browser chat path, cron,
heartbeat, compaction, and the agent loop.

Compaction uses this same internal function as an LLM call, but the compaction
trigger, summary range selection, and summary-row persistence stay in the
orchestrator/context layer, not in `openai.rs`.

---

## 6. Concurrency

`llm_max_concurrent_requests` is an integer. Bootstrap does not seed a default
row. If the key is missing at startup, only the runtime limiter treats it as
`0`.

- `0` means unlimited and creates no semaphore.
- A positive integer creates an in-process semaphore with that many permits.
- Negative values are invalid.
- Values above the server maximum are invalid.
- Runtime chat-completion calls acquire a permit before making the external
  `POST /chat/completions` request and release it when the request completes.

The cap is provider-wide, not per-user and not per-session. It is a backend
protection knob for weaker providers or deployments without an external
gateway. Plexus admins are responsible for choosing the value for their
deployment. It is not a product rate-limit system and does not coordinate
across multiple Plexus server processes.

Tests must prove the semaphore limits concurrent chat-completion calls.

---

## 7. External Mock LLM Service

M1b adds a sibling repository:

```text
~/Documents/GitHub/Plexus-mock-llm
```

This service is not part of the Plexus workspace and is not a production
dependency. It exists for deterministic local development and manual smoke
testing.

Implementation shape:

- FastAPI application;
- run with the existing Miniforge/conda environment named `Plexus` or the
  closest clearly named Plexus environment;
- no Docker requirement in M1b;
- small README with startup command and the Plexus config values to paste into
  `/api/admin/config`.

Required API:

- `GET /v1/models`
- `POST /v1/chat/completions`

Default model:

```text
plexus-fake-qa
```

Default bearer key:

```text
plexus-mock-key
```

The service should require `Authorization: Bearer plexus-mock-key` so local
manual tests exercise the same auth path as real provider validation.

`GET /v1/models` returns an OpenAI-compatible models list containing
`plexus-fake-qa`.

`POST /v1/chat/completions` reads the last user message and returns a
deterministic assistant response from fixtures. Initial fixtures:

| User message | Assistant response |
|---|---|
| `hello` | `hi` |
| `hi` | `hello` |
| `ping` | `pong` |
| `who are you?` | `I am plexus-fake-qa.` |

Unknown prompts return `I do not have a fixture for that.` instead of calling a
real model.

The mock service does not implement streaming, embeddings, images, tool calls,
or native non-OpenAI provider protocols in M1b.

---

## 8. CI Fake Provider Strategy

Automated Plexus tests must stay hermetic. They cannot require the sibling
`Plexus-mock-llm` repository to exist or be running.

Plexus integration tests may define a tiny test-only OpenAI-compatible HTTP
server under the `plexus-server` test tree. That fake server is allowed only in
tests and must not be reachable from production code.

The test fake covers protocol and failure cases:

- valid `/models` response;
- missing configured model;
- unauthorized response;
- malformed models response;
- delayed chat responses for semaphore tests;
- valid `/chat/completions` response.

This keeps production Plexus free of fake behavior while preserving stable CI.

---

## 9. Error Handling

Admin provider-validation failures return `400 Bad Request` with
`ErrorCode::InvalidArgs`.

Examples:

- missing effective `llm_endpoint`, `llm_api_key`, or `llm_model`;
- empty provider identity string;
- invalid endpoint URL;
- unreachable provider;
- provider timeout;
- provider returns 401/403;
- malformed models response;
- configured model missing from the models response;
- submitted `llm_api_key` equals the redaction marker.

Runtime chat-completion failures should map through existing shared Plexus error
codes rather than introducing new server-local error enums. If a future error
variant is genuinely needed, it belongs in `plexus-common`, not in
`plexus-server/src/openai.rs`.

No error response may include the submitted API key.

---

## 10. Test Plan

M1b implementation should start with failing tests for the new behavior.

Required automated tests:

- successful provider identity patch persists after fake `/models` validation;
- first provider setup without all three identity keys is rejected;
- changing one identity key after a valid setup validates against the merged
  effective config;
- direct OpenAI-client validation rejects unauthorized `/models`, malformed
  `/models`, and models responses missing `llm_model`;
- admin-route validation failures reject before committing unrelated config
  writes, including missing model and invalid endpoint cases;
- `GET /api/admin/config` redacts configured `llm_api_key`;
- sending `llm_api_key: "<redacted>"` in a patch is rejected;
- negative and above-maximum `llm_max_concurrent_requests` values are rejected;
- valid fake provider completes a non-streaming chat completion call;
- `llm_max_concurrent_requests` caps simultaneous chat-completion calls.

Manual smoke with `Plexus-mock-llm`:

1. Start the FastAPI mock service.
2. Patch Plexus admin config with:
   - `llm_endpoint`: mock service `/v1` base URL;
   - `llm_api_key`: `plexus-mock-key`;
   - `llm_model`: `plexus-fake-qa`.
3. Confirm provider validation succeeds.
4. Confirm `GET /api/admin/config` redacts the stored API key.
5. Confirm mock chat completion returns `hi` for `hello` either directly
   against the mock service or through the automated OpenAI-client test. M1b
   has no public browser chat endpoint; the Plexus end-to-end chat path starts
   in M1c.

Real provider smoke is optional after M1b and must not be part of CI.

---

## 11. Docs To Update During Implementation

When M1b implementation lands, update:

- `docs/API.yaml` to remove the M1a provider-key deferral wording and document
  redacted `llm_api_key` reads;
- `docs/SCHEMA.md` to mark `llm_endpoint`, `llm_api_key`, and `llm_model` as
  active in M1b;
- `docs/DECISIONS.md` only if implementation discovers a real ADR change;
- `README.md` or developer docs with the `Plexus-mock-llm` smoke path;
- this living M1 spec with final status and verification evidence.

---

## 12. Exit Criteria

M1b is complete when:

- provider identity config is accepted only after `/models` validation;
- failed validation is atomic and leaves existing DB config unchanged;
- `GET /api/admin/config` does not reveal the raw LLM API key;
- production LLM calls go through `plexus-server/src/openai.rs`;
- no production Plexus code contains fake-model behavior;
- the external FastAPI mock LLM service can validate and answer deterministic
  chat completions;
- Plexus CI tests use only hermetic test fakes;
- the provider-wide chat-completion semaphore is tested;
- docs are synced with the implemented behavior.

---

## 13. Verification Evidence

M1b was verified on 2026-05-13 from branch `rebuild-m1-M1b`.

Required checks all exited 0:

- `rtk git status --short`
- `rtk cargo fmt --all -- --check`
- `rtk cargo clippy --workspace --all-targets -- -D warnings`
- `rtk bash scripts/reset-postgres18-and-test.sh`
- `rtk env PLEXUS_TEST_DATABASE_URL=postgres://plexus:plexus@127.0.0.1:5432/plexus cargo test --workspace --all-targets`
- `rtk conda run -n Plexus pytest -q` in `../Plexus-mock-llm`
- `rtk git diff --check`
- `docs/API.yaml` validated with `ruamel.yaml`

PostgreSQL 18 verification used container `plexus` from `pgvector/pgvector:pg18`.
After both PostgreSQL-backed test runs, only the persistent `plexus` database
matched `plexus%`, and the persistent `plexus.public` schema contained no
tables. No test tables or rows landed in the persistent database.
