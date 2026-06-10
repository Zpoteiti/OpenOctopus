# Plexus M1b LLM Provider Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the M1b OpenAI-compatible LLM foundation: deterministic external mock service, validated admin LLM config, redacted secret reads, internal non-streaming chat calls, and provider-wide concurrency limiting.

**Architecture:** Plexus production code has one server-side OpenAI-compatible client module at `plexus-server/src/openai.rs`. That module owns external HTTP calls, retry/backoff, provider response parsing, secret-bearing request construction, and concurrency permits; it does not read or write DB messages and does not make compaction decisions. The sibling `Plexus-mock-llm` FastAPI service is a local/manual dev target, while Plexus CI uses hermetic test-only fake HTTP servers.

**Tech Stack:** Rust 2024, Axum, Tokio, SQLx/Postgres, Reqwest with rustls, `plexus-common::LlmApiKey`, FastAPI, Miniforge/conda env `Plexus`.

**Post-implementation alignment notes:** The verified implementation enforces
the canonical `llm_max_concurrent_requests` maximum of `1_000_000`, redacts the
configured API key on `GET /api/admin/config`, uses direct OpenAI-client tests
for malformed/unauthorized `/models` responses, and uses admin-route tests for
successful validation, missing-model atomic rejection, endpoint/config
pre-validation, secret redaction, redaction-marker rejection, concurrency
bounds, and runtime-limit refresh after commit.

---

## Scope Check

The approved M1b spec has two deliverables:

- a sibling FastAPI mock LLM service for manual deterministic testing;
- Plexus server LLM provider foundation code and tests.

They are small enough to keep in one plan because the mock service directly exercises the same OpenAI-compatible contract used by Plexus, but the tasks keep the write sets separate.

---

## File Structure

Create outside the Plexus repo:

- `../Plexus-mock-llm/app/main.py` — FastAPI OpenAI-compatible mock server.
- `../Plexus-mock-llm/app/__init__.py` — package marker.
- `../Plexus-mock-llm/tests/test_mock_llm.py` — FastAPI contract tests.
- `../Plexus-mock-llm/requirements.txt` — Python runtime/test dependencies for the `Plexus` conda env.
- `../Plexus-mock-llm/README.md` — startup and Plexus config instructions.
- `../Plexus-mock-llm/.gitignore` — Python cache and env ignores.

Create in Plexus:

- `plexus-server/src/openai.rs` — OpenAI-compatible validation, chat completion, retry/backoff, and concurrency boundary.
- `plexus-server/tests/support/fake_openai.rs` — hermetic OpenAI-compatible test server.
- `plexus-server/tests/m1b_openai_client.rs` — direct openai client tests.
- `plexus-server/tests/m1b_admin_config.rs` — admin config validation/redaction tests.

Modify in Plexus:

- `Cargo.toml` — add `reqwest` workspace dependency and Tokio `sync` feature.
- `plexus-server/Cargo.toml` — depend on `reqwest`.
- `plexus-server/src/lib.rs` — export `openai`.
- `plexus-server/src/app.rs` — hold shared `OpenAiRuntime`.
- `plexus-server/src/main.rs` — initialize runtime concurrency from persisted config.
- `plexus-server/src/db/system_config.rs` — accept/validate M1b keys, default concurrency to `0`, and redact read responses.
- `plexus-server/src/routes/admin.rs` — validate provider identity before DB write and refresh runtime concurrency after commit.
- `plexus-server/tests/support/mod.rs` — export `fake_openai`.
- `docs/API.yaml`, `docs/SCHEMA.md`, `docs/reference/superpowers/specs/2026-05-12-plexus-m1-living-design.md` — sync implementation status and any contract details discovered during implementation.

---

## Task 1: Create External FastAPI Mock LLM Service

**Files:**
- Create: `../Plexus-mock-llm/app/main.py`
- Create: `../Plexus-mock-llm/app/__init__.py`
- Create: `../Plexus-mock-llm/tests/test_mock_llm.py`
- Create: `../Plexus-mock-llm/requirements.txt`
- Create: `../Plexus-mock-llm/README.md`
- Create: `../Plexus-mock-llm/.gitignore`

- [ ] **Step 1: Create the sibling service directory**

Run:

```bash
rtk mkdir -p ../Plexus-mock-llm/app ../Plexus-mock-llm/tests
```

Expected: directories exist beside `Plexus`, not inside it.

- [ ] **Step 2: Add dependency file**

Create `../Plexus-mock-llm/requirements.txt`:

```text
fastapi>=0.115,<1
uvicorn[standard]>=0.32,<1
pytest>=8,<9
httpx>=0.27,<1
```

- [ ] **Step 3: Add the FastAPI app**

Create `../Plexus-mock-llm/app/__init__.py` as an empty file.

Create `../Plexus-mock-llm/app/main.py`:

```python
from __future__ import annotations

import os
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request

MODEL_ID = "plexus-fake-qa"
API_KEY = os.environ.get("PLEXUS_MOCK_LLM_API_KEY", "plexus-mock-key")

FIXTURES = {
    "hello": "hi",
    "hi": "hello",
    "ping": "pong",
    "who are you?": "I am plexus-fake-qa.",
}

app = FastAPI(title="Plexus Mock LLM")


def require_auth(authorization: str | None) -> None:
    expected = f"Bearer {API_KEY}"
    if authorization != expected:
        raise HTTPException(
            status_code=401,
            detail={
                "error": {
                    "message": "invalid bearer token",
                    "type": "authentication_error",
                    "code": "invalid_api_key",
                }
            },
        )


def content_to_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                text = item.get("text")
                if isinstance(text, str):
                    parts.append(text)
        return " ".join(parts)
    return ""


def last_user_text(messages: list[Any]) -> str:
    for message in reversed(messages):
        if isinstance(message, dict) and message.get("role") == "user":
            return content_to_text(message.get("content")).strip()
    return ""


@app.get("/v1/models")
async def list_models(authorization: str | None = Header(default=None)) -> dict[str, Any]:
    require_auth(authorization)
    return {
        "object": "list",
        "data": [
            {
                "id": MODEL_ID,
                "object": "model",
                "created": 0,
                "owned_by": "plexus",
            }
        ],
    }


@app.post("/v1/chat/completions")
async def chat_completions(
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    require_auth(authorization)
    body = await request.json()
    model = body.get("model")
    if model != MODEL_ID:
        raise HTTPException(
            status_code=404,
            detail={
                "error": {
                    "message": f"model not found: {model}",
                    "type": "invalid_request_error",
                    "code": "model_not_found",
                }
            },
        )

    messages = body.get("messages")
    if not isinstance(messages, list):
        raise HTTPException(
            status_code=400,
            detail={
                "error": {
                    "message": "messages must be an array",
                    "type": "invalid_request_error",
                    "code": "invalid_messages",
                }
            },
        )

    prompt = last_user_text(messages)
    answer = FIXTURES.get(prompt.lower(), "I do not have a fixture for that.")
    return {
        "id": "chatcmpl_plexus_fake",
        "object": "chat.completion",
        "created": 0,
        "model": MODEL_ID,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": answer},
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
    }
```

- [ ] **Step 4: Add FastAPI contract tests**

Create `../Plexus-mock-llm/tests/test_mock_llm.py`:

```python
from fastapi.testclient import TestClient

from app.main import MODEL_ID, app


client = TestClient(app)
AUTH = {"Authorization": "Bearer plexus-mock-key"}


def test_models_requires_bearer_token() -> None:
    response = client.get("/v1/models")
    assert response.status_code == 401


def test_models_returns_fake_model() -> None:
    response = client.get("/v1/models", headers=AUTH)
    assert response.status_code == 200
    body = response.json()
    assert body["object"] == "list"
    assert body["data"][0]["id"] == MODEL_ID


def test_chat_returns_fixture_response() -> None:
    response = client.post(
        "/v1/chat/completions",
        headers=AUTH,
        json={
            "model": MODEL_ID,
            "stream": False,
            "messages": [{"role": "user", "content": "hello"}],
        },
    )
    assert response.status_code == 200
    assert response.json()["choices"][0]["message"]["content"] == "hi"


def test_chat_unknown_prompt_is_stable() -> None:
    response = client.post(
        "/v1/chat/completions",
        headers=AUTH,
        json={
            "model": MODEL_ID,
            "stream": False,
            "messages": [{"role": "user", "content": "unmapped"}],
        },
    )
    assert response.status_code == 200
    assert response.json()["choices"][0]["message"]["content"] == (
        "I do not have a fixture for that."
    )
```

- [ ] **Step 5: Add README and ignores**

Create `../Plexus-mock-llm/.gitignore`:

```text
__pycache__/
.pytest_cache/
.mypy_cache/
.ruff_cache/
.venv/
```

Create `../Plexus-mock-llm/README.md`:

````markdown
# Plexus Mock LLM

Deterministic OpenAI-compatible mock service for Plexus M1b local testing.

## Run

```bash
conda activate Plexus
pip install -r requirements.txt
uvicorn app.main:app --host 127.0.0.1 --port 8089
```

## Plexus Admin Config

Patch Plexus with:

```json
{
  "llm_endpoint": "http://127.0.0.1:8089/v1",
  "llm_api_key": "plexus-mock-key",
  "llm_model": "plexus-fake-qa",
  "llm_max_concurrent_requests": 0
}
```

`GET /v1/models` returns `plexus-fake-qa`.
`POST /v1/chat/completions` returns deterministic fixture responses:

| User message | Assistant response |
|---|---|
| `hello` | `hi` |
| `hi` | `hello` |
| `ping` | `pong` |
| `who are you?` | `I am plexus-fake-qa.` |

Unknown prompts return `I do not have a fixture for that.`
````

- [ ] **Step 6: Verify the mock service tests**

From `../Plexus-mock-llm`, run:

```bash
rtk conda run -n Plexus pip install -r requirements.txt
rtk conda run -n Plexus pytest -q
```

Expected: all tests pass.

- [ ] **Step 7: Commit the mock service**

From `../Plexus-mock-llm`, run:

```bash
rtk git init
rtk git add app tests requirements.txt README.md .gitignore
rtk git commit -m "feat: add deterministic Plexus mock LLM"
```

Expected: sibling repository has one commit. Do not add this directory to the Plexus repository.

---

## Task 2: Add Rust Dependency and Module Skeleton

**Files:**
- Modify: `Cargo.toml`
- Modify: `plexus-server/Cargo.toml`
- Modify: `plexus-server/src/lib.rs`
- Create: `plexus-server/src/openai.rs`

- [ ] **Step 1: Add `reqwest` and Tokio sync support**

Edit root `Cargo.toml`:

```toml
tokio = { version = "1", features = ["fs", "process", "macros", "rt-multi-thread", "time", "sync"] }
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
```

Edit `plexus-server/Cargo.toml` dependencies:

```toml
reqwest.workspace = true
```

- [ ] **Step 2: Export the OpenAI module**

Edit `plexus-server/src/lib.rs`:

```rust
pub mod app;
pub mod auth;
pub mod config;
pub mod db;
pub mod error;
pub mod openai;
pub mod routes;
```

- [ ] **Step 3: Add compiling `openai.rs` skeleton**

Create `plexus-server/src/openai.rs`:

```rust
use crate::error::ApiError;
use axum::http::StatusCode;
use plexus_common::{ErrorCode, LlmApiKey};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};

pub const REDACTED_LLM_API_KEY: &str = "<redacted>";
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug)]
pub struct OpenAiConfig {
    pub endpoint: Url,
    pub api_key: LlmApiKey,
    pub model: String,
}

#[derive(Clone)]
pub struct OpenAiClient {
    http: reqwest::Client,
}

impl Default for OpenAiClient {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenAiClient {
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::new(),
        }
    }
}

#[derive(Clone)]
pub struct OpenAiRuntime {
    client: OpenAiClient,
    limiter: Arc<RwLock<Option<Arc<Semaphore>>>>,
}

impl Default for OpenAiRuntime {
    fn default() -> Self {
        Self::new(0)
    }
}

impl OpenAiRuntime {
    pub fn new(limit: i64) -> Self {
        Self {
            client: OpenAiClient::new(),
            limiter: Arc::new(RwLock::new(limit_to_semaphore(limit))),
        }
    }

    pub fn client(&self) -> &OpenAiClient {
        &self.client
    }

    pub async fn set_concurrency_limit(&self, limit: i64) -> Result<(), ApiError> {
        if limit < 0 {
            return Err(ApiError::invalid_args(
                "llm_max_concurrent_requests must be zero or positive",
            ));
        }
        *self.limiter.write().await = limit_to_semaphore(limit);
        Ok(())
    }

    async fn acquire_permit(&self) -> Result<Option<OwnedSemaphorePermit>, ApiError> {
        let limiter = self.limiter.read().await.clone();
        match limiter {
            Some(semaphore) => semaphore.acquire_owned().await.map(Some).map_err(|_| {
                ApiError::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    ErrorCode::HttpError,
                    "LLM concurrency limiter is closed",
                )
            }),
            None => Ok(None),
        }
    }
}

fn limit_to_semaphore(limit: i64) -> Option<Arc<Semaphore>> {
    if limit > 0 {
        Some(Arc::new(Semaphore::new(limit as usize)))
    } else {
        None
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ChatRole {
    System,
    User,
    Assistant,
}

#[derive(Clone, Debug, Serialize)]
pub struct ChatMessage {
    pub role: ChatRole,
    pub content: String,
}

#[derive(Clone, Debug)]
pub struct ChatCompletionRequest {
    pub messages: Vec<ChatMessage>,
    pub max_tokens: Option<u32>,
    pub temperature: Option<f32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChatCompletionResponse {
    pub content: String,
    pub finish_reason: Option<String>,
}

#[derive(Deserialize)]
struct ModelsResponse {
    data: Vec<ModelInfo>,
}

#[derive(Deserialize)]
struct ModelInfo {
    id: String,
}

#[derive(Serialize)]
struct ChatRequestBody<'a> {
    model: &'a str,
    messages: &'a [ChatMessage],
    stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
}

#[derive(Deserialize)]
struct ChatResponseBody {
    choices: Vec<ChatChoice>,
}

#[derive(Deserialize)]
struct ChatChoice {
    message: ChatResponseMessage,
    finish_reason: Option<String>,
}

#[derive(Deserialize)]
struct ChatResponseMessage {
    content: Option<String>,
}

fn invalid_provider_config(message: impl Into<String>) -> ApiError {
    ApiError::new(StatusCode::BAD_REQUEST, ErrorCode::InvalidArgs, message)
}

fn provider_http_error(message: impl Into<String>) -> ApiError {
    ApiError::new(StatusCode::BAD_GATEWAY, ErrorCode::HttpError, message)
}
```

- [ ] **Step 4: Run compile check and commit**

Run:

```bash
rtk cargo check -p plexus-server
```

Expected: compile succeeds.

Commit:

```bash
rtk git add Cargo.toml Cargo.lock plexus-server/Cargo.toml plexus-server/src/lib.rs plexus-server/src/openai.rs
rtk git commit -m "feat(server): add OpenAI client module skeleton"
```

---

## Task 3: Add Hermetic Fake OpenAI Test Server

**Files:**
- Create: `plexus-server/tests/support/fake_openai.rs`
- Modify: `plexus-server/tests/support/mod.rs`
- Create: `plexus-server/tests/m1b_openai_client.rs`

- [ ] **Step 1: Add test fake module export**

Append to `plexus-server/tests/support/mod.rs`:

```rust
pub mod fake_openai;
```

- [ ] **Step 2: Add the fake OpenAI server helper**

Create `plexus-server/tests/support/fake_openai.rs`:

```rust
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use serde_json::{Value, json};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

#[derive(Clone)]
struct FakeState {
    model: String,
    api_key: String,
    mode: FakeMode,
    delay: Duration,
    in_flight: Arc<AtomicUsize>,
    max_in_flight: Arc<AtomicUsize>,
}

#[derive(Clone)]
enum FakeMode {
    Valid,
    MissingModel,
    MalformedModels,
}

pub struct FakeOpenAi {
    pub base_url: String,
    shutdown: Option<oneshot::Sender<()>>,
    handle: JoinHandle<()>,
    max_in_flight: Arc<AtomicUsize>,
}

impl FakeOpenAi {
    pub async fn valid() -> Self {
        Self::spawn(FakeMode::Valid, Duration::ZERO).await
    }

    pub async fn missing_model() -> Self {
        Self::spawn(FakeMode::MissingModel, Duration::ZERO).await
    }

    pub async fn malformed_models() -> Self {
        Self::spawn(FakeMode::MalformedModels, Duration::ZERO).await
    }

    pub async fn delayed(delay: Duration) -> Self {
        Self::spawn(FakeMode::Valid, delay).await
    }

    pub fn model(&self) -> &'static str {
        "plexus-fake-qa"
    }

    pub fn api_key(&self) -> &'static str {
        "plexus-mock-key"
    }

    pub fn max_in_flight(&self) -> usize {
        self.max_in_flight.load(Ordering::SeqCst)
    }

    async fn spawn(mode: FakeMode, delay: Duration) -> Self {
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let state = FakeState {
            model: "plexus-fake-qa".to_string(),
            api_key: "plexus-mock-key".to_string(),
            mode,
            delay,
            in_flight,
            max_in_flight: max_in_flight.clone(),
        };

        let router = Router::new()
            .route("/v1/models", get(models))
            .route("/v1/chat/completions", post(chat))
            .with_state(state);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        });

        Self {
            base_url: format!("http://{addr}/v1"),
            shutdown: Some(shutdown_tx),
            handle,
            max_in_flight,
        }
    }
}

impl Drop for FakeOpenAi {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.handle.abort();
    }
}

async fn models(State(state): State<FakeState>, headers: HeaderMap) -> (StatusCode, Json<Value>) {
    if !authorized(&state, &headers) {
        return (StatusCode::UNAUTHORIZED, Json(error("invalid_api_key")));
    }

    match state.mode {
        FakeMode::Valid => (
            StatusCode::OK,
            Json(json!({
                "object": "list",
                "data": [{"id": state.model, "object": "model"}]
            })),
        ),
        FakeMode::MissingModel => (
            StatusCode::OK,
            Json(json!({
                "object": "list",
                "data": [{"id": "different-model", "object": "model"}]
            })),
        ),
        FakeMode::MalformedModels => (StatusCode::OK, Json(json!({"data": "bad"}))),
    }
}

async fn chat(
    State(state): State<FakeState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    if !authorized(&state, &headers) {
        return (StatusCode::UNAUTHORIZED, Json(error("invalid_api_key")));
    }

    let current = state.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
    update_max(&state.max_in_flight, current);
    if !state.delay.is_zero() {
        tokio::time::sleep(state.delay).await;
    }
    state.in_flight.fetch_sub(1, Ordering::SeqCst);

    let stream = body.get("stream").and_then(Value::as_bool);
    if stream != Some(false) {
        return (StatusCode::BAD_REQUEST, Json(error("stream_must_be_false")));
    }

    let last_user = body["messages"]
        .as_array()
        .and_then(|messages| {
            messages
                .iter()
                .rev()
                .find(|message| message["role"] == "user")
        })
        .and_then(|message| message["content"].as_str())
        .unwrap_or_default();

    let content = match last_user {
        "hello" => "hi",
        "ping" => "pong",
        _ => "I do not have a fixture for that.",
    };

    (
        StatusCode::OK,
        Json(json!({
            "id": "chatcmpl_test",
            "object": "chat.completion",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": "stop"
            }]
        })),
    )
}

fn authorized(state: &FakeState, headers: &HeaderMap) -> bool {
    let expected = format!("Bearer {}", state.api_key);
    headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        == Some(expected.as_str())
}

fn error(code: &str) -> Value {
    json!({"error": {"message": code, "type": "invalid_request_error", "code": code}})
}

fn update_max(max: &AtomicUsize, current: usize) {
    let mut observed = max.load(Ordering::SeqCst);
    while current > observed {
        match max.compare_exchange(observed, current, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => break,
            Err(next) => observed = next,
        }
    }
}
```

- [ ] **Step 3: Add initial failing client tests**

Create `plexus-server/tests/m1b_openai_client.rs`:

```rust
mod support;

use plexus_common::LlmApiKey;
use plexus_server::openai::{
    ChatCompletionRequest, ChatMessage, ChatRole, OpenAiClient, OpenAiConfig, OpenAiRuntime,
};
use support::fake_openai::FakeOpenAi;
use std::time::{Duration, Instant};

fn config(fake: &FakeOpenAi) -> OpenAiConfig {
    OpenAiConfig {
        endpoint: fake.base_url.parse().unwrap(),
        api_key: LlmApiKey::new(fake.api_key().to_string()),
        model: fake.model().to_string(),
    }
}

#[tokio::test]
async fn validate_config_accepts_model_from_models_response() {
    let fake = FakeOpenAi::valid().await;
    OpenAiClient::new()
        .validate_config(&config(&fake))
        .await
        .expect("valid config");
}

#[tokio::test]
async fn validate_config_rejects_missing_model() {
    let fake = FakeOpenAi::missing_model().await;
    let err = OpenAiClient::new()
        .validate_config(&config(&fake))
        .await
        .expect_err("missing model should reject");
    assert_eq!(err.status, axum::http::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn validate_config_rejects_unauthorized_models_response() {
    let fake = FakeOpenAi::valid().await;
    let mut cfg = config(&fake);
    cfg.api_key = LlmApiKey::new("wrong-key".to_string());
    let err = OpenAiClient::new()
        .validate_config(&cfg)
        .await
        .expect_err("bad key should reject");
    assert_eq!(err.status, axum::http::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn validate_config_rejects_malformed_models_response() {
    let fake = FakeOpenAi::malformed_models().await;
    let err = OpenAiClient::new()
        .validate_config(&config(&fake))
        .await
        .expect_err("malformed response should reject");
    assert_eq!(err.status, axum::http::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn chat_completion_sends_non_streaming_request_and_returns_content() {
    let fake = FakeOpenAi::valid().await;
    let response = OpenAiClient::new()
        .chat_completion(
            &config(&fake),
            ChatCompletionRequest {
                messages: vec![ChatMessage {
                    role: ChatRole::User,
                    content: "hello".to_string(),
                }],
                max_tokens: None,
                temperature: None,
            },
        )
        .await
        .expect("chat response");

    assert_eq!(response.content, "hi");
    assert_eq!(response.finish_reason.as_deref(), Some("stop"));
}

#[tokio::test]
async fn runtime_concurrency_limit_caps_in_flight_chat_requests() {
    let fake = FakeOpenAi::delayed(Duration::from_millis(150)).await;
    let runtime = OpenAiRuntime::new(1);
    let cfg = config(&fake);
    let request = ChatCompletionRequest {
        messages: vec![ChatMessage {
            role: ChatRole::User,
            content: "ping".to_string(),
        }],
        max_tokens: None,
        temperature: None,
    };

    let started = Instant::now();
    let (left, right) = tokio::join!(
        runtime.chat_completion(&cfg, request.clone()),
        runtime.chat_completion(&cfg, request),
    );

    left.expect("left response");
    right.expect("right response");
    assert_eq!(fake.max_in_flight(), 1);
    assert!(started.elapsed() >= Duration::from_millis(250));
}
```

- [ ] **Step 4: Run client tests to verify they fail**

Run:

```bash
rtk cargo test -p plexus-server --test m1b_openai_client
```

Expected: compile fails because `validate_config`, `chat_completion`, and runtime chat methods are not implemented.

---

## Task 4: Implement OpenAI Client Validation, Chat, Retry, and Runtime Limit

**Files:**
- Modify: `plexus-server/src/openai.rs`
- Test: `plexus-server/tests/m1b_openai_client.rs`

- [ ] **Step 1: Add URL joining and retry helpers**

Add to `plexus-server/src/openai.rs`:

```rust
fn endpoint_url(endpoint: &Url, suffix: &str) -> Result<Url, ApiError> {
    let mut url = endpoint.clone();
    let base = url.path().trim_end_matches('/');
    url.set_path(&format!("{base}/{suffix}"));
    Ok(url)
}

fn is_transient_status(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::REQUEST_TIMEOUT
        || status == reqwest::StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

fn retry_delay(attempt: usize) -> Duration {
    match attempt {
        0 => Duration::from_millis(100),
        1 => Duration::from_millis(250),
        _ => Duration::from_millis(500),
    }
}
```

- [ ] **Step 2: Implement `/models` validation**

Add to `impl OpenAiClient` in `plexus-server/src/openai.rs`:

```rust
pub async fn validate_config(&self, cfg: &OpenAiConfig) -> Result<(), ApiError> {
    let url = endpoint_url(&cfg.endpoint, "models")?;
    let response = self
        .http
        .get(url)
        .bearer_auth(cfg.api_key.expose_secret())
        .timeout(DEFAULT_REQUEST_TIMEOUT)
        .send()
        .await
        .map_err(|err| invalid_provider_config(format!("LLM models request failed: {err}")))?;

    if !response.status().is_success() {
        return Err(invalid_provider_config(format!(
            "LLM models request returned HTTP {}",
            response.status()
        )));
    }

    let models = response
        .json::<ModelsResponse>()
        .await
        .map_err(|err| invalid_provider_config(format!("LLM models response was malformed: {err}")))?;

    if models.data.iter().any(|model| model.id == cfg.model) {
        Ok(())
    } else {
        Err(invalid_provider_config(format!(
            "LLM model '{}' was not listed by provider",
            cfg.model
        )))
    }
}
```

- [ ] **Step 3: Implement chat completion with transient retry**

Add to `impl OpenAiClient`:

```rust
pub async fn chat_completion(
    &self,
    cfg: &OpenAiConfig,
    request: ChatCompletionRequest,
) -> Result<ChatCompletionResponse, ApiError> {
    let url = endpoint_url(&cfg.endpoint, "chat/completions")?;
    let body = ChatRequestBody {
        model: &cfg.model,
        messages: &request.messages,
        stream: false,
        max_tokens: request.max_tokens,
        temperature: request.temperature,
    };

    let mut last_error: Option<ApiError> = None;
    for attempt in 0..3 {
        let result = self
            .http
            .post(url.clone())
            .bearer_auth(cfg.api_key.expose_secret())
            .timeout(DEFAULT_REQUEST_TIMEOUT)
            .json(&body)
            .send()
            .await;

        let response = match result {
            Ok(response) => response,
            Err(err) => {
                last_error = Some(provider_http_error(format!("LLM chat request failed: {err}")));
                if attempt < 2 {
                    tokio::time::sleep(retry_delay(attempt)).await;
                    continue;
                }
                break;
            }
        };

        let status = response.status();
        if !status.is_success() {
            last_error = Some(provider_http_error(format!(
                "LLM chat request returned HTTP {status}"
            )));
            if attempt < 2 && is_transient_status(status) {
                tokio::time::sleep(retry_delay(attempt)).await;
                continue;
            }
            break;
        }

        let parsed = response
            .json::<ChatResponseBody>()
            .await
            .map_err(|err| provider_http_error(format!("LLM chat response was malformed: {err}")))?;
        let choice = parsed
            .choices
            .into_iter()
            .next()
            .ok_or_else(|| provider_http_error("LLM chat response had no choices"))?;
        let content = choice
            .message
            .content
            .ok_or_else(|| provider_http_error("LLM chat response had no assistant content"))?;

        return Ok(ChatCompletionResponse {
            content,
            finish_reason: choice.finish_reason,
        });
    }

    Err(last_error.unwrap_or_else(|| provider_http_error("LLM chat request failed")))
}
```

- [ ] **Step 4: Implement runtime chat wrapper with permit acquisition**

Add to `impl OpenAiRuntime`:

```rust
pub async fn chat_completion(
    &self,
    cfg: &OpenAiConfig,
    request: ChatCompletionRequest,
) -> Result<ChatCompletionResponse, ApiError> {
    let _permit = self.acquire_permit().await?;
    self.client.chat_completion(cfg, request).await
}
```

- [ ] **Step 5: Run client tests to verify they pass**

Run:

```bash
rtk cargo test -p plexus-server --test m1b_openai_client
```

Expected: all `m1b_openai_client` tests pass.

- [ ] **Step 6: Commit**

Run:

```bash
rtk git add Cargo.toml Cargo.lock plexus-server/Cargo.toml plexus-server/src/lib.rs plexus-server/src/openai.rs plexus-server/tests/support/mod.rs plexus-server/tests/support/fake_openai.rs plexus-server/tests/m1b_openai_client.rs
rtk git commit -m "feat(server): add OpenAI-compatible LLM client"
```

---

## Task 5: Wire Admin Config Validation and Secret Redaction

**Files:**
- Modify: `plexus-server/src/app.rs`
- Modify: `plexus-server/src/main.rs`
- Modify: `plexus-server/src/db/system_config.rs`
- Modify: `plexus-server/src/routes/admin.rs`
- Create: `plexus-server/tests/m1b_admin_config.rs`

- [ ] **Step 1: Add failing admin config tests**

Create `plexus-server/tests/m1b_admin_config.rs`:

```rust
mod support;

use axum::{
    body::Body,
    http::{Method, Request, StatusCode, header},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use support::{TestApp, fake_openai::FakeOpenAi};
use tower::ServiceExt;

async fn json_request(
    app: axum::Router,
    method: Method,
    path: &str,
    body: Value,
    auth: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(token) = auth {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }

    let response = app
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, json)
}

async fn register_admin(app: axum::Router) -> String {
    let (status, body) = json_request(
        app,
        Method::POST,
        "/api/auth/register",
        json!({
            "email": "admin@example.com",
            "password": "correct horse battery staple",
            "name": "Admin",
            "admin_token": "test-admin-token"
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    body["jwt"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn admin_can_set_valid_llm_identity_after_models_validation() {
    let app = TestApp::spawn().await;
    let token = register_admin(app.router.clone()).await;
    let fake = FakeOpenAi::valid().await;

    let (status, body) = json_request(
        app.router.clone(),
        Method::PATCH,
        "/api/admin/config",
        json!({
            "llm_endpoint": fake.base_url,
            "llm_api_key": fake.api_key(),
            "llm_model": fake.model(),
            "llm_max_concurrent_requests": 0
        }),
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["llm_endpoint"], fake.base_url);
    assert_eq!(body["llm_api_key"], "<redacted>");
    assert_eq!(body["llm_model"], fake.model());

    let stored: (serde_json::Value,) =
        sqlx::query_as("SELECT value FROM system_config WHERE key = 'llm_api_key'")
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert_eq!(stored.0, json!(fake.api_key()));
}

#[tokio::test]
async fn invalid_llm_identity_patch_is_atomic() {
    let app = TestApp::spawn().await;
    let token = register_admin(app.router.clone()).await;
    let fake = FakeOpenAi::missing_model().await;

    let (status, _) = json_request(
        app.router.clone(),
        Method::PATCH,
        "/api/admin/config",
        json!({
            "quota_bytes": 999,
            "llm_endpoint": fake.base_url,
            "llm_api_key": fake.api_key(),
            "llm_model": fake.model()
        }),
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    let quota: (serde_json::Value,) =
        sqlx::query_as("SELECT value FROM system_config WHERE key = 'quota_bytes'")
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert_ne!(quota.0, json!(999));
}

#[tokio::test]
async fn first_llm_identity_setup_requires_all_three_keys() {
    let app = TestApp::spawn().await;
    let token = register_admin(app.router.clone()).await;
    let fake = FakeOpenAi::valid().await;

    let (status, body) = json_request(
        app.router.clone(),
        Method::PATCH,
        "/api/admin/config",
        json!({
            "llm_endpoint": fake.base_url,
            "llm_model": fake.model()
        }),
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "invalid_args");
}

#[tokio::test]
async fn llm_identity_patch_can_reuse_stored_key() {
    let app = TestApp::spawn().await;
    let token = register_admin(app.router.clone()).await;
    let fake = FakeOpenAi::valid().await;

    let (status, _) = json_request(
        app.router.clone(),
        Method::PATCH,
        "/api/admin/config",
        json!({
            "llm_endpoint": fake.base_url,
            "llm_api_key": fake.api_key(),
            "llm_model": fake.model()
        }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = json_request(
        app.router.clone(),
        Method::PATCH,
        "/api/admin/config",
        json!({"llm_model": fake.model()}),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["llm_api_key"], "<redacted>");
}

#[tokio::test]
async fn redaction_marker_is_rejected_as_new_api_key() {
    let app = TestApp::spawn().await;
    let token = register_admin(app.router.clone()).await;
    let fake = FakeOpenAi::valid().await;

    let (status, _) = json_request(
        app.router.clone(),
        Method::PATCH,
        "/api/admin/config",
        json!({
            "llm_endpoint": fake.base_url,
            "llm_api_key": "<redacted>",
            "llm_model": fake.model()
        }),
        Some(&token),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
}
```

- [ ] **Step 2: Run admin tests to verify they fail**

Run:

```bash
rtk cargo test -p plexus-server --test m1b_admin_config
```

Expected: tests fail because provider identity keys are still rejected or redaction is not implemented.

- [ ] **Step 3: Add OpenAI runtime to app state**

Edit `plexus-server/src/app.rs`:

```rust
use crate::{config::ServerConfig, openai::OpenAiRuntime, routes};
use axum::Router;
use sqlx::PgPool;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    inner: Arc<AppStateInner>,
}

pub struct AppStateInner {
    pub pool: PgPool,
    pub config: ServerConfig,
    pub openai: OpenAiRuntime,
}

impl AppState {
    pub fn new(pool: PgPool, config: ServerConfig) -> Self {
        Self::new_with_openai_runtime(pool, config, OpenAiRuntime::default())
    }

    pub fn new_with_openai_runtime(
        pool: PgPool,
        config: ServerConfig,
        openai: OpenAiRuntime,
    ) -> Self {
        Self {
            inner: Arc::new(AppStateInner {
                pool,
                config,
                openai,
            }),
        }
    }

    pub fn pool(&self) -> &PgPool {
        &self.inner.pool
    }

    pub fn config(&self) -> &ServerConfig {
        &self.inner.config
    }

    pub fn openai(&self) -> &OpenAiRuntime {
        &self.inner.openai
    }
}

pub fn router(state: AppState) -> Router {
    routes::router().with_state(state)
}
```

- [ ] **Step 4: Initialize runtime concurrency from persisted config**

Edit `plexus-server/src/main.rs`:

```rust
use plexus_server::{app, config, db, openai::OpenAiRuntime};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = config::ServerConfig::from_env()?;
    tokio::fs::create_dir_all(&cfg.workspace_root).await?;
    let pool = db::connect(&cfg.database_url).await?;
    db::bootstrap(&pool).await?;
    let llm_limit = db::system_config::current_concurrency_limit(&pool).await?;
    let openai = OpenAiRuntime::new(llm_limit);

    let listener = tokio::net::TcpListener::bind(cfg.bind).await?;
    axum::serve(
        listener,
        app::router(app::AppState::new_with_openai_runtime(pool, cfg, openai)),
    )
    .await?;
    Ok(())
}
```

- [ ] **Step 5: Update system config defaults and validation**

In `plexus-server/src/db/system_config.rs`, replace the key constants and default for concurrency:

```rust
pub const SUPPORTED_CONFIG_KEYS: &[&str] = &[
    "quota_bytes",
    "shared_workspace_quota_bytes",
    "llm_max_context_tokens",
    "llm_compaction_threshold_tokens",
    "llm_max_concurrent_requests",
    "llm_endpoint",
    "llm_api_key",
    "llm_model",
];

pub const LLM_IDENTITY_KEYS: &[&str] = &["llm_endpoint", "llm_api_key", "llm_model"];
pub const MAX_CONCURRENCY_LIMIT: i64 = 1_000_000;
```

Change the seeded `llm_max_concurrent_requests` default:

```rust
("llm_max_concurrent_requests", json!(0)),
```

Replace deferred-key rejection in `validate_patch` with supported-key validation:

```rust
if !SUPPORTED_CONFIG_KEYS.contains(&key.as_str()) {
    return Err(ApiError::invalid_args(format!(
        "unsupported config key: {key}"
    )));
}
validate_value(&key, &value)?;
out.insert(key, value);
```

Extend `validate_value`:

```rust
"llm_endpoint" | "llm_api_key" | "llm_model" => non_empty_string(key, value),
"llm_max_concurrent_requests" => non_negative_i64(key, value)
    .and_then(validate_concurrency_limit)
    .map(|_| ()),
```

Add helpers:

```rust
pub fn identity_changed(values: &BTreeMap<String, Value>) -> bool {
    values
        .keys()
        .any(|key| LLM_IDENTITY_KEYS.contains(&key.as_str()))
}

pub fn redact_for_response(mut values: BTreeMap<String, Value>) -> BTreeMap<String, Value> {
    if values.contains_key("llm_api_key") {
        values.insert("llm_api_key".to_string(), json!(crate::openai::REDACTED_LLM_API_KEY));
    }
    values
}

pub fn concurrency_limit(values: &BTreeMap<String, Value>) -> Option<i64> {
    values
        .get("llm_max_concurrent_requests")
        .and_then(Value::as_i64)
}

pub async fn current_concurrency_limit(pool: &PgPool) -> Result<i64, sqlx::Error> {
    let row: Option<(Value,)> =
        sqlx::query_as("SELECT value FROM system_config WHERE key = 'llm_max_concurrent_requests'")
            .fetch_optional(pool)
            .await?;
    Ok(row.and_then(|(value,)| value.as_i64()).unwrap_or(0))
}

pub fn merged_llm_config(
    current: &BTreeMap<String, Value>,
    patch: &BTreeMap<String, Value>,
) -> Result<crate::openai::OpenAiConfig, ApiError> {
    let endpoint = merged_string(current, patch, "llm_endpoint")?;
    let api_key = merged_string(current, patch, "llm_api_key")?;
    let model = merged_string(current, patch, "llm_model")?;
    if api_key == crate::openai::REDACTED_LLM_API_KEY {
        return Err(ApiError::invalid_args("llm_api_key cannot be <redacted>"));
    }
    let endpoint: reqwest::Url = endpoint
        .parse()
        .map_err(|_| ApiError::invalid_args("llm_endpoint must be an absolute URL"))?;
    if !matches!(endpoint.scheme(), "http" | "https") || endpoint.host_str().is_none() {
        return Err(ApiError::invalid_args(
            "llm_endpoint must be an absolute http or https URL",
        ));
    }
    Ok(crate::openai::OpenAiConfig {
        endpoint,
        api_key: plexus_common::LlmApiKey::new(api_key),
        model,
    })
}

fn merged_string(
    current: &BTreeMap<String, Value>,
    patch: &BTreeMap<String, Value>,
    key: &str,
) -> Result<String, ApiError> {
    let value = patch.get(key).or_else(|| current.get(key)).ok_or_else(|| {
        ApiError::invalid_args(format!("{key} is required for LLM provider validation"))
    })?;
    let text = value
        .as_str()
        .ok_or_else(|| ApiError::invalid_args(format!("{key} must be a string")))?
        .trim()
        .to_string();
    if text.is_empty() {
        return Err(ApiError::invalid_args(format!("{key} must not be empty")));
    }
    Ok(text)
}

fn non_empty_string(key: &str, value: &Value) -> Result<(), ApiError> {
    let text = value
        .as_str()
        .ok_or_else(|| ApiError::invalid_args(format!("{key} must be a string")))?;
    if text.trim().is_empty() {
        return Err(ApiError::invalid_args(format!("{key} must not be empty")));
    }
    if key == "llm_api_key" && text == crate::openai::REDACTED_LLM_API_KEY {
        return Err(ApiError::invalid_args("llm_api_key cannot be <redacted>"));
    }
    Ok(())
}

fn non_negative_i64(key: &str, value: &Value) -> Result<i64, ApiError> {
    let n = value
        .as_i64()
        .ok_or_else(|| ApiError::invalid_args(format!("{key} must be an integer")))?;
    if n < 0 {
        return Err(ApiError::invalid_args(format!("{key} must be zero or positive")));
    }
    Ok(n)
}

fn validate_concurrency_limit(value: i64) -> Result<i64, ApiError> {
    if value > MAX_CONCURRENCY_LIMIT {
        return Err(ApiError::invalid_args(format!(
            "llm_max_concurrent_requests must be at most {MAX_CONCURRENCY_LIMIT}"
        )));
    }
    Ok(value)
}
```

- [ ] **Step 6: Wire provider validation into admin route**

Edit `plexus-server/src/routes/admin.rs`:

```rust
pub async fn get_config(
    _admin: AdminUser,
    State(state): State<AppState>,
) -> Result<Json<BTreeMap<String, Value>>, ApiError> {
    let values = system_config::get_all(state.pool())
        .await
        .map_err(ApiError::from_sqlx)?;
    Ok(Json(system_config::redact_for_response(values)))
}

pub async fn patch_config(
    _admin: AdminUser,
    State(state): State<AppState>,
    Json(input): Json<BTreeMap<String, Value>>,
) -> Result<Json<BTreeMap<String, Value>>, ApiError> {
    let values = system_config::validate_patch(input)?;
    let current = system_config::get_all(state.pool())
        .await
        .map_err(ApiError::from_sqlx)?;

    if system_config::identity_changed(&values) {
        let cfg = system_config::merged_llm_config(&current, &values)?;
        state.openai().client().validate_config(&cfg).await?;
    }

    let mut tx = state.pool().begin().await.map_err(ApiError::from_sqlx)?;
    system_config::set_many(&mut tx, &values)
        .await
        .map_err(ApiError::from_sqlx)?;
    tx.commit().await.map_err(ApiError::from_sqlx)?;

    if let Some(limit) = system_config::concurrency_limit(&values) {
        state.openai().set_concurrency_limit(limit).await?;
    }

    let current = system_config::get_all(state.pool())
        .await
        .map_err(ApiError::from_sqlx)?;
    Ok(Json(system_config::redact_for_response(current)))
}
```

- [ ] **Step 7: Run admin tests**

Run:

```bash
rtk cargo test -p plexus-server --test m1b_admin_config
```

Expected: all M1b admin config tests pass.

- [ ] **Step 8: Run M1a admin tests to catch regressions**

Run:

```bash
rtk cargo test -p plexus-server --test m1a_admin_config
```

Expected: M1a admin tests pass. If `responses_do_not_leak_known_secrets` still asserts that `llm_api_key` is absent, update that assertion to allow `"<redacted>"` only after a key exists and to reject raw key material.

- [ ] **Step 9: Commit**

Run:

```bash
rtk git add plexus-server/src/app.rs plexus-server/src/main.rs plexus-server/src/db/system_config.rs plexus-server/src/routes/admin.rs plexus-server/tests/m1b_admin_config.rs plexus-server/tests/m1a_admin_config.rs
rtk git commit -m "feat(server): validate LLM admin config"
```

---

## Task 6: Sync Docs and M1b Status

**Files:**
- Modify: `docs/API.yaml`
- Modify: `docs/SCHEMA.md`
- Modify: `docs/reference/superpowers/specs/2026-05-12-plexus-m1-living-design.md`
- Modify: `README.md`

- [ ] **Step 1: Update API docs for active M1b provider keys**

In `docs/API.yaml` under `/api/admin/config`, remove M1a wording that says `llm_endpoint`, `llm_api_key`, and `llm_model` are rejected. Replace it with:

```yaml
        M1b accepts `llm_endpoint`, `llm_api_key`, and `llm_model` on PATCH
        after validating the merged provider identity with `GET
        {llm_endpoint}/models`. `GET /api/admin/config` redacts a configured
        `llm_api_key` as `"<redacted>"`; callers rotate the key by sending a
        new value and keep the existing key by omitting `llm_api_key`.
```

- [ ] **Step 2: Update schema docs**

In `docs/SCHEMA.md`, replace the M1a implementation note for provider identity keys with:

```markdown
M1b accepts `llm_endpoint`, `llm_api_key`, and `llm_model` only after provider
validation succeeds. `llm_api_key` is stored in `system_config` but redacted in
admin config read responses.
```

- [ ] **Step 3: Add mock service pointer to README**

Add a short developer note to `README.md`:

````markdown
### Mock LLM for M1b development

The deterministic OpenAI-compatible mock service lives beside this repository at
`../Plexus-mock-llm`. Start it with:

```bash
cd ../Plexus-mock-llm
conda activate Plexus
uvicorn app.main:app --host 127.0.0.1 --port 8089
```

Use these admin config values:

```json
{
  "llm_endpoint": "http://127.0.0.1:8089/v1",
  "llm_api_key": "plexus-mock-key",
  "llm_model": "plexus-fake-qa",
  "llm_max_concurrent_requests": 0
}
```
````

- [ ] **Step 4: Mark M1b implementation in progress**

In `docs/reference/superpowers/specs/2026-05-12-plexus-m1-living-design.md`, change M1b status from `Approved` to `Implementing` when implementation starts. Do not mark it `Verified` until all verification commands pass.

- [ ] **Step 5: Commit docs**

Run:

```bash
rtk git add docs/API.yaml docs/SCHEMA.md docs/reference/superpowers/specs/2026-05-12-plexus-m1-living-design.md README.md
rtk git commit -m "docs: document M1b LLM config behavior"
```

---

## Task 7: Full Verification

**Files:**
- No new files.
- Verification covers the full workspace and the sibling mock service.

- [ ] **Step 1: Format**

Run:

```bash
rtk cargo fmt --all -- --check
```

Expected: exits 0.

- [ ] **Step 2: Clippy**

Run:

```bash
rtk cargo clippy --workspace --all-targets -- -D warnings
```

Expected: exits 0.

- [ ] **Step 3: Rust tests with Postgres**

Run:

```bash
rtk env PLEXUS_TEST_DATABASE_URL=postgres://plexus:plexus@127.0.0.1:5432/plexus cargo test --workspace --all-targets
```

Expected: exits 0. If Postgres is not running, run `rtk bash scripts/reset-postgres18-and-test.sh` and use its test output as the verification evidence.

- [ ] **Step 4: Mock LLM tests**

From `../Plexus-mock-llm`, run:

```bash
rtk conda run -n Plexus pytest -q
```

Expected: exits 0.

- [ ] **Step 5: Manual mock smoke**

Start mock server:

From `../Plexus-mock-llm`, run:

```bash
rtk conda run -n Plexus uvicorn app.main:app --host 127.0.0.1 --port 8089
```

In a second shell, verify the external mock directly:

```bash
curl -sS http://127.0.0.1:8089/v1/models \
  -H 'Authorization: Bearer plexus-mock-key'
```

Then run Plexus, register an admin, and patch `/api/admin/config` with:

```json
{
  "llm_endpoint": "http://127.0.0.1:8089/v1",
  "llm_api_key": "plexus-mock-key",
  "llm_model": "plexus-fake-qa"
}
```

Expected: the patch succeeds only after mock `/models` validation, and
`GET /api/admin/config` returns `"llm_api_key": "<redacted>"`. M1b has no public
browser chat endpoint, so `hello -> hi` is covered by the direct mock check and
the OpenAI-client test; Plexus end-to-end chat smoke starts in M1c.

- [ ] **Step 6: Mark M1b verified and commit**

Only after every previous verification step passes, update `docs/reference/superpowers/specs/2026-05-12-plexus-m1-living-design.md`:

```markdown
| Overall M1 state | M1b verified; M1c planning next |
| Current focus | Write the `M1c` browser chat path sub-spec |
```

Change the M1b table row status to `Verified` and add the verification commands and outcomes in the status section.

Commit:

```bash
rtk git add docs/reference/superpowers/specs/2026-05-12-plexus-m1-living-design.md
rtk git commit -m "docs: mark M1b verified"
```

---

## Self-Review Checklist

- Spec coverage: external mock service, `openai.rs`, admin validation, redaction, `stream=false`, `0` concurrency semantics, hermetic fake server, retry/backoff ownership, no DB writes in `openai.rs`, compaction outside provider, and future vision retry boundary all have tasks.
- TDD: Rust behavior starts with failing tests in Tasks 3 and 5; mock service has contract tests in Task 1.
- No production fake behavior: fake OpenAI server is under `plexus-server/tests/support`; FastAPI mock is a sibling repository.
- Error ownership: plan maps provider validation errors through `ApiError` and existing `ErrorCode` values; no new server-local public error enum is introduced.
- Verification: final commands include format, clippy, workspace tests, Postgres-backed tests, and mock service tests.
