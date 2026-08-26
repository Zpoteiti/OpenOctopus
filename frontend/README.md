# OpenOctopus browser app

The React application is served by the Python Server in production. During development, Vite proxies `/api` and `/health` to `http://127.0.0.1:8080`, preserving the same-origin cookie contract.

## Develop

```bash
npm ci
npm run dev
```

Start the OpenOctopus Server separately on port 8080, then open `http://127.0.0.1:5173`.

## Verify

```bash
npm run generate:api
npm run lint
npm run typecheck
npm test
npm run build
npm run e2e
```

`generate:api` updates `src/api/openapi.d.ts` from `../docs/API.yaml`. Commit the generated file whenever the API contract changes.

The Playwright smoke uses a real FastAPI Server, PostgreSQL, and S3-compatible object storage, plus the local provider stub in `e2e/provider_stub.py`.

## Localization

English is the default language. Translation resources live in `src/i18n/resources.ts`; components render user-facing copy through `react-i18next`. The language and light/dark preference are the only browser-local settings.
