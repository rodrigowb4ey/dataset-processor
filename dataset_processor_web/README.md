# Dataset Processor Web

React + TypeScript + Mantine UI client for the Dataset Processor API.

## Local development

```bash
npm ci
npm run dev
```

The web app runs at `http://localhost:5173`.

## Environment variables

- `VITE_API_BASE_URL` (default: `/api`)
- `VITE_API_PROXY_TARGET` (default: `http://localhost:8000`)

With default values, Vite proxies `/api/*` to the backend and avoids CORS issues in local development.

## Scripts

```bash
npm run dev
npm run lint
npm run typecheck
npm run test
npm run build
```
