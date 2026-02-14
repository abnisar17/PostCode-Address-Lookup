# Postcode Address Lookup — Frontend

SvelteKit SPA for looking up UK postcodes and addresses. Talks to the backend API at `localhost:8000`.

## Stack

- **SvelteKit** with Svelte 5 (runes) — static adapter (SPA)
- **Tailwind CSS v4** — via Vite plugin
- **TypeScript** — strict mode
- **Prettier + ESLint** — formatting and linting

## Quick Start

```sh
npm install
npm run dev
```

The dev server starts at `http://localhost:5173`. Make sure the backend API is running at `http://localhost:8000` (or set `VITE_API_BASE_URL` in `.env`).

## Scripts

| Command              | Description                          |
|----------------------|--------------------------------------|
| `npm run dev`        | Start dev server                     |
| `npm run build`      | Production build (static)            |
| `npm run preview`    | Preview production build             |
| `npm run check`      | Svelte/TS type checking              |
| `npm run lint`       | ESLint                               |
| `npm run lint:fix`   | ESLint with auto-fix                 |
| `npm run format`     | Prettier format all files            |
| `npm run format:check` | Check formatting without writing   |

Or from the project root via Make:

```sh
make fe-dev       # Start dev server
make fe-build     # Production build
make fe-lint      # ESLint
make fe-format    # Prettier
```

## Project Structure

```
src/
├── app.css                    # Tailwind entry
├── app.html                   # HTML shell
├── routes/
│   ├── +layout.js             # SPA mode (ssr = false)
│   ├── +layout.svelte         # App shell: header, main, footer
│   └── +page.svelte           # Main page: search + results
└── lib/
    ├── api/
    │   ├── types.ts           # TS interfaces (mirrors backend schemas)
    │   └── client.ts          # Typed fetch wrapper
    └── components/
        ├── PostcodeSearch.svelte   # Input + autocomplete dropdown
        ├── AddressList.svelte      # Postcode header + address cards
        ├── AddressCard.svelte      # Single address display
        ├── StatusBar.svelte        # Footer API health indicator
        └── Spinner.svelte          # Loading indicator
```

## Docker

```sh
docker compose up frontend
```

Builds the SPA and serves via nginx on port 3000.
