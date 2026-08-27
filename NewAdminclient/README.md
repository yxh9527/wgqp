# NewAdminclient

Vue 2 + Element UI replacement for `adminclient`.

## Scope
- Keep existing API routes and session keys
- Migrate shared layout and report pages first
- Continue page-by-page replacement from the old iView app

## Deployment

This application uses Vue Router history mode. The static server must fall back
unknown page paths to `index.html`, otherwise direct access to routes such as
`/agent-aggs-detail` returns 404. An Nginx example is provided in
`nginx.conf.example`.
