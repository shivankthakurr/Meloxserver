# Listen Together — WebSocket Server

Melox-client-compatible sync server. Handles rooms, host-authoritative
playback sync, chat, reconnection sessions, and a self-ping keep-alive
so free-tier hosts don't sleep it.

## Run locally
```
npm install
npm start
```
Server listens on `ws://localhost:8080` (or `$PORT`). Visiting
`http://localhost:8080` in a browser shows "server is alive" — this is the
health-check endpoint used for keep-alive.

## Deploy to Render (recommended, free tier)
1. Push this folder to a GitHub repo.
2. On Render.com: New → Web Service → connect that repo.
3. Build Command: `npm install`
4. Start Command: `npm start`
5. Deploy. Render automatically sets `RENDER_EXTERNAL_URL` — the server
   uses this to ping itself every 5 minutes, so the free instance stays
   awake. No extra config needed on Render.

## Deploy elsewhere (Railway / Fly.io)
Same build/start commands. These platforms don't auto-set
`RENDER_EXTERNAL_URL`, so add an environment variable manually:
```
SELF_URL=https://your-app-url.up.railway.app
```

## After deploying
Take the `wss://your-app.onrender.com` URL and add it to
`ListenTogetherServers.kt` in the Android app's `ServersJson` list.

## What it implements
- create_room / join_room / approve_join / reject_join
- playback_action → sync_playback (with server_time stamped for latency compensation)
- kick_user / transfer_host
- request_sync / reconnect (session tokens, 5-min grace period on disconnect)
- chat
- Self-ping keep-alive every 5 min (see above)

## Notes / next steps
- Currently in-memory only — restart wipes all rooms. For production, consider Redis for room state if you run multiple server instances.
- No auth/rate-limiting yet — add basic abuse protection before public release.
- `approve_join` currently trusts client-reported userId/username; harden if opening this publicly.
- Keep-alive pings your own instance — this counts as normal traffic and won't wake a fully-slept instance from *another* machine; it just prevents the sleep from happening in the first place while at least one request cycle keeps firing.
