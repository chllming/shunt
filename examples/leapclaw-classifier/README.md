# Self-hosted classifier lane over an SSH tunnel

This example wires shunt's auto-mode safety-classifier lane to a small LLM
running on a remote GPU host, reached over a localhost-bound SSH tunnel. It
pairs with the custom classifier prompt in `../classifier-harness-prompt.txt`.

## Why

Claude Code's auto mode fires a safety-classifier model call before gated
Bash/Write/Edit tools. Routing that call to a cheap, always-resident local model
(instead of a pooled frontier account) keeps it fast, free, and independent of
the pool's rate limits. Running the model on a remote GPU box while keeping it
localhost-bound — reachable only through SSH — avoids exposing an
unauthenticated inference port on the network.

## Topology

```
Claude Code (auto mode)
  -> shunt (127.0.0.1:8082)                     # detects the classifier call,
                                                #   swaps in the custom prompt,
                                                #   routes to the classifier lane
  -> 127.0.0.1:11436                            # local end of the SSH tunnel
  -> ssh -L over ProxyJump <jump host>          # SSH is the auth/crypto boundary
  -> <model host>:127.0.0.1:11434 (Ollama, GPU) # localhost-bound; no network port
```

## Pieces

- `tunnel.sh` — supervised SSH tunnel (`ssh -N -L` in a restart loop) exposing the
  remote Ollama as `127.0.0.1:11436` locally. Parameterized via environment
  variables; localhost-bound on both ends.
- `ollama-keepalive.conf` — systemd drop-in for the model host: binds Ollama to
  localhost and keeps the classifier model resident (no cold-start latency).
- `shunt-classifier-lane.toml` — the shunt `[server]` + `[[accounts]]` snippet
  for the `local` classifier lane.

## Setup

On the model host:

```bash
curl -fsSL https://ollama.com/install.sh | sh
sudo install -D -m0644 ollama-keepalive.conf /etc/systemd/system/ollama.service.d/10-keepalive.conf
sudo systemctl daemon-reload && sudo systemctl restart ollama
ollama pull qwen2.5:7b-instruct
```

Locally (where shunt runs):

```bash
# start the supervised tunnel (adjust env vars for your hosts/key)
nohup bash tunnel.sh >/dev/null 2>&1 &

# confirm the model is reachable through the tunnel
curl -s http://127.0.0.1:11436/v1/models

# merge shunt-classifier-lane.toml into ~/.config/shunt/config.toml, then
shunt restart
```

## Notes

- Provider `local` is required so shunt permits the loopback upstream and sends
  no auth header. Ollama does not authenticate; SSH does.
- If the tunnel drops, shunt's request errors and Claude Code fails closed
  (blocks the gated action) — the safe direction. The supervisor reconnects
  automatically.
- No secrets are committed here: SSH keys, host credentials, and the concrete
  cluster identity live outside the repo. The host/user/port defaults in
  `tunnel.sh` are placeholders; override them via environment variables.
