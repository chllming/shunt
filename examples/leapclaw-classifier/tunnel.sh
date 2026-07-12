#!/usr/bin/env bash
# Supervised SSH tunnel that exposes a remote, localhost-bound Ollama
# classifier as a local port for shunt's classifier lane.
#
# Topology:
#   shunt (local) -> 127.0.0.1:${LOCAL_PORT}
#                 -> ssh -L over ProxyJump ${JUMP_HOST}
#                 -> ${TARGET_USER}@${TARGET_HOST}:${TARGET_PORT} (Ollama, localhost-bound)
#
# SSH is the auth/encryption boundary; the model host never exposes Ollama on
# the network. Restarts automatically if the connection drops.
#
# Configure via environment (all optional; defaults shown):
set -u

LOCAL_PORT="${LEAPCLAW_TUNNEL_LOCAL_PORT:-11436}"
TARGET_USER="${LEAPCLAW_TUNNEL_TARGET_USER:-christianlanng}"
TARGET_HOST="${LEAPCLAW_TUNNEL_TARGET_HOST:-100.65.10.117}"
TARGET_PORT="${LEAPCLAW_TUNNEL_TARGET_PORT:-11434}"
JUMP_HOST="${LEAPCLAW_TUNNEL_JUMP_HOST:-leapclaw-gateway}"
SSH_KEY="${LEAPCLAW_TUNNEL_SSH_KEY:-${HOME}/.ssh/hetzner_ed25519}"
LOG="${LEAPCLAW_TUNNEL_LOG:-/tmp/leapclaw-classifier-tunnel.log}"

echo "[$(date -Is)] tunnel supervisor start (local ${LOCAL_PORT} -> ${TARGET_HOST}:${TARGET_PORT} via ${JUMP_HOST})" >>"${LOG}"
while true; do
  ssh -N \
    -o BatchMode=yes \
    -o ExitOnForwardFailure=yes \
    -o ServerAliveInterval=15 \
    -o ServerAliveCountMax=3 \
    -o StrictHostKeyChecking=accept-new \
    -o IdentitiesOnly=yes \
    -i "${SSH_KEY}" \
    -o "ProxyJump=${JUMP_HOST}" \
    -L "127.0.0.1:${LOCAL_PORT}:127.0.0.1:${TARGET_PORT}" \
    "${TARGET_USER}@${TARGET_HOST}" >>"${LOG}" 2>&1
  echo "[$(date -Is)] tunnel exited ($?); restarting in 3s" >>"${LOG}"
  sleep 3
done
