#!/bin/sh
# Entrypoint for discord-reader-daemon.
#
# Docker file secrets are mounted root-owned and read-only, so the unprivileged
# daemon user cannot read them directly. This wrapper copies the credential to
# the container's /tmp tmpfs (readable by the daemon user only) and then drops
# every privilege before exec'ing the daemon. If we are already unprivileged
# (for example with systemd credentials), nothing is copied and the daemon is
# exec'd as-is.
set -eu

if [ "$(id -u)" = "0" ]; then
    if [ -n "${DISCORD_TOKEN_FILE:-}" ] && [ -r "${DISCORD_TOKEN_FILE}" ]; then
        install -m 0400 -o 10001 -g 10001 "${DISCORD_TOKEN_FILE}" /tmp/discord_token
        export DISCORD_TOKEN_FILE=/tmp/discord_token
    fi
    exec setpriv --reuid=10001 --regid=10001 --clear-groups "$@"
fi

exec "$@"
