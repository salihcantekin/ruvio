#!/usr/bin/env bash
# Writes a throwaway CA, a server certificate for localhost / 127.0.0.1 and a client
# certificate signed by that CA into <dir>, for the TLS client tests.
#
#   scripts/make_test_tls.sh <dir>
#
# Files: ca.pem, server.pem, server.key, client.pem, client.key. Test material only.
set -euo pipefail

dir="$1"

mkdir -p "$dir"
cd "$dir"

openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj "/CN=ruvio test ca" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -keyout ca.key -out ca.pem 2>/dev/null

issue() {
  local name="$1" subject="$2" extension="$3"

  openssl req -newkey rsa:2048 -nodes -subj "/CN=$subject" \
    -keyout "$name.key" -out "$name.csr" 2>/dev/null
  printf '%s\n' "$extension" >"$name.ext"
  openssl x509 -req -in "$name.csr" -CA ca.pem -CAkey ca.key -CAcreateserial -days 2 \
    -extfile "$name.ext" -out "$name.pem" 2>/dev/null
}

issue server localhost "subjectAltName=DNS:localhost,IP:127.0.0.1
extendedKeyUsage=serverAuth"
issue client "ruvio test client" "extendedKeyUsage=clientAuth"
