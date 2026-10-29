# Ruvio UI

A browser console for a running Ruvio server or cluster: a dashboard of time charts you choose (memory breakdown, network bytes, command rates and more), a read-only cluster view (groups, shards, slots, replicas, coordinators), keys, commands, ACL users and Pub/Sub.

- Source: https://github.com/salihcantekin/ruvio (`clients/ruvio-ui`)
- Documentation: https://ruviodb.com/get-started/#ui
- Server image: https://hub.docker.com/r/salihcantekin/ruvio

## Run it

```bash
docker run --rm -p 8080:8080 \
  -e RUVIO_UI_TARGET_HOST=127.0.0.1 \
  -e RUVIO_UI_TARGET_PORT=6379 \
  salihcantekin/ruvio-ui:linux
```

Open http://127.0.0.1:8080. `RUVIO_UI_TARGET_HOST` and `RUVIO_UI_TARGET_PORT` are the Ruvio server (the same address `redis-cli` uses); port `8080` is only the UI. Sign-in is the server's ACL user; an empty user and password connect as `default`. Set `RUVIO_UI_TARGET_PASSWORD` to prefill the password. With `ruvio coordinator`, `RUVIO_UI_COORDINATORS` (`host:port,host:port`) and `RUVIO_UI_COORDINATOR_SECRET` add the coordinators to the Cluster screen; the UI only reads from them.

## Tags

`linux` and `latest` are the same multi-platform image (amd64 and arm64).

## License

GNU Affero General Public License v3.0 (`AGPL-3.0-only`); the text is in the image at `/usr/local/share/ruvio-ui/LICENSE`.
