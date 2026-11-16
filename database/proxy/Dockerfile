FROM alpine/socat:1.8.1.1@sha256:7f9a06753033f2b7de18edc2353f2c15153413d95a039163c6db270fc7a6c3b0
EXPOSE 8088
ENTRYPOINT ["/bin/sh", "-c", "exec socat TCP-LISTEN:8088,fork,reuseaddr TCP:172.17.0.1:${TUNNEL_PORT:?}"]
