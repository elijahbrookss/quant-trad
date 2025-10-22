# syntax=docker/dockerfile:1

FROM ghcr.io/ibkrcanada/ibgateway:stable

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        xvfb \
        x11vnc \
        fluxbox \
        gosu \
    && rm -rf /var/lib/apt/lists/*

COPY docker/ibkr-gateway/entrypoint.sh /usr/local/bin/ibkr-entrypoint.sh
RUN chmod +x /usr/local/bin/ibkr-entrypoint.sh

ENV DISPLAY=:0 \
    XVFB_WHD=1280x720x16 \
    IBC_CONFIG_DIR=/config \
    LOG_PATH=/var/log/ibgateway

ENTRYPOINT ["/usr/local/bin/ibkr-entrypoint.sh"]
