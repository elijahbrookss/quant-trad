# syntax=docker/dockerfile:1

FROM ubuntu:22.04

ARG DEBIAN_FRONTEND=noninteractive
ARG IBC_VERSION=3.19.1
ARG IBC_DOWNLOAD_URL=https://github.com/IbcAlpha/IBC/releases/download/${IBC_VERSION}/IBC-${IBC_VERSION}-linux-x64.zip
ARG IB_GATEWAY_VERSION=10.25.2
ARG IB_GATEWAY_DOWNLOAD_URL=https://download2.interactivebrokers.com/portal/DownloadController?action=downloadNew&path=IBGateway/${IB_GATEWAY_VERSION}/IBGatewayLinux-${IB_GATEWAY_VERSION}-x64.sh

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        fluxbox \
        gosu \
        unzip \
        wget \
        xvfb \
        x11vnc \
        openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp

# Install IBC controller
RUN wget -O ibc.zip "${IBC_DOWNLOAD_URL}" \
    && unzip -q ibc.zip \
    && mv IBC /opt/ibc \
    && rm ibc.zip

# Install IB Gateway silently
RUN wget -O ibgateway-installer.sh "${IB_GATEWAY_DOWNLOAD_URL}" \
    && chmod +x ibgateway-installer.sh \
    && ./ibgateway-installer.sh -q -dir /opt/ibgateway \
    && rm ibgateway-installer.sh

COPY docker/ibkr-gateway/entrypoint.sh /usr/local/bin/ibkr-entrypoint.sh
RUN chmod +x /usr/local/bin/ibkr-entrypoint.sh

ENV DISPLAY=:0 \
    XVFB_WHD=1280x720x16 \
    IBC_CONFIG_DIR=/config \
    LOG_PATH=/var/log/ibgateway \
    IBC_PATH=/opt/ibc \
    TWS_PATH=/opt/ibgateway \
    JAVA_PATH=/usr/bin/java

ENTRYPOINT ["/usr/local/bin/ibkr-entrypoint.sh"]
