# syntax=docker/dockerfile:1

FROM ubuntu:22.04

ARG DEBIAN_FRONTEND=noninteractive
ARG IBC_VERSION=3.19.1
ARG IBC_DOWNLOAD_URL=https://github.com/IbcAlpha/IBC/releases/download/${IBC_VERSION}/IBC-${IBC_VERSION}-linux-x64.zip
ARG IB_GATEWAY_VERSION=10.25.2
ARG IB_GATEWAY_DOWNLOAD_URL=https://download2.interactivebrokers.com/portal/DownloadController?action=downloadNew&path=IBGateway/${IB_GATEWAY_VERSION}/IBGatewayLinux-${IB_GATEWAY_VERSION}-x64.sh
ARG IBG_DIR=/opt/ibgateway/ibgateway/${IB_GATEWAY_VERSION}

# Install dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        fluxbox \
        gosu \
        unzip \
        wget \
        xvfb \
        x11vnc \
        openjdk-17-jre \
        x11-utils \
        openjfx \
    && rm -rf /var/lib/apt/lists/*

# Install networking tools (kept as a separate layer for debugging purposes)
RUN apt-get update && apt-get install -y --no-install-recommends \
    iproute2 netcat-openbsd python3-pip python3 \
    && pip install --no-cache-dir ib_insync \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp

# Install IBC controller
RUN wget -O ibc.zip "${IBC_DOWNLOAD_URL}" \
    && unzip -q ibc.zip -d ./ibc_output \
    && mv ./ibc_output /opt/ibc \
    && chmod +x /opt/ibc /opt/ibc/scripts/*.sh \
    && rm ibc.zip

# Install IB Gateway silently | separate for caching (file takes 3+ mins to download)
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
 && curl -fL "$IB_GATEWAY_DOWNLOAD_URL" -o /tmp/ibgateway-installer.sh \
 && chmod +x /tmp/ibgateway-installer.sh \
 && /tmp/ibgateway-installer.sh -q -dir /opt/ibgateway || /tmp/ibgateway-installer.sh -q \
 && rm -f /tmp/ibgateway-installer.sh \
 && rm -rf /var/lib/apt/lists/*


COPY docker/ibkr-gateway/ibgateway-installer.sh ibgateway-installer.sh

# Fix line endings if needed & set execute permissions
RUN chmod +x ibgateway-installer.sh \
    && mkdir -p ${IBG_DIR} \
    && mkdir -p /home/ibkr/Jts && chmod -R 755 /home/ibkr/Jts \
    && ./ibgateway-installer.sh -q -dir ${IBG_DIR} \
    && rm ibgateway-installer.sh


# Setup IB Gateway directories and Healthcheck scripts
COPY docker/ibkr-gateway/entrypoint.sh /usr/local/bin/ibkr-entrypoint.sh
COPY docker/ibkr-gateway/ibkr_gateway_healthchk.sh /usr/local/bin/ibkr_gateway_healthchk.sh

RUN chmod +x /usr/local/bin/ibkr-entrypoint.sh

ENV DISPLAY=:0 \
    XVFB_WHD=1280x720x24 \
    IBC_CONFIG_DIR=/config \
    LOG_PATH=/var/log/ibgateway \
    IBC_PATH=/opt/ibc \
    TWS_PATH=/opt/ibgateway/ibgateway \
    JAVA_PATH=/usr/bin/java \
    JFX_PATH=/usr/share/openjfx/lib

# Configure IB Gateway to use JavaFX modules
RUN echo "--module-path=${JFX_PATH}" >> ${IBG_DIR}/ibgateway.vmoptions && \
    echo "--add-modules=javafx.controls,javafx.fxml,javafx.swing,javafx.web,javafx.graphics" >> ${IBG_DIR}/ibgateway.vmoptions 

ENTRYPOINT ["/usr/local/bin/ibkr-entrypoint.sh"]

# Start VNC server attached to that display
# x11vnc -display :0 -nopw -forever -shared -rfbport 5900 &