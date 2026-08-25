ARG NODE_IMAGE=docker.io/library/node@sha256:2cf067cfed83d5ea958367df9f966191a942351a2df77d6f0193e162b5febfc0
ARG PYTHON_IMAGE=docker.io/library/python@sha256:a116514e19457bcb7af7efe9c3dd0b9b71e85b317694e7882a1c52aa15a78134

FROM ${NODE_IMAGE} AS node_runtime

FROM ${PYTHON_IMAGE}

COPY --from=node_runtime /usr/local/bin/node /usr/local/bin/node

WORKDIR /workspace

RUN python --version && node --version

ENTRYPOINT []
CMD ["node", "--version"]
