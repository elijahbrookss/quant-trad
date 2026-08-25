ARG NODE_IMAGE=docker.io/library/node@sha256:2cf067cfed83d5ea958367df9f966191a942351a2df77d6f0193e162b5febfc0
ARG PYTHON_IMAGE=docker.io/library/python@sha256:a116514e19457bcb7af7efe9c3dd0b9b71e85b317694e7882a1c52aa15a78134

FROM ${NODE_IMAGE} AS node_runtime

FROM ${PYTHON_IMAGE} AS runtime

COPY --from=node_runtime /usr/local/bin/node /usr/local/bin/node

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /workspace

COPY .qt-assurance-wheelhouse/python-wheel-manifest.lock.json /opt/qt-assurance/python-wheel-manifest.lock.json
COPY .qt-assurance-wheelhouse/requirements.hashed.txt /opt/qt-assurance/requirements.hashed.txt
COPY .qt-assurance-wheelhouse/wheelhouse/ /opt/qt-assurance/wheelhouse/

RUN ["python", "-m", "pip", "install", "--no-cache-dir", "--no-index", "--no-deps", "--only-binary=:all:", "--require-hashes", "--find-links=/opt/qt-assurance/wheelhouse", "-r", "/opt/qt-assurance/requirements.hashed.txt"]
RUN ["python", "-m", "pip", "check"]
RUN ["python", "--version"]
RUN ["node", "--version"]

ENTRYPOINT []
CMD ["node", "--version"]
