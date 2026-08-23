# syntax=docker/dockerfile:1

# Base install with dependencies shared across dev/build stages
FROM node:20-alpine AS base
WORKDIR /app

COPY portal/frontend/package*.json ./
RUN npm ci

COPY portal/frontend/ ./

# Development server image – runs Vite directly for live debugging
FROM base AS dev
ENV NODE_ENV=development
EXPOSE 5173
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", "5173"]

# Production build stage
FROM base AS build
ENV NODE_ENV=production \
    VITE_LOG_LEVEL=debug
RUN npm run build

# Shared production image served by nginx. Both entrypoints use the same
# immutable assets and private backend proxy; prod-v2 selects the operator
# console as its root document.
FROM nginx:1.27-alpine AS prod-base
ARG QT_SOURCE_REVISION
ARG QT_SOURCE_TREE_HASH
ENV NODE_ENV=production
ENV QT_IMAGE_SOURCE_REVISION="$QT_SOURCE_REVISION" \
    QT_IMAGE_SOURCE_TREE_HASH="$QT_SOURCE_TREE_HASH"
WORKDIR /usr/share/nginx/html
COPY --from=build /app/dist ./
COPY portal/frontend/nginx.conf /etc/nginx/conf.d/default.conf
LABEL org.opencontainers.image.revision="$QT_SOURCE_REVISION" \
      io.quanttrad.source-tree-hash="$QT_SOURCE_TREE_HASH"
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]

FROM prod-base AS prod

FROM prod-base AS prod-v2
RUN cp /usr/share/nginx/html/v2.html /usr/share/nginx/html/index.html
