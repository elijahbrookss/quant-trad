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

# Final production image served by nginx
FROM nginx:1.27-alpine AS prod
ENV NODE_ENV=production
WORKDIR /usr/share/nginx/html
COPY --from=build /app/dist ./
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
