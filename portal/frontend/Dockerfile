# syntax=docker/dockerfile:1
FROM node:20-alpine AS build
WORKDIR /app

COPY portal/frontend/package*.json ./
RUN npm ci

COPY portal/frontend/ ./
RUN npm run build

FROM nginx:1.27-alpine
ENV NODE_ENV=production
WORKDIR /usr/share/nginx/html

COPY --from=build /app/dist ./

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
