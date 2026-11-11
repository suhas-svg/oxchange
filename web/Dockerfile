# The dashboard: built with Node, served by nginx, which also forwards the
# API and both WebSockets to the Gateway so the browser sees one origin.
FROM node:22-alpine AS build
WORKDIR /web
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY . .
RUN npm run typecheck && npm test && npx vite build

FROM nginxinc/nginx-unprivileged:1.27-alpine
# Rendered into /etc/nginx/conf.d at start; that directory must be writable
# (a tmpfs in Compose, an emptyDir in Kubernetes) when the root is read-only.
COPY deploy/default.conf.template /etc/nginx/templates/default.conf.template
ENV GATEWAY_URL=http://gateway:8080     NGINX_ENTRYPOINT_LOCAL_RESOLVERS=1
COPY --from=build /web/dist /usr/share/nginx/html
EXPOSE 8080
