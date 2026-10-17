# syntax=docker/dockerfile:1
FROM node:24-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && npm install -g @anthropic-ai/claude-code \
 && npm cache clean --force \
 && mkdir -p /work \
 && chown node:node /work
COPY server.ts /app/server.ts
USER node
WORKDIR /app
ENV NODE_ENV=production PORT=8788
EXPOSE 8788
CMD ["node", "server.ts"]
