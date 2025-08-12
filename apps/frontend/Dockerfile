# syntax=docker/dockerfile:1.7-labs
ARG TARGETPLATFORM=linux/arm64
FROM --platform=$TARGETPLATFORM node:20-alpine
WORKDIR /app
COPY package.json package-lock.json* pnpm-lock.yaml* /app/
RUN corepack enable && (pnpm install || npm install)
COPY . /app
EXPOSE 5173
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", "5173"]
