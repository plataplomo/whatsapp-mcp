# ─── Build stage: compile Go bridge with CGO for go-sqlite3 ───
FROM golang:1.25-bookworm AS bridge-builder

WORKDIR /build

# Install C compiler for CGO (go-sqlite3 needs it)
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy bridge source
COPY whatsapp-bridge/ ./

# Download deps + build with CGO enabled
ENV CGO_ENABLED=1
RUN go mod download && go build -o whatsapp-bridge .

# ─── Runtime stage: slim image with bridge + MCP server ───
FROM python:3.12-slim-bookworm

# Install runtime deps: libsqlite3 (for CGO binary), ffmpeg (voice messages), curl (healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsqlite3-0 \
    ffmpeg \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy compiled bridge binary
COPY --from=bridge-builder /build/whatsapp-bridge /app/whatsapp-bridge

# Copy MCP server source
COPY whatsapp-mcp-server/ /app/whatsapp-mcp-server/

# Install MCP server deps with uv (fast, no venv needed — uv manages it)
RUN pip install --no-cache-dir uv \
    && cd /app/whatsapp-mcp-server && uv sync \
    && rm -rf /root/.cache/uv

WORKDIR /app

# Persistent data: WhatsApp session + SQLite message store
VOLUME /app/store

# Bridge REST API
EXPOSE 8080

# Start bridge by default. MCP server is launched on-demand by Hermes via stdio.
CMD ["/app/whatsapp-bridge"]
