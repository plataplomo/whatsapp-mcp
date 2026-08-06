#!/bin/bash
export WHATSAPP_API_BASE_URL="http://localhost:8181/api"
export WHATSAPP_MESSAGES_DB_PATH="/home/demute/code/whatsapp-mcp/store/messages.db"
cd /home/demute/code/whatsapp-mcp/whatsapp-mcp-server
exec uv run main.py
