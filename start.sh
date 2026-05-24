#!/bin/bash
# start.sh — Fixed: python3 แทน curl+jq, graceful shutdown

# 1. uvicorn (background)
echo "Starting FastAPI Server..."
uvicorn main:app --host 0.0.0.0 --port 8000 &
UVICORN_PID=$!

# 2. ngrok
if [ ! -z "$NGROK_AUTHTOKEN" ]; then
    echo "Configuring Ngrok..."
    ngrok config add-authtoken $NGROK_AUTHTOKEN
fi
echo "Starting Ngrok..."
ngrok http 8000 --log=stdout > ngrok.log &
NGROK_PID=$!
sleep 3

# 3. Show URL — python3 แทน curl+jq (curl ไม่มีใน Docker image นี้)
NGROK_URL=$(python3 -c "
import urllib.request, json
try:
    d = json.loads(urllib.request.urlopen('http://localhost:4040/api/tunnels').read())
    print(d['tunnels'][0]['public_url'])
except Exception as e:
    print('(ngrok not ready)')
" 2>/dev/null)

echo "========================================================="
echo "✅ Mock Server is Running!"
echo "📱 Web UI (Local)   : http://localhost:8000"
echo "🌍 Databricks URL   : ${NGROK_URL}/api/send_campaign"
echo "🔄 Reset demo cards : curl -X POST http://localhost:8000/api/reset"
echo "🛑 Ctrl+C = graceful shutdown (flushes Pub/Sub buffer)"
echo "========================================================="

# 4. Graceful shutdown trap
cleanup() {
    echo ""
    echo "[SHUTDOWN] Flushing Pub/Sub buffer & exporting CSV..."
    kill -SIGINT $UVICORN_PID 2>/dev/null
    wait $UVICORN_PID 2>/dev/null
    echo "[SHUTDOWN] Done ✅"
    kill $NGROK_PID 2>/dev/null
    exit 0
}
trap cleanup SIGTERM SIGINT

# 5. Wait (uvicorn is the real main process)
wait $UVICORN_PID