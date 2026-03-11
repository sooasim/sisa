#!/bin/bash
# 한국 호스팅 Linux 서버에서 Flask 앱 실행 (Docker 미사용 시)
# 사용: chmod +x run_linux.sh && ./run_linux.sh

cd "$(dirname "$0")"
export RUN_HEADLESS=1
PORT=${PORT:-5000}

if [ -d "venv" ]; then
  source venv/bin/activate
fi
exec gunicorn web_form:app --bind 0.0.0.0:${PORT} --workers=2
