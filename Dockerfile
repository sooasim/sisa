# Railway 등 서버에서 Selenium + Chrome 자동 결제 실행용
FROM --platform=linux/amd64 python:3.11-slim

# Chrome 설치에 필요한 패키지 + 런타임 의존성
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    ca-certificates \
    unzip \
    curl \
    && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-linux-signing-key.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-linux-signing-key.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends google-chrome-stable \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

# Railway/한국호스팅 공통: PORT, 자동결제 시 헤드리스 모드
ENV PORT=5000
ENV RUN_HEADLESS=1
EXPOSE 5000

# Flask 앱 실행 (gunicorn). auto_kvan.py는 Flask가 subprocess로 호출
CMD ["sh", "-c", "gunicorn web_form:app --bind 0.0.0.0:${PORT} --workers=2"]
