#!/bin/bash
# Ubuntu/Debian에서 Google Chrome 설치 (auto_kvan.py 자동 결제용)
# 사용: chmod +x install_chrome_ubuntu.sh && sudo ./install_chrome_ubuntu.sh

set -e
apt-get update
apt-get install -y wget gnupg ca-certificates
wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-linux-signing-key.gpg
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-linux-signing-key.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list
apt-get update
apt-get install -y google-chrome-stable
echo "Google Chrome 설치 완료. Selenium은 설치된 Chrome을 자동으로 사용합니다."
