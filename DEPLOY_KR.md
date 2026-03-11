# 한국 호스팅 배포 가이드 (SISA K-VAN 자동 결제)

한국 서버에 올리면 결제 요청이 **한국 IP**로 나가서 카드사에서 국내 결제로 인식됩니다.

---

## cPanel 호스팅이면 될까요?

- **cPanel 공유 호스팅(Shared Hosting)**  
  → **이 프로젝트에는 맞지 않습니다.**  
  - 서버에 Chrome 설치 불가, Python 앱을 상시 실행(gunicorn)하기 어렵고, Selenium 실행 제한이 있는 경우가 많습니다.

- **cPanel이 붙은 VPS / 전용 서버**  
  → **가능합니다.**  
  - SSH 접속이 되고, 루트 또는 sudo로 Chrome 설치·gunicorn 실행이 가능하면 됩니다.  
  - 이 경우 아래 "방법 B: Docker 없이 Linux 서버" 또는 "방법 A: Docker"를 따르시면 됩니다.

정리: **cPanel만 지원하는 공유 호스팅은 사용할 수 없고**, **cPanel + VPS(또는 전용 서버)** 형태면 사용 가능합니다.

---

## 1. 업로드할 파일 목록

아래 파일만 서버에 올리세요. **venv, .git, __pycache__** 폴더는 제외합니다.

```
web_form.py          # Flask 웹 앱
auto_kvan.py         # K-VAN 자동 결제 스크립트
requirements.txt     # Python 패키지 목록
Dockerfile           # Docker 사용 시
.dockerignore        # Docker 빌드 시 제외 파일
index.html
login.html
terms.html
agency-register.html
run_linux.sh         # Linux에서 Docker 없이 실행 시 (실행 권한 부여)
install_chrome_ubuntu.sh  # Ubuntu/Debian에서 Chrome 설치 (Docker 미사용 시)
```

**중요: 데이터(결제/대행사 정보) 파일은 따로 보관됩니다.**

- 애플리케이션 데이터는 **`SISA_DATA_DIR`** 환경변수로 지정한 폴더(없으면 `./data`)에 저장됩니다.
- 이 폴더 안에 아래와 같은 파일/폴더가 생깁니다.
  - `admin_state.json`, `hq_state.json`, `current_order.json`, `last_result.json`
  - `kvan_results.xlsx`
  - `sessions/orders/`, `sessions/results/`
- **코드 업데이트(Git pull, 파일 업로드) 시에는 이 데이터 폴더를 덮어쓰지 마세요.**  
  → 이렇게 하면 대행사 정보와 결제 내역이 초기화되지 않고 계속 유지됩니다.

---

## 2. 배포 방법 (둘 중 하나 선택)

### 방법 A: Docker 지원 호스팅 (권장)

호스팅에서 Docker를 지원하면 가장 간단합니다.

1. 위 파일들을 서버에 업로드(또는 Git clone).
2. 터미널에서 프로젝트 폴더로 이동:
   ```bash
   cd /경로/프로젝트
   ```
3. Docker 이미지 빌드 및 실행:
   ```bash
   docker build -t sisa-kvan .
   docker run -d -p 5000:5000 --name sisa-app sisa-kvan
   ```
4. 포트 5000으로 웹 접속 (호스팅에서 지정한 도메인 또는 IP:5000).

**환경 변수(선택):**  
- `PORT` – 호스팅이 지정한 포트가 있으면 그에 맞춤.  
- `HQ_ADMIN_USER`, `HQ_ADMIN_PASSWORD` – 본사 어드민 로그인 계정(기본: admin / admin1234).

---

### 방법 B: Docker 없이 Linux 서버 (VPS 등)

Ubuntu/Debian 계열 서버에서 직접 실행하는 경우입니다.

#### 2-1. Chrome 설치 (자동 결제용, 한 번만 실행)

```bash
chmod +x install_chrome_ubuntu.sh
sudo ./install_chrome_ubuntu.sh
```

#### 2-2. Python 가상환경 및 패키지

```bash
cd /경로/프로젝트
python3 -m venv venv
source venv/bin/activate   # Linux/Mac
pip install -r requirements.txt
```

#### 2-3. 서버 모드로 실행

```bash
export RUN_HEADLESS=1
chmod +x run_linux.sh
./run_linux.sh
```

또는 직접:

```bash
export RUN_HEADLESS=1
gunicorn web_form:app --bind 0.0.0.0:5000 --workers=2
```

**항상 켜 두려면** systemd 서비스로 등록하거나, 호스팅에서 제공하는 “앱 실행” 방식에 위 gunicorn 명령을 넣으면 됩니다.

---

## 3. 주소 알려주실 때 확인할 것

배포 후 아래 정보를 알려주시면 다음 설정(도메인 연결, 방화벽 등)을 안내할 수 있습니다.

- **접속 주소**: 예) `https://도메인.com` 또는 `http://IP:5000`
- **Docker 사용 여부**: 사용 / 미사용
- **호스팅 업체명**(선택): 가비아, 카페24, AWS 등

---

## 4. 참고

- **본사 로그인**: `/login.html` → 아이디 `admin`, 비밀번호 `admin1234` (환경 변수로 변경 가능)
- **대행사**: 본사 어드민에서 승인 후 발급된 아이디/비밀번호로 동일 로그인 페이지에서 접속
- **결제 테스트**: `/payment` 또는 대행사에서 생성한 `/pay/세션ID` 로 접속 후 결제하기
