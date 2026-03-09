# worldsisa.com - K-VAN 자동 입력 도우미

이 프로젝트는 `https://store.k-van.app` 에 접속해서

- 엑셀에 저장된 아이디 / 비밀번호 / PIN 으로 자동 로그인
- `https://store.k-van.app/face-to-face-payment` 페이지의 폼을 엑셀 데이터로 자동 입력

을 수행하는 **Python + Selenium** 스크립트 예제입니다.

> ⚠️ 실제 서비스 운영 전, 반드시 테스트 계정 / 소액으로 충분히 테스트해 보세요.

---

## 1. 사전 준비

- Windows 10
- Python 3.9 이상 설치
- 크롬 브라우저 설치 (크롬 기준 예제)

### 의존성 설치

터미널(파워셸)에서 `selenium` 폴더로 이동한 뒤:

```bash
cd c:\Users\ATA\Downloads\selenium
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
```

---

## 2. 엑셀 파일 구조

`kvan_input.xlsx` 라는 이름의 엑셀 파일(동일 폴더 위치)을 사용한다고 가정합니다.  
첫 번째 시트의 첫 행(1행)에 아래와 같이 **헤더(열 이름)** 를 넣어주세요.

| A열           | B열             | C열        | D열            | E열             | F열   | G열   |
| ------------- | --------------- | ---------- | -------------- | ---------------- | ----- | ----- |
| login_id      | login_password  | login_pin  | customer_name  | customer_phone   | amount| memo  |

2행부터는 실제 데이터를 입력합니다.

예)

| login_id | login_password | login_pin | customer_name | customer_phone | amount | memo                |
| -------- | -------------- | --------- | ------------- | -------------- | ------ | ------------------- |
| m3313    | k2255          | 2424      | 홍길동        | 01012345678    | 50000  | 샤넬 지갑 대면결제  |

---

## 3. 스크립트 실행 방법

1. `kvan_input.xlsx` 를 `c:\Users\ATA\Downloads\selenium` 폴더에 저장
2. 가상환경 활성화 후:

```bash
cd c:\Users\ATA\Downloads\selenium
.\venv\Scripts\activate
python auto_kvan.py
```

브라우저가 자동으로 열리고:

- `https://store.k-van.app/sign-in` 에 접속
- 엑셀의 첫 번째 행 정보로 로그인
- `https://store.k-van.app/face-to-face-payment` 로 이동하여 각 행마다 폼 자동 입력

을 시도합니다.

---

## 4. 셀렉터(입력창 위치) 수정

이 스크립트는 **예상되는 `name`/`id` 값** 으로 작성되어 있습니다.  
만약 사이트 HTML 구조가 다르면, `auto_kvan.py` 안의 **셀렉터 상수 부분** 을 실제 값에 맞게 수정해야 합니다.

- 크롬에서 `F12` 누르고 → Elements 탭에서 입력창을 클릭 → `name` / `id` / `placeholder` 값을 확인 후
- `auto_kvan.py` 의 `SIGN_IN_SELECTORS`, `FACE_TO_FACE_SELECTORS` 부분을 수정하세요.

"# sisa"  
