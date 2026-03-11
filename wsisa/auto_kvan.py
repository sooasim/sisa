import os
import time
import json
from dataclasses import dataclass
from pathlib import Path
from typing import List
from datetime import datetime
import random
import re

from openpyxl import load_workbook, Workbook
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
import pymysql

# 서버(Railway 등)에서 실행 시 헤드리스 + 자동 종료
def _is_server_env() -> bool:
    return bool(os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("RUN_HEADLESS"))


def _step_start(label: str) -> float:
    """단계별 속도 측정을 위한 헬퍼."""
    t0 = time.perf_counter()
    print(f"[STEP] {label} 시작")
    return t0


def _step_end(label: str, t0: float) -> None:
    dt = time.perf_counter() - t0
    print(f"[STEP] {label} 완료 ({dt:.2f}s)")


SIGN_IN_URL = "https://store.k-van.app/sign-in"
FACE_TO_FACE_URL = "https://store.k-van.app/face-to-face-payment"

# 코드와 데이터 경로 분리: SISA_DATA_DIR (없으면 ./data)
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("SISA_DATA_DIR") or (BASE_DIR / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# 입력 데이터 JSON 파일 (web_form.py 가 생성)
ORDER_JSON_PATH = DATA_DIR / "current_order.json"

# 결제 결과를 저장할 엑셀 / JSON 파일
RESULT_EXCEL_PATH = DATA_DIR / "kvan_results.xlsx"
RESULT_JSON_PATH = DATA_DIR / "last_result.json"
HQ_STATE_PATH = DATA_DIR / "hq_state.json"

# 세션별 주문/결과 디렉토리 (동시 여러 세션용)
SESSION_ORDER_DIR = DATA_DIR / "sessions" / "orders"
SESSION_RESULT_DIR = DATA_DIR / "sessions" / "results"
SESSION_ORDER_DIR.mkdir(parents=True, exist_ok=True)
SESSION_RESULT_DIR.mkdir(parents=True, exist_ok=True)

# 어드민 세션 상태 JSON (본사 + 대행사 공용)
ADMIN_STATE_PATH = DATA_DIR / "admin_state.json"

# 옥션 상품 리스트 (본사 홈페이지 auction.html 기반)
AUCTION_ITEMS: list[dict] = []
AUCTION_LOADED = False


def _load_auction_items() -> None:
    """본사 auction.html 에 포함된 csvData 를 파싱하여 상품 리스트를 메모리에 로드."""
    global AUCTION_ITEMS, AUCTION_LOADED
    if AUCTION_LOADED:
        return
    try:
        # wsisa 폴더 기준 상위 경로에 auction.html 이 있다고 가정
        root = BASE_DIR.parent
        auction_path = root / "auction.html"
        if not auction_path.exists():
            AUCTION_LOADED = True
            return
        text = auction_path.read_text(encoding="utf-8", errors="ignore")
        # 첫 번째 백틱 블록(csvData) 추출
        m = re.search(r"const csvData\s*=\s*`([^`]+)`", text, re.DOTALL)
        if not m:
            AUCTION_LOADED = True
            return
        csv_block = m.group(1).strip()
        lines = [ln.strip() for ln in csv_block.splitlines() if ln.strip()]
        if not lines:
            AUCTION_LOADED = True
            return
        # 첫 줄은 헤더
        for row in lines[1:]:
            parts = [p.strip() for p in row.split(",")]
            if not parts or len(parts) < 3:
                continue
            try:
                price = int(parts[0])
            except ValueError:
                continue
            brand = parts[1]
            model = parts[2]
            AUCTION_ITEMS.append({"price": price, "name": f"{brand} {model}"})
    except Exception as e:  # noqa: BLE001
        print(f"[WARN] auction.html 파싱 실패: {e}")
    finally:
        AUCTION_LOADED = True


def _choose_product_name_for_amount(amount: int) -> str:
    """amount 와 동일한 금액의 옥션 상품들 중 최대 10개에서 랜덤 1개 선택."""
    _load_auction_items()
    if not AUCTION_ITEMS:
        return "SISA 글로벌 옥션 상품"
    candidates = [it for it in AUCTION_ITEMS if it.get("price") == amount]
    if not candidates:
        return "SISA 글로벌 옥션 상품"
    if len(candidates) > 10:
        candidates = random.sample(candidates, 10)
    chosen = random.choice(candidates)
    return chosen.get("name") or "SISA 글로벌 옥션 상품"

# MySQL 환경 변수 (Railway 용) - web_form.py 와 동일하게
DB_HOST = os.environ.get("MYSQLHOST") or os.environ.get("MYSQL_HOST") or "localhost"
DB_PORT = int(os.environ.get("MYSQLPORT") or os.environ.get("MYSQL_PORT") or "3306")
DB_USER = os.environ.get("MYSQLUSER") or os.environ.get("MYSQL_USER") or "root"
DB_PASSWORD = os.environ.get("MYSQLPASSWORD") or os.environ.get("MYSQL_PASSWORD") or ""
DB_NAME = (
    os.environ.get("MYSQL_DATABASE")
    or os.environ.get("MYSQLDATABASE")
    or os.environ.get("MYSQL_DB")
    or "railway"
)


def get_db():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )

# JSON / 결과 엑셀에서 공통으로 사용하는 필드 목록
HEADERS: List[str] = [
    "login_id",
    "login_password",
    "login_pin",
    "card_type",  # personal / business
    "card_number",
    "expiry_mm",
    "expiry_yy",
    "card_password",
    "installment_months",
    "phone_number",
    "customer_name",
    "resident_front",
    "amount",
    "product_name",
]


# 로그인 페이지 셀렉터
# 제공해주신 로그인 화면 HTML 기준:
# - 아이디 / 비밀번호 input 은 name 이 없고 label 텍스트로만 구분
SIGN_IN_SELECTORS = {
    # 기본: <label>아이디</label> 바로 아래 첫 번째 input
    "id_primary": (
        By.XPATH,
        "//label[normalize-space(text())='아이디']/following::input[1]",
    ),
    # 보조1: placeholder 에 '아이디' 가 포함된 input
    "id_placeholder": (
        By.XPATH,
        "//input[contains(@placeholder,'아이디')]",
    ),
    # 보조2: 로그인 폼 안의 첫 번째 text input (password 제외)
    "id_fallback": (
        By.XPATH,
        "(//input[@type='text' or not(@type)])[1]",
    ),
    # 기본: <label>비밀번호</label> 바로 아래 첫 번째 input
    "password_primary": (
        By.XPATH,
        "//label[normalize-space(text())='비밀번호']/following::input[1]",
    ),
    # 보조: type='password' 인 첫 번째 input
    "password_fallback": (
        By.XPATH,
        "(//input[@type='password'])[1]",
    ),
    # 로그인 버튼 (텍스트가 '로그인' 인 버튼)
    "submit_primary": (
        By.XPATH,
        "//button[contains(normalize-space(.), '로그인')]",
    ),
}

# PIN 팝업 셀렉터 (로그인 이후 2차 인증)
PIN_POPUP_SELECTORS = {
    # "PIN을 입력해 주세요" 문구가 포함된 영역 안의 첫 번째 input
    "input": (
        By.XPATH,
        "//*[contains(text(), 'PIN') and contains(text(), '입력')]/ancestor::div[1]//input",
    ),
    # 확인 버튼
    "confirm": (
        By.XPATH,
        "//button[contains(normalize-space(.), '확인')]",
    ),
}


def _set_session_ttl_to_5min(driver: webdriver.Chrome, max_wait: float = 10.0) -> bool:
    """
    세션 유효시간 Select 컴포넌트( id='session-ttl' )를
    '5분 (일반 결제)' 값으로 변경한다.

    - 1) 트리거 버튼(#session-ttl)을 클릭해 드롭다운을 연다.
    - 2) 드롭다운 안에서 '5분 (일반 결제)' 텍스트가 있는 옵션을 클릭한다.
    - 3) 다시 트리거의 span[data-slot="select-value"] 텍스트가 '5분' 으로
         시작하는지 확인해 실제로 변경되었는지 검증한다.
    - 위 과정을 max_wait 초 동안 반복 시도하고, 성공하면 True, 아니면 False.
    """
    end_time = time.time() + max_wait

    while time.time() < end_time:
        changed = False

        # 1) 트리거 버튼 클릭 (드롭다운 열기)
        try:
            trigger = driver.find_element(By.ID, "session-ttl")
            driver.execute_script("arguments[0].click();", trigger)
            time.sleep(0.2)
        except Exception:
            # 아직 로딩 중일 수 있으므로 바로 실패로 보지 않는다.
            time.sleep(0.2)

        # 2) 드롭다운 옵션 중 '5분 (일반 결제)' 또는 '5분' 이 포함된 항목 클릭
        try:
            # aria-controls 로 연결된 리스트가 있을 가능성이 높지만,
            # 여기서는 보이는 모든 노드 중에서 텍스트 기준으로 옵션을 찾는다.
            candidates = driver.find_elements(By.XPATH, "//*[contains(normalize-space(.),'5분 (일반 결제)') or contains(normalize-space(.),'5분')]")
            visible_opts = [el for el in candidates if el.is_displayed()]
            if visible_opts:
                driver.execute_script("arguments[0].click();", visible_opts[0])
                changed = True
                time.sleep(0.3)
        except Exception:
            # 옵션이 아직 생성되지 않았거나 구조가 다른 경우가 있을 수 있으므로 무시하고 재시도
            pass

        # 3) 실제로 트리거의 표시 값이 '5분 (일반 결제)' 로 바뀌었는지 확인
        try:
            trigger = driver.find_element(By.ID, "session-ttl")
            value_span = trigger.find_element(By.CSS_SELECTOR, "span[data-slot='select-value']")
            text = (value_span.text or "").strip()
            if text.startswith("5분"):
                print(f"[INFO] 세션 유효시간이 '{text}' 로 설정되었습니다.")
                return True
        except Exception:
            # 아직 반영 전이거나 요소를 못 찾은 경우, 아래에서 재시도
            pass

        # 클릭을 했다고 판단되었지만 검증이 실패한 경우에도 반복해서 재시도
        time.sleep(0.3)

    print("[WARN] 세션 유효시간을 '5분 (일반 결제)' 로 변경하지 못했습니다.")
    return False


def _find_input_quick(
    driver: webdriver.Chrome,
    css_list: list[str],
    max_wait: float = 3.0,
) -> webdriver.Chrome | None:
    """
    여러 CSS 셀렉터 후보를 짧게(0.2초 간격) 반복해서 검사하며,
    화면에 보이는 첫 번째 input 을 찾는다.
    """
    end = time.time() + max_wait
    while time.time() < end:
        for sel in css_list:
            try:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
            except Exception:
                continue
            for el in els:
                try:
                    if el.is_displayed():
                        return el
                except Exception:
                    continue
        time.sleep(0.2)
    return None


def _click_button_by_text_retry(
    driver: webdriver.Chrome,
    texts: list[str],
    total_timeout: float,
    description: str,
) -> bool:
    """
    주어진 텍스트 중 하나를 포함하는 버튼/링크를 찾을 때까지 반복 클릭 시도.

    - JS 로 innerText 기반 탐색 후 클릭
    - 안 되면 XPATH 로 button[@aria-label 포함]까지 포함해서 짧게 대기
    - total_timeout 초 안에 성공하면 True, 아니면 False
    """
    end_time = time.time() + total_timeout

    js_code = """
const targets = Array.from(document.querySelectorAll('button,a,div,span'));
for (const el of targets) {
  const t = (el.innerText || '').trim();
  if (!t) continue;
  const norm = t.replace(/\\s+/g, '');
  const target = (arguments[0] || '').trim();
  const targetNorm = target.replace(/\\s+/g, '');
  if (!target) continue;
  if (t.includes(target) || norm.includes(targetNorm)) {
    if (el.offsetParent !== null) {
      el.click();
      return true;
    }
  }
}
return false;
"""

    print(f"[STEP] 버튼 클릭 재시도 시작: {description}")

    while time.time() < end_time:
        # 1차: JS 기반 탐색
        for txt in texts:
            try:
                clicked = driver.execute_script(js_code, txt)
                if clicked:
                    # JS 상으로는 클릭이 실행되었지만,
                    # 실제 상태 변경 여부는 상위 로직에서 따로 검증해야 하므로
                    # 여기서는 '성공'이 아니라 '클릭 시도'로만 기록한다.
                    print(f"[INFO] '{txt}' 텍스트로 {description} 버튼 클릭 시도(JS).")
                    time.sleep(0.3)
                    return True
            except Exception:
                # DOM 로딩 시점 등으로 실패할 수 있으므로 무시하고 다음 시도
                continue

        # 2차: XPATH 기반 짧은 대기
        for txt in texts:
            try:
                wait = WebDriverWait(driver, 0.7)
                # XPath union(|)을 사용해야 하므로 or 대신 | 사용
                xp = (
                    f"//button[contains(normalize-space(.),'{txt}')]"
                    f" | //button[contains(@aria-label,'{txt}')]"
                )
                btn = wait.until(
                    EC.element_to_be_clickable((By.XPATH, xp))
                )
                btn.click()
                print(f"[INFO] '{txt}' 텍스트로 {description} 버튼 클릭 시도(XPATH).")
                time.sleep(0.3)
                return True
            except TimeoutException:
                continue

        time.sleep(0.3)

    print(f"[WARN] {total_timeout:.1f}초 동안 시도했지만 '{description}' 버튼을 찾지 못했습니다.")
    return False


def _click_notice_if_present(driver: webdriver.Chrome) -> None:
    """첫 화면 공지의 '확인 후 로그인' 버튼을 찾아 클릭.

    이전에 실제로 잘 동작하던 단순한 구조로 되돌렸습니다.
    """
    # 1차: XPATH 로 직접 버튼 찾기 (최대 5초)
    try:
        wait = WebDriverWait(driver, 5)
        btn = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(normalize-space(.),'확인 후 로그인')]")
            )
        )
        btn.click()
        time.sleep(0.3)
        return
    except TimeoutException:
        # 2차: JS로 전체 DOM 을 훑으면서 텍스트 기준으로 재시도
        try:
            driver.execute_script(
                """
const targets = Array.from(document.querySelectorAll('button,a,div,span'));
for (const el of targets) {
  const t = (el.innerText || '').trim();
  if (!t) continue;
  const norm = t.replace(/\\s+/g, '');
  if (t.includes('확인 후 로그인') || norm.includes('확인후로그인')) {
    if (el.offsetParent !== null) {
      el.click();
      return;
    }
  }
}
"""
            )
            time.sleep(0.3)
        except Exception:
            # 공지 팝업이 없거나 이미 닫혀 있으면 조용히 통과
            pass


# 대면결제 페이지 셀렉터 (필요 시 사이트 구조에 맞게 수정)
FACE_TO_FACE_SELECTORS = {
    # 결제 정보
    "card_personal_checkbox": (
        By.XPATH,
        "//span[normalize-space(text())='개인카드']/preceding-sibling::div[1]",
    ),
    "card_business_checkbox": (
        By.XPATH,
        "//span[contains(normalize-space(text()), '사업자')]/preceding-sibling::div[1]",
    ),
    # 카드번호: label 텍스트 기준으로 바로 아래 input
    "card_number": (
        By.XPATH,
        "//label[normalize-space(text())='카드번호']/following::input[1]",
    ),
    # MM / YY 는 label 텍스트 기준으로 select 요소를 찾는다
    "expiry_mm": (
        By.XPATH,
        "//label[normalize-space(text())='MM']/following::select[1]",
    ),
    "expiry_yy": (
        By.XPATH,
        "//label[normalize-space(text())='YY']/following::select[1]",
    ),
    # 비밀번호 앞 두 자리
    "card_password": (
        By.XPATH,
        "//label[normalize-space(text())='비밀번호']/following::input[1]",
    ),
    # 할부개월 select
    "installment_select": (
        By.XPATH,
        "//label[contains(normalize-space(text()), '할부개월')]/following::select[1]",
    ),
    # 연락처 / 이름 / 주민번호 앞자리
    "phone_prefix": (By.XPATH, "//select[contains(@name, 'phone') or contains(., '010')]"),
    "phone_number": (By.XPATH, "//input[@placeholder='연락처를 입력해주세요.']"),
    "customer_name": (
        By.XPATH,
        "//label[normalize-space(text())='이름']/following::input[1]",
    ),
    "resident_front": (
        By.XPATH,
        "//label[contains(normalize-space(text()), '주민등록번호 앞자리')]/following::input[1]",
    ),
    "business_reg_no": (
        By.XPATH,
        "//input[@placeholder='10자리 사업자등록번호를 입력해주세요.']",
    ),
    # 상품정보
    "product_name": (By.XPATH, "//label[contains(., '상품명')]/following::input[1]"),
    "product_price": (By.XPATH, "//label[contains(., '판매가격')]/following::input[1]"),
    # 버튼
    "submit_button": (By.XPATH, "//button[contains(., '결제하기')]"),
}


@dataclass
class PaymentRow:
    login_id: str
    login_password: str
    login_pin: str
    card_type: str
    card_number: str
    expiry_mm: str
    expiry_yy: str
    card_password: str
    installment_months: str
    phone_number: str
    customer_name: str
    resident_front: str
    amount: int
    product_name: str


def load_order_from_json(path: str) -> PaymentRow:
    """web_form.py 가 생성한 JSON 파일에서 1건의 주문 데이터를 읽어온다."""
    if not Path(path).exists():
        raise FileNotFoundError(
            f"{path} 파일을 찾을 수 없습니다. web_form.py 의 웹 폼에서 먼저 데이터를 저장해 주세요."
        )

    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    # 금액 / 상품명 / 로그인 ID 가 없으면 더미값 채워서라도 진행할 수 있게 한다.
    if "amount" not in raw or raw.get("amount") in ("", None, 0, "0"):
        raw["amount"] = 100000  # 기본 10만원
    if "product_name" not in raw or not str(raw.get("product_name") or "").strip():
        raw["product_name"] = "SISA 테스트 상품"
    if "login_id" not in raw or not str(raw.get("login_id") or "").strip():
        raw["login_id"] = "m1234"

    missing = [k for k in HEADERS if k not in raw or raw[k] in ("", None)]
    if missing:
        raise ValueError(f"JSON 데이터에 누락된 필드가 있습니다: {missing}")

    try:
        amount_int = int(raw["amount"])
    except (TypeError, ValueError) as e:
        raise ValueError(f"amount 값이 숫자가 아닙니다: {raw['amount']!r}") from e

    card_type = str(raw.get("card_type", "personal")).strip().lower()
    if card_type not in ("personal", "business"):
        card_type = "personal"

    return PaymentRow(
        login_id=str(raw["login_id"]).strip(),
        login_password=str(raw["login_password"]).strip(),
        login_pin=str(raw["login_pin"]).strip(),
        card_type=card_type,
        card_number=str(raw["card_number"]).strip(),
        expiry_mm=str(raw["expiry_mm"]).strip(),
        expiry_yy=str(raw["expiry_yy"]).strip(),
        card_password=str(raw["card_password"]).strip(),
        installment_months=str(raw["installment_months"]).strip(),
        phone_number=str(raw["phone_number"]).strip(),
        customer_name=str(raw["customer_name"]).strip(),
        resident_front=str(raw["resident_front"]).strip(),
        amount=amount_int,
        product_name=str(raw["product_name"]).strip(),
    )


def _get_chromedriver_path() -> str | None:
    """로컬 tool 폴더 또는 환경변수에 지정된 ChromeDriver 경로."""
    env_path = os.environ.get("CHROMEDRIVER_PATH", "").strip()
    if env_path and Path(env_path).exists():
        return env_path
    base = Path(__file__).resolve().parent / "tool"
    for name in ("chromedriver.exe", "chromedriver"):
        p = base / name
        if p.exists():
            return str(p)
    return None


def create_driver(headless: bool | None = None) -> webdriver.Chrome:
    if headless is None:
        headless = _is_server_env()
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-setuid-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
    else:
        options.add_argument("--start-maximized")
    # Railway 등 서버: 시스템에 설치된 Chrome 사용
    if _is_server_env():
        for binary in ("/usr/bin/google-chrome-stable", "/usr/bin/google-chrome", "/usr/bin/chromium"):
            if Path(binary).exists():
                options.binary_location = binary
                break
    service = None
    driver_path = _get_chromedriver_path()
    if driver_path:
        service = Service(executable_path=driver_path)
    driver = webdriver.Chrome(service=service, options=options) if service else webdriver.Chrome(options=options)
    driver.implicitly_wait(5)
    return driver


def _parse_amount(text: str) -> int:
    text = (text or "").replace("원", "").replace(",", "").strip()
    try:
        return int(text) if text else 0
    except ValueError:
        return 0


def _scrape_dashboard_and_store(driver: webdriver.Chrome) -> None:
    """로그인 후 대시보드 화면에서 매출 요약 정보를 크롤링하여 DB에 저장."""
    try:
        wait = WebDriverWait(driver, 5)
        # 대시보드가 렌더링될 시간을 약간 준다
        time.sleep(2.0)

        # 각 블록은 '월 매출', '전일 매출', '정산 예정 금액', '나의 크레딧' 등의 텍스트를 기준으로 탐색
        def find_block(label_text: str):
            try:
                label_el = wait.until(
                    EC.presence_of_element_located(
                        (By.XPATH, f"//*[normalize-space(text())='{label_text}']")
                    )
                )
                return label_el.find_element(By.XPATH, "./ancestor::div[1]")
            except TimeoutException:
                return None

        monthly_block = find_block("월 매출")
        yesterday_block = find_block("전일 매출")
        settlement_block = find_block("정산 예정 금액")
        credit_block = find_block("나의 크레딧")

        monthly_sales = monthly_approved_cnt = monthly_approved_amt = 0
        monthly_canceled_cnt = monthly_canceled_amt = 0

        if monthly_block:
            # 첫 번째 금액 (월 매출)
            try:
                amt_el = monthly_block.find_element(By.XPATH, ".//*[contains(text(),'원')]")
                monthly_sales = _parse_amount(amt_el.text)
            except Exception:
                pass
            # 승인 / 취소 블록들
            try:
                approve_el = monthly_block.find_element(
                    By.XPATH, ".//*[contains(normalize-space(text()),'승인')]/ancestor::div[1]"
                )
                nums = approve_el.text.splitlines()
                if len(nums) >= 2:
                    monthly_approved_cnt = _parse_amount(nums[0])
                    monthly_approved_amt = _parse_amount(nums[1])
            except Exception:
                pass
            try:
                cancel_el = monthly_block.find_element(
                    By.XPATH, ".//*[contains(normalize-space(text()),'취소')]/ancestor::div[1]"
                )
                nums = cancel_el.text.splitlines()
                if len(nums) >= 2:
                    monthly_canceled_cnt = _parse_amount(nums[0])
                    monthly_canceled_amt = _parse_amount(nums[1])
            except Exception:
                pass

        yesterday_sales = yesterday_approved_cnt = yesterday_approved_amt = 0
        yesterday_canceled_cnt = yesterday_canceled_amt = 0
        if yesterday_block:
            try:
                amt_el = yesterday_block.find_element(By.XPATH, ".//*[contains(text(),'원')]")
                yesterday_sales = _parse_amount(amt_el.text)
            except Exception:
                pass
            try:
                approve_el = yesterday_block.find_element(
                    By.XPATH, ".//*[contains(normalize-space(text()),'승인')]/ancestor::div[1]"
                )
                nums = approve_el.text.splitlines()
                if len(nums) >= 2:
                    yesterday_approved_cnt = _parse_amount(nums[0])
                    yesterday_approved_amt = _parse_amount(nums[1])
            except Exception:
                pass
            try:
                cancel_el = yesterday_block.find_element(
                    By.XPATH, ".//*[contains(normalize-space(text()),'취소')]/ancestor::div[1]"
                )
                nums = cancel_el.text.splitlines()
                if len(nums) >= 2:
                    yesterday_canceled_cnt = _parse_amount(nums[0])
                    yesterday_canceled_amt = _parse_amount(nums[1])
            except Exception:
                pass

        settlement_expected = today_settlement_expected = 0
        if settlement_block:
            try:
                amt_el = settlement_block.find_element(By.XPATH, ".//*[contains(text(),'원')]")
                settlement_expected = _parse_amount(amt_el.text)
            except Exception:
                pass
            # "금일 정산 예정금" 이라는 텍스트가 있으면 그 주변에서 숫자를 찾는다
            try:
                today_el = settlement_block.find_element(
                    By.XPATH, ".//*[contains(normalize-space(text()),'금일 정산 예정금')]/following::div[1]"
                )
                today_settlement_expected = _parse_amount(today_el.text)
            except Exception:
                pass

        credit_amount = 0
        if credit_block:
            try:
                amt_el = credit_block.find_element(By.XPATH, ".//*[contains(text(),'원')]")
                credit_amount = _parse_amount(amt_el.text)
            except Exception:
                pass

        # 최근 거래 내역 텍스트를 간단히 요약 저장 (상세 구조를 몰라서 전체 텍스트 저장)
        recent_summary = ""
        try:
            recent_container = driver.find_element(
                By.XPATH, "//*[contains(normalize-space(text()),'최근 거래 내역')]/ancestor::section[1]"
            )
            recent_summary = recent_container.text.strip()
        except Exception:
            pass

        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO kvan_dashboard (
                  captured_at,
                  monthly_sales_amount,
                  monthly_approved_count,
                  monthly_approved_amount,
                  monthly_canceled_count,
                  monthly_canceled_amount,
                  yesterday_sales_amount,
                  yesterday_approved_count,
                  yesterday_approved_amount,
                  yesterday_canceled_count,
                  yesterday_canceled_amount,
                  settlement_expected_amount,
                  today_settlement_expected_amount,
                  credit_amount,
                  recent_tx_summary
                )
                VALUES (NOW(), %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    monthly_sales,
                    monthly_approved_cnt,
                    monthly_approved_amt,
                    monthly_canceled_cnt,
                    monthly_canceled_amt,
                    yesterday_sales,
                    yesterday_approved_cnt,
                    yesterday_approved_amt,
                    yesterday_canceled_cnt,
                    yesterday_canceled_amt,
                    settlement_expected,
                    today_settlement_expected,
                    credit_amount,
                    recent_summary,
                ),
            )
        conn.commit()
        conn.close()
    except Exception as e:
        # 대시보드 크롤링 실패는 치명적이지 않으므로 로그만 남기고 계속 진행
        print(f"[WARN] 대시보드 크롤링/DB 저장 실패: {e}")


def _go_to_payment_link_page(driver: webdriver.Chrome) -> None:
    """좌측 사이드바에서 '결제링크 관리' 메뉴까지만 이동 (리스트 화면)."""
    try:
        t0 = _step_start("결제링크 관리 메뉴 이동")
        wait = WebDriverWait(driver, 5)
        # 사이드바의 '결제링크 관리' 항목 클릭
        link_btn = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//*[contains(normalize-space(text()),'결제링크 관리')]")
            )
        )
        link_btn.click()
        time.sleep(0.3)
        _step_end("결제링크 관리 메뉴 이동", t0)
    except Exception as e:
        print(f"[WARN] 결제링크 관리/생성 페이지 이동 실패: {e}")


def _go_to_create_link_page(driver: webdriver.Chrome) -> bool:
    """
    결제링크 관리 화면에서 '+ 생성' 버튼을 눌러
    결제링크 생성 화면(또는 모달)로 진입한다.

    - 단순 텍스트 매칭뿐 아니라,
      '결제링크 관리' 헤더 주변에서 버튼을 위치 기반으로 찾아 클릭하는
      다른 패러다임을 사용한다.
    """
    t0 = _step_start("+ 생성 버튼 클릭 (헤더/위치 기반 탐색)")
    try:
        # 최대 약 8~10초 동안(12회) 반복해서 버튼을 찾는다.
        for _ in range(12):
            try:
                clicked = driver.execute_script(
                    """
const clickCreateButton = () => {
  // 1. 텍스트/aria-label 에 '생성' 이 포함된 버튼/링크를 우선 시도
  const primary = Array.from(document.querySelectorAll('button,a[role="button"]')).find(el => {
    if (!el.offsetParent) return false;
    const t = (el.innerText || '').trim();
    const label = (el.getAttribute('aria-label') || '').trim();
    return t.includes('생성') || label.includes('생성');
  });
  if (primary) {
    primary.click();
    return true;
  }

  // 2. '결제링크 관리' 헤더 근처에서 버튼을 찾는다.
  const headings = Array.from(document.querySelectorAll('h1,h2,h3'));
  const title = headings.find(el => {
    const t = (el.innerText || '').trim();
    return t.includes('결제링크 관리');
  });

  if (title) {
    let container = title.closest('header,section,div') || document.body;
    let btns = Array.from(container.querySelectorAll('button,a[role="button"]'))
      .filter(el => el.offsetParent !== null);

    if (btns.length > 0) {
      // 오른쪽 위에 있을수록, 그리고 크기가 어느 정도 되는 버튼을 우선 선택
      let best = null;
      let bestScore = -Infinity;
      for (const el of btns) {
        const r = el.getBoundingClientRect();
        const width = r.right - r.left;
        const height = r.bottom - r.top;
        // 너무 작은(아이콘 점 등) 요소는 제외
        if (width < 16 || height < 16) continue;
        // 화면의 오른쪽/위쪽에 있을수록 점수가 높도록 가중치 부여
        const score = r.right * 1.0 - r.top * 0.2;
        if (score > bestScore) {
          bestScore = score;
          best = el;
        }
      }
      if (best) {
        best.click();
        return true;
      }
    }
  }

  // 3. 위 방식으로도 찾지 못하면 실패
  return false;
};
return clickCreateButton();
"""
                )
                if clicked:
                    time.sleep(0.5)
                    _step_end("+ 생성 버튼 클릭 (헤더/위치 기반 탐색)", t0)
                    return True
            except Exception:
                # DOM 이 아직 완전히 준비되지 않았을 수 있으므로 잠시 후 재시도
                pass

            time.sleep(0.7)

        print("[WARN] 여러 번 시도했지만 '+ 생성' 버튼을 찾지 못했습니다.")
        _step_end("+ 생성 버튼 클릭 (헤더/위치 기반 탐색)", t0)
        return False
    except Exception as e:  # noqa: BLE001
        print(f"[WARN] '+ 생성' 버튼 클릭 실패: {e}")
        _step_end("+ 생성 버튼 클릭 (헤더/위치 기반 탐색)", t0)
        return False


def _store_kvan_link_for_session(session_id: str, link: str) -> None:
    """admin_state.json 의 해당 세션에 K-VAN 결제 링크를 매핑해서 저장."""
    if not session_id or not link:
        return
    try:
        if not ADMIN_STATE_PATH.exists():
            return
        with open(ADMIN_STATE_PATH, "r", encoding="utf-8") as f:
            state = json.load(f)
        sessions = state.get("sessions") or []
        history = state.get("history") or []
        updated = False
        for s in sessions:
            if str(s.get("id")) == str(session_id):
                s["kvan_link"] = link
                updated = True
                break
        if updated:
            state["sessions"] = sessions
            state["history"] = history
            with open(ADMIN_STATE_PATH, "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:  # noqa: BLE001
        print(f"[WARN] admin_state 에 K-VAN 링크 저장 실패: {e}")


def _fill_payment_link_form_and_get_url(
    driver: webdriver.Chrome, row: PaymentRow, session_id: str
) -> str | None:
    """결제링크 생성 페이지에서 폼을 채우고, 생성된 https://store.k-van.app... 링크를 리턴."""
    t0_all = _step_start("결제링크 생성 폼 작성 전체")
    wait = WebDriverWait(driver, 5)
    time.sleep(0.3)

    # 1. 금액 입력
    t0 = _step_start("1. 금액 입력")
    amount_input = None
    amount_xpaths = [
        "//*[contains(normalize-space(text()),'금액')]/following::input[1]",
        "//input[@type='number' or @inputmode='decimal']",
    ]
    for xp in amount_xpaths:
        try:
            amount_input = wait.until(EC.visibility_of_element_located((By.XPATH, xp)))
            break
        except TimeoutException:
            continue
    if amount_input:
        try:
            # 금액 입력창이 화면 중앙에 오도록 스크롤
            driver.execute_script(
                "arguments[0].scrollIntoView({behavior:'smooth',block:'center'});",
                amount_input,
            )
        except Exception:
            pass
        amount_input.clear()
        amount_input.send_keys(str(row.amount))
        time.sleep(0.2)
    _step_end("1. 금액 입력", t0)

    # 2. 상품명: 옥션 리스트에서 amount 에 맞는 상품명 선택
    t0 = _step_start("2. 상품명 입력")
    product_name = _choose_product_name_for_amount(row.amount)
    try:
        name_input = wait.until(
            EC.visibility_of_element_located(
                (By.XPATH, "//*[contains(normalize-space(text()),'상품명')]/following::input[1]")
            )
        )
        name_input.clear()
        name_input.send_keys(product_name)
        time.sleep(0.2)
    except TimeoutException:
        pass
    _step_end("2. 상품명 입력", t0)

    # 3. 상품설명
    t0 = _step_start("3. 상품설명 입력")
    desc_text = "글로벌 중고명품 경매사이트  구매 대행 서비스 즉시구매 결제 및 예치금"
    try:
        desc_input = wait.until(
            EC.visibility_of_element_located(
                (
                    By.XPATH,
                    "//*[contains(normalize-space(text()),'상품설명') or contains(normalize-space(text()),'상품 설명')]/following::textarea[1]",
                )
            )
        )
        desc_input.clear()
        desc_input.send_keys(desc_text)
        time.sleep(0.2)
    except TimeoutException:
        # textarea 대신 input 일 수도 있으므로 보조 XPATH 시도
        try:
            desc_input = wait.until(
                EC.visibility_of_element_located(
                    (
                        By.XPATH,
                        "//*[contains(normalize-space(text()),'상품설명') or contains(normalize-space(text()),'상품 설명')]/following::input[1]",
                    )
                )
            )
            desc_input.clear()
            desc_input.send_keys(desc_text)
            time.sleep(0.2)
        except TimeoutException:
            pass
    _step_end("3. 상품설명 입력", t0)

    # 폼 아래쪽에 있는 링크 생성 버튼과 기타 항목들이 보이도록 스크롤을 조금 내린다.
    try:
        # 상품설명까지 입력한 후, 아래쪽에 있는 세션유효시간/링크 생성 버튼들이
        # 화면에 들어오도록 충분히 스크롤 다운
        driver.execute_script("window.scrollBy(0, 800);")
        time.sleep(0.3)
    except Exception:
        pass

    # 1-1. 세션유효시간: 콤보박스(#session-ttl)를 '5분 (일반 결제)' 로 변경
    t0 = _step_start("4. 세션유효시간 선택 (3분→5분)")
    changed_to_5min = _set_session_ttl_to_5min(driver, max_wait=10.0)
    if not changed_to_5min:
        print(
            "[WARN] 10초 동안 세션유효시간을 '5분 (일반 결제)' 로 변경하지 못했습니다. "
            "현재 값(기본 3분 등)을 유지한 채 링크 생성 단계로 진행합니다."
        )
    _step_end("4. 세션유효시간 선택 (3분→5분)", t0)

    # 2. 링크 복사/생성 버튼 클릭
    t0 = _step_start("5. 링크 복사/생성 버튼 클릭")
    used_copy_button = False
    try:
        # 1차: '링크 복사' 문구가 있는 버튼을 우선 시도
        copy_btn = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(normalize-space(.),'링크 복사')]")
            )
        )
        copy_btn.click()
        used_copy_button = True
        time.sleep(0.4)
    except TimeoutException:
        # 2차: '링크 생성하기' 또는 '링크 생성' 문구가 있는 버튼
        try:
            create_btn = wait.until(
                EC.element_to_be_clickable(
                    (
                        By.XPATH,
                        "//button[contains(normalize-space(.),'링크 생성하기') or contains(normalize-space(.),'링크 생성')]",
                    )
                )
            )
            create_btn.click()
            time.sleep(0.4)
        except TimeoutException:
            # 3차: 현재 폼 내에서 type='submit' 이고 '생성' 텍스트가 포함된 버튼
            try:
                create_btn = wait.until(
                    EC.element_to_be_clickable(
                        (
                            By.XPATH,
                            "//form//button[@type='submit' and contains(normalize-space(.),'생성')]",
                        )
                    )
                )
                create_btn.click()
                time.sleep(0.4)
            except TimeoutException:
                print("[WARN] '링크 복사' / '링크 생성하기' / '생성' 버튼을 찾지 못했습니다.")
                return None
    _step_end("5. 링크 복사/생성 버튼 클릭", t0)

    # "결제 링크를 생성 하시겠습니까?" 팝업에서 '생성하기' 버튼 클릭
    t0 = _step_start("6. 생성 확인 팝업 처리 및 최종 링크 추출")
    if not used_copy_button:
        # '링크 생성하기' 플로우인 경우에만 확인 팝업이 뜬다.
        try:
            wait.until(
                EC.visibility_of_element_located(
                    (
                        By.XPATH,
                        "//*[contains(normalize-space(text()),'결제 링크를 생성 하시겠습니까')]",
                    )
                )
            )
            confirm_btn = wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//button[contains(normalize-space(text()),'생성하기')]")
                )
            )
            confirm_btn.click()
            time.sleep(0.8)
        except TimeoutException:
            print("[WARN] 결제 링크 생성 확인 팝업을 찾지 못했습니다.")

    # 생성된 링크 중 'https://store.k-van.app' 로 시작하는 첫 번째 링크 확보
    link_text = None
    try:
        link_el = wait.until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    (
                        "//*[contains(text(),'https://store.k-van.app') "
                        "or @value[contains(.,'https://store.k-van.app')] "
                        "or contains(@href,'https://store.k-van.app') "
                        "or contains(@data-clipboard-text,'https://store.k-van.app')]"
                    ),
                )
            )
        )
        # 텍스트, value, href, data-clipboard-text 순으로 링크를 추출
        link_text = (link_el.text or "").strip()
        if not link_text:
            link_text = (link_el.get_attribute("value") or "").strip()
        if not link_text:
            link_text = (link_el.get_attribute("href") or "").strip()
        if not link_text:
            link_text = (link_el.get_attribute("data-clipboard-text") or "").strip()
    except TimeoutException:
        print("[WARN] 생성된 결제 링크 텍스트를 찾지 못했습니다.")

    if link_text and "https://store.k-van.app" in link_text:
        _store_kvan_link_for_session(session_id, link_text)
        _step_end("6. 생성 확인 팝업 처리 및 최종 링크 추출", t0)
        _step_end("결제링크 생성 폼 작성 전체", t0_all)
        return link_text

    _step_end("6. 생성 확인 팝업 처리 및 최종 링크 추출", t0)
    _step_end("결제링크 생성 폼 작성 전체", t0_all)
    return None

def sign_in(driver: webdriver.Chrome, row: PaymentRow) -> None:
    t0_all = _step_start("로그인 전체")
    driver.get(SIGN_IN_URL)
    # 로그인 화면 전체는 비교적 빨리 뜨므로, 글로벌 wait 은 최소한으로만 사용한다.
    wait = WebDriverWait(driver, 3)

    # 공지 팝업(확인 후 로그인 / 로그인 버튼)이 있으면 먼저 처리
    _click_notice_if_present(driver)

    # 아이디 입력창 찾기: 가장 단순한 CSS 셀렉터를 우선 사용
    t0 = _step_start("아이디 입력")
    id_input = _find_input_quick(
        driver,
        [
            "input[placeholder*='아이디']",
            "input[name*='id']",
            "input[type='text']",
        ],
        max_wait=3.0,
    )
    if not id_input:
        # 기존 XPATH 후보들도 한 번 더 시도 (역순으로 – 가장 느린 fallback 먼저)
        id_locators = [
            SIGN_IN_SELECTORS["id_fallback"],
            SIGN_IN_SELECTORS["id_placeholder"],
            SIGN_IN_SELECTORS["id_primary"],
        ]
        for loc in id_locators:
            try:
                id_input = wait.until(EC.visibility_of_element_located(loc))
                break
            except TimeoutException:
                continue

    if not id_input:
        print("[ERROR] 아이디 입력창을 찾지 못했습니다. 로그인 단계를 종료합니다.")
        _step_end("아이디 입력", t0)
        _step_end("로그인 전체", t0_all)
        return

    id_input.clear()
    id_input.send_keys(row.login_id)
    _step_end("아이디 입력", t0)

    # 비밀번호 입력창 찾기
    t0 = _step_start("비밀번호 입력")
    pw_input = _find_input_quick(
        driver,
        [
            "input[type='password']",
            "input[placeholder*='비밀번호']",
        ],
        max_wait=3.0,
    )
    if not pw_input:
        pw_locators = [
            SIGN_IN_SELECTORS["password_fallback"],
            SIGN_IN_SELECTORS["password_primary"],
        ]
        for loc in pw_locators:
            try:
                pw_input = wait.until(EC.visibility_of_element_located(loc))
                break
            except TimeoutException:
                continue

    if not pw_input:
        print("[ERROR] 비밀번호 입력창을 찾지 못했습니다. 로그인 단계를 종료합니다.")
        _step_end("비밀번호 입력", t0)
        _step_end("로그인 전체", t0_all)
        return

    pw_input.clear()
    pw_input.send_keys(row.login_password)
    _step_end("비밀번호 입력", t0)

    # 로그인 버튼 찾기
    t0 = _step_start("로그인 버튼 클릭")
    submit_btn = wait.until(
        EC.element_to_be_clickable(SIGN_IN_SELECTORS["submit_primary"])
    )
    submit_btn.click()
    _step_end("로그인 버튼 클릭", t0)

    # 2차 인증 PIN 팝업 처리 (있으면 입력, 없으면 통과)
    try:
        # 로그인 버튼 클릭 후 사람이 화면을 보는 것처럼 약간 대기
        time.sleep(0.5)
        pin_wait = WebDriverWait(driver, 3)
        pin_input_container = pin_wait.until(
            EC.visibility_of_element_located(PIN_POPUP_SELECTORS["input"])
        )
        # 위에서 찾은 컨테이너 안의 input 이나 자신이 input 일 수 있음
        if pin_input_container.tag_name.lower() == "input":
            pin_input = pin_input_container
        else:
            pin_input = pin_input_container.find_element(By.XPATH, ".//input")

        t0_pin = _step_start("PIN 입력 및 확인")
        pin_input.clear()
        pin_input.send_keys(row.login_pin)
        # 사람이 확인 내용을 읽는 것처럼 잠시 대기
        time.sleep(0.4)

        confirm_btn = driver.find_element(*PIN_POPUP_SELECTORS["confirm"])
        confirm_btn.click()
        # 서버가 토큰을 발급하고 홈으로 이동할 시간을 충분히 준다
        time.sleep(1.0)
        _step_end("PIN 입력 및 확인", t0_pin)
    except TimeoutException:
        # PIN 팝업이 없는 계정/환경일 수 있으므로 조용히 통과
        pass

    # 로그인 완료까지 잠시 대기 (홈 또는 다른 보호된 페이지로 진입)
    try:
        t0 = _step_start("로그인 후 리다이렉트 대기")
        wait.until(EC.url_contains("store.k-van.app"))
        _step_end("로그인 후 리다이렉트 대기", t0)
    except TimeoutException:
        print("[WARN] 로그인 후 리다이렉트 URL을 확인하지 못했습니다.")
        _step_end("로그인 후 리다이렉트 대기", t0)
        _step_end("로그인 전체", t0_all)
        return

    # 서버 환경에서만 대시보드 크롤링 수행 (로컬 테스트에서는 생략)
    if _is_server_env():
        _scrape_dashboard_and_store(driver)

    _step_end("로그인 전체", t0_all)


def _click_box(driver: webdriver.Chrome, locator: tuple) -> None:
    """단순 클릭 (div 기반 토글 박스용)."""
    el = driver.find_element(*locator)
    el.click()


def _select_dropdown_any(driver: webdriver.Chrome, locator: tuple, text: str) -> None:
    el = driver.find_element(*locator)
    tag = el.tag_name.lower()

    if tag == "select":
        Select(el).select_by_visible_text(text)
        return

    el.click()
    el.clear()
    el.send_keys(text)
    el.send_keys(Keys.ENTER)


def fill_face_to_face_form(driver: webdriver.Chrome, row: PaymentRow) -> None:
    # 기존 대면결제 URL — 결제링크 생성 플로우가 안정화되면 이 부분을 대체 가능
    driver.get(FACE_TO_FACE_URL)
    wait = WebDriverWait(driver, 30)

    # 페이지가 완전히 렌더링될 수 있도록 사람처럼 잠시 대기
    time.sleep(2.0)

    # 1) 상품명 먼저 입력
    product_input = wait.until(
        EC.visibility_of_element_located(FACE_TO_FACE_SELECTORS["product_name"])
    )
    driver.execute_script(
        "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});",
        product_input,
    )
    time.sleep(0.8)
    product_input.clear()
    product_input.send_keys(row.product_name or "잡화")
    time.sleep(0.6)

    # 2) 판매가격(결제금액) 입력
    try:
        price_input = wait.until(
            EC.visibility_of_element_located(FACE_TO_FACE_SELECTORS["product_price"])
        )
    except TimeoutException:
        # 기본 셀렉터로 못 찾으면 몇 가지 대체 XPATH 를 시도
        alt_xpaths = [
            "//*[contains(text(), '판매가격') or contains(text(), '결제금액')]/following::input[1]",
            "//input[@inputmode='decimal' or @type='number']",
        ]
        price_input = None
        for xp in alt_xpaths:
            try:
                price_input = wait.until(
                    EC.visibility_of_element_located((By.XPATH, xp))
                )
                break
            except TimeoutException:
                continue
        if price_input is None:
            raise RuntimeError("판매가격(결제금액) 입력창을 찾지 못했습니다.")

    driver.execute_script(
        "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});",
        price_input,
    )
    time.sleep(0.8)
    price_input.clear()
    price_input.send_keys(str(row.amount))
    time.sleep(0.8)

    # 3) 아래 결제 정보 입력창들이 보이도록 스크롤을 조금 더 내림
    try:
        scroll_container = wait.until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//div[contains(@class,'overflow-y-auto') and contains(@class,'min-h-screen')]",
                )
            )
        )
        driver.execute_script(
            "arguments[0].scrollTo({top: arguments[0].scrollTop + 600, behavior: 'smooth'});",
            scroll_container,
        )
    except TimeoutException:
        # 컨테이너를 못 찾으면 윈도우 스크롤로 폴백
        driver.execute_script("window.scrollBy(0, 600);")
    time.sleep(1.5)

    # 결제 정보 - 카드 종류 체크 (개인 / 법인)
    # 페이지 기본값이 개인카드이므로, 개인카드는 굳이 클릭하지 않는다.
    if row.card_type == "business":
        try:
            _click_box(driver, FACE_TO_FACE_SELECTORS["card_business_checkbox"])
            time.sleep(0.8)
        except Exception:
            pass

    # 카드번호
    card_input = wait.until(
        EC.visibility_of_element_located(FACE_TO_FACE_SELECTORS["card_number"])
    )
    card_input.clear()
    card_input.send_keys(row.card_number)

    # 유효기간 MM / YY
    try:
        _select_dropdown_any(driver, FACE_TO_FACE_SELECTORS["expiry_mm"], row.expiry_mm)
    except Exception:
        mm_input = driver.find_element(*FACE_TO_FACE_SELECTORS["expiry_mm"])
        mm_input.clear()
        mm_input.send_keys(row.expiry_mm)

    try:
        _select_dropdown_any(driver, FACE_TO_FACE_SELECTORS["expiry_yy"], row.expiry_yy)
    except Exception:
        yy_input = driver.find_element(*FACE_TO_FACE_SELECTORS["expiry_yy"])
        yy_input.clear()
        yy_input.send_keys(row.expiry_yy)

    # 비밀번호 앞 두 자리
    pw2_input = wait.until(
        EC.visibility_of_element_located(FACE_TO_FACE_SELECTORS["card_password"])
    )
    pw2_input.clear()
    pw2_input.send_keys(row.card_password)

    # 할부개월
    try:
        _select_dropdown_any(
            driver, FACE_TO_FACE_SELECTORS["installment_select"], row.installment_months
        )
    except Exception:
        pass

    # 연락처
    try:
        _select_dropdown_any(driver, FACE_TO_FACE_SELECTORS["phone_prefix"], "010")
    except Exception:
        pass

    phone_input = wait.until(
        EC.visibility_of_element_located(FACE_TO_FACE_SELECTORS["phone_number"])
    )
    phone_input.clear()
    phone_input.send_keys(row.phone_number)

    # 이름
    name_input = wait.until(
        EC.visibility_of_element_located(FACE_TO_FACE_SELECTORS["customer_name"])
    )
    name_input.clear()
    name_input.send_keys(row.customer_name)

    # 주민등록번호 앞자리 / 사업자등록번호
    if row.card_type == "business":
        # 사업자 카드인 경우: 사업자등록번호 입력칸이 나타남
        try:
            biz_input = wait.until(
                EC.visibility_of_element_located(
                    FACE_TO_FACE_SELECTORS["business_reg_no"]
                )
            )
            biz_input.clear()
            biz_input.send_keys(row.resident_front)
        except Exception:
            # 셀렉터가 맞지 않거나 필드가 안 보이면 조용히 통과
            pass
    else:
        # 개인카드인 경우: 주민등록번호 앞자리 입력
        res_input = wait.until(
            EC.visibility_of_element_located(FACE_TO_FACE_SELECTORS["resident_front"])
        )
        res_input.clear()
        res_input.send_keys(row.resident_front)

    # 버튼이 "결제하기" 로 바뀔 때까지 기다렸다가 클릭
    try:
        submit_btn = wait.until(
            EC.element_to_be_clickable(FACE_TO_FACE_SELECTORS["submit_button"])
        )
    except TimeoutException:
        raise RuntimeError("결제하기 버튼을 찾지 못했습니다. 셀렉터를 확인하세요.")

    submit_btn.click()


def confirm_popup_and_get_result(driver: webdriver.Chrome) -> tuple[str, str]:
    wait = WebDriverWait(driver, 30)

    # 결제 확인 팝업에서 "결제" 또는 "확인" 버튼 클릭 (텍스트 기준으로 찾기)
    clicked_confirm = False
    time.sleep(1.0)

    # 팝업 타이틀(합계 ~ 결제)이 보일 때까지 먼저 대기 (있으면)
    try:
        wait.until(
            EC.visibility_of_element_located(
                (
                    By.XPATH,
                    "//*[contains(normalize-space(.), '합계') and contains(normalize-space(.), '결제')]",
                )
            )
        )
    except TimeoutException:
        # 팝업 타이틀을 못 찾더라도 계속 진행 (환경에 따라 구조가 다를 수 있음)
        pass

    # 결제 버튼 → 확인 버튼 순서로 여러 패턴을 시도
    button_xpaths = [
        # SweetAlert2 팝업 전용 (가장 우선)
        "//div[contains(@class,'swal2-actions')]//button[contains(@class,'swal2-confirm')]",
        "//button[contains(@class,'swal2-confirm') and contains(normalize-space(.), '결제')]",
        # 버튼 자체 텍스트에 "결제"
        "//button[contains(normalize-space(.), '결제')]",
        # 버튼 내부 span 등에 "결제"
        "//button[.//span[contains(normalize-space(normalize-space(.)), '결제')]]",
        # 일부 환경에서는 '확인'만 표시될 수 있음
        "//button[contains(normalize-space(.), '확인')]",
        "//button[.//span[contains(normalize-space(.), '확인')]]",
    ]

    for xp in button_xpaths:
        try:
            btn = wait.until(
                EC.element_to_be_clickable(
                    (
                        By.XPATH,
                        xp,
                    )
                )
            )
            # 버튼이 가려져 있을 수 있으므로 가운데로 스크롤
            try:
                driver.execute_script(
                    "arguments[0].scrollIntoView({behavior:'smooth', block:'center'});",
                    btn,
                )
                time.sleep(0.5)
            except Exception:
                pass

            try:
                btn.click()
            except Exception:
                # 일반 click 이 안 먹히면 JS 로 강제 클릭
                driver.execute_script("arguments[0].click();", btn)

            clicked_confirm = True
            break
        except TimeoutException:
            continue

    if not clicked_confirm:
        print("확인/결제 팝업 버튼을 찾지 못했습니다. 바로 결과 화면이 떴을 수 있습니다.")

    # 결과 화면의 h1 / p 텍스트 읽기
    try:
        title_el = wait.until(
            EC.visibility_of_element_located(
                (By.XPATH, "//h1[contains(., '결제')]")
            )
        )
        title_text = title_el.text.strip()
    except TimeoutException:
        return "unknown", "결과 제목을 찾지 못했습니다."

    try:
        message_el = driver.find_element(
            By.XPATH,
            "//p[contains(@class, 'text-red-500') or contains(@class, 'text-green-500')]",
        )
        message_text = message_el.text.strip()
    except Exception:
        message_text = ""

    status = "success" if "완료" in title_text else "fail" if "실패" in title_text else "unknown"
    return status, f"{title_text} / {message_text}"


def save_result_to_excel(
    path: str, row: PaymentRow, status: str, message: str
) -> None:
    """결제 결과를 별도 엑셀 파일(RESULT_EXCEL_PATH)에 누적 저장.

    기존 버전에서 생성된 엑셀은 card_type 열이 없을 수 있으므로,
    필요하면 결과 시트의 헤더를 최신 HEADERS 구조로 자동 마이그레이션한다.
    """

    def _ensure_latest_header(ws) -> None:
        """기존 결과 시트의 헤더를 최신 HEADERS 구조로 맞춘다."""
        header_row = [cell.value for cell in next(ws.iter_rows(min_row=1, max_row=1))]
        if not any(header_row):
            # 헤더가 비어 있으면 새로 작성
            ws.append(HEADERS + ["result_status", "result_message"])
            return

        # 이전 버전: card_type 이 없던 경우 (열 개수로 판단)
        old_headers = [h for h in HEADERS if h != "card_type"]
        if header_row[: len(old_headers)] == old_headers and len(header_row) == len(
            old_headers
        ) + 2:
            # 4번째 열에 card_type 열 삽입 (기존 데이터는 기본값 없음)
            ws.insert_cols(4)
            ws.cell(row=1, column=4, value="card_type")

    path_obj = Path(path)
    if path_obj.exists():
        wb = load_workbook(path)
        ws = wb.active
    else:
        wb = Workbook()
        ws = wb.active
        ws.title = "results"
        ws.append(HEADERS + ["result_status", "result_message"])

    # 기존 파일인 경우 헤더를 최신 구조로 보정
    _ensure_latest_header(ws)

    data = [getattr(row, key) for key in HEADERS]
    ws.append(data + [status, message])

    try:
        wb.save(path)
    except PermissionError:
        # 파일이 엑셀 등에서 열려 있어서 저장이 안 되는 경우
        print("kvan_results.xlsx 저장에 실패했습니다. 엑셀 파일을 닫은 뒤 다시 실행해 주세요.")


def save_result_to_json(path: str, status: str, message: str) -> None:
    """web_form.py 에서 읽어갈 수 있도록 마지막 결제 결과를 JSON 으로도 저장."""
    payload = {
        "status": status,
        "message": message,
    }
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except OSError:
        # 파일 시스템 오류는 치명적이지 않으므로 조용히 무시
        pass


def append_transaction_to_hq(
    row,
    status: str,
    message: str,
    session_id: str,
) -> None:
    """결제 결과를 본사 어드민용 거래 리스트에 추가."""
    try:
        now = datetime.utcnow()
        tx_id = datetime.utcnow().strftime("TX%Y%m%d%H%M%S%f")

        # 세션 ID 로부터 실제 대행사 ID 를 추적 (admin_state.json 참조)
        agency_id = ""
        if session_id:
            admin_path = DATA_DIR / "admin_state.json"
            if admin_path.exists():
                try:
                    with admin_path.open("r", encoding="utf-8") as f:
                        admin_state = json.load(f)
                except Exception:
                    admin_state = {}
                sessions = admin_state.get("sessions") or []
                history = admin_state.get("history") or []
                for s in sessions:
                    if str(s.get("id")) == str(session_id) and s.get("agency_id"):
                        agency_id = s["agency_id"]
                        break
                if not agency_id:
                    for h in history:
                        if str(h.get("id")) == str(session_id) and h.get("agency_id"):
                            agency_id = h["agency_id"]
                            break

        # 금액은 정수 변환 시도 (실패 시 0)
        try:
            amount_int = int(getattr(row, "amount", 0) or 0)
        except (ValueError, TypeError):
            amount_int = 0

        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO transactions
                (id, created_at, agency_id, amount, customer_name, phone_number,
                 card_type, resident_front, status, message, settlement_status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    tx_id,
                    now,
                    agency_id or "",
                    amount_int,
                    getattr(row, "customer_name", ""),
                    getattr(row, "phone_number", ""),
                    getattr(row, "card_type", ""),
                    getattr(row, "resident_front", ""),
                    status,
                    message,
                    "미정산",
                ),
            )
        conn.commit()
        conn.close()
    except Exception:
        # HQ 집계 실패는 결제 자체에는 영향을 주지 않으므로 조용히 무시
        pass


def main() -> None:
    # 명령행 인자로 세션 ID 를 받을 수 있게 함:
    # python auto_kvan.py            -> 기본 단일 모드 (current_order.json 사용)
    # python auto_kvan.py SESSIONID  -> 세션별 orders/SESSIONID.json 사용
    import sys

    session_id = sys.argv[1].strip() if len(sys.argv) > 1 else ""

    if session_id:
        SESSION_ORDER_DIR.mkdir(parents=True, exist_ok=True)
        SESSION_RESULT_DIR.mkdir(parents=True, exist_ok=True)
        order_path = SESSION_ORDER_DIR / f"{session_id}.json"
        result_json_path = SESSION_RESULT_DIR / f"{session_id}.json"
    else:
        order_path = Path(ORDER_JSON_PATH)
        result_json_path = Path(RESULT_JSON_PATH)

    print("JSON 주문 데이터를 읽습니다...")
    try:
        row = load_order_from_json(str(order_path))
    except FileNotFoundError as e:
        print(e)
        return
    except ValueError as e:
        print(f"입력 데이터 오류: {e}")
        return

    driver = create_driver()
    try:
        print("K-VAN 가맹점 페이지에 로그인 중...")
        sign_in(driver, row)
        print("로그인 및 대시보드 정보 수집 완료. 결제링크 관리 페이지로 이동합니다...")
        _go_to_payment_link_page(driver)

        print("결제링크 관리 화면에서 '+ 생성' 버튼을 눌러 생성 페이지로 이동합니다...")
        moved = _go_to_create_link_page(driver)
        if not moved:
            print("[ERROR] '+ 생성' 버튼 클릭 후 생성 페이지로 이동하지 못했습니다. 폼 작성 단계는 건너뜁니다.")
            status = "error"
            msg = "결제링크 생성 페이지 진입 실패(생성 버튼 미동작)."
            save_result_to_excel(RESULT_EXCEL_PATH, row, status, msg)
            save_result_to_json(str(result_json_path), status, msg)
            append_transaction_to_hq(row, status, msg, session_id=session_id)
            return

        print("결제링크 생성 페이지에서 폼을 채우고 링크를 생성합니다...")
        link_url = _fill_payment_link_form_and_get_url(driver, row, session_id)
        if link_url:
            print(f"생성된 결제 링크: {link_url}")
        else:
            print("결제 링크 생성에 실패했거나 링크를 찾지 못했습니다.")

        # (선택) 필요 시 기존 대면결제 플로우 유지/활용
        try:
            fill_face_to_face_form(driver, row)
            status, msg = confirm_popup_and_get_result(driver)
            print(f"결제 결과: {status} - {msg}")
        except Exception as e:  # noqa: BLE001
            status = "error"
            msg = str(e)
            print(f"결제 처리 중 오류 발생: {e}")

        save_result_to_excel(RESULT_EXCEL_PATH, row, status, msg)
        save_result_to_json(str(result_json_path), status, msg)
        append_transaction_to_hq(row, status, msg, session_id=session_id)

        # 세션 모드인 경우, 어드민 상태에 세션 결과를 반영하고 세션을 히스토리로 이동
        if session_id:
            admin_path = DATA_DIR / "admin_state.json"
            if admin_path.exists():
                try:
                    with open(admin_path, "r", encoding="utf-8") as f:
                        admin_state = json.load(f)
                except Exception:
                    admin_state = {}
                sessions = admin_state.get("sessions") or []
                history = admin_state.get("history") or []

                new_sessions: list = []
                found = False
                for s in sessions:
                    if str(s.get("id")) == str(session_id):
                        found = True
                        human_status = (
                            "결제완료"
                            if status == "success"
                            else "결제실패"
                            if status in ("fail", "error")
                            else "알수없음"
                        )
                        entry = {
                            "id": session_id,
                            "amount": str(row.amount),
                            "installment": row.installment_months,
                            "status": human_status,
                            "created_at": s.get("created_at")
                            or datetime.utcnow().isoformat(),
                            "finished_at": datetime.utcnow().isoformat(),
                            "result_message": msg,
                            "customer_name": row.customer_name,
                            "phone_number": row.phone_number,
                            "product_name": row.product_name,
                            "settled": "정산전",
                        }
                        history.append(entry)
                    else:
                        new_sessions.append(s)

                if found:
                    admin_state["sessions"] = new_sessions
                    admin_state["history"] = history
                    try:
                        with open(admin_path, "w", encoding="utf-8") as f:
                            json.dump(admin_state, f, ensure_ascii=False, indent=2)
                    except OSError:
                        pass
        print(f"결과가 {RESULT_EXCEL_PATH} 파일에 기록되었습니다.")
        if not _is_server_env():
            input("브라우저를 확인한 뒤, 종료하려면 Enter 키를 누르세요...")
    finally:
        driver.quit()


if __name__ == "__main__":
    main()


