import os
import time
import json
from dataclasses import dataclass
from pathlib import Path
from typing import List
from datetime import datetime, timedelta
import random
import re

from openpyxl import load_workbook, Workbook
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
import pymysql

# 웹 어드민에서 볼 수 있는 간단한 로그 파일 (HQ 어드민에서 tail 형태로 노출)
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("SISA_DATA_DIR") or (BASE_DIR / "data"))
ADMIN_LOG_PATH = DATA_DIR / "hq_logs.log"


def _append_admin_log(source: str, message: str) -> None:
    try:
        ADMIN_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(ADMIN_LOG_PATH, "a", encoding="utf-8") as f:
            ts = datetime.utcnow().isoformat()
            f.write(f"{ts} [{source}] {message}\n")
    except Exception:
        pass

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

def _has_payment_links_quick(driver: webdriver.Chrome, retries: int = 5, delay: float = 1.0) -> bool:
    """
    결제링크 관리 화면에 실제 결제 링크 카드가 존재하는지 가볍게 확인한다.

    - '생성된 결제 링크가 없습니다' 문구가 보이면 즉시 False.
    - '거래 내역' 아이콘/버튼이 보이면 True.
    - 위 둘 다 아닌 경우, 짧게 여러 번 재시도 후 결과를 반환한다.
    """
    for attempt in range(retries):
        try:
            # 1) "생성된 결제 링크가 없습니다" 안내 문구가 있으면 링크 없음
            empty_msgs = driver.find_elements(
                By.XPATH,
                "//*[contains(normalize-space(.),'생성된 결제 링크가 없습니다')]",
            )
            if empty_msgs:
                print(f"[EMPTY_CHECK] 결제링크 없음 문구 감지 (attempt={attempt})")
                return False

            # 2) '거래 내역' 버튼/아이콘이 하나라도 있으면 링크가 있다고 판단
            icons = driver.find_elements(
                By.XPATH,
                "//button[@title='거래 내역']"
                " | //button[contains(normalize-space(.),'거래 내역')]"
                " | //button[contains(normalize-space(.),'거래내역')]"
                " | //button[.//svg[contains(@class,'lucide-receipt')]]",
            )
            if icons:
                print(f"[EMPTY_CHECK] 거래 내역 아이콘 감지 → 링크 존재 (attempt={attempt}, count={len(icons)})")
                return True
        except Exception as e:
            print(f"[EMPTY_CHECK] 링크 존재 여부 확인 중 예외 (attempt={attempt}): {e}")

        time.sleep(delay)

    print("[EMPTY_CHECK] 여러 번 확인했으나 링크를 찾지 못했습니다 (빈 화면으로 간주).")
    return False


def _go_to_payment_link(driver: webdriver.Chrome, max_attempts: int = 12) -> bool:
    """
    /payment-link 화면으로 안정적으로 진입하기 위한 헬퍼.

    - 단순 driver.get 으로 /dashboard 로 리다이렉트되는 경우가 있어,
      여러 번 재시도 + 대시보드 사이드 메뉴 클릭까지 시도한다.
    - 성공 시 True, 끝까지 실패 시 False 반환.
    """
    url_target = "https://store.k-van.app/payment-link"

    for attempt in range(max_attempts):
        cur = driver.current_url or ""
        if "payment-link" in cur:
            print(f"[NAV] 이미 /payment-link 에 위치 (attempt={attempt}, url={cur})")
            return True

        print(f"[NAV] /payment-link 진입 시도 (attempt={attempt}, current_url={cur})")
        try:
            driver.get(url_target)
        except Exception as e:
            print(f"[NAV] driver.get({url_target}) 중 예외: {e}")

        # URL 이 곧바로 바뀌는지 3초 정도만 기다린다.
        try:
            WebDriverWait(driver, 3).until(EC.url_contains("payment-link"))
            print(f"[NAV] URL 기반 /payment-link 진입 성공 (attempt={attempt}, url={driver.current_url})")
            return True
        except Exception:
            # 여전히 대시보드 등이라면, 메뉴 클릭 방식도 시도해 본다.
            pass

        try:
            # 사이드 메뉴/내비게이션에서 '결제링크' 관련 항목을 찾아 클릭 시도
            nav_btn = None
            candidates = driver.find_elements(
                By.XPATH,
                "//a[contains(@href,'payment-link')]"
                " | //button[contains(@href,'payment-link')]"
                " | //a[contains(normalize-space(.),'결제링크')]"
                " | //a[contains(normalize-space(.),'결제 링크')]"
                " | //button[contains(normalize-space(.),'결제링크')]"
                " | //button[contains(normalize-space(.),'결제 링크')]",
            )
            if candidates:
                nav_btn = candidates[0]

            if nav_btn:
                driver.execute_script(
                    "arguments[0].scrollIntoView({behavior:'instant',block:'center'});",
                    nav_btn,
                )
                time.sleep(0.1)
                driver.execute_script("arguments[0].click();", nav_btn)
                try:
                    WebDriverWait(driver, 3).until(EC.url_contains("payment-link"))
                    print(f"[NAV] 메뉴 클릭으로 /payment-link 진입 성공 (attempt={attempt}, url={driver.current_url})")
                    return True
                except Exception:
                    print("[NAV] 메뉴 클릭 후에도 /payment-link 로 전환되지 않음, 재시도 예정.")
        except Exception as e_nav:
            print(f"[NAV] 메뉴 기반 /payment-link 진입 시도 중 예외: {e_nav}")

        time.sleep(0.5)

    print("[NAV][ERROR] 여러 차례 시도했으나 /payment-link 로 진입하지 못했습니다.")
    return False

# 코드와 데이터 경로 분리: SISA_DATA_DIR (없으면 ./data)
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("SISA_DATA_DIR") or (BASE_DIR / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

WAKEUP_FLAG_PATH = DATA_DIR / "crawler_wakeup.flag"

# 로컬 테스트 모드: DB 에는 아무 것도 쓰지 않고, 크롤링/매크로 동작만 확인할 때 사용
# - SISA_LOCAL_TEST 가 명시되면 그 값을 따르고
# - 없으면 "서버 환경이 아니면" 기본적으로 LOCAL_TEST=True 로 동작하게 만든다.
_local_flag = os.environ.get("SISA_LOCAL_TEST")
if _local_flag is None:
    LOCAL_TEST = not _is_server_env()
else:
    LOCAL_TEST = _local_flag.strip().lower() in ("1", "true", "yes", "y")


def signal_crawler_wakeup() -> None:
    """
    K-VAN 크롤러(kvan_crawler.py)에 "즉시 다시 크롤링해 달라"는 신호를 남긴다.

    - DATA_DIR/crawler_wakeup.flag 파일에 타임스탬프를 기록하는 방식으로 구현.
    - 크롤러는 대기(sleep) 중 이 파일을 감지하면 대기를 즉시 종료하고 다음 사이클을 시작한다.
    """
    try:
        WAKEUP_FLAG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(WAKEUP_FLAG_PATH, "w", encoding="utf-8") as f:
            f.write(time.strftime("%Y-%m-%d %H:%M:%S"))
        print(f"[WAKEUP] 크롤러 깨우기 플래그 생성: {WAKEUP_FLAG_PATH}")
    except Exception as e:
        print(f"[WAKEUP][WARN] 크롤러 깨우기 플래그 생성 실패: {e}")


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


def _has_active_sessions(window_minutes: int = 10) -> bool:
    """
    admin_state.json 기준으로 '결제중' 세션이 있거나,
    최근 window_minutes 분 이내에 생성된 세션이 있으면 True.

    - 링크 생성 매크로(auto_kvan)가 새 세션을 만들면 크롤러가 4~7초 주기로 동작하도록
      크롤러에서 이 함수를 사용한다.
    """
    try:
        if not ADMIN_STATE_PATH.exists():
            return False
        with open(ADMIN_STATE_PATH, "r", encoding="utf-8") as f:
            st = json.load(f)
        sessions = st.get("sessions") or []
        history = st.get("history") or []

        cutoff = datetime.utcnow() - timedelta(minutes=window_minutes)

        # 1) 진행 중 세션
        for s in sessions:
            status = str(s.get("status") or "결제중")
            if status == "결제중":
                return True

        # 2) 최근에 생성된 세션 (예: 매크로가 방금 만든 세션)
        for s in sessions:
            ts = s.get("created_at")
            if not ts:
                continue
            try:
                dt = datetime.fromisoformat(ts)
            except Exception:
                continue
            if dt >= cutoff:
                return True

        # history 도 참고하고 싶으면 여기서 추가 검사 가능
        for h in history:
            ts = h.get("created_at")
            if not ts:
                continue
            try:
                dt = datetime.fromisoformat(ts)
            except Exception:
                continue
            if dt >= cutoff and h.get("status") == "결제중":
                return True

        return False
    except Exception as e:
        print(f"[WARN] _has_active_sessions 검사 실패: {e}")
        return False


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
    # 이 함수는 최대한 빨리(한 번의 시도 안에서) 3분 트리거를 열고,
    # 5분 또는 60분 옵션을 클릭 시도한 다음, 바로 다음 단계(링크 생성)로 넘어가기 위한 것이다.
    # 사용자가 5분/60분 모두 허용한다고 하였으므로, 여기서는 "최대한 클릭 시도"만 하고
    # 성공 여부에 관계 없이 True 를 반환해 흐름을 빠르게 진행시킨다.

    # 0) 현재 값 로깅 (디버깅용)
    try:
        trigger = driver.find_element(By.ID, "session-ttl")
        value_span = trigger.find_element(By.CSS_SELECTOR, "span[data-slot='select-value']")
        before = (value_span.text or "").strip()
        print(f"[DEBUG] 세션유효시간 초기 값(before)='{before}'")
        if before.startswith("5분") or before.startswith("60분"):
            print(f"[INFO] 세션 유효시간이 이미 '{before}' 로 설정되어 있습니다.")
            return True
    except Exception as e:
        print(f"[DEBUG] 세션유효시간 초기값 읽기 실패: {e}")

    # 1) 트리거 버튼 클릭 (3분 콤보박스 열기)
    try:
        trigger = driver.find_element(By.ID, "session-ttl")
        driver.execute_script("arguments[0].click();", trigger)
        time.sleep(0.15)
    except Exception as e:
        print(f"[DEBUG] 세션유효시간 트리거 클릭 실패: {e}")

    # 2) 드롭다운 안에서 '5분 (일반 결제)' 또는 '5분' 텍스트 옵션 클릭 시도
    try:
        candidates = driver.find_elements(
            By.XPATH,
            "//*[contains(normalize-space(.),'5분 (일반 결제)') or contains(normalize-space(.),'5분')]",
        )
        visible_opts = [el for el in candidates if el.is_displayed()]
        print(f"[DEBUG] 5분 옵션 후보 개수={len(candidates)}, 표시되는 개수={len(visible_opts)}")
        if visible_opts:
            driver.execute_script("arguments[0].click();", visible_opts[0])
            time.sleep(0.2)
    except Exception as e:
        print(f"[DEBUG] 5분 옵션 텍스트 기반 클릭 실패: {e}")

    # 3) 여전히 안 보인다면, 드롭다운 내에서 다시 '3분' 위치를 기준으로 스크롤 후 5분 재시도
    try:
        three_opts = driver.find_elements(
            By.XPATH,
            "//*[contains(normalize-space(.),'3분 (빠른 결제)') or contains(normalize-space(.),'3분')]",
        )
        three_visible = [el for el in three_opts if el.is_displayed()]
        print(f"[DEBUG] 3분 옵션 후보 개수={len(three_opts)}, 표시되는 개수={len(three_visible)}")
        if three_visible:
            three_el = three_visible[0]
            driver.execute_script(
                "arguments[0].scrollIntoView({behavior:'instant', block:'start'});", three_el
            )
            time.sleep(0.1)
            candidates2 = driver.find_elements(
                By.XPATH,
                "//*[contains(normalize-space(.),'5분 (일반 결제)') or contains(normalize-space(.),'5분')]",
            )
            visible2 = [el for el in candidates2 if el.is_displayed()]
            print(f"[DEBUG] 5분 옵션(재검색) 후보 개수={len(candidates2)}, 표시되는 개수={len(visible2)}")
            if visible2:
                driver.execute_script("arguments[0].click();", visible2[0])
                time.sleep(0.2)
    except Exception as e:
        print(f"[DEBUG] 3분 기준 스크롤 후 5분 클릭 시도 실패: {e}")

    # 4) 그래도 안 되면, 3분 클릭 위치에서 위로 30px 좌표 클릭 폴백 (60분 선택 가능)
    try:
        trigger = driver.find_element(By.ID, "session-ttl")
        rect = driver.execute_script(
            """
const el = arguments[0];
const r = el.getBoundingClientRect();
return { x: (r.left + r.right) / 2, y: (r.top + r.bottom) / 2, h: r.bottom - r.top };
""",
            trigger,
        )
        if rect and "x" in rect and "y" in rect:
            from selenium.webdriver.common.action_chains import ActionChains

            actions = ActionChains(driver)
            try:
                actions.move_by_offset(
                    -actions.w3c_actions.pointer_action.x,
                    -actions.w3c_actions.pointer_action.y,
                )
            except Exception:
                pass
            target_x = rect["x"]
            target_y = rect["y"] - 30  # 요청하신 대로 위로 30px 이동
            print(f"[DEBUG] 좌표 기반 5분/60분 클릭 폴백 시도: x={target_x}, y={target_y}")
            actions.move_by_offset(target_x, target_y).click().perform()
            time.sleep(0.2)
    except Exception as e:
        print(f"[DEBUG] 좌표 기반 5분/60분 클릭 폴백 실패: {e}")

    # 5) 최종 값 로그만 남기고, 다음 단계(링크 생성)로 진행
    try:
        trigger = driver.find_element(By.ID, "session-ttl")
        value_span = trigger.find_element(By.CSS_SELECTOR, "span[data-slot='select-value']")
        after = (value_span.text or "").strip()
        print(f"[DEBUG] 세션유효시간 최종 값(after)='{after}'")
    except Exception as e:
        print(f"[DEBUG] 세션유효시간 최종 값 확인 실패: {e}")

    # 성공 여부와 상관 없이, 시간을 더 쓰지 않고 바로 다음 단계로 진행한다.
    print("[INFO] 세션유효시간 변경 시도를 마쳤습니다. 현재 값에 상관없이 링크 생성 단계로 이동합니다.")
    return True


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
        raw["login_id"] = "m3313"

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
    """로그인 후 대시보드 화면에서 매출 요약 정보를 크롤링하여 DB에 저장.

    로컬 테스트 모드(LOCAL_TEST=True)에서는 DB 에 쓰지 않고 바로 리턴한다.
    """
    if LOCAL_TEST:
        print("[LOCAL_TEST] 대시보드 크롤링/DB 저장을 건너뜁니다.")
        return
    try:
        # 대시보드가 렌더링될 시간을 아주 짧게만 준다
        time.sleep(0.5)
        # 너무 오래 기다리지 않도록 짧은 wait 사용
        wait = WebDriverWait(driver, 3)

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


def _ensure_kvan_transactions_table() -> None:
    """kvan_transactions 테이블이 없으면 생성."""
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS kvan_transactions (
                  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                  captured_at DATETIME NOT NULL,
                  merchant_name VARCHAR(255) DEFAULT '',
                  pg_name VARCHAR(100) DEFAULT '',
                  mid VARCHAR(100) DEFAULT '',
                  fee_rate VARCHAR(50) DEFAULT '',
                  tx_type VARCHAR(50) DEFAULT '',
                  amount BIGINT DEFAULT 0,
                  cancel_amount BIGINT DEFAULT 0,
                  payable_amount BIGINT DEFAULT 0,
                  card_company VARCHAR(100) DEFAULT '',
                  card_number VARCHAR(64) DEFAULT '',
                  installment VARCHAR(50) DEFAULT '',
                  approval_no VARCHAR(100) DEFAULT '',
                  registered_at VARCHAR(50) DEFAULT '',
                  raw_text TEXT
                ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
                """
            )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[WARN] kvan_transactions 테이블 생성 실패: {e}")


def _scrape_transactions_and_store(driver: webdriver.Chrome) -> None:
    """
    K-VAN 결제/취소 거래내역 페이지(/transactions)의 테이블을 크롤링하여
    kvan_transactions 테이블에 저장.
    """
    if LOCAL_TEST:
        print("[LOCAL_TEST] /transactions 크롤링/DB 저장을 건너뜁니다.")
        # 페이지 이동과 화면 구조만 빠르게 확인 (항상 실제 새로고침)
        if "transactions" in driver.current_url:
            driver.refresh()
        else:
            driver.get("https://store.k-van.app/transactions")
        return

    try:
        _ensure_kvan_transactions_table()
        # 첫 방문 또는 재방문 시 항상 최신 데이터를 보도록 새로고침/이동
        if "transactions" in driver.current_url:
            driver.refresh()
        else:
            driver.get("https://store.k-van.app/transactions")
        wait = WebDriverWait(driver, 10)

        # 실제 테이블이 렌더링될 때까지 대기 (tbody > tr)
        wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//table//tbody//tr")
            )
        )

        # 헤더 텍스트를 기준으로 컬럼 인덱스 매핑
        header_cells = driver.find_elements(By.XPATH, "//table//thead//tr[1]//th")
        headers = [h.text.strip() for h in header_cells]

        def idx(sub: str) -> int:
            for i, h in enumerate(headers):
                if sub in h:
                    return i
            return -1

        idx_merchant = idx("가맹점명")
        idx_pg = idx("PG사")
        idx_mid = idx("MID")
        idx_fee = idx("수수료율")
        idx_type = idx("결제 유형")
        idx_amt = idx("결제 금액")
        idx_cancel = idx("취소 금액")
        idx_payable = idx("지급예정금액")
        idx_cardco = idx("카드사")
        idx_cardno = idx("카드번호")
        idx_inst = idx("할부")
        idx_approval = idx("승인번호")
        idx_reg = idx("등록일")

        rows = driver.find_elements(By.XPATH, "//table//tbody//tr")
        if not rows:
            print("[INFO] /transactions 테이블에 표시된 거래 내역이 없습니다.")
            return

        conn = get_db()
        inserted = 0
        with conn.cursor() as cur:
            for tr in rows:
                try:
                    cells = tr.find_elements(By.XPATH, ".//td")
                    texts = [c.text.strip() for c in cells]
                    if not any(texts):
                        continue

                    def get(i: int) -> str:
                        return texts[i] if 0 <= i < len(texts) else ""

                    merchant = get(idx_merchant)
                    pg_name = get(idx_pg)
                    mid = get(idx_mid)
                    fee_rate = get(idx_fee)
                    tx_type = get(idx_type)
                    amount = _parse_amount(get(idx_amt))
                    cancel_amount = _parse_amount(get(idx_cancel))
                    payable_amount = _parse_amount(get(idx_payable))
                    card_company = get(idx_cardco)
                    card_number = get(idx_cardno)
                    installment = get(idx_inst)
                    approval_no = get(idx_approval)
                    registered_at = get(idx_reg)
                    raw_text = " | ".join(texts)

                    cur.execute(
                        """
                        INSERT INTO kvan_transactions (
                          captured_at,
                          merchant_name,
                          pg_name,
                          mid,
                          fee_rate,
                          tx_type,
                          amount,
                          cancel_amount,
                          payable_amount,
                          card_company,
                          card_number,
                          installment,
                          approval_no,
                          registered_at,
                          raw_text
                        )
                        VALUES (NOW(), %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (
                            merchant,
                            pg_name,
                            mid,
                            fee_rate,
                            tx_type,
                            amount,
                            cancel_amount,
                            payable_amount,
                            card_company,
                            card_number,
                            installment,
                            approval_no,
                            registered_at,
                            raw_text,
                        ),
                    )
                    inserted += 1
                except Exception as e_row:
                    print(f"[WARN] 거래내역 한 행 파싱/저장 중 오류: {e_row}")
                    continue

        conn.commit()
        conn.close()
        print(f"[INFO] /transactions 에서 {inserted}건의 거래내역을 kvan_transactions 에 저장했습니다.")
    except Exception as e:
        print(f"[WARN] 거래내역(/transactions) 크롤링/DB 저장 실패: {e}")


def _sync_kvan_to_transactions() -> bool:
    """
    kvan_transactions 에 쌓인 K-VAN 거래내역을
    내부 transactions 테이블과 최대한 매핑하고,
    필요하면 새 거래 레코드를 생성하여 본사/대행사 정산 데이터와 연결한다.

    우선순위:
    1) approval_no(승인번호) 기준으로 기존 transactions 찾기
    2) 없으면 amount + 날짜(+ agency_id) 기준으로 가장 최근 거래 매핑
    3) 그래도 없으면 신규 transactions 레코드 INSERT

    agency_id 매핑:
    - agencies.kvan_mid 와 kvan_transactions.mid 를 비교해 일치하는 대행사를 찾는다.
    """
    if LOCAL_TEST:
        print("[LOCAL_TEST] kvan_transactions → transactions 매핑/생성을 건너뜁니다.")
        return False

    updated = 0
    inserted = 0
    try:
        conn = get_db()
        with conn.cursor() as cur:
            # 0) MID -> agency_id 매핑 테이블 생성
            agency_mid_map: dict[str, str] = {}
            try:
                cur.execute("SELECT id, kvan_mid FROM agencies")
                for ag in cur.fetchall():
                    m = (ag.get("kvan_mid") or "").strip()
                    if m:
                        agency_mid_map[m] = ag["id"]
            except Exception as e_ag:
                print(f"[WARN] agencies.kvan_mid 조회 중 오류(계속 진행): {e_ag}")

            # 1) 최근 K-VAN 거래 200건만 사용
            cur.execute(
                """
                SELECT id, captured_at, merchant_name, mid, tx_type,
                       amount, approval_no, registered_at
                FROM kvan_transactions
                ORDER BY captured_at DESC
                LIMIT 200
                """
            )
            krows = cur.fetchall()

            for kr in krows:
                amt = kr.get("amount") or 0
                approval = (kr.get("approval_no") or "").strip()
                mid = (kr.get("mid") or "").strip()
                tx_type = (kr.get("tx_type") or "").strip()
                reg = (kr.get("registered_at") or "").strip()
                if not amt or not approval:
                    # 금액/승인번호가 없으면 내부 거래와 매핑하기 어려우므로 건너뜀
                    continue

                # 등록일에서 날짜 부분만 추출 (예: '2026-03-12 10:20:30' -> '2026-03-12')
                reg_date = reg.split(" ")[0] if reg else ""

                # MID -> agency_id 매핑
                agency_id: str | None = None
                if mid and mid in agency_mid_map:
                    agency_id = agency_mid_map[mid]

                # K-VAN 결제유형 기준으로 내부 status 유추
                tx_status = "other"
                tx_type_text = tx_type or ""
                if "승인" in tx_type_text:
                    tx_status = "success"
                elif "취소" in tx_type_text or "실패" in tx_type_text or "오류" in tx_type_text:
                    tx_status = "fail"

                # 1단계: 승인번호로 기존 거래 찾기
                cur.execute(
                    """
                    SELECT id, agency_id
                    FROM transactions
                    WHERE kvan_approval_no = %s
                    LIMIT 1
                    """,
                    (approval,),
                )
                tx = cur.fetchone()
                if tx:
                    tx_id = tx["id"]
                    # agency_id 가 비어 있고, 이번 K-VAN 에서 대행사를 알 수 있다면 채워 넣는다.
                    cur.execute(
                        """
                        UPDATE transactions
                        SET amount = COALESCE(amount, %s),
                            status = %s,
                            kvan_mid = %s,
                            kvan_approval_no = %s,
                            kvan_tx_type = %s,
                            kvan_registered_at = %s,
                            agency_id = COALESCE(agency_id, %s)
                        WHERE id = %s
                        """,
                        (amt, tx_status, mid, approval, tx_type, reg, agency_id, tx_id),
                    )
                    updated += 1
                    continue

                # 2단계: 금액 + 날짜(+ agency_id) 기준으로 기존 거래 찾기
                params: list = [amt, reg_date, reg_date]
                sql = """
                    SELECT id, agency_id
                    FROM transactions
                    WHERE amount = %s
                      AND (%s = '' OR DATE(created_at) = %s)
                """
                if agency_id:
                    sql += " AND agency_id = %s"
                    params.append(agency_id)
                sql += """
                    ORDER BY created_at DESC
                    LIMIT 1
                """
                cur.execute(sql, tuple(params))
                tx = cur.fetchone()
                if tx:
                    tx_id = tx["id"]
                    cur.execute(
                        """
                        UPDATE transactions
                        SET status = %s,
                            kvan_mid = %s,
                            kvan_approval_no = %s,
                            kvan_tx_type = %s,
                            kvan_registered_at = %s
                        WHERE id = %s
                        """,
                        (tx_status, mid, approval, tx_type, reg, tx_id),
                    )
                    updated += 1
                    continue

                # 3단계: 매칭되는 기존 거래가 없으면 새 transactions 레코드 생성
                new_tx_id = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")[-18:]
                message = f"K-VAN {tx_type or '거래'} 자동 연동 (MID={mid}, 승인번호={approval})"
                cur.execute(
                    """
                    INSERT INTO transactions (
                      id,
                      created_at,
                      agency_id,
                      amount,
                      customer_name,
                      phone_number,
                      card_type,
                      resident_front,
                      status,
                      message,
                      settlement_status,
                      settled_at,
                      kvan_mid,
                      kvan_approval_no,
                      kvan_tx_type,
                      kvan_registered_at
                    )
                    VALUES (
                      %s, NOW(), %s, %s,
                      '', '', '', '',
                      %s, %s,
                      '미정산', NULL,
                      %s, %s, %s, %s
                    )
                    """,
                    (
                        new_tx_id,
                        agency_id,
                        amt,
                        tx_status,
                        message,
                        mid,
                        approval,
                        tx_type,
                        reg,
                    ),
                )
                inserted += 1

        conn.commit()
        conn.close()
        if updated or inserted:
            print(
                f"[INFO] kvan_transactions 기반으로 내부 거래 매핑/생성 완료 "
                f"(updated={updated}, inserted={inserted})"
            )
    except Exception as e:
        print(f"[WARN] K-VAN ↔ 내부 transactions 매핑/생성 중 오류: {e}")
    return bool(updated or inserted)


def _ensure_kvan_links_table() -> None:
    """kvan_links 테이블이 없으면 생성."""
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS kvan_links (
                  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                  captured_at DATETIME NOT NULL,
                  title VARCHAR(255) DEFAULT '',
                  amount BIGINT DEFAULT 0,
                  ttl_label VARCHAR(100) DEFAULT '',
                  status VARCHAR(100) DEFAULT '',
                  kvan_link VARCHAR(512) DEFAULT '',
                  mid VARCHAR(100) DEFAULT '',
                  kvan_session_id VARCHAR(100) DEFAULT '',
                  raw_text TEXT
                ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
                """
            )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[WARN] kvan_links 테이블 생성 실패: {e}")


def _scrape_payment_links_and_store(driver: webdriver.Chrome) -> None:
    """
    K-VAN 결제링크 관리 페이지(/payment-link)의 리스트를 크롤링하여
    kvan_links 테이블에 저장.

    화면 구조는 React 카드/테이블 형태일 수 있으므로,
    - 링크 URL (https://store.k-van.app/...)
    - 인근 텍스트(상품명, 금액, 유효시간, 상태, MID, 세션ID 등)를 모두 raw_text 로 저장하고
    - 자주 쓰는 필드(제목/금액/유효시간/status/MID/세션ID)는 휴리스틱하게 추출한다.
    """
    if LOCAL_TEST:
        print("[LOCAL_TEST] /payment-link 크롤링/DB 저장을 건너뜁니다.")
        # 항상 실제 새로고침 또는 첫 진입 (진입 실패 시에도 DB 작업은 건너뛰고 종료)
        if not _go_to_payment_link(driver):
            raise RuntimeError("[NAV] LOCAL_TEST 모드에서 /payment-link 로 진입하지 못했습니다.")
        driver.refresh()
        return

    try:
        _ensure_kvan_links_table()
        # /payment-link 로 안정적으로 진입 후, 항상 새로고침
        if not _go_to_payment_link(driver):
            raise RuntimeError("[NAV] /payment-link 로 진입하지 못해 링크 리스트 크롤링을 중단합니다.")
        driver.refresh()
        wait = WebDriverWait(driver, 10)

        # 실제 카드/테이블이 렌더링될 때까지 대기:
        # 링크 텍스트나 input.value 에 'https://store.k-van.app' 가 포함된 요소가 나타날 때까지
        wait.until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//*[contains(text(),'https://store.k-van.app') "
                    "or contains(@value,'https://store.k-van.app')]",
                )
            )
        )

        # 각 링크 요소를 기준으로 상위 카드 컨테이너를 찾는다.
        link_elements = driver.find_elements(
            By.XPATH,
            "//*[contains(text(),'https://store.k-van.app') "
            "or contains(@value,'https://store.k-van.app')]",
        )
        if not link_elements:
            print("[INFO] /payment-link 에 표시된 결제링크가 없습니다.")
            return

        conn = get_db()
        inserted = 0
        with conn.cursor() as cur:
            for el in link_elements:
                try:
                    # 링크 문자열 추출
                    link_text = (el.text or "").strip()
                    if not link_text:
                        link_text = (el.get_attribute("value") or "").strip()
                    if not link_text:
                        link_text = (el.get_attribute("href") or "").strip()
                    if not link_text:
                        continue

                    # 카드/행 컨테이너: 가장 가까운 div[role='row'] 또는 카드형 div
                    container = el
                    for _ in range(5):
                        container = container.find_element(By.XPATH, "./parent::*")
                        cls = container.get_attribute("class") or ""
                        if "border" in cls or "rounded" in cls or "shadow" in cls or "row" in cls:
                            break

                    card_text = container.text.strip()

                    # 제목/상품명: 첫 줄 또는 '상품명' 이라는 단어가 포함된 줄
                    lines = [ln.strip() for ln in card_text.splitlines() if ln.strip()]
                    title = lines[0] if lines else ""
                    for ln in lines:
                        if "상품명" in ln:
                            title = ln
                            break

                    # 금액: '원' 이 포함된 숫자
                    amount = 0
                    for ln in lines:
                        if "원" in ln:
                            amt = _parse_amount(ln)
                            if amt:
                                amount = amt
                                break

                    # 유효시간/TTL: '분' 텍스트가 있는 줄 추출 (예: '60분 (긴 결제 플로우)')
                    ttl_label = ""
                    for ln in lines:
                        if "분" in ln and "유효" in ln or "세션" in ln:
                            ttl_label = ln
                            break

                    # 상태: '사용중', '만료', '취소' 등 단어가 포함된 줄 추출
                    status = ""
                    for ln in lines:
                        if any(k in ln for k in ("사용", "만료", "취소", "대기", "완료")):
                            status = ln
                            break

                    # MID / 세션ID: 'MID' 또는 '세션' 텍스트 기반
                    mid = ""
                    kvan_session_id = ""
                    for ln in lines:
                        if "MID" in ln.upper():
                            mid = ln
                        if "세션" in ln or "Session" in ln:
                            kvan_session_id = ln

                    cur.execute(
                        """
                        INSERT INTO kvan_links (
                          captured_at,
                          title,
                          amount,
                          ttl_label,
                          status,
                          kvan_link,
                          mid,
                          kvan_session_id,
                          raw_text
                        )
                        VALUES (NOW(), %s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (
                            title,
                            amount,
                            ttl_label,
                            status,
                            link_text,
                            mid,
                            kvan_session_id,
                            card_text,
                        ),
                    )
                    inserted += 1
                except Exception as e_row:
                    print(f"[WARN] 결제링크 카드 파싱/저장 중 오류: {e_row}")
                    continue

        conn.commit()
        conn.close()
        print(f"[INFO] /payment-link 에서 {inserted}건의 결제링크 정보를 kvan_links 에 저장했습니다.")
    except Exception as e:
        print(f"[WARN] 결제링크 관리(/payment-link) 크롤링/DB 저장 실패: {e}")


def _sync_popup_transaction_to_internal(
    session_id: str,
    amount: int,
    approval_no: str,
    card_number: str,
    registered_at: str,
    customer_name: str,
) -> None:
    """
    결제링크 관리 화면의 '거래 내역' 팝업에서 얻은 승인 정보를
    내부 transactions 테이블과 admin_state.json 에 반영한다.

    - session_id 를 기준으로 admin_state.json 에서 agency_id 를 찾는다.
    - kvan_approval_no 가 같은 transactions 가 있으면 업데이트,
      없으면 새 레코드를 INSERT 한다.
    """
    if LOCAL_TEST:
        print("[LOCAL_TEST] 팝업 기반 transactions 동기화를 건너뜁니다.")
        return

    approval_no = (approval_no or "").strip()
    if not approval_no or not amount:
        return

    # admin_state.json 에서 agency_id 찾기
    agency_id: str | None = None
    try:
        if ADMIN_STATE_PATH.exists():
            with open(ADMIN_STATE_PATH, "r", encoding="utf-8") as f:
                st = json.load(f)
            sessions = st.get("sessions") or []
            history = st.get("history") or []
            for s in list(sessions) + list(history):
                if str(s.get("id")) == str(session_id):
                    agency_id = (s.get("agency_id") or "").strip() or None
                    break
    except Exception as e:
        print(f"[WARN] popup 기반 agency_id 조회 실패: {e}")

    try:
        conn = get_db()
        with conn.cursor() as cur:
            # 1) 승인번호로 기존 거래 찾기
            cur.execute(
                """
                SELECT id FROM transactions
                WHERE kvan_approval_no = %s
                LIMIT 1
                """,
                (approval_no,),
            )
            row = cur.fetchone()
            if row:
                tx_id = row["id"]
                cur.execute(
                    """
                    UPDATE transactions
                    SET amount = COALESCE(amount, %s),
                        customer_name = COALESCE(customer_name, %s),
                        status = 'success',
                        kvan_registered_at = %s,
                        agency_id = COALESCE(agency_id, %s)
                    WHERE id = %s
                    """,
                    (amount, customer_name or "", registered_at, agency_id, tx_id),
                )
            else:
                # 2) 새 레코드 생성
                new_tx_id = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")[-18:]
                message = (
                    f"K-VAN 결제 승인 (세션ID={session_id}, 승인번호={approval_no}, 카드={card_number})"
                )
                cur.execute(
                    """
                    INSERT INTO transactions (
                      id,
                      created_at,
                      agency_id,
                      amount,
                      customer_name,
                      phone_number,
                      card_type,
                      resident_front,
                      status,
                      message,
                      settlement_status,
                      settled_at,
                      kvan_mid,
                      kvan_approval_no,
                      kvan_tx_type,
                      kvan_registered_at
                    )
                    VALUES (
                      %s, NOW(), %s, %s,
                      %s, '', '', '',
                      'success', %s,
                      '미정산', NULL,
                      '', %s, '결제 승인', %s
                    )
                    """,
                    (
                        new_tx_id,
                        agency_id,
                        amount,
                        customer_name or "",
                        message,
                        approval_no,
                        registered_at,
                    ),
                )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[WARN] popup 기반 transactions 동기화 실패: {e}")


def _close_dialog(dialog) -> None:
    """
    '거래 내역' 팝업을 안전하게 닫고, 오버레이까지 사라질 때까지 잠시 대기한다.

    - dialog 내부의 닫기 버튼(data-slot='dialog-close')를 우선 클릭
    - 실패하면 오버레이(div[data-slot='dialog-overlay'])를 클릭 시도
    - 마지막으로, 해당 dialog 자체가 DOM 에서 사라질 때까지 최대 2초 대기
    """
    try:
        driver = dialog.parent  # WebElement 가 생성된 WebDriver 인스턴스
        # 1차: X 버튼 클릭
        try:
            close_btn = dialog.find_element(
                By.XPATH, ".//button[@data-slot='dialog-close']"
            )
            driver.execute_script("arguments[0].click();", close_btn)
        except Exception:
            # 2차: 오버레이 클릭 (배경을 클릭해 닫히는 타입일 수 있음)
            try:
                overlay = driver.find_element(
                    By.XPATH,
                    "//div[@data-slot='dialog-overlay' and @data-state='open']",
                )
                driver.execute_script("arguments[0].click();", overlay)
            except Exception:
                pass

        # 3차: dialog/오버레이가 실제로 사라질 때까지 잠시 대기
        try:
            WebDriverWait(driver, 2).until_not(
                EC.presence_of_element_located(
                    (
                        By.XPATH,
                        "//div[@role='dialog' and .//h2[normalize-space()='거래 내역']]",
                    )
                )
            )
        except TimeoutException:
            # 완전히 사라지지 않아도, 이후 로직에서 다시 한 번 시도하게 둔다.
            pass
        time.sleep(0.05)
    except Exception:
        pass


def _click_trash_and_confirm(card, wait: WebDriverWait) -> bool:
    """
    카드 안 휴지통 버튼(title='삭제') 클릭 후,
    확인 팝업의 붉은 '삭제' 버튼을 클릭한다.
    """
    try:
        driver = wait._driver  # WebDriverWait 에 연결된 driver

        # 1) 카드 안의 휴지통 아이콘 클릭
        trash_btn = card.find_element(
            By.XPATH,
            ".//button[@title='삭제']"
            " | .//button[.//svg[contains(@class,'lucide-trash') or contains(@class,'lucide-trash-2')]]",
        )
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", trash_btn)
        time.sleep(0.05)
        driver.execute_script("arguments[0].click();", trash_btn)

        # 2) 휴지통 클릭 후 뜨는 경고 다이얼로그(빨간 '삭제' 버튼)가 나타날 때까지 대기
        try:
            alert = wait.until(
                EC.visibility_of_element_located(
                    (
                        By.XPATH,
                        "//div[@role='alertdialog']"
                        " | //div[@data-slot='alert-dialog-content']",
                    )
                )
            )
        except TimeoutException:
            print("[WARN] 휴지통 클릭 후 경고 다이얼로그를 찾지 못했습니다.")
            return False

        # 3) 다이얼로그 안의 붉은 '삭제' 버튼 클릭
        try:
            confirm_btn = alert.find_element(
                By.XPATH,
                ".//button[normalize-space()='삭제']",
            )
        except Exception:
            # fallback: 화면 전체에서라도 '삭제' 버튼을 찾는다
            confirm_btn = wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//button[normalize-space()='삭제']")
                )
            )

        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", confirm_btn)
        time.sleep(0.05)
        driver.execute_script("arguments[0].click();", confirm_btn)

        # 4) alert 다이얼로그/오버레이가 사라질 때까지 잠시 대기
        try:
            WebDriverWait(driver, 2).until_not(
                EC.presence_of_element_located(
                    (
                        By.XPATH,
                        "//div[@data-slot='alert-dialog-overlay' and @data-state='open']",
                    )
                )
            )
        except TimeoutException:
            pass

        time.sleep(0.2)
        return True
    except Exception as e:
        print(f"[WARN] 휴지통/삭제 버튼 처리 중 오류: {e}")
        return False


def _is_session_already_processed(session_id: str) -> bool:
    """
    admin_state.json.history 에서 이미 has_approval 또는 deleted 플래그가 있는 세션이면 True.
    """
    if not session_id:
        return False
    try:
        if not ADMIN_STATE_PATH.exists():
            return False
        with open(ADMIN_STATE_PATH, "r", encoding="utf-8") as f:
            st = json.load(f)
        history = st.get("history") or []
        for h in history:
            if str(h.get("id")) == str(session_id):
                if h.get("has_approval") or h.get("deleted"):
                    return True
        return False
    except Exception as e:
        print(f"[WARN] _is_session_already_processed 실패: {e}")
        return False


def _mark_session_checked(session_id: str, title: str, has_approval: bool) -> None:
    """
    admin_state.json.history 에 has_approval 플래그를 기록해
    다음 크롤링에서 중복 검사하지 않게 한다.
    """
    if not session_id:
        return
    try:
        st = {"sessions": [], "history": []}
        if ADMIN_STATE_PATH.exists():
            with open(ADMIN_STATE_PATH, "r", encoding="utf-8") as f:
                st = json.load(f)
        sessions = st.get("sessions") or []
        history = st.get("history") or []

        found = False
        for h in history:
            if str(h.get("id")) == str(session_id):
                h["has_approval"] = bool(has_approval)
                h["checked_title"] = title
                found = True
                break
        if not found:
            history.append(
                {
                    "id": session_id,
                    "has_approval": bool(has_approval),
                    "checked_title": title,
                }
            )
        st["sessions"] = sessions
        st["history"] = history
        with open(ADMIN_STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(st, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[WARN] _mark_session_checked 실패: {e}")


def _mark_session_deleted(session_id: str, title: str) -> None:
    _mark_session_checked(session_id, title, has_approval=False)
    try:
        if not ADMIN_STATE_PATH.exists():
            return
        with open(ADMIN_STATE_PATH, "r", encoding="utf-8") as f:
            st = json.load(f)
        sessions = st.get("sessions") or []
        history = st.get("history") or []
        for h in history:
            if str(h.get("id")) == str(session_id):
                h["deleted"] = True
        st["sessions"] = sessions
        st["history"] = history
        with open(ADMIN_STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(st, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[WARN] _mark_session_deleted 실패: {e}")


def _scan_payment_link_popups_and_sync(driver: webdriver.Chrome) -> bool:
    """
    결제링크 관리(/payment-link) 화면에서 각 카드의 '거래 내역' 버튼을 클릭해
    팝업의 '결제 승인' 정보를 읽고 내부 DB와 세션에 반영한다.

    반환값: 이번 사이클에서 새로운 승인/삭제/상태 변경이 있으면 True, 아니면 False.
    """
    changed = False
    try:
        t0 = _step_start("결제링크 관리 팝업 기반 동기화")
        wait = WebDriverWait(driver, 5)

        # 항상 결제링크 관리 URL 을 강제로 맞추고,
        # '권한 확인 중...' 스피너가 끝나고 실제 카드 리스트가 나올 때까지
        # 0.5초 간격으로 짧게 여러 번 시도한다 (최대 약 5초).
        if not _go_to_payment_link(driver):
            _step_end("결제링크 관리 팝업 기반 동기화", t0)
            raise RuntimeError("[NAV] /payment-link 로 진입하지 못해 팝업 기반 동기화를 중단합니다.")

        max_tries = 12  # 0.5초 * 12 ≈ 6초
        icons_found = False
        for attempt in range(max_tries):
            icons = driver.find_elements(
                By.XPATH,
                "//button[@title='거래 내역']"
                " | //button[contains(normalize-space(.),'거래 내역')]"
                " | //button[contains(normalize-space(.),'거래내역')]"
                " | //button[.//svg[contains(@class,'lucide-receipt')]]",
            )
            if icons:
                print(
                    f"[POPUP_DEBUG] '거래 내역' 아이콘 감지 (attempt={attempt}, count={len(icons)}, url={driver.current_url})"
                )
                icons_found = True
                break
            else:
                print(
                    f"[POPUP_DEBUG] 아이콘 없음 (attempt={attempt}, url={driver.current_url}) – 0.5초 후 재시도"
                )
            time.sleep(0.5)

        if not icons_found:
            print("[WARN] 결제링크 관리 화면에서 '거래 내역' 아이콘이 표시되지 않았습니다.")
            _step_end("결제링크 관리 팝업 기반 동기화", t0)
            return False

        # 카드 컨테이너 (각 카드 안에 '거래 내역' 버튼이 있는 것만)
        cards = driver.find_elements(
            By.XPATH,
            "//div[.//button[@title='거래 내역']"
            "      or .//button[contains(normalize-space(.),'거래 내역')]"
            "      or .//button[contains(normalize-space(.),'거래내역')]"
            "      or .//button[.//svg[contains(@class,'lucide-receipt')]]]",
        )
        if not cards:
            _step_end("결제링크 관리 팝업 기반 동기화", t0)
            return False

        # 모든 카드를 순차적으로 처리 (세션ID 기준으로 신규만)
        for idx, card in enumerate(cards, start=1):
            try:
                card_text = card.text or ""
                lines = [ln.strip() for ln in card_text.splitlines() if ln.strip()]
                print(f"[CARD_DEBUG] 원시 카드 텍스트 (index={idx}): {lines}")
                # 1단계: 상태 배지(span[data-slot='badge'])에 '만료' 가 있는지 최우선으로 확인
                is_expired = False
                try:
                    badge_spans = card.find_elements(
                        By.XPATH, ".//span[@data-slot='badge']"
                    )
                    badge_texts = [b.text.strip() for b in badge_spans if b.text.strip()]
                    if any("만료" in bt for bt in badge_texts):
                        is_expired = True
                        print(f"[TTL_DEBUG] 상태 배지에서 '만료' 감지 → is_expired=True (badges={badge_texts})")
                    else:
                        print(f"[TTL_DEBUG] 상태 배지들={badge_texts} → '만료' 없음")
                except Exception as e:
                    print(f"[TTL_DEBUG] 상태 배지 확인 중 오류: {e}")

                # 2단계: 배지에서 '만료'가 아니고, 유효시간 줄에 '분' 이 있으면 만료 아님으로 강제
                if not is_expired and any("분" in ln for ln in lines):
                    print("[TTL_DEBUG] 상태 배지에는 '만료' 없고, 유효시간 라인에 '분' 포함 → 만료 아님으로 간주")
                    is_expired = False

                session_id = ""
                product_title = ""
                for ln in lines:
                    if not product_title:
                        product_title = ln  # 첫 줄 정도를 제목으로 사용
                    # 1순위: "세션 ID:" 라벨이 있는 경우 (예전 UI)
                    if "세션 ID" in ln:
                        parts = ln.split("세션 ID:")
                        if len(parts) > 1:
                            session_id = parts[1].strip()
                            continue
                    # 2순위: 최근 UI처럼 KEY 로 시작하는 세션ID 가 단독으로 나오는 경우
                    if not session_id and "KEY20" in ln:
                        session_id = ln.strip()

                # 세션 ID 가 없는 행(헤더/틀 행 등)은 실제 결제링크 카드가 아니므로 건너뜀
                if not session_id:
                    print(f"[CARD_DEBUG] 세션ID 없음 → 헤더/비카드로 판단, 건너뜀 (index={idx}, title={product_title})")
                    continue

                print(f"[CARD_DEBUG] 인덱스={idx}, 세션ID={session_id}, 제목={product_title}, is_expired={is_expired}")
                if _is_session_already_processed(session_id):
                    print(f"[CARD_DEBUG] 이미 처리된 세션 → 건너뜀 (session_id={session_id})")
                    continue

                # 카드 안의 '거래 내역' 버튼 클릭
                try:
                    btn = card.find_element(
                        By.XPATH,
                        ".//button[@title='거래 내역']"
                        " | .//button[.//svg[contains(@class,'lucide-receipt')]]",
                    )
                except Exception:
                    continue

                # 거래 내역 버튼 클릭은 DOM 변동/레이아웃 지연 때문에 실패할 수 있으므로
                # 0.3초 간격으로 여러 번 재시도하면서, 한 번이라도 성공하면 다음 단계로 진행한다.
                click_ok = False
                for _ in range(10):
                    try:
                        driver.execute_script(
                            "arguments[0].scrollIntoView({behavior:'instant',block:'center'});",
                            btn,
                        )
                        time.sleep(0.05)
                        driver.execute_script("arguments[0].click();", btn)
                        click_ok = True
                        break
                    except Exception as e_click:
                        print(f"[CARD_DEBUG] 거래 내역 버튼 클릭 재시도: {e_click}")
                        time.sleep(0.3)

                if not click_ok:
                    print("[WARN] 거래 내역 버튼 클릭 실패(여러 차례 재시도 후) → 다음 카드로 진행")
                    continue

                # 팝업(dialog) 대기
                try:
                    dialog = wait.until(
                        EC.visibility_of_element_located(
                            (
                                By.XPATH,
                                "//div[@role='dialog' and .//h2[normalize-space()='거래 내역']]",
                            )
                        )
                    )
                except TimeoutException:
                    print("[WARN] '거래 내역' 팝업을 찾지 못했습니다.")
                    continue

                popup_text = dialog.text or ""
                # "거래 내역이 없습니다" / "거래 내역 없음" 등 문구 변화를 모두 허용
                has_no_history = (
                    "거래 내역이 없습니다" in popup_text
                    or "거래 내역 없음" in popup_text
                )

                if has_no_history:
                    _close_dialog(dialog)
                    if is_expired:
                        if _click_trash_and_confirm(card, wait):
                            print(f"[INFO] 만료 + 거래내역 없음 세션 삭제 시도: {session_id}")
                            _mark_session_deleted(session_id, product_title)
                            changed = True
                    else:
                        _mark_session_checked(session_id, product_title, has_approval=False)
                    continue

                # 거래 내역이 하나 이상 있는 경우: 첫 번째 행 기준으로 승인 정보 파싱
                try:
                    # 일부 레이아웃(모바일 카드형)에서는 table 구조가 없을 수 있으므로,
                    # 우선 테이블 행 존재 여부를 확인하고, 없으면 "구조화된 내역 없음" 으로 처리한다.
                    rows = dialog.find_elements(By.XPATH, ".//table//tbody//tr")
                    if not rows:
                        # 구조화된 테이블이 없으면, 승인 여부를 판단할 수 없으므로
                        # 삭제는 절대 하지 않고, 단순히 "확인됨(미승인/기타)" 상태로만 표시한다.
                        _mark_session_checked(session_id, product_title, has_approval=False)
                        continue

                    row = rows[0]
                    tx_type = row.find_element(
                        By.XPATH, ".//span[contains(@data-slot,'badge')]"
                    ).text.strip()
                    amount_text = row.find_element(
                        By.XPATH, ".//td[3]//span"
                    ).text.strip()
                    amt = _parse_amount(amount_text)
                    approval_no = row.find_element(
                        By.XPATH, ".//td[4]"
                    ).text.strip()
                    customer_name = row.find_element(
                        By.XPATH, ".//td[5]"
                    ).text.strip()
                    card_number = row.find_element(
                        By.XPATH, ".//td[6]//span"
                    ).text.strip()
                    registered_at = row.find_element(
                        By.XPATH, ".//td[7]"
                    ).text.strip()

                    # 팝업 헤더에서 세션 ID 다시 추출 시도
                    try:
                        desc_el = dialog.find_element(
                            By.XPATH, ".//p[contains(@data-slot,'dialog-description')]"
                        )
                        desc_text = desc_el.text or ""
                        m = re.search(r"세션 ID:\s*([A-Z0-9]+)", desc_text)
                        if m:
                            session_id = m.group(1)
                    except Exception:
                        pass

                    if "결제 승인" in tx_type and amt:
                        _sync_popup_transaction_to_internal(
                            session_id=session_id,
                            amount=amt,
                            approval_no=approval_no,
                            card_number=card_number,
                            registered_at=registered_at,
                            customer_name=customer_name,
                        )
                        _mark_session_checked(session_id, product_title, has_approval=True)
                        changed = True
                except NoSuchElementException as e_row:
                    # 예상한 테이블/셀 구조가 없으면, 삭제는 하지 않고
                    # 단순히 "확인했지만 구조 불명" 상태로만 표시하고 다음 카드로 넘어간다.
                    print(f"[WARN] '거래 내역' 팝업 파싱 중 요소를 찾지 못했습니다: {e_row}")
                    _mark_session_checked(session_id, product_title, has_approval=False)
                except Exception as e_row:
                    print(f"[WARN] '거래 내역' 팝업 파싱 중 오류: {e_row}")
                finally:
                    _close_dialog(dialog)

            except StaleElementReferenceException as e_card:
                # 카드가 DOM 에서 사라진 경우(이미 삭제되었거나 새로고침됨)는
                # 추가 시도를 중단하고 현재 카드 루프를 빠져나온다.
                print(f"[WARN] 결제링크 카드 처리 중 StaleElement 오류: {e_card}")
                break
            except Exception as e_card:
                print(f"[WARN] 결제링크 카드 처리 중 오류: {e_card}")
                continue

        _step_end("결제링크 관리 팝업 기반 동기화", t0)
    except Exception as e:
        print(f"[WARN] 결제링크 팝업 동기화 전반 오류: {e}")
    return changed


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
    # 전체 폼에서는 기다리는 시간을 최소화한다.
    wait = WebDriverWait(driver, 3)
    time.sleep(0.2)

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

    # 폼 아래쪽에 있는 세션유효시간/링크 생성 버튼들이 보이도록 스크롤을 충분히 내린다.
    try:
        # 기존보다 3배 정도 더 아래로 내려서, 60분 및 '링크 생성하기' 버튼이 모두 보이도록 한다.
        driver.execute_script("window.scrollBy(0, 1800);")
        time.sleep(0.3)
    except Exception:
        pass

    # 1-1. 세션유효시간: 콤보박스(#session-ttl)를 '5분 (일반 결제)' 로 변경
    # 4. 세션유효시간은 더 이상 오래 시도하지 않고, 한 번만 빠르게 시도 후 바로 진행
    t0 = _step_start("4. 세션유효시간 선택 (3분→5분, 빠른 시도)")
    try:
        _set_session_ttl_to_5min(driver, max_wait=2.0)
    except Exception:
        # 실패해도 기본값(3분)으로 진행
        pass
    _step_end("4. 세션유효시간 선택 (3분→5분, 빠른 시도)", t0)

    # 5. '링크 생성하기' 버튼 클릭
    t0 = _step_start("5. '링크 생성하기' 버튼 클릭")
    used_copy_button = False
    # 1단계: 반드시 '링크 생성하기' 텍스트 버튼을 먼저 누른다.
    try:
        # 너무 오래 기다리지 않도록, 짧은 반복으로 직접 찾는다.
        clicked = False
        end_ts = time.time() + 2.0
        while time.time() < end_ts and not clicked:
            try:
                create_btn = driver.find_element(
                    By.XPATH, "//button[contains(normalize-space(.),'링크 생성하기')]"
                )
                driver.execute_script(
                    "arguments[0].scrollIntoView({behavior:'instant', block:'center'});",
                    create_btn,
                )
                time.sleep(0.05)
                driver.execute_script("arguments[0].click();", create_btn)
                clicked = True
                break
            except Exception:
                time.sleep(0.1)
        if not clicked:
            print("[WARN] '링크 생성하기' 버튼을 2초 내에 찾지 못했습니다.")
            _step_end("5. '링크 생성하기' 버튼 클릭", t0)
            _step_end("결제링크 생성 폼 작성 전체", t0_all)
            return None
        time.sleep(0.3)
    except Exception:
        print("[WARN] '링크 생성하기' 버튼 클릭 중 예외가 발생했습니다.")
        _step_end("5. '링크 생성하기' 버튼 클릭", t0)
        _step_end("결제링크 생성 폼 작성 전체", t0_all)
        return None

    # 2단계: '링크 생성하기' 가 눌린 뒤에 나타나는 '링크 복사' 버튼이 있으면 눌러준다.
    try:
        copy_btn = WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(normalize-space(.),'링크 복사')]")
            )
        )
        driver.execute_script(
            "arguments[0].scrollIntoView({behavior:'instant', block:'center'});",
            copy_btn,
        )
        time.sleep(0.1)
        driver.execute_script("arguments[0].click();", copy_btn)
        used_copy_button = True
        time.sleep(0.4)
    except TimeoutException:
        # 링크 복사 버튼이 없는 구조일 수도 있으므로, 이 경우에는 생성 버튼만 누른 상태로 진행한다.
        print("[DEBUG] '링크 복사' 버튼을 찾지 못했습니다. 생성 버튼만 누른 상태로 진행합니다.")
    _step_end("5. '링크 생성하기' 버튼 클릭", t0)

    # 6. 팝업/페이지 내 '생성하기' 버튼 한 번 깔끔하게, 최대한 빠르게 클릭
    t0 = _step_start("6. 팝업 '생성하기' 버튼 클릭")
    if not used_copy_button:
        try:
            # dialog 안의 '생성하기' 버튼을 우선적으로 찾는다.
            confirm_btn = WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable(
                    (
                        By.XPATH,
                        "//div[@role='dialog' and @data-state='open']"
                        "//button[contains(normalize-space(.),'생성하기') or contains(normalize-space(.),'생성 하기')]",
                    )
                )
            )
        except TimeoutException:
            # dialog 를 못 찾으면 화면 전체에서 '생성하기' 버튼을 찾는다.
            try:
                confirm_btn = WebDriverWait(driver, 2).until(
                    EC.element_to_be_clickable(
                        (
                            By.XPATH,
                            "//button[contains(normalize-space(.),'생성하기') or contains(normalize-space(.),'생성 하기')]",
                        )
                    )
                )
            except TimeoutException:
                print("[WARN] 화면에서 '생성하기' 버튼을 찾지 못했습니다.")
                _step_end("6. 팝업 '생성하기' 버튼 클릭", t0)
                _step_end("결제링크 생성 폼 작성 전체", t0_all)
                return None

        # 한 가지 방법만: scrollIntoView + JS click (가장 안정적인 패턴)
        try:
            driver.execute_script(
                "arguments[0].scrollIntoView({behavior:'instant', block:'center'});",
                confirm_btn,
            )
            # 너무 길게 기다리지 않고 바로 클릭
            time.sleep(0.05)
            driver.execute_script("arguments[0].click();", confirm_btn)
            print("[INFO] '생성하기' 버튼(JS click) 실행.")
        except Exception as e:
            print(f"[WARN] '생성하기' 버튼 클릭 중 오류: {e}")
            _step_end("6. 팝업 '생성하기' 버튼 클릭", t0)
            _step_end("결제링크 생성 폼 작성 전체", t0_all)
            return None
    _step_end("6. 팝업 '생성하기' 버튼 클릭", t0)

    # 7. 생성된 링크 추출 + 링크 복사 아이콘 클릭 + 클립보드 복사
    t0 = _step_start("7. 생성된 링크 추출")
    link_text = None
    try:
        # readonly input.value 형태의 링크를 우선적으로 찾는다.
        url_input = WebDriverWait(driver, 3).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//input[@readonly and contains(@value,'https://store.k-van.app')]",
                )
            )
        )
        link_text = (url_input.get_attribute("value") or "").strip()
        if link_text:
            print(f"[INFO] 생성된 결제 링크: {link_text}")

            # (1) 링크 복사 아이콘(버튼)을 클릭해서 K-VAN 내부 복사 로직 실행
            try:
                # 아이콘이 늦게 뜰 수 있으니 짧게 여러 번 재시도 (최대 2초)
                end_copy = time.time() + 2.0
                clicked_copy = False
                while time.time() < end_copy and not clicked_copy:
                    try:
                        copy_btn = WebDriverWait(driver, 0.5).until(
                            EC.element_to_be_clickable(
                                (
                                    By.XPATH,
                                    "//button[.//svg[contains(@class,'copy') or contains(@class,'lucide-copy')]]",
                                )
                            )
                        )
                        driver.execute_script(
                            "arguments[0].scrollIntoView({behavior:'instant', block:'center'});",
                            copy_btn,
                        )
                        time.sleep(0.05)
                        driver.execute_script("arguments[0].click();", copy_btn)
                        clicked_copy = True
                        print("[INFO] K-VAN 화면의 '링크 복사' 아이콘 버튼 클릭.")
                        break
                    except TimeoutException:
                        # 아직 아이콘이 안 뜬 경우 → 0.2초 정도 기다리고 다시 시도
                        time.sleep(0.2)

            except TimeoutException:
                print("[WARN] '링크 복사' 아이콘 버튼을 찾지 못했습니다. K-VAN 내부 복사는 생략합니다.")

            # (2) 브라우저 클립보드 API 로도 한 번 더 복사 시도
            try:
                driver.execute_script(
                    """
try {
  const text = arguments[0] || '';
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(text).then(() => {
      console.log('[K-VAN] navigator.clipboard.writeText 성공');
    }).catch(e => {
      console.log('[K-VAN] navigator.clipboard.writeText 실패', e);
    });
  } else {
    const ta = document.createElement('textarea');
    ta.value = text;
    document.body.appendChild(ta);
    ta.select();
    try {
      document.execCommand('copy');
      console.log('[K-VAN] document.execCommand(\"copy\") 성공');
    } catch (e) {
      console.log('[K-VAN] document.execCommand(\"copy\") 실패', e);
    }
    document.body.removeChild(ta);
  }
} catch (e) {
  console.log('[K-VAN] 클립보드 복사 전체 실패', e);
}
""",
                    link_text,
                )
                print("[INFO] 생성된 링크를 클립보드에 복사 시도했습니다.")
            except Exception as e:
                print(f"[WARN] 클립보드 복사 시도 중 오류: {e}")
    except TimeoutException:
        print("[WARN] 생성된 결제 링크 input 을 찾지 못했습니다.")

    if link_text and "https://store.k-van.app" in link_text:
        _store_kvan_link_for_session(session_id, link_text)
        _step_end("7. 생성된 링크 추출", t0)
        _step_end("결제링크 생성 폼 작성 전체", t0_all)
        return link_text

    _step_end("7. 생성된 링크 추출", t0)
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
        # 리다이렉트는 최대 1초만 확인하고, 바로 다음 단계로 진행한다.
        t0 = _step_start("로그인 후 리다이렉트 대기")
        short_wait = WebDriverWait(driver, 1)
        short_wait.until(EC.url_contains("store.k-van.app"))
        _step_end("로그인 후 리다이렉트 대기", t0)
    except TimeoutException:
        print("[WARN] 로그인 후 리다이렉트 URL을 1초 내에 확인하지 못했습니다. 다음 단계로 계속 진행합니다.")
        _step_end("로그인 후 리다이렉트 대기", t0)

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
    """
    auto_kvan 링크 생성 메인 엔트리.
    - 세션 ID 또는 기본 JSON 을 읽어 K-VAN 결제 링크를 생성한다.
    - 전체 실행 시간이 30분(1800초)을 넘으면 안전하게 종료한다.
    """
    import sys

    start_ts = time.time()

    def _check_timeout(stage: str) -> None:
        """30분 타임아웃 체크. 초과 시 TimeoutError 를 발생시킨다."""
        elapsed = time.time() - start_ts
        if elapsed > 1800:
            raise TimeoutError(f"auto_kvan.py 30분 초과 타임아웃 (stage={stage}, elapsed={elapsed:.1f}s)")

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
        # 세션 ID 기반 링크 생성 모드에서는, 주문 JSON 이 없어도
        # admin_state.json 에 저장된 세션 정보(금액/할부)를 기반으로
        # 최소한의 PaymentRow 를 구성해 링크 생성을 시도한다.
        if session_id:
            try:
                amount_val = 0
                installment_val = "일시불"
                if ADMIN_STATE_PATH.exists():
                    with open(ADMIN_STATE_PATH, "r", encoding="utf-8") as f:
                        st = json.load(f)
                    sessions = st.get("sessions") or []
                    for s in sessions:
                        if str(s.get("id")) == str(session_id):
                            amount_val = int(str(s.get("amount") or "0").replace(",", "") or "0")
                            installment_val = str(s.get("installment") or "일시불")
                            break
                login_id = os.environ.get("K_VAN_ID", "m3313")
                login_pw = os.environ.get("K_VAN_PW", "1234")
                login_pin = os.environ.get("K_VAN_PIN", "2424")
                if amount_val <= 0:
                    amount_val = 100000  # 최소 더미 금액
                _append_admin_log(
                    "AUTO",
                    f"주문 JSON 없이 세션 정보로 링크 생성 시도 "
                    f"session_id={session_id}, amount={amount_val}, installment={installment_val}",
                )
                row = PaymentRow(
                    login_id=login_id,
                    login_password=login_pw,
                    login_pin=login_pin,
                    card_type="personal",
                    card_number="",
                    expiry_mm="",
                    expiry_yy="",
                    card_password="",
                    installment_months=installment_val,
                    phone_number="",
                    customer_name="",
                    resident_front="",
                    amount=amount_val,
                    product_name=f"SISA 세션 {session_id}",
                )
            except Exception as e2:  # noqa: BLE001
                _append_admin_log(
                    "AUTO",
                    f"[ERROR] 세션 기반 기본 주문 데이터 구성 실패 session_id={session_id}: {e2}",
                )
                print(e)
                return
        else:
            _append_admin_log("AUTO", f"주문 JSON 없음 session_id={session_id or '-'} path={order_path}")
            print(e)
            return
    except ValueError as e:
        print(f"입력 데이터 오류: {e}")
        return

    _check_timeout("after_load_order")

    driver = create_driver()
    try:
        _check_timeout("before_sign_in")
        _append_admin_log("AUTO", f"K-VAN 로그인 시작 session_id={session_id or '-'}")
        print("K-VAN 가맹점 페이지에 로그인 중...")
        sign_in(driver, row)
        _check_timeout("after_sign_in")

        _append_admin_log("AUTO", "로그인 완료, 결제링크 관리 페이지로 이동")
        print("로그인 완료. 결제링크 관리 페이지로 이동합니다...")
        _go_to_payment_link_page(driver)
        _check_timeout("after_go_payment_link")

        _append_admin_log("AUTO", "결제링크 생성 페이지 진입 시도")
        print("결제링크 관리 화면에서 '+ 생성' 버튼을 눌러 생성 페이지로 이동합니다...")
        moved = _go_to_create_link_page(driver)
        _check_timeout("after_go_create")
        if not moved:
            _append_admin_log("AUTO", "[ERROR] 링크 생성 페이지 진입 실패 (+ 생성 버튼 동작 안 함)")
            print("[ERROR] '+ 생성' 버튼 클릭 후 생성 페이지로 이동하지 못했습니다. 폼 작성 단계는 건너뜁니다.")
            status = "error"
            msg = "결제링크 생성 페이지 진입 실패(생성 버튼 미동작)."
            save_result_to_excel(RESULT_EXCEL_PATH, row, status, msg)
            save_result_to_json(str(result_json_path), status, msg)
            return

        _append_admin_log("AUTO", "결제링크 생성 폼 작성/전송 시작")
        print("결제링크 생성 페이지에서 폼을 채우고 링크를 생성합니다...")
        link_url = _fill_payment_link_form_and_get_url(driver, row, session_id)
        _check_timeout("after_fill_form")
        if link_url:
            status = "link_created"
            msg = "결제 링크가 생성되었습니다. 고객이 링크로 결제하면 K-VAN 크롤러가 상태를 반영합니다."
            _append_admin_log("AUTO", f"결제 링크 생성 완료 session_id={session_id or '-'} link={link_url}")
            print(f"생성된 결제 링크: {link_url}")
            # 링크가 새로 생성되었으므로, 크롤러에 즉시 다시 크롤링하도록 신호를 보낸다.
            try:
                signal_crawler_wakeup()
            except Exception as e:
                print(f"[WAKEUP][WARN] 크롤러 깨우기 신호 전송 중 오류: {e}")
        else:
            status = "error"
            msg = "결제 링크 생성에 실패했거나 링크를 찾지 못했습니다."
            _append_admin_log("AUTO", "[ERROR] 결제 링크 생성 실패 또는 링크 미발견")
            print(msg)

        # 결과는 참고용 엑셀/JSON 에만 남기고, 실제 결제/정산 정보는
        # K-VAN 크롤러(kvan_crawler.py)와 DB 동기화를 기준으로 관리한다.
        save_result_to_excel(RESULT_EXCEL_PATH, row, status, msg)
        save_result_to_json(str(result_json_path), status, msg)

        if not _is_server_env():
            input("브라우저를 확인한 뒤, 종료하려면 Enter 키를 누르세요...")
    except TimeoutError as te:
        print(f"[ERROR] {te}")
    finally:
        driver.quit()


if __name__ == "__main__":
    main()

