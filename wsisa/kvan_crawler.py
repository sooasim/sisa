"""
kvan_crawler.py - K-VAN 결제링크 모니터링 크롤러 (v3)

역할:
  - K-VAN(store.k-van.app) 에 로그인 후, 결제링크/거래내역을 주기적으로 크롤링
  - 만료된 결제링크 → 거래없음이면 K-VAN 에서 직접 삭제 (휴지통 버튼 클릭 + 확인)
  - 만료된 결제링크 → 거래있으면 내부 DB 기록 + 어드민 알림
  - 어드민/대행사 페이지에서 wakeup 플래그 생성 시 즉시 사이클 재개

핵심 설계 원칙:
  1. /payment-link 이동 후 반드시 driver.refresh() → _wait_payment_link_page_ready() 실행
     (React 초기 렌더링 시 "없음" placeholder 가 먼저 표시되는 문제 방어)
  2. 링크 존재 확인 (_has_payment_links_quick) 은 반드시 refresh/wait 이후에만 호출
  3. LOCAL_TEST=True 이어도 K-VAN UI 조작(팝업 스캔/삭제)은 항상 실행
     (LOCAL_TEST 는 DB 쓰기만 JSON 으로 대체하는 플래그)

사이클 흐름:
  1. /payment-link 이동 → refresh → 페이지 로드 완료 대기
  2. 결제링크 목록 크롤링 → JSON/DB 저장
  3. 링크 카드 존재 확인 (아이콘 우선 - "없음" 문구 후순위)
  4. 링크 있음 → 팝업 스캔:
     만료+거래없음 → 휴지통 버튼 클릭 → 삭제 확인 버튼 클릭
     만료+거래있음 → DB/JSON 기록 + 어드민 알림
  5. /transactions 이동 → 거래내역 크롤링 → 내부 DB 동기화
  6. 대기: 활성 세션/변화 있으면 4-7초, 없으면 10분
     (대기 중 wakeup 플래그 감지 시 즉시 재개)

환경변수:
  K_VAN_ID         로그인 아이디 (기본: m3313)
  K_VAN_PW         로그인 비밀번호 (기본: 1234)
  K_VAN_DEBUG      디버그 로그 출력 여부 (기본: 1)
  SISA_LOCAL_TEST  로컬 테스트 모드 (0=서버 DB 사용, 1=JSON 파일 사용)
"""
import os
import time
import random
import argparse

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import (
    TimeoutException,
    InvalidSessionIdException,
    StaleElementReferenceException,
    NoSuchElementException,
    ElementClickInterceptedException,
)

from auto_kvan import (
    _append_admin_log,
    _is_server_env,
    create_driver,
    _scrape_transactions_and_store,
    _scrape_payment_links_and_store,
    mark_expired_sessions_from_kvan_links,
    _sync_kvan_to_transactions,
    _scan_payment_link_popups_and_sync,
    _has_active_sessions,
    _has_payment_links_quick,
    _go_to_payment_link,
    _wait_payment_link_page_ready,
    SIGN_IN_URL,
    SIGN_IN_SELECTORS,
    LOCAL_TEST,
    WAKEUP_FLAG_PATH,
    DATA_DIR,
)

# ──────────────────────────────────────────────────────────────
# 상수
# ──────────────────────────────────────────────────────────────
DEBUG = os.environ.get("K_VAN_DEBUG", "1") == "1"
HEARTBEAT_PATH = DATA_DIR / "kvan_crawler.heartbeat"

ACTIVE_DELAY_MIN = 4    # 활성 상태 최소 대기 (초)
ACTIVE_DELAY_MAX = 7    # 활성 상태 최대 대기 (초)
IDLE_DELAY = 600        # 비활성 상태 대기 (초, 10분)


# ──────────────────────────────────────────────────────────────
# 유틸리티
# ──────────────────────────────────────────────────────────────
def _dbg(msg: str) -> None:
    """디버그 로그 (K_VAN_DEBUG=1 일 때만 출력)."""
    if DEBUG:
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[crawler][DEBUG {ts}] {msg}")


def _alog(msg: str) -> None:
    """어드민 페이지 로그 박스에 기록."""
    try:
        _append_admin_log("CRAWLER", msg)
    except Exception:
        pass


def _heartbeat() -> None:
    """크롤러 생존 신호 파일 갱신."""
    try:
        HEARTBEAT_PATH.parent.mkdir(parents=True, exist_ok=True)
        HEARTBEAT_PATH.write_text(str(time.time()), encoding="utf-8")
    except Exception:
        pass


def _wait_with_wakeup(seconds: int) -> None:
    """
    지정된 시간만큼 1초 단위로 대기.
    DATA_DIR/crawler_wakeup.flag 파일이 생성되면 즉시 반환.
    """
    for _ in range(max(1, seconds)):
        _heartbeat()
        time.sleep(1)
        try:
            if WAKEUP_FLAG_PATH.exists():
                print(f"[crawler][WAKEUP] wakeup 플래그 감지 → 즉시 다음 사이클")
                WAKEUP_FLAG_PATH.unlink(missing_ok=True)
                return
        except Exception:
            pass


# =========================
# 빠른 삭제 자동화 설정
# =========================

NO_TX_TEXTS = (
    "거래내역이 없습니다",
    "거래 내역이 없습니다",
    "조회된 거래내역이 없습니다",
)

EXPIRED_TARGET_XPATHS = [
    "//*[normalize-space(text())='만료']",
    "//*[contains(@title,'만료')]",
    "//*[contains(@aria-label,'만료')]",
    "//*[contains(@class,'expired')]",
]

TX_TARGET_XPATHS = [
    ".//*[contains(normalize-space(.), '거래내역')]",
    ".//*[contains(normalize-space(.), '거래 내역')]",
    ".//*[contains(normalize-space(.), '내역')]",
    ".//*[contains(@title, '거래내역')]",
    ".//*[contains(@title, '거래 내역')]",
    ".//*[contains(@title, '내역')]",
    ".//*[contains(@aria-label, '거래내역')]",
    ".//*[contains(@aria-label, '거래 내역')]",
    ".//*[contains(@aria-label, '내역')]",
    ".//*[contains(@class, 'history')]",
    ".//*[contains(@class, 'transaction')]",
    ".//*[contains(@class, 'receipt')]",
    ".//*[contains(@data-testid, 'history')]",
    ".//*[contains(@data-testid, 'transaction')]",
    ".//*[contains(@data-icon, 'history')]",
    ".//*[contains(@data-icon, 'receipt')]",
]

TRASH_TARGET_XPATHS = [
    ".//*[contains(normalize-space(.), '삭제')]",
    ".//*[contains(@title, '삭제')]",
    ".//*[contains(@title, '휴지통')]",
    ".//*[contains(@aria-label, '삭제')]",
    ".//*[contains(@aria-label, '휴지통')]",
    ".//*[contains(@class, 'trash')]",
    ".//*[contains(@class, 'delete')]",
    ".//*[contains(@data-testid, 'delete')]",
    ".//*[contains(@data-icon, 'trash')]",
]

DIALOG_XPATHS = [
    "//div[@role='dialog']",
    "//div[contains(@class,'modal')]",
    "//div[contains(@class,'dialog')]",
    "//div[contains(@class,'popup')]",
]

CLOSE_TARGET_XPATHS = [
    ".//*[contains(normalize-space(.), '닫기')]",
    ".//*[contains(@title, '닫기')]",
    ".//*[contains(@aria-label, '닫기')]",
    ".//*[contains(@class, 'close')]",
]

CONFIRM_DELETE_XPATHS = [
    ".//*[self::button or self::a][contains(normalize-space(.), '삭제')]",
    ".//*[self::button or self::a][contains(normalize-space(.), '확인')]",
    ".//*[self::button or self::a][normalize-space(.)='예']",
    ".//*[@role='button'][contains(normalize-space(.), '삭제')]",
    ".//*[@role='button'][contains(normalize-space(.), '확인')]",
    ".//*[@role='button'][normalize-space(.)='예']",
]


def _safe_text(el) -> str:
    try:
        return " ".join((el.text or "").split())
    except Exception:
        return ""


def _find_visible_elements(root, xpaths: list):
    found = []
    for xp in xpaths:
        try:
            els = root.find_elements(By.XPATH, xp)
        except Exception:
            continue
        for el in els:
            try:
                if el.is_displayed():
                    found.append(el)
            except Exception:
                continue
    return found


def _nearest_clickable(el):
    try:
        return el.find_element(
            By.XPATH,
            "ancestor-or-self::*[self::button or self::a or @role='button' or @onclick or contains(@class,'btn') or contains(@class,'icon')][1]",
        )
    except Exception:
        return None


def _row_container_from(el):
    row_xpaths = [
        "ancestor-or-self::tr[1]",
        "ancestor-or-self::li[1]",
        "ancestor-or-self::div[contains(@class,'row')][1]",
        "ancestor-or-self::div[contains(@class,'item')][1]",
        "ancestor-or-self::div[contains(@class,'card')][1]",
        "ancestor-or-self::div[contains(@class,'list')][1]",
    ]
    for xp in row_xpaths:
        try:
            row = el.find_element(By.XPATH, xp)
            if row.is_displayed():
                return row
        except Exception:
            continue
    return None


def _find_action_in_row(row, target_xpaths: list):
    targets = _find_visible_elements(row, target_xpaths)
    for t in targets:
        try:
            clickable = _nearest_clickable(t) or t
            if clickable and clickable.is_displayed():
                return clickable
        except Exception:
            continue
    return None


def _click_fast(driver, el) -> bool:
    try:
        driver.execute_script(
            "arguments[0].scrollIntoView({block:'center', inline:'center'});", el
        )
    except Exception:
        pass

    try:
        el.click()
        return True
    except (ElementClickInterceptedException, StaleElementReferenceException, Exception):
        try:
            driver.execute_script("arguments[0].click();", el)
            return True
        except Exception:
            return False


def _has_visible_text(driver, texts) -> bool:
    for txt in texts:
        xp = f'//*[contains(normalize-space(.), "{txt}")]'
        try:
            els = driver.find_elements(By.XPATH, xp)
        except Exception:
            continue
        for el in els:
            try:
                if el.is_displayed():
                    return True
            except Exception:
                continue
    return False


def _wait_visible_text(driver, texts, timeout: float = 1.8) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        if _has_visible_text(driver, texts):
            return True
        time.sleep(0.05)
    return False


def _find_visible_dialog_root(driver):
    for xp in DIALOG_XPATHS:
        try:
            dialogs = driver.find_elements(By.XPATH, xp)
        except Exception:
            continue
        for dlg in dialogs:
            try:
                if dlg.is_displayed():
                    return dlg
            except Exception:
                continue
    return None


def _close_history_popup_fast(driver) -> None:
    dlg = _find_visible_dialog_root(driver)
    root = dlg or driver

    close_btn = _find_action_in_row(root, CLOSE_TARGET_XPATHS)
    if close_btn:
        _click_fast(driver, close_btn)
        time.sleep(0.05)
        return

    # 닫기 버튼이 없으면 ESC
    try:
        body = driver.find_element(By.TAG_NAME, "body")
        body.send_keys(Keys.ESCAPE)
        time.sleep(0.05)
    except Exception:
        pass


def _confirm_delete_fast(driver) -> bool:
    dlg = _find_visible_dialog_root(driver)
    root = dlg or driver

    btn = _find_action_in_row(root, CONFIRM_DELETE_XPATHS)
    if not btn:
        return False

    if not _click_fast(driver, btn):
        return False

    time.sleep(0.05)
    return True


def _candidate_expired_rows(driver):
    rows = []
    seen = set()

    for xp in EXPIRED_TARGET_XPATHS:
        try:
            expired_marks = driver.find_elements(By.XPATH, xp)
        except Exception:
            continue

        for mark in expired_marks:
            try:
                if not mark.is_displayed():
                    continue
            except Exception:
                continue

            row = _row_container_from(mark)
            if not row:
                continue

            # 삭제/거래내역 액션이 있는 행만 후보로 채택
            tx_btn = _find_action_in_row(row, TX_TARGET_XPATHS)
            trash_btn = _find_action_in_row(row, TRASH_TARGET_XPATHS)
            if not tx_btn or not trash_btn:
                continue

            try:
                key = (int(row.rect.get("y", 0)), _safe_text(row)[:120])
            except Exception:
                key = (_safe_text(row)[:120],)

            if key not in seen:
                seen.add(key)
                rows.append(row)

    return rows


def _delete_one_expired_no_tx_row(driver) -> bool:
    rows = _candidate_expired_rows(driver)
    if not rows:
        _dbg("삭제 후보 행 없음 (만료 + 액션버튼)")
        return False

    for row in rows:
        row_desc = _safe_text(row)[:100]
        _dbg(f"삭제 후보 검사 시작: {row_desc}")

        tx_btn = _find_action_in_row(row, TX_TARGET_XPATHS)
        trash_btn = _find_action_in_row(row, TRASH_TARGET_XPATHS)
        if not tx_btn or not trash_btn:
            _dbg("거래내역 버튼 또는 삭제 버튼을 찾지 못해 스킵")
            continue

        # 거래내역 열기
        if not _click_fast(driver, tx_btn):
            _dbg("거래내역 버튼 클릭 실패")
            continue

        no_tx = _wait_visible_text(driver, NO_TX_TEXTS, timeout=1.8)
        _dbg(f"거래내역 없음 판정={no_tx}")

        # 거래내역 팝업 닫기
        _close_history_popup_fast(driver)

        if not no_tx:
            _dbg("거래내역 존재 → 삭제 안 함")
            continue

        # 팝업 닫은 뒤 다시 삭제 버튼 확보
        try:
            trash_btn = _find_action_in_row(row, TRASH_TARGET_XPATHS)
        except Exception:
            trash_btn = None

        if not trash_btn:
            _dbg("팝업 닫은 뒤 삭제 버튼 재탐색 실패")
            continue

        if not _click_fast(driver, trash_btn):
            _dbg("휴지통 버튼 클릭 실패")
            continue

        if not _confirm_delete_fast(driver):
            _dbg("삭제 확인 버튼 클릭 실패")
            continue

        try:
            WebDriverWait(driver, 2, poll_frequency=0.05).until(EC.staleness_of(row))
        except TimeoutException:
            pass

        _alog(f"만료 + 거래내역 없음 링크 삭제 완료: {row_desc}")
        _dbg(f"만료 + 거래내역 없음 링크 삭제 완료: {row_desc}")
        return True

    return False


def _delete_expired_links_with_no_transactions(
    driver,
    max_delete_per_cycle: int = 20,
) -> int:
    """
    결제링크 관리 화면에서
    - '만료' 표시가 있고
    - 거래내역 아이콘을 눌렀을 때 '거래내역이 없습니다'가 보이는 행만
    즉시 휴지통 삭제한다.
    """
    if not _go_to_payment_link(driver):
        _dbg("삭제 루틴: /payment-link 진입 실패")
        return 0

    try:
        _wait_payment_link_page_ready(driver)
    except Exception:
        pass

    deleted = 0

    # 한 번 삭제하면 DOM 이 바뀌므로 매번 후보를 새로 찾는다.
    while deleted < max_delete_per_cycle:
        ok = _delete_one_expired_no_tx_row(driver)
        if not ok:
            break
        deleted += 1
        time.sleep(0.1)

    if deleted:
        _dbg(f"삭제 루틴 종료: 총 {deleted}건 삭제")
    else:
        _dbg("삭제 루틴 종료: 삭제 건수 0")

    return deleted


def _find_element(driver: webdriver.Chrome, locators: list, timeout: float = 0.8):
    """locator 목록을 순서대로 시도해 가장 먼저 발견된 요소를 반환. 없으면 None."""
    for loc in locators:
        try:
            return WebDriverWait(driver, timeout).until(
                EC.visibility_of_element_located(loc)
            )
        except TimeoutException:
            continue
    return None


# ──────────────────────────────────────────────────────────────
# 로그인
# ──────────────────────────────────────────────────────────────
def _login(driver: webdriver.Chrome) -> bool:
    """
    K-VAN 로그인 수행. 성공하면 True, 실패하면 False 반환.

    단계:
      1. SIGN_IN_URL 로 이동
      2. 공지 팝업 "확인 후 로그인" 버튼 처리 (있을 경우)
      3. 아이디/비밀번호 JS 직접 주입 (send_keys 보다 빠름)
      4. 로그인 버튼 클릭
      5. SSO(Keycloak) 리다이렉트 완료 대기 (store.k-van.app, 최대 18초)
    """
    login_id = os.environ.get("K_VAN_ID", "m3313")
    login_pw = os.environ.get("K_VAN_PW", "1234")
    t0 = time.time()
    _dbg(f"로그인 시작 (id={login_id}, url={SIGN_IN_URL})")

    driver.get(SIGN_IN_URL)

    # ── 공지 팝업 처리 (최대 1초) ──
    try:
        btn = WebDriverWait(driver, 1).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(normalize-space(.),'확인 후 로그인')]")
            )
        )
        btn.click()
        _dbg("공지 팝업 '확인 후 로그인' 클릭 완료")
        time.sleep(0.3)
    except TimeoutException:
        _dbg("공지 팝업 없음 (정상)")

    # ── 아이디 입력창 탐색 ──
    id_input = _find_element(driver, [
        (By.CSS_SELECTOR, "input[placeholder*='아이디']"),
        (By.CSS_SELECTOR, "input[name*='id']"),
        (By.CSS_SELECTOR, "input[type='text']"),
        SIGN_IN_SELECTORS.get("id_primary",    (By.CSS_SELECTOR, "input[type='text']")),
        SIGN_IN_SELECTORS.get("id_placeholder", (By.CSS_SELECTOR, "input[type='text']")),
        SIGN_IN_SELECTORS.get("id_fallback",    (By.CSS_SELECTOR, "input[type='text']")),
    ])
    if id_input is None:
        print("[ERROR][crawler] 아이디 입력창을 찾지 못했습니다.")
        return False

    # ── 비밀번호 입력창 탐색 ──
    pw_input = _find_element(driver, [
        (By.CSS_SELECTOR, "input[type='password']"),
        SIGN_IN_SELECTORS.get("password_primary", (By.CSS_SELECTOR, "input[type='password']")),
        SIGN_IN_SELECTORS.get("password_fallback", (By.CSS_SELECTOR, "input[type='password']")),
    ])
    if pw_input is None:
        print("[ERROR][crawler] 비밀번호 입력창을 찾지 못했습니다.")
        return False

    # ── JS 로 값 직접 설정 (속도 최적화) ──
    driver.execute_script(
        """
        arguments[0].value = arguments[2];
        arguments[1].value = arguments[3];
        arguments[0].dispatchEvent(new Event('input', {bubbles:true}));
        arguments[1].dispatchEvent(new Event('input', {bubbles:true}));
        """,
        id_input, pw_input, login_id, login_pw,
    )
    _dbg("아이디/비밀번호 JS 주입 완료")

    # ── 로그인 버튼 클릭 ──
    submit = WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable(SIGN_IN_SELECTORS["submit_primary"])
    )
    submit.click()
    _dbg("로그인 버튼 클릭 완료")

    # ── SSO 리다이렉트 완료 대기 ──
    # sso.oneque.net(Keycloak) → store.k-van.app 으로 리다이렉트될 때까지 최대 18초
    try:
        WebDriverWait(driver, 18).until(
            lambda d: (
                "store.k-van.app" in (d.current_url or "")
                and "sso." not in (d.current_url or "")
            )
        )
        _dbg(f"SSO 리다이렉트 완료 (elapsed={time.time()-t0:.1f}s, url={driver.current_url})")
    except TimeoutException:
        print(f"[WARN][crawler] SSO 리다이렉트 타임아웃(18s). 현재 URL={driver.current_url} 로 계속 진행.")

    _dbg(f"로그인 완료 (total_elapsed={time.time()-t0:.1f}s, url={driver.current_url})")
    return True


# ──────────────────────────────────────────────────────────────
# 결제링크 페이지 새로고침 + 로드 대기
# ──────────────────────────────────────────────────────────────
def _navigate_and_refresh_payment_link(driver: webdriver.Chrome) -> bool:
    """
    /payment-link 로 이동 후 반드시 refresh + wait 를 수행한다.

    이 함수가 필요한 이유:
    - Next.js/React SSR 환경에서 최초 렌더 시 "생성된 결제 링크가 없습니다"
      placeholder 를 먼저 그리고, API 응답이 오면 실제 카드로 교체한다.
    - driver.get() 만으로는 placeholder 단계에서 멈추는 경우가 있다.
    - driver.refresh() 를 통해 실제 API 응답을 받은 후의 DOM 을 확인한다.

    반환값: 성공하면 True, 진입 자체가 실패하면 False.
    """
    if not _go_to_payment_link(driver):
        print("[crawler][ERROR] /payment-link 진입 실패")
        return False
    # 반드시 refresh: React placeholder("없음") 가 아닌 실제 카드 DOM 을 얻기 위해
    driver.refresh()
    _wait_payment_link_page_ready(driver)
    return True


# ──────────────────────────────────────────────────────────────
# 1사이클 실행
# ──────────────────────────────────────────────────────────────
def _run_cycle(driver: webdriver.Chrome, cycle: int) -> tuple:
    """
    크롤링 1사이클을 실행한다.

    반환: (has_links: bool, had_changes: bool)
      - has_links:   이번 사이클 종료 시점에 결제링크 카드가 남아 있는지
      - had_changes: 이번 사이클에서 신규 승인/삭제/갱신이 발생했는지
    """
    had_changes = False
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[crawler] 사이클 {cycle} 시작: {ts}")
    _alog(f"크롤링 사이클 시작: {ts}")
    _dbg(f"사이클 {cycle} 시작, URL={driver.current_url}")

    # ── STEP 1: /payment-link 이동 → refresh → 페이지 로드 완료 ──
    # 반드시 refresh 수행: React placeholder 대신 실제 카드 DOM 확보
    if not _navigate_and_refresh_payment_link(driver):
        raise RuntimeError("[NAV] /payment-link refresh 진입 실패")
    _dbg(f"STEP1 완료: /payment-link refresh 후 URL={driver.current_url}")

    # ── STEP 2: 결제링크 목록 크롤링 → JSON(LOCAL_TEST) 또는 DB 저장 ──
    # 주의: _scrape_payment_links_and_store 는 내부적으로 또 _go_to_payment_link +
    #       driver.refresh() + _wait_payment_link_page_ready 를 수행한다.
    #       STEP1 에서 이미 준비했더라도 함수 내 refresh 가 한 번 더 실행되는 것은 정상.
    t0 = time.time()
    _dbg("STEP2 시작: 결제링크 목록 크롤링")
    _scrape_payment_links_and_store(driver)
    _dbg(f"STEP2 완료: 결제링크 크롤링 (elapsed={time.time()-t0:.1f}s)")

    # ── STEP 3: 만료 세션 DB/JSON 반영 ──
    try:
        mark_expired_sessions_from_kvan_links()
        _dbg("STEP3 완료: 만료 세션 반영")
    except Exception as e:
        _dbg(f"STEP3 스킵: 만료 세션 반영 중 예외 ({e})")

    # ── STEP 4: 결제링크 카드 존재 확인 ──
    # _scrape_payment_links_and_store 종료 후 driver 는 /payment-link 에 위치.
    # refresh + wait 가 이미 완료된 상태이므로 아이콘이 DOM 에 있으면 즉시 True 반환.
    has_links = _has_payment_links_quick(driver)
    _dbg(f"STEP4 완료: 결제링크 존재 여부 has_links={has_links}")

    if not has_links:
        # /payment-link 에 링크가 없더라도 한 번 더 refresh 후 재확인
        # (STEP2 내부 refresh 와 시간차가 있을 수 있으므로)
        _dbg("STEP4-retry: 링크 없음 → 한 번 더 refresh 후 재확인")
        driver.refresh()
        _wait_payment_link_page_ready(driver)
        has_links = _has_payment_links_quick(driver)
        _dbg(f"STEP4-retry 완료: has_links={has_links}")

    # ── STEP 5: 결제링크 팝업 스캔 ──
    # - 만료 카드: 거래 내역 버튼 클릭 → 팝업에서 거래 유무 확인
    #   거래없음 → 휴지통 버튼 클릭 → 삭제 확인 → K-VAN 에서 제거
    #   거래있음 → 내부 DB/JSON 기록 + 어드민 알림
    # - 비만료 카드: 활성 세션이 있을 때만 팝업 확인 (불필요한 클릭 방지)
    if has_links:
        active_for_popup = _has_active_sessions(window_minutes=30)
        t0 = time.time()
        _dbg(f"STEP5 시작: 팝업 스캔 (active_sessions={active_for_popup})")
        if _scan_payment_link_popups_and_sync(driver, allow_popup_for_non_expired=active_for_popup):
            had_changes = True
            _dbg("STEP5: 팝업 스캔에서 신규 변화 감지 (had_changes=True)")
        _dbg(f"STEP5 완료: 팝업 스캔 (elapsed={time.time()-t0:.1f}s)")

    # ── STEP 6: 거래내역 크롤링 ──
    # /transactions 로 이동 → 새 거래내역을 kvan_transactions 에 저장
    t0 = time.time()
    _dbg("STEP6 시작: 거래내역 크롤링")
    _scrape_transactions_and_store(driver)
    _dbg(f"STEP6 완료: 거래내역 크롤링 (elapsed={time.time()-t0:.1f}s)")

    # ── STEP 7: K-VAN 테이블 → 내부 transactions 동기화 ──
    t0 = time.time()
    _dbg("STEP7 시작: 내부 DB 동기화")
    if _sync_kvan_to_transactions():
        had_changes = True
        _dbg("STEP7: 동기화에서 신규 데이터 발견 (had_changes=True)")
    _dbg(f"STEP7 완료: 내부 DB 동기화 (elapsed={time.time()-t0:.1f}s)")

    # ── STEP 8: 사이클 종료 최종 확인 ──
    # 팝업 스캔에서 카드가 삭제되었을 수 있으므로, /payment-link 에서 카드 수를 재확인한다.
    # 다음 사이클의 대기 시간(짧은 사이클 vs 10분)을 결정하는 데 사용된다.
    try:
        if _navigate_and_refresh_payment_link(driver):
            has_links = _has_payment_links_quick(driver)
            _dbg(f"STEP8 완료: 사이클 종료 최종 확인 has_links={has_links}")
    except Exception as e:
        _dbg(f"STEP8 스킵: 최종 확인 중 예외 ({e})")

    _alog(
        f"사이클 {cycle} 완료 (has_links={has_links}, had_changes={had_changes})"
    )
    return has_links, had_changes


# ──────────────────────────────────────────────────────────────
# 메인 루프
# ──────────────────────────────────────────────────────────────
def run_crawler_loop(max_cycles: int = 0, max_runtime_sec: int = 0) -> None:
    """
    크롤러 메인 루프.

    Args:
        max_cycles:      테스트용 최대 사이클 수 (0 = 무제한)
        max_runtime_sec: 테스트용 최대 실행 시간(초) (0 = 무제한)
    """
    driver = create_driver(headless=_is_server_env())
    try:
        # ── 로그인 ──────────────────────────────────────────────
        print("[crawler] K-VAN 로그인 시작")
        _alog("K-VAN 로그인 시작")
        _heartbeat()

        if not _login(driver):
            print("[crawler][ERROR] 로그인 실패. 크롤러를 종료합니다.")
            _alog("[ERROR] 로그인 실패. 크롤러 종료.")
            return

        print("[crawler] 로그인 완료. 크롤링 루프 시작.")
        _alog("로그인 완료. 크롤링 루프 시작")
        if LOCAL_TEST:
            print("[crawler] LOCAL_TEST 모드: 크롤링 데이터는 JSON 파일에 저장됩니다.")
            print(f"[crawler] 저장 경로: {DATA_DIR / 'local_db'}")

        # ── 초기 /payment-link 진입 ─────────────────────────────
        _dbg(f"로그인 직후 URL={driver.current_url}")
        try:
            if _navigate_and_refresh_payment_link(driver):
                _dbg(f"초기 /payment-link refresh 진입 성공, URL={driver.current_url}")
            else:
                _dbg("초기 /payment-link 진입 실패 (첫 사이클에서 재시도)")
        except Exception as e:
            _dbg(f"초기 /payment-link 진입 중 예외: {e}")

        # ── 크롤링 루프 ─────────────────────────────────────────
        cycle = 0
        started_at = time.time()

        while True:
            # 런타임/사이클 제한 체크
            if max_runtime_sec > 0 and (time.time() - started_at) >= max_runtime_sec:
                print(f"[crawler] 최대 실행시간 도달 ({max_runtime_sec}s). 루프 종료.")
                _alog(f"최대 실행시간 도달 ({max_runtime_sec}s). 루프 종료.")
                break
            if max_cycles > 0 and cycle >= max_cycles:
                print(f"[crawler] 최대 사이클 도달 ({cycle}/{max_cycles}). 루프 종료.")
                _alog(f"최대 사이클 도달 ({cycle}/{max_cycles}). 루프 종료.")
                break

            _heartbeat()
            has_links = False
            had_changes = False

            try:
                has_links, had_changes = _run_cycle(driver, cycle)

            except RuntimeError as e:
                # [NAV] 가 포함된 RuntimeError 는 내비게이션 치명 오류 → 루프 종료
                if "[NAV]" in str(e):
                    print(f"[crawler][ERROR] 내비게이션 오류로 루프 중단: {e}")
                    _alog(f"[ERROR] 내비게이션 오류: {e}")
                    break
                # 그 외 RuntimeError 는 재로그인 후 계속
                print(f"[crawler][WARN] RuntimeError: {e}. 재로그인 시도.")
                _alog(f"[WARN] RuntimeError: {e}")
                try:
                    _login(driver)
                except Exception as e2:
                    print(f"[crawler][ERROR] 재로그인 실패: {e2}")
                    _alog(f"[ERROR] 재로그인 실패: {e2}")

            except InvalidSessionIdException:
                print("[crawler][ERROR] 브라우저 세션 만료. 크롤러 종료.")
                _alog("[ERROR] 브라우저 세션 만료. 크롤러 종료.")
                break

            except Exception as e:
                # 크롤링 중 예외 → 현재 URL 기록 → 재로그인 → 다음 사이클
                try:
                    cur_url = driver.current_url
                except Exception:
                    cur_url = "(세션 무효)"
                print(f"[crawler][WARN] 사이클 {cycle} 오류: {e} (URL={cur_url})")
                _alog(f"[WARN] 사이클 {cycle} 오류: {e}")
                _dbg(f"예외 상세: {e!r}")
                try:
                    _login(driver)
                except Exception as e2:
                    print(f"[crawler][ERROR] 재로그인 실패: {e2}")
                    _alog(f"[ERROR] 재로그인 실패: {e2}")

            cycle += 1

            # ── 다음 사이클 대기 시간 결정 ──────────────────────
            # 결제링크 존재 or 신규 변화 or 활성 세션 → 짧은 대기 (4~7초)
            # 세 조건 모두 없음 → 10분 대기 (wakeup 플래그 감지 시 즉시 재개)
            active = _has_active_sessions(window_minutes=10)
            if has_links or had_changes or active:
                delay = random.randint(ACTIVE_DELAY_MIN, ACTIVE_DELAY_MAX)
            else:
                delay = IDLE_DELAY

            print(
                f"[crawler] 다음 사이클까지 {delay}초 대기 "
                f"(has_links={has_links}, had_changes={had_changes}, active_sessions={active})"
            )
            _alog(
                f"다음 사이클까지 {delay}초 대기 "
                f"(links={has_links}, changes={had_changes}, active={active})"
            )
            _wait_with_wakeup(delay)

    finally:
        _alog("크롤러 정상 종료")
        _heartbeat()
        try:
            driver.quit()
        except Exception:
            pass


# ──────────────────────────────────────────────────────────────
# CLI 진입점
# ──────────────────────────────────────────────────────────────
def _parse_args() -> tuple:
    p = argparse.ArgumentParser(description="K-VAN 크롤러")
    p.add_argument(
        "--max-cycles",
        type=int,
        default=int(os.environ.get("K_VAN_CRAWLER_MAX_CYCLES", "0")),
        help="최대 사이클 수 (0=무제한, 테스트용)",
    )
    p.add_argument(
        "--max-seconds",
        type=int,
        default=int(os.environ.get("K_VAN_CRAWLER_MAX_SECONDS", "0")),
        help="최대 실행 시간(초) (0=무제한, 테스트용)",
    )
    args = p.parse_args()
    return max(0, args.max_cycles), max(0, args.max_seconds)


if __name__ == "__main__":
    mc, ms = _parse_args()
    run_crawler_loop(max_cycles=mc, max_runtime_sec=ms)
