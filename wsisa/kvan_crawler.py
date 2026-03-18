"""
kvan_crawler.py - K-VAN 결제링크 모니터링 크롤러 (v2 - 전면 재작성)

역할:
  - K-VAN(store.k-van.app) 에 로그인 후, 결제링크/거래내역을 주기적으로 크롤링
  - 만료된 결제링크가 거래내역이 없으면 K-VAN 사이트에서 직접 삭제 (휴지통 클릭)
  - 만료된 결제링크에 거래내역이 있으면 내부 DB에 기록 + 어드민 알림
  - 어드민/대행사 페이지에서 wakeup 요청 시 즉시 사이클 재개

사이클 흐름:
  1. /payment-link 이동 → 페이지 로드 대기
  2. 결제링크 카드 존재 여부 확인 (아이콘 우선, "없음" 문구 후순위)
  3. 카드가 있으면:
     a. 팝업 스캔 (만료+거래없음 → 삭제, 만료+거래있음 → 기록)
     b. /transactions 이동 → 거래내역 크롤링 → 내부 DB 동기화
     c. 4~7초 대기
  4. 카드가 없으면:
     a. /transactions 이동 → 거래내역 크롤링 → 내부 DB 동기화
     b. 10분 대기 (wakeup 시 즉시 재개)
  5. 오류 발생 시 재로그인 후 계속

환경변수:
  K_VAN_ID        로그인 아이디 (기본: m3313)
  K_VAN_PW        로그인 비밀번호 (기본: 1234)
  K_VAN_DEBUG     디버그 로그 출력 (기본: 1)
  SISA_LOCAL_TEST 로컬 테스트 모드 (DB 쓰기 → JSON 파일)
"""
import os
import sys
import time
import random
import argparse

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, InvalidSessionIdException

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
# 설정
# ──────────────────────────────────────────────────────────────
DEBUG = os.environ.get("K_VAN_DEBUG", "1") == "1"
HEARTBEAT_PATH = DATA_DIR / "kvan_crawler.heartbeat"

# 대기 시간 상수
ACTIVE_DELAY_MIN = 4     # 활성 상태 최소 대기 (초)
ACTIVE_DELAY_MAX = 7     # 활성 상태 최대 대기 (초)
IDLE_DELAY = 600         # 비활성 상태 대기 (초, 10분)


# ──────────────────────────────────────────────────────────────
# 유틸리티
# ──────────────────────────────────────────────────────────────
def _dbg(msg: str) -> None:
    """디버그 로그 (K_VAN_DEBUG=1 일 때만 출력)."""
    if DEBUG:
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[crawler][DEBUG {ts}] {msg}")


def _alog(msg: str) -> None:
    """어드민 페이지 로그에 기록."""
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
    지정된 시간만큼 대기하되, wakeup 플래그 감지 시 즉시 반환.
    1초 간격으로 확인하여 빠르게 반응한다.
    """
    elapsed = 0
    while elapsed < seconds:
        _heartbeat()
        time.sleep(1)
        elapsed += 1
        try:
            if WAKEUP_FLAG_PATH.exists():
                print("[crawler][WAKEUP] wakeup 플래그 감지 → 즉시 다음 사이클")
                WAKEUP_FLAG_PATH.unlink(missing_ok=True)
                return
        except Exception:
            pass


# ──────────────────────────────────────────────────────────────
# 로그인
# ──────────────────────────────────────────────────────────────
def _login(driver: webdriver.Chrome) -> bool:
    """
    K-VAN 로그인. 성공하면 True, 실패하면 False.
    - JS로 아이디/비밀번호 직접 주입 (속도 최적화)
    - SSO(Keycloak) 리다이렉트 완료까지 최대 18초 대기
    """
    login_id = os.environ.get("K_VAN_ID", "m3313")
    login_pw = os.environ.get("K_VAN_PW", "1234")
    t0 = time.time()
    _dbg(f"로그인 시작 (URL={SIGN_IN_URL}, id={login_id})")

    driver.get(SIGN_IN_URL)

    # 공지 팝업 "확인 후 로그인" 버튼 처리 (최대 1초)
    try:
        btn = WebDriverWait(driver, 1).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(normalize-space(.),'확인 후 로그인')]")
            )
        )
        btn.click()
        _dbg("공지 팝업 클릭 완료")
        time.sleep(0.3)
    except TimeoutException:
        pass

    # 아이디 입력창 찾기
    id_locators = [
        (By.CSS_SELECTOR, "input[placeholder*='아이디']"),
        (By.CSS_SELECTOR, "input[name*='id']"),
        (By.CSS_SELECTOR, "input[type='text']"),
        SIGN_IN_SELECTORS.get("id_primary", (By.CSS_SELECTOR, "input")),
    ]
    id_input = _find_element_fast(driver, id_locators)
    if not id_input:
        print("[ERROR] 아이디 입력창을 찾지 못했습니다.")
        return False

    # 비밀번호 입력창 찾기
    pw_locators = [
        (By.CSS_SELECTOR, "input[type='password']"),
        SIGN_IN_SELECTORS.get("password_primary", (By.CSS_SELECTOR, "input[type='password']")),
    ]
    pw_input = _find_element_fast(driver, pw_locators)
    if not pw_input:
        print("[ERROR] 비밀번호 입력창을 찾지 못했습니다.")
        return False

    # JS로 한 번에 입력 (send_keys보다 빠름)
    driver.execute_script(
        """
        arguments[0].value = arguments[2];
        arguments[1].value = arguments[3];
        arguments[0].dispatchEvent(new Event('input', {bubbles:true}));
        arguments[1].dispatchEvent(new Event('input', {bubbles:true}));
        """,
        id_input, pw_input, login_id, login_pw,
    )

    # 로그인 버튼 클릭
    submit = WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable(SIGN_IN_SELECTORS["submit_primary"])
    )
    submit.click()
    _dbg("로그인 버튼 클릭 완료")

    # SSO 리다이렉트 완료 대기 (store.k-van.app 도달까지)
    try:
        WebDriverWait(driver, 18).until(
            lambda d: "store.k-van.app" in (d.current_url or "") and "sso." not in (d.current_url or "")
        )
    except TimeoutException:
        print("[WARN] SSO 리다이렉트 타임아웃(18초). 현재 URL로 계속 진행.")

    _dbg(f"로그인 완료 (elapsed={time.time() - t0:.1f}s, url={driver.current_url})")
    return True


def _find_element_fast(driver: webdriver.Chrome, locators: list, timeout: float = 0.5):
    """여러 locator를 순차 시도하여 가장 먼저 찾은 요소를 반환."""
    for loc in locators:
        try:
            el = WebDriverWait(driver, timeout).until(
                EC.visibility_of_element_located(loc)
            )
            return el
        except TimeoutException:
            continue
    return None


# ──────────────────────────────────────────────────────────────
# 사이클 실행
# ──────────────────────────────────────────────────────────────
def _run_cycle(driver: webdriver.Chrome, cycle: int) -> tuple[bool, bool]:
    """
    크롤링 1사이클 실행.

    반환값: (has_links, had_changes)
      - has_links:   결제링크 카드가 존재하는지
      - had_changes: 이번 사이클에서 신규 변화(삭제/승인/거래)가 있었는지
    """
    had_changes = False
    _dbg(f"사이클 {cycle} 시작, URL={driver.current_url}")

    # ── STEP 1: /payment-link 이동 + 페이지 로드 대기 ──────────
    if not _go_to_payment_link(driver):
        raise RuntimeError("[NAV] /payment-link 진입 실패")

    _wait_payment_link_page_ready(driver)

    # ── STEP 2: 결제링크 목록 크롤링 → DB/JSON 저장 ────────────
    t0 = time.time()
    _scrape_payment_links_and_store(driver)
    _dbg(f"결제링크 크롤링 완료 (elapsed={time.time() - t0:.1f}s)")

    # 만료 세션 반영 (DB/JSON 기반)
    try:
        mark_expired_sessions_from_kvan_links()
    except Exception as e:
        _dbg(f"만료 세션 반영 스킵: {e}")

    # ── STEP 3: 결제링크 카드 존재 여부 확인 ────────────────────
    has_links = _has_payment_links_quick(driver)
    _dbg(f"결제링크 존재 여부: {has_links}")

    # ── STEP 4: 팝업 스캔 (링크 있을 때만) ──────────────────────
    if has_links:
        t0 = time.time()
        active = _has_active_sessions(window_minutes=30)
        _dbg(f"팝업 스캔 시작 (active_sessions={active})")
        if _scan_payment_link_popups_and_sync(driver, allow_popup_for_non_expired=active):
            had_changes = True
            _dbg("팝업 스캔: 신규 변화 감지")
        _dbg(f"팝업 스캔 완료 (elapsed={time.time() - t0:.1f}s)")

    # ── STEP 5: /transactions 거래내역 크롤링 ──────────────────
    t0 = time.time()
    _scrape_transactions_and_store(driver)
    _dbg(f"거래내역 크롤링 완료 (elapsed={time.time() - t0:.1f}s)")

    # ── STEP 6: K-VAN → 내부 DB 동기화 ─────────────────────────
    t0 = time.time()
    if _sync_kvan_to_transactions():
        had_changes = True
        _dbg("내부 DB 동기화: 신규 데이터 반영")
    _dbg(f"DB 동기화 완료 (elapsed={time.time() - t0:.1f}s)")

    # ── STEP 7: 사이클 종료 - /payment-link 복귀 후 최종 확인 ──
    # 팝업 스캔에서 삭제 처리 후 카드가 줄었는지 최종 확인
    if has_links:
        try:
            if _go_to_payment_link(driver):
                has_links = _has_payment_links_quick(driver)
                _dbg(f"사이클 종료 최종 확인: has_links={has_links}")
        except Exception as e:
            _dbg(f"최종 확인 중 예외: {e}")

    return has_links, had_changes


# ──────────────────────────────────────────────────────────────
# 메인 루프
# ──────────────────────────────────────────────────────────────
def run_crawler_loop(max_cycles: int = 0, max_runtime_sec: int = 0) -> None:
    """
    크롤러 메인 루프.

    Args:
        max_cycles:      테스트용 최대 사이클 수 (0=무제한)
        max_runtime_sec: 테스트용 최대 실행 시간 (0=무제한)
    """
    driver = create_driver(headless=_is_server_env())
    try:
        # ── 로그인 ──
        print("[crawler] K-VAN 로그인 시작")
        _alog("K-VAN 로그인 시작")
        _heartbeat()

        if not _login(driver):
            print("[crawler][ERROR] 로그인 실패. 크롤러를 종료합니다.")
            return

        print("[crawler] 로그인 완료. 크롤링 루프 시작.")
        _alog("로그인 완료. 크롤링 루프 시작")
        if LOCAL_TEST:
            print("[crawler] LOCAL_TEST 모드: DB 저장은 JSON 파일로 대체됩니다.")

        # ── 결제링크 페이지로 초기 진입 ──
        try:
            _go_to_payment_link(driver)
            _dbg(f"초기 /payment-link 진입 완료, URL={driver.current_url}")
        except Exception as e:
            _dbg(f"초기 /payment-link 진입 실패: {e}")

        # ── 크롤링 루프 ──
        cycle = 0
        started_at = time.time()

        while True:
            # 런타임 제한 체크
            if max_runtime_sec > 0 and (time.time() - started_at) >= max_runtime_sec:
                print(f"[crawler] 최대 실행시간 도달 ({max_runtime_sec}s). 종료.")
                break
            if max_cycles > 0 and cycle >= max_cycles:
                print(f"[crawler] 최대 사이클 도달 ({cycle}/{max_cycles}). 종료.")
                break

            _heartbeat()
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"[crawler] 사이클 {cycle} 시작: {ts}")

            has_links = False
            had_changes = False

            try:
                has_links, had_changes = _run_cycle(driver, cycle)
            except RuntimeError as e:
                if "[NAV]" in str(e):
                    print(f"[crawler][ERROR] 내비게이션 오류로 중단: {e}")
                    _alog(f"[ERROR] 내비게이션 오류: {e}")
                    break
                raise
            except InvalidSessionIdException:
                print("[crawler][ERROR] 브라우저 세션 만료. 크롤러 종료.")
                _alog("[ERROR] 브라우저 세션 만료")
                break
            except Exception as e:
                print(f"[crawler][WARN] 사이클 {cycle} 오류: {e}")
                _alog(f"[WARN] 크롤링 오류: {e}")
                _dbg(f"예외 발생: {e!r}")
                # 재로그인 시도
                try:
                    _dbg("재로그인 시도")
                    _login(driver)
                except Exception as e2:
                    print(f"[crawler][ERROR] 재로그인 실패: {e2}")
                    _alog(f"[ERROR] 재로그인 실패: {e2}")

            cycle += 1

            # ── 대기 시간 결정 ──
            # 결제링크 존재 / 신규 변화 / 활성 세션 → 짧은 주기 (4~7초)
            # 그 외 → 10분 대기 (wakeup 요청 시 즉시 재개)
            active = _has_active_sessions(window_minutes=10)
            if has_links or had_changes or active:
                delay = random.randint(ACTIVE_DELAY_MIN, ACTIVE_DELAY_MAX)
            else:
                delay = IDLE_DELAY

            print(
                f"[crawler] 다음 사이클까지 {delay}초 대기 "
                f"(links={has_links}, changes={had_changes}, active={active})"
            )
            _wait_with_wakeup(delay)

    finally:
        _alog("크롤러 종료")
        _heartbeat()
        try:
            driver.quit()
        except Exception:
            pass


# ──────────────────────────────────────────────────────────────
# CLI 진입점
# ──────────────────────────────────────────────────────────────
def _parse_args() -> tuple[int, int]:
    p = argparse.ArgumentParser(description="K-VAN 크롤러")
    p.add_argument(
        "--max-cycles", type=int,
        default=int(os.environ.get("K_VAN_CRAWLER_MAX_CYCLES", "0")),
        help="최대 사이클 수 (0=무제한)",
    )
    p.add_argument(
        "--max-seconds", type=int,
        default=int(os.environ.get("K_VAN_CRAWLER_MAX_SECONDS", "0")),
        help="최대 실행 시간(초) (0=무제한)",
    )
    args = p.parse_args()
    return max(0, args.max_cycles), max(0, args.max_seconds)


if __name__ == "__main__":
    mc, ms = _parse_args()
    run_crawler_loop(max_cycles=mc, max_runtime_sec=ms)
