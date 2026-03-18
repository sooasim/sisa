import os
import time
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
    _scrape_dashboard_and_store,
    _scrape_transactions_and_store,
    _scrape_payment_links_and_store,
    mark_expired_sessions_from_kvan_links,
    _sync_kvan_to_transactions,
    _scan_payment_link_popups_and_sync,
    _has_active_sessions,
    SIGN_IN_URL,
    SIGN_IN_SELECTORS,
    PIN_POPUP_SELECTORS,
    LOCAL_TEST,
    _go_to_payment_link,
    _has_payment_links_quick,
    _wait_payment_link_page_ready,
    WAKEUP_FLAG_PATH,
    DATA_DIR,
)


# 로컬 테스트용 디버그 플래그 (기본 ON: "1")
DEBUG_CRAWLER = os.environ.get("K_VAN_DEBUG", "1") == "1"
HEARTBEAT_PATH = DATA_DIR / "kvan_crawler.heartbeat"


def _dbg(msg: str) -> None:
    """로컬 디버깅용 상세 로그 출력."""
    if DEBUG_CRAWLER:
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[crawler][DEBUG {now}] {msg}")


def _alog(msg: str) -> None:
    """HQ 어드민 로그 박스에 표시될 크롤러 로그."""
    try:
        _append_admin_log("CRAWLER", msg)
    except Exception:
        pass


def _touch_heartbeat() -> None:
    """크롤러 생존 신호 파일 갱신."""
    try:
        HEARTBEAT_PATH.parent.mkdir(parents=True, exist_ok=True)
        HEARTBEAT_PATH.write_text(str(time.time()), encoding="utf-8")
    except Exception:
        pass


def _simple_sign_in(driver: webdriver.Chrome) -> None:
    """
    크롤링 전용 단순 로그인:
    - 아이디: 환경변수 K_VAN_ID 또는 기본값 'm3313'
    - 비밀번호: 환경변수 K_VAN_PW 또는 기본값 '1234'
    - PIN: 환경변수 K_VAN_PIN 또는 기본값 '2424'
    """
    login_id = os.environ.get("K_VAN_ID", "m3313")
    login_pw = os.environ.get("K_VAN_PW", "1234")
    login_pin = os.environ.get("K_VAN_PIN", "2424")

    t0_all = time.time()
    _dbg(f"_simple_sign_in 시작 (URL={SIGN_IN_URL}, id={login_id})")

    # 로그인 페이지로 이동 (불필요한 대기 없이 바로 요소 탐색 시작)
    driver.get(SIGN_IN_URL)
    _dbg("SIGN_IN_URL 로 이동 완료, 즉시 입력 필드 탐색 시작")
    wait = WebDriverWait(driver, 3)

    # 로그인 페이지 공지 팝업 처리 (확인 후 로그인) – 최대 1초만 대기
    try:
        t0_popup = time.time()
        btn = WebDriverWait(driver, 1).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(normalize-space(.),'확인 후 로그인')]")
            )
        )
        btn.click()
        _dbg(f"'확인 후 로그인' 팝업 클릭 완료 (elapsed={time.time() - t0_popup:.2f}s)")
        time.sleep(0.3)
    except TimeoutException:
        _dbg("'확인 후 로그인' 팝업 없음 또는 1초 내 미표시, 바로 로그인 진행")
        pass

    # 아이디 + 비밀번호를 가능한 한 빠르게 입력 (JS로 value 직접 설정)
    t0_inputs = time.time()
    _dbg("아이디/비밀번호 입력 필드 탐색 시작")

    # 1순위: CSS 셀렉터 기반(빠름), 2순위: 기존 SIGN_IN_SELECTORS (fallback, 짧은 timeout)
    id_locators = [
        (By.CSS_SELECTOR, "input[placeholder*='아이디']"),
        (By.CSS_SELECTOR, "input[name*='id']"),
        (By.CSS_SELECTOR, "input[type='text']"),
        SIGN_IN_SELECTORS["id_primary"],
        SIGN_IN_SELECTORS["id_placeholder"],
        SIGN_IN_SELECTORS["id_fallback"],
    ]

    id_input = None
    for loc in id_locators:
        try:
            _dbg(f"아이디 입력창 대기 시도: locator={loc}")
            id_input = WebDriverWait(driver, 0.5).until(
                EC.visibility_of_element_located(loc)
            )
            _dbg("아이디 입력창 발견 (가장 먼저 매칭된 locator)")
            break
        except TimeoutException:
            _dbg("아이디 입력창 탐색 타임아웃 (0.5초) – 다음 locator 로 재시도")
            continue
    if not id_input:
        print("[ERROR][crawler] 아이디 입력창을 찾지 못했습니다.")
        _dbg("아이디 입력창 탐색 실패, _simple_sign_in 종료")
        return

    pw_locators = [
        (By.CSS_SELECTOR, "input[type='password']"),
        SIGN_IN_SELECTORS["password_primary"],
        SIGN_IN_SELECTORS["password_fallback"],
    ]

    pw_input = None
    for loc in pw_locators:
        try:
            _dbg(f"비밀번호 입력창 대기 시도: locator={loc}")
            pw_input = WebDriverWait(driver, 0.5).until(
                EC.visibility_of_element_located(loc)
            )
            _dbg("비밀번호 입력창 발견 (가장 먼저 매칭된 locator)")
            break
        except TimeoutException:
            _dbg("비밀번호 입력창 탐색 타임아웃 (0.5초) – 다음 locator 로 재시도")
            continue
    if not pw_input:
        print("[ERROR][crawler] 비밀번호 입력창을 찾지 못했습니다.")
        _dbg("비밀번호 입력창 탐색 실패, _simple_sign_in 종료")
        return

    # send_keys 대신 JS 로 한 번에 채우기 (속도 향상)
    _dbg(
        f"아이디/비밀번호 입력 필드 확보 (elapsed={time.time() - t0_inputs:.2f}s), JS 로 값 설정"
    )
    driver.execute_script(
        """
arguments[0].value = arguments[2];
arguments[1].value = arguments[3];
arguments[0].dispatchEvent(new Event('input', {bubbles:true}));
arguments[1].dispatchEvent(new Event('input', {bubbles:true}));
""",
        id_input,
        pw_input,
        login_id,
        login_pw,
    )

    # 로그인 버튼 (최대 2초)
    t0_submit = time.time()
    submit_btn = WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable(SIGN_IN_SELECTORS["submit_primary"])
    )
    submit_btn.click()
    _dbg(f"로그인 버튼 클릭 완료 (elapsed={time.time() - t0_submit:.2f}s)")

    # 2026-03: PIN 입력 단계가 사라짐. SSO(Keycloak) 경유 시 리다이렉트가 수 초 걸리므로 18초 대기.
    # 로그인 후 반드시 store.k-van.app 으로 와야 크롤링 가능 (sso.oneque.net 에 머물면 결제링크 크롤링 실패).
    try:
        t0_redirect = time.time()

        def _store_ready(d):
            cur = d.current_url or ""
            return "store.k-van.app" in cur and "sso.oneque.net" not in cur

        WebDriverWait(driver, 18).until(_store_ready)
        _dbg(f"로그인 후 store.k-van.app 안정 도달 (elapsed={time.time() - t0_redirect:.2f}s)")
    except TimeoutException:
        print("[WARN][crawler] 로그인 후 store.k-van.app 리다이렉트 실패(18초). 현재 URL로 진행.")
    finally:
        _dbg(f"_simple_sign_in 전체 완료 (total_elapsed={time.time() - t0_all:.2f}s, url={driver.current_url})")


def _wait_with_wakeup(total_delay: int) -> None:
    """
    total_delay 초 동안 대기하되, auto_kvan 이 남긴 wakeup 플래그가 있으면 즉시 대기를 종료한다.

    - DATA_DIR/crawler_wakeup.flag 파일이 존재하면 바로 삭제하고 반환.
    - 그렇지 않으면 1초 단위로 쪼개서 sleep 을 반복.
    """
    waited = 0
    while waited < total_delay:
        _touch_heartbeat()
        # 1초 단위로 쪼갬
        step = min(1, total_delay - waited)
        time.sleep(step)
        waited += step
        try:
            if WAKEUP_FLAG_PATH.exists():
                print(f"[crawler][WAKEUP] wakeup 플래그 감지 → 즉시 다음 사이클로 진행 ({WAKEUP_FLAG_PATH})")
                try:
                    WAKEUP_FLAG_PATH.unlink()
                except Exception:
                    pass
                return
        except Exception:
            # 파일 시스템 오류는 무시하고 남은 대기를 계속 진행
            pass


def run_crawler_loop(max_cycles: int = 0, max_runtime_sec: int = 0) -> None:
    """
    링크/거래 상태 추적용 크롤러 메인 루프.

    1사이클 흐름:
    ─ [결제링크 있음]
        결제링크 관리 크롤링 → 만료 처리 → 팝업 스캔
        → 결제 및 취소내역 크롤링 → 내부 DB 동기화
        → 변화 있으면 4~7초 / 없으면 10분 대기
    ─ [결제링크 없음]
        결제 및 취소내역 페이지로 이동 → 새로고침 → 크롤링 → 동기화
        → 결제링크 관리로 복귀 → 새로고침 → 링크 재확인
            ├ 링크 있으면 → 팝업 스캔 → 4~7초 대기
            └ 링크 없으면 → 10분 대기
    ─ 대기 중 어드민/대행사 페이지에서 wakeup 요청 시 즉시 재개
    """
    import random

    driver = create_driver(headless=_is_server_env())
    try:
        print("[crawler] K-VAN 로그인 시작")
        _alog("K-VAN 로그인 시작")
        _touch_heartbeat()
        _simple_sign_in(driver)
        print("[crawler] 로그인 완료. 주기 크롤링 루프 시작.")
        _alog("로그인 완료. 주기 크롤링 루프 시작")
        if LOCAL_TEST:
            print("[crawler] LOCAL_TEST 모드: DB 관련 쓰기는 auto_kvan 에서 모두 건너뜁니다.")
            print("[crawler] 참고: DB 저장/동기화까지 검증하려면 SISA_LOCAL_TEST=0 으로 실행하세요.")
        _dbg(f"로그인 직후 current_url={driver.current_url}")

        # 로그인 직후 결제링크 관리 화면으로 이동해 권한/세션을 확정시킨다.
        try:
            _dbg("로그인 직후 결제링크 관리 페이지로 직접 이동 시도")
            if not _go_to_payment_link(driver):
                _dbg("로그인 직후 /payment-link 진입 실패 (다음 루프에서 재시도)")
            else:
                _dbg(f"결제링크 관리 첫 진입 성공, URL={driver.current_url}")
        except Exception as e:
            _dbg(f"결제링크 관리 첫 진입 중 예외 발생: {e!r}")

        backup_interval = int(os.environ.get("K_VAN_CRAWL_INTERVAL", "600"))
        last_backup_ts = 0.0
        cycle = 0

        started_ts = time.time()
        while True:
            if max_runtime_sec > 0 and (time.time() - started_ts) >= max_runtime_sec:
                msg = f"테스트 종료: 최대 실행시간 도달 ({max_runtime_sec}s)"
                print(f"[crawler] {msg}")
                _alog(msg)
                break
            _touch_heartbeat()
            loop_start = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"[crawler] 크롤링 사이클 시작: {loop_start}")
            _alog(f"크롤링 사이클 시작: {loop_start}")
            _dbg(f"사이클 {cycle} 시작, 현재 URL={driver.current_url}")

            had_new = False
            has_links = False
            stop_by_runtime = False

            def _runtime_guard() -> bool:
                nonlocal stop_by_runtime
                if max_runtime_sec > 0 and (time.time() - started_ts) >= max_runtime_sec:
                    stop_by_runtime = True
                    msg = f"테스트 종료: 최대 실행시간 도달 ({max_runtime_sec}s)"
                    print(f"[crawler] {msg}")
                    _alog(msg)
                    return True
                return False

            try:
                _dbg("대시보드 크롤링은 건너뜀 (결제링크 관리 → 거래내역 순으로 처리)")

                # ── 1) 결제링크 관리 크롤링 (이동/새로고침 포함) ──────────────
                t_links = time.time()
                _dbg("결제링크 목록 크롤링 시작")
                _scrape_payment_links_and_store(driver)
                _dbg(f"결제링크 목록 크롤링 종료 (elapsed={time.time() - t_links:.2f}s, url={driver.current_url})")
                try:
                    mark_expired_sessions_from_kvan_links()
                except Exception as _e:
                    _dbg(f"링크 만료 세션 반영 스킵: {_e}")
                if _runtime_guard():
                    break

                t_check = time.time()
                has_links = _has_payment_links_quick(driver)
                _dbg(f"결제링크 존재 확인 완료 (has_links={has_links}, elapsed={time.time() - t_check:.2f}s)")
                active_for_popup = _has_active_sessions(window_minutes=30)

                if has_links:
                    # ── [링크 있음] 팝업 스캔 → 거래내역 → 동기화 ────────────
                    t_popup = time.time()
                    _dbg("결제링크 팝업 스캔 시작 (_scan_payment_link_popups_and_sync)")
                    if _scan_payment_link_popups_and_sync(
                        driver,
                        allow_popup_for_non_expired=active_for_popup,
                    ):
                        had_new = True
                        _dbg("결제링크 팝업 스캔에서 신규 변화 감지 (had_new=True)")
                    _dbg(f"결제링크 팝업 스캔 종료 (elapsed={time.time() - t_popup:.2f}s)")
                    if _runtime_guard():
                        break

                    t_tx = time.time()
                    _dbg("거래내역 크롤링 시작")
                    _scrape_transactions_and_store(driver)
                    _dbg(f"거래내역 크롤링 종료 (elapsed={time.time() - t_tx:.2f}s)")
                    if _runtime_guard():
                        break

                    t_sync = time.time()
                    _dbg("K-VAN → 내부 transactions 동기화 시작")
                    if _sync_kvan_to_transactions():
                        had_new = True
                        _dbg("K-VAN → transactions 동기화에서 신규/갱신 발생 (had_new=True)")
                    _dbg(f"K-VAN → transactions 동기화 종료 (elapsed={time.time() - t_sync:.2f}s)")
                    if _runtime_guard():
                        break

                else:
                    # ── [링크 없음] 거래내역 확인 → 결제링크 관리 복귀 ─────────
                    _alog("결제링크 없음 → 결제 및 취소내역 페이지로 이동해 신규 거래 확인")
                    _dbg("결제링크 없음 → /transactions 로 이동")

                    t_tx = time.time()
                    _scrape_transactions_and_store(driver)
                    _dbg(f"거래내역 크롤링 종료 (elapsed={time.time() - t_tx:.2f}s)")
                    if _runtime_guard():
                        break

                    t_sync = time.time()
                    _dbg("K-VAN → 내부 transactions 동기화 시작")
                    if _sync_kvan_to_transactions():
                        had_new = True
                        _dbg("거래내역에서 신규 데이터 발견 (had_new=True)")
                    _dbg(f"K-VAN → transactions 동기화 종료 (elapsed={time.time() - t_sync:.2f}s)")
                    if _runtime_guard():
                        break

                    # 결제링크 관리로 복귀 → 새로고침 → 링크 재확인
                    _alog("결제링크 관리 페이지로 복귀 → 새로고침 후 링크 재확인")
                    _dbg("결제링크 없음 사이클: /payment-link 로 복귀 후 새로고침")
                    try:
                        if _go_to_payment_link(driver):
                            driver.refresh()
                            _wait_payment_link_page_ready(driver)
                            has_links_retry = _has_payment_links_quick(driver)
                            if has_links_retry:
                                has_links = True
                                _alog("복귀 후 결제링크 발견 → 팝업 스캔 진행")
                                _dbg("복귀 후 결제링크 발견 → 팝업 스캔 진행")
                                if _scan_payment_link_popups_and_sync(
                                    driver,
                                    allow_popup_for_non_expired=active_for_popup,
                                ):
                                    had_new = True
                            else:
                                _alog("복귀 후에도 결제링크 없음 → 10분 대기 예정")
                                _dbg("복귀 후에도 결제링크 없음 → 10분 대기로 전환")
                        else:
                            _dbg("/payment-link 복귀 실패 (다음 루프에서 재시도)")
                    except Exception as e_retry:
                        _dbg(f"결제링크 복귀 재확인 중 예외: {e_retry!r}")
                    if _runtime_guard():
                        break

            except Exception as e:
                try:
                    cur_url = driver.current_url
                except Exception:
                    cur_url = "unknown (invalid session)"

                if isinstance(e, RuntimeError) and "[NAV]" in str(e):
                    print(f"[crawler][ERROR] 내비게이션 오류로 크롤링을 중단합니다: {e}")
                    _alog(f"[ERROR] 내비게이션 오류로 중단: {e}")
                    _dbg(f"내비게이션 RuntimeError 발생: {e!r}, 현재 URL={cur_url}")
                    break

                print(f"[crawler][WARN] 크롤링 중 오류: {e}")
                _alog(f"[WARN] 크롤링 중 오류: {e}")
                _dbg(f"크롤링 중 예외 발생: {e!r}, 현재 URL={cur_url}, 재로그인 시도")
                try:
                    _simple_sign_in(driver)
                except Exception as e2:
                    print(f"[crawler][ERROR] 재로그인 시도 중 오류: {e2}")
                    _alog(f"[ERROR] 재로그인 시도 중 오류: {e2}")
                    _dbg(f"재로그인 단계에서 예외 발생: {e2!r}")

            if stop_by_runtime:
                break

            cycle += 1

            if max_cycles > 0 and cycle >= max_cycles:
                msg = f"테스트 종료: 최대 사이클 도달 ({cycle}/{max_cycles})"
                print(f"[crawler] {msg}")
                _alog(msg)
                break

            # 대기 시간 결정:
            # - 결제링크가 있거나, 신규 거래 발생, 활성 세션이 있으면: 4~7초 짧은 주기
            # - 결제링크도 없고 신규 거래도 없으면: 10분 대기 (wakeup 요청 시 즉시 재개)
            active = _has_active_sessions(window_minutes=10)
            if had_new or has_links or active:
                delay = random.randint(4, 7)
            else:
                delay = 600
            print(
                f"[crawler] 다음 크롤링까지 {delay}초 대기 "
                f"(has_links={has_links}, had_new={had_new}, active_sessions={active})"
            )
            _alog(
                f"다음 크롤링까지 {delay}초 대기 "
                f"(has_links={has_links}, had_new={had_new}, active_sessions={active})"
            )
            _wait_with_wakeup(delay)

            now_ts = time.time()
            if backup_interval > 0 and now_ts - last_backup_ts >= backup_interval:
                print(f"[crawler] 백업 주기({backup_interval}s) 도달 - 크롤링 주기 정상 동작 확인.")
                _alog(f"백업 주기({backup_interval}s) 도달 - 크롤링 주기 정상")
                last_backup_ts = now_ts
    finally:
        _alog("크롤러 종료 (driver.quit)")
        _touch_heartbeat()
        try:
            driver.quit()
        except Exception as e:
            # 드라이버 세션이 이미 종료된 경우는 정상 종료로 간주
            print(f"[crawler][WARN] driver.quit 종료 처리 중 예외(무시): {e}")


def _parse_args() -> tuple[int, int]:
    p = argparse.ArgumentParser(description="K-VAN 크롤러 실행")
    p.add_argument(
        "--max-cycles",
        type=int,
        default=int(os.environ.get("K_VAN_CRAWLER_MAX_CYCLES", "0")),
        help="테스트용 최대 사이클 수 (0=무제한)",
    )
    p.add_argument(
        "--max-seconds",
        type=int,
        default=int(os.environ.get("K_VAN_CRAWLER_MAX_SECONDS", "0")),
        help="테스트용 최대 실행 시간(초) (0=무제한)",
    )
    args = p.parse_args()
    return max(0, args.max_cycles), max(0, args.max_seconds)


if __name__ == "__main__":
    mc, ms = _parse_args()
    run_crawler_loop(max_cycles=mc, max_runtime_sec=ms)

