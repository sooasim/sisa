import os
import time

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, InvalidSessionIdException

from auto_kvan import (
    _is_server_env,
    create_driver,
    _scrape_dashboard_and_store,
    _scrape_transactions_and_store,
    _scrape_payment_links_and_store,
    _sync_kvan_to_transactions,
    _scan_payment_link_popups_and_sync,
    _has_active_sessions,
    SIGN_IN_URL,
    SIGN_IN_SELECTORS,
    PIN_POPUP_SELECTORS,
    LOCAL_TEST,
    _go_to_payment_link,
    _has_payment_links_quick,
    WAKEUP_FLAG_PATH,
)


# 로컬 테스트용 디버그 플래그 (기본 ON: "1")
DEBUG_CRAWLER = os.environ.get("K_VAN_DEBUG", "1") == "1"


def _dbg(msg: str) -> None:
    """로컬 디버깅용 상세 로그 출력."""
    if DEBUG_CRAWLER:
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[crawler][DEBUG {now}] {msg}")


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

    # 2026-03: PIN 입력 단계가 사라짐 → 더 이상 PIN 팝업을 기다리지 않고 바로 리다이렉트만 확인

    # 리다이렉트 확인 (최대 2초)
    try:
        t0_redirect = time.time()
        WebDriverWait(driver, 2).until(
            EC.url_contains("store.k-van.app")
        )
        _dbg(f"로그인 후 URL 리다이렉트 감지 (elapsed={time.time() - t0_redirect:.2f}s)")
    except TimeoutException:
        print("[WARN][crawler] 로그인 후 리다이렉트 URL 확인 실패(계속 진행).")
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


def run_crawler_loop() -> None:
    """
    링크/거래/대시보드 상태 추적용 크롤러 메인 루프.
    - 링크 생성/결제는 auto_kvan.py 가 담당
    - 이 크롤러는 3~5초 주기로 /dashboard, /transactions, /payment-link 를 순회하며
      DB(kvan_dashboard, kvan_transactions, kvan_links)를 갱신
    - 추가로, 환경 변수 K_VAN_CRAWL_INTERVAL 이 600 이상으로 지정되어 있으면
      그 주기를 "백업용 최소 간격" 으로 사용 (3~5초 추적은 그대로 유지)
    """
    import random

    driver = create_driver(headless=_is_server_env())
    try:
        print("[crawler] K-VAN 로그인 시작")
        _simple_sign_in(driver)
        print("[crawler] 로그인 완료. 주기 크롤링 루프 시작.")
        if LOCAL_TEST:
            print("[crawler] LOCAL_TEST 모드: DB 관련 쓰기는 auto_kvan 에서 모두 건너뜁니다.")
        _dbg(f"로그인 직후 current_url={driver.current_url}")

        # 로그인 직후에는 대시보드 크롤링을 하지 말고,
        # 바로 결제링크 관리 화면으로 한 번 이동해서 권한/세션을 확정시킨다.
        try:
            _dbg("로그인 직후 결제링크 관리 페이지로 직접 이동 시도 (/payment-link, 다중 재시도 포함)")
            if not _go_to_payment_link(driver):
                _dbg("로그인 직후 /payment-link 진입 실패 (다음 루프에서 다시 시도 예정)")
            else:
                _dbg(f"결제링크 관리 첫 진입 성공, URL={driver.current_url}")
        except Exception as e:
            _dbg(f"결제링크 관리 첫 진입 중 예외 발생: {e!r} (다음 루프에서 재시도)")

        # 빠른 추적(4~7초) + 선택적 백업 간격
        backup_interval = int(os.environ.get("K_VAN_CRAWL_INTERVAL", "600"))
        last_backup_ts = 0.0
        cycle = 0
        empty_cycles = 0  # 연속으로 "링크 없음"으로 판정된 사이클 수

        while True:
            loop_start = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"[crawler] 크롤링 사이클 시작: {loop_start}")
            _dbg(f"사이클 {cycle} 시작, 현재 URL={driver.current_url}")

            had_new = False

            try:
                # 2026-03: 대시보드 화면이 '권한 확인 중...' 스피너와 리다이렉트 구조로 바뀌어
                # 자주 멈추는 문제가 있어, 크롤러에서는 대시보드 크롤링을 건너뛰고
                # 결제링크 관리 → 거래내역 순으로만 처리한다 (로컬 테스트 우선).
                _dbg("대시보드 크롤링은 건너뜀 (결제링크 관리 → 거래내역 순으로 처리)")

                # 1) 결제링크 관리 리스트 크롤링 (항상 새로고침/이동)
                t_links = time.time()
                _dbg("결제링크 목록 크롤링 시작 (_scrape_payment_links_and_store)")
                _scrape_payment_links_and_store(driver)
                _dbg(f"결제링크 목록 크롤링 종료 (elapsed={time.time() - t_links:.2f}s, url={driver.current_url})")

                # 1-1) 현재 결제링크가 화면에 남아 있는지 간단히 확인
                # (팝업 스캔 여부를 결정하는 용도, empty_cycles 업데이트는 루프 끝에서 수행)
                has_links = _has_payment_links_quick(driver)

                # 2) 결제링크 관리 화면의 '거래 내역' 팝업을 통해
                #    세션ID/금액/승인번호/카드번호 정보를 내부 DB와 세션에 반영
                #    (링크가 있을 때만 의미가 있으므로, 링크가 전혀 없으면 스킵)
                if has_links:
                    t_popup = time.time()
                    _dbg("결제링크 팝업 스캔 시작 (_scan_payment_link_popups_and_sync)")
                    if _scan_payment_link_popups_and_sync(driver):
                        had_new = True
                        _dbg("결제링크 팝업 스캔에서 신규 변화 감지 (had_new=True)")
                    _dbg(f"결제링크 팝업 스캔 종료 (elapsed={time.time() - t_popup:.2f}s, url={driver.current_url})")

                # 3) 결제 및 취소 내역(/transactions)으로 이동해서 새로고침 후
                #    추가 승인/취소 내역을 DB(kvan_transactions)에 반영
                t_tx = time.time()
                _dbg("거래내역 크롤링 시작 (_scrape_transactions_and_store)")
                _scrape_transactions_and_store(driver)
                _dbg(f"거래내역 크롤링 종료 (elapsed={time.time() - t_tx:.2f}s, url={driver.current_url})")

                # 4) K-VAN 테이블(kvan_transactions, kvan_links) → 내부 transactions 매핑/갱신
                t_sync = time.time()
                _dbg("K-VAN → 내부 transactions 동기화 시작 (_sync_kvan_to_transactions)")
                if _sync_kvan_to_transactions():
                    had_new = True
                    _dbg("K-VAN → transactions 동기화에서 신규/갱신 발생 (had_new=True)")
                _dbg(f"K-VAN → transactions 동기화 종료 (elapsed={time.time() - t_sync:.2f}s)")

                # 5) 한 사이클을 마무리하기 전에 다시 결제링크 관리 화면으로 돌아와
                #    새로고침 후 "링크 존재 여부"를 최종적으로 한 번 더 확인한다.
                try:
                    if _go_to_payment_link(driver):
                        driver.refresh()
                        has_links_end = _has_payment_links_quick(driver)
                        if has_links_end:
                            empty_cycles = 0
                            _dbg("사이클 종료 시점: 결제링크가 하나 이상 존재 → empty_cycles=0 으로 리셋")
                        else:
                            empty_cycles += 1
                            _dbg(f"사이클 종료 시점: 결제링크 없음으로 판정 → empty_cycles={empty_cycles}")
                    else:
                        _dbg("사이클 종료 시점: /payment-link 로 돌아오지 못함(다음 루프에서 재시도)")
                except Exception as e_nav_end:
                    _dbg(f"사이클 종료 시점 결제링크 재확인 중 예외: {e_nav_end!r}")
            except Exception as e:
                # 내비게이션 관련 치명적 오류(RuntimeError)는 "여기서 멈추고" 디버그를 남긴 뒤 루프를 종료한다.
                try:
                    cur_url = driver.current_url
                except Exception:
                    cur_url = "unknown (invalid session)"

                if isinstance(e, RuntimeError) and "[NAV]" in str(e):
                    print(f"[crawler][ERROR] 내비게이션 오류로 크롤링을 중단합니다: {e}")
                    _dbg(f"내비게이션 RuntimeError 발생: {e!r}, 현재 URL={cur_url}")
                    break

                print(f"[crawler][WARN] 크롤링 중 오류: {e}")
                _dbg(f"크롤링 중 예외 발생: {e!r}, 현재 URL={cur_url}, 재로그인 시도")
                # 에러 발생 시 재로그인 시도 후 다음 루프에서 재시도
                try:
                    _simple_sign_in(driver)
                except Exception as e2:
                    print(f"[crawler][ERROR] 재로그인 시도 중 오류: {e2}")
                    _dbg(f"재로그인 단계에서 예외 발생: {e2!r}")

            cycle += 1

            # 활성 세션/신규 변화/링크 존재 여부에 따라 대기 시간 결정
            # - 링크가 없고(empty_cycles>=3) 최근 3사이클 연속으로 비어 있으면: 10분에 한 번만 체크
            # - 그 외에는 기존 로직과 동일하게 4~7초 주기 유지
            active = _has_active_sessions(window_minutes=10)
            if empty_cycles >= 3:
                delay = 600
            elif active or had_new:
                delay = random.randint(4, 7)
            else:
                delay = 600
            print(
                f"[crawler] 다음 크롤링까지 {delay}초 대기 "
                f"(active_sessions={active}, had_new={had_new}, empty_cycles={empty_cycles})"
            )
            _wait_with_wakeup(delay)

            # backup_interval 이상 지나면, 타임스탬프만 갱신
            # (지금은 동일한 크롤링을 수행하므로 별도의 추가 작업은 필요 없음)
            now_ts = time.time()
            if backup_interval > 0 and now_ts - last_backup_ts >= backup_interval:
                print(f"[crawler] 백업 주기({backup_interval}s) 도달 - 크롤링 주기 정상 동작 확인.")
                last_backup_ts = now_ts
    finally:
        driver.quit()


if __name__ == "__main__":
    run_crawler_loop()

