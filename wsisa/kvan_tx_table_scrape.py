# -*- coding: utf-8 -*-
"""
K-VAN /transactions 테이블 → 스냅샷 행 추출 (kvan_crawler / auto_kvan 공통).

구버전: tbody 우선 대기 + thead 첫 줄 단순 .text 헤더
신버전: th innerHTML 기반 infer (placeholder 등)
→ 둘 다 시도해 유효 스냅샷이 나온 첫 전략을 채택.
"""
from __future__ import annotations

import os
import re
import time
from datetime import datetime

from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from kvan_link_common import (
    build_kvan_transactions_snapshots,
    infer_kvan_transaction_header_cell_label,
)


def _cell_txt(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\n", " ").strip())


def _score_header_labels(txts: list[str]) -> int:
    joined = " ".join(txts)
    score = len([x for x in txts if (x or "").strip()]) * 2
    if "승인번호" in joined:
        score += 80
    if "결제 금액" in joined or "결제금액" in joined:
        score += 40
    if "거래일시" in joined or "등록일" in joined:
        score += 35
    if "거래 유형" in joined or "거래유형" in joined:
        score += 25
    if "결제 유형" in joined or "결제유형" in joined:
        score += 30
    if "MID" in joined:
        score += 10
    return score


def _collect_infer_header_candidates(driver: webdriver.Chrome) -> list[list[str]]:
    header_candidates: list[list[str]] = []
    for hr in driver.find_elements(By.XPATH, "//table//thead//tr"):
        try:
            cells = hr.find_elements(By.XPATH, ".//th|.//td")
            txts: list[str] = []
            for c in cells:
                try:
                    html = c.get_attribute("innerHTML") or ""
                    lab = infer_kvan_transaction_header_cell_label(html)
                    if not (lab or "").strip():
                        lab = _cell_txt(c.text)
                    txts.append(lab if (lab or "").strip() else "")
                except Exception:
                    txts.append("")
            if any((x or "").strip() for x in txts):
                header_candidates.append(txts)
        except Exception:
            continue
    return header_candidates


def _simple_headers_tr1(driver: webdriver.Chrome) -> list[str]:
    cells = driver.find_elements(By.XPATH, "//table//thead//tr[1]//th")
    if not cells:
        cells = driver.find_elements(By.XPATH, "//table//thead//tr[1]//td")
    return [_cell_txt(c.text) for c in cells]


def _simple_headers_best_row(driver: webdriver.Chrome) -> list[str]:
    best: list[str] = []
    best_sc = -1
    for hr in driver.find_elements(By.XPATH, "//table//thead//tr"):
        try:
            cells = hr.find_elements(By.XPATH, ".//th|.//td")
            txts = [_cell_txt(c.text) for c in cells]
            sc = _score_header_labels(txts)
            if sc > best_sc:
                best_sc, best = sc, txts
        except Exception:
            continue
    return best


def _collect_body_rows(driver: webdriver.Chrome) -> list[list[str]]:
    body_rows: list[list[str]] = []
    for tr in driver.find_elements(By.XPATH, "//table//tbody//tr"):
        try:
            cells = tr.find_elements(By.XPATH, ".//td")
            texts = [_cell_txt(c.text) for c in cells]
            if not any(texts):
                continue
            body_rows.append(texts)
        except StaleElementReferenceException:
            continue
        except Exception as e_row:
            print(f"[WARN] 거래내역 행 읽기 오류: {e_row}")
    return body_rows


def extract_kvan_transactions_from_page(
    driver: webdriver.Chrome,
    *,
    navigate: bool = True,
) -> tuple[
    list[dict],
    list[list[str]],
    str,
    list[tuple[str, list[str]]],
    list[str],
]:
    """
    현재 또는 /transactions 페이지에서 테이블을 읽어 스냅샷 행을 만든다.

    Returns:
        snapshot_rows, body_rows, used_header_label, header_attempts, h_tr1_debug
    """
    if navigate:
        if "transactions" in (driver.current_url or ""):
            try:
                driver.refresh()
            except Exception:
                pass
        else:
            driver.get("https://store.k-van.app/transactions")

    try:
        time.sleep(0.25)
        wait_body_sec = int(os.environ.get("K_VAN_TX_WAIT_BODY_SEC", "20"))
        WebDriverWait(driver, max(8, wait_body_sec)).until(
            EC.presence_of_element_located((By.XPATH, "//table//tbody//tr"))
        )
        try:
            driver.execute_script(
                "window.scrollTo(0, Math.max(document.body.scrollHeight, document.documentElement.scrollHeight));"
            )
            time.sleep(0.35)
        except Exception:
            pass
        for _ in range(15):
            if driver.find_elements(By.XPATH, "//table//thead//th"):
                break
            time.sleep(0.2)
    except TimeoutException:
        return [], [], "timeout", [], []

    body_rows = _collect_body_rows(driver)
    captured_iso = datetime.utcnow().isoformat()
    h_tr1 = _simple_headers_tr1(driver)

    header_attempts: list[tuple[str, list[str]]] = []
    if any((x or "").strip() for x in h_tr1):
        header_attempts.append(("legacy_thead_tr1_text", h_tr1))
    h_best = _simple_headers_best_row(driver)
    if h_best and h_best != h_tr1:
        header_attempts.append(("legacy_thead_best_text", h_best))

    infer_cands = _collect_infer_header_candidates(driver)
    infer_sorted = sorted(infer_cands, key=_score_header_labels, reverse=True)
    for i, cand in enumerate(infer_sorted[:6]):
        header_attempts.append((f"infer_row_rank{i}", cand))

    snapshot_rows: list[dict] = []
    used_label = ""
    for label, headers in header_attempts:
        if not headers:
            continue
        snap = build_kvan_transactions_snapshots(
            headers, body_rows, captured_iso=captured_iso
        )
        if snap:
            snapshot_rows = snap
            used_label = label
            break

    return snapshot_rows, body_rows, used_label, header_attempts, h_tr1
