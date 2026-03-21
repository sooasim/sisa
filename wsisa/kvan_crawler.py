# -*- coding: utf-8 -*-
"""
K-VAN 통합 실행 파일
- 로컬: JSON 저장소 사용
- 서버: Railway MySQL 사용
- 기본 실행 모드: crawl
- create 모드: 결제 링크 생성
- crawl 모드: 로그인 → (대시보드 요약 DB) → /payment-link → 만료+거래없음 즉시 삭제 → 링크/거래 동기화

스케줄(로컬/서버 동일, 환경변수로 조절):
    K_VAN_IDLE_SLEEP_SEC         장시간 대기(기본 180)
    K_VAN_ACTIVE_SLEEP_SEC       활성/신규 거래 시(기본 2)
    K_VAN_MEDIUM_SLEEP_SEC       중간(기본 30)
    K_VAN_STARTUP_FAST_CYCLES    초반 빠른 사이클 횟수(기본 3)
    K_VAN_STARTUP_SLEEP_SEC      초반 대기(기본 2)
    K_VAN_ACTIVE_SESSION_WINDOW_MINUTES  '최근 세션' 판정 창(기본 3, 예전 10분은 과도)
    K_VAN_POPUP_SESSION_WINDOW_MINUTES   팝업 허용용(기본 30)
배포 환경 감지: RAILWAY_ENVIRONMENT, RUN_HEADLESS, SISA_SERVER=1, K_VAN_SERVER=1

실행:
    python kvan_crawler.py
    python kvan_crawler.py --mode crawl
    python kvan_crawler.py --mode create --session-id 202603180001
"""

from __future__ import annotations

import os
import re
import json
import time
import argparse
import hashlib
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, parse_qs, unquote, quote
from typing import Optional, List, Dict, Any

import pymysql
from kvan_link_common import (
    append_payment_notification,
    ensure_kvan_links_internal_session_column,
    ensure_kvan_links_link_created_at,
    extract_kvan_session_key_from_url,
    load_kvan_link_preserved_by_url,
    parse_amount_won,
    parse_kvan_link_ui_created_at,
    upsert_kvan_link_creation_seed,
)
from kvan_tx_table_scrape import extract_kvan_transactions_from_page
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
    ElementClickInterceptedException,
)

# =========================================================
# 환경 / 경로
# =========================================================

FILE_DIR = Path(__file__).resolve().parent
# web_form.py / auto_kvan.py 와 동일하게 리포지토리 루트의 data/ 를 쓴다.
# (예전: wsisa/data 만 쓰면 로컬에서 crawler_wakeup.flag 가 웹과 달라 크롤이 안 깨어남)
PROJECT_ROOT = FILE_DIR.parent

_raw_data_dir = os.environ.get("SISA_DATA_DIR", "").strip()
if _raw_data_dir:
    DATA_DIR = Path(_raw_data_dir)
else:
    app_data = Path("/app/data")
    if app_data.exists():
        DATA_DIR = app_data
    else:
        DATA_DIR = PROJECT_ROOT / "data"

DATA_DIR.mkdir(parents=True, exist_ok=True)

ADMIN_LOG_PATH = DATA_DIR / "hq_logs.log"
WAKEUP_FLAG_PATH = DATA_DIR / "crawler_wakeup.flag"
HEARTBEAT_PATH = DATA_DIR / "kvan_crawler.heartbeat"
PAYMENT_NOTIFICATIONS_PATH = DATA_DIR / "payment_notifications.json"
TRACE_LOG_PATH = DATA_DIR / "kvan_trace.log"

ORDER_JSON_PATH = DATA_DIR / "current_order.json"
RESULT_JSON_PATH = DATA_DIR / "last_result.json"

SESSION_ORDER_DIR = DATA_DIR / "sessions" / "orders"
SESSION_RESULT_DIR = DATA_DIR / "sessions" / "results"
SESSION_ORDER_DIR.mkdir(parents=True, exist_ok=True)
SESSION_RESULT_DIR.mkdir(parents=True, exist_ok=True)

ADMIN_STATE_PATH = DATA_DIR / "admin_state.json"
# 본사 HQ 어드민 "만료된 결제 링크 (거래 내역 있음)" 섹션 데이터 (web_form.py 와 동일 파일명)
EXPIRED_WITH_TRANSACTIONS_PATH = DATA_DIR / "expired_with_transactions.json"
LOCAL_DB_DIR = DATA_DIR / "local_db"
LOCAL_DB_DIR.mkdir(parents=True, exist_ok=True)

DEBUG_CRAWLER = os.environ.get("K_VAN_DEBUG", "1") == "1"
TRACE_CRAWLER = os.environ.get("K_VAN_TRACE", "1") == "1"


def _build_mysql_url_from_env() -> str:
    host = os.environ.get("MYSQLHOST") or os.environ.get("MYSQL_HOST") or "localhost"
    port = os.environ.get("MYSQLPORT") or os.environ.get("MYSQL_PORT") or "3306"
    user = os.environ.get("MYSQLUSER") or os.environ.get("MYSQL_USER") or "root"
    password = os.environ.get("MYSQLPASSWORD") or os.environ.get("MYSQL_PASSWORD") or ""
    db = (
        os.environ.get("MYSQL_DATABASE")
        or os.environ.get("MYSQLDATABASE")
        or os.environ.get("MYSQL_DB")
        or "railway"
    )
    return f"mysql://{quote(str(user))}:{quote(str(password))}@{host}:{port}/{db}"


DATABASE_URL = (
    os.environ.get("DATABASE_URL")
    or os.environ.get("MYSQL_URL")
    or _build_mysql_url_from_env()
)


def _is_server_env() -> bool:
    """Railway·Docker·헤드리스 등 배포 환경 감지 (미설정 시 로컬로 간주)."""
    s = str(os.environ.get("SISA_SERVER", "")).strip().lower()
    k = str(os.environ.get("K_VAN_SERVER", "")).strip().lower()
    truthy = ("1", "true", "yes", "y", "on")
    mysql_host = str(
        os.environ.get("MYSQLHOST")
        or os.environ.get("MYSQL_HOST")
        or ""
    ).strip().lower()
    return bool(
        os.environ.get("RAILWAY_ENVIRONMENT")
        or os.environ.get("RAILWAY_PRIVATE_DOMAIN")
        or os.environ.get("RAILWAY_TCP_PROXY_DOMAIN")
        or os.environ.get("RUN_HEADLESS")
        or mysql_host.endswith(".railway.internal")
        or "railway" in mysql_host
        or s in truthy
        or k in truthy
    )


_local_flag = os.environ.get("SISA_LOCAL_TEST")
_json_flag = os.environ.get("K_VAN_USE_JSON")
if _local_flag is not None:
    LOCAL_TEST = _local_flag.strip().lower() in ("1", "true", "yes", "y")
elif _json_flag is not None:
    LOCAL_TEST = _json_flag.strip().lower() in ("1", "true", "yes", "y", "on")
else:
    # 기본 정책: 로컬은 JSON(임시), 서버는 DB.
    LOCAL_TEST = not _is_server_env()


def _use_json_store() -> bool:
    return bool(LOCAL_TEST)


# =========================================================
# 로그 / 상태
# =========================================================

def _append_admin_log(source: str, message: str) -> None:
    try:
        ADMIN_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(ADMIN_LOG_PATH, "a", encoding="utf-8") as f:
            ts = datetime.utcnow().isoformat()
            f.write(f"{ts} [{source}] {message}\n")
    except Exception:
        pass


def _dbg(msg: str) -> None:
    if DEBUG_CRAWLER:
        print(f"[crawler][DEBUG {time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def _alog(msg: str) -> None:
    _append_admin_log("CRAWLER", msg)


def _trace(step: str, **fields) -> None:
    """
    거래내역 크롤링 디버깅용 상세 트레이스.
    K_VAN_TRACE=1 이면 hq_logs.log + kvan_trace.log 에 기록한다.
    """
    if not TRACE_CRAWLER:
        return
    parts = []
    for k, v in fields.items():
        try:
            parts.append(f"{k}={v}")
        except Exception:
            parts.append(f"{k}=<unrepr>")
    line = f"[TRACE] {step}" + (f" | {' | '.join(parts)}" if parts else "")
    _alog(line)
    try:
        TRACE_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(TRACE_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(f"{datetime.utcnow().isoformat()} {line}\n")
    except Exception:
        pass


def _touch_heartbeat() -> None:
    try:
        HEARTBEAT_PATH.parent.mkdir(parents=True, exist_ok=True)
        HEARTBEAT_PATH.write_text(str(time.time()), encoding="utf-8")
    except Exception:
        pass


def signal_crawler_wakeup() -> None:
    try:
        WAKEUP_FLAG_PATH.parent.mkdir(parents=True, exist_ok=True)
        WAKEUP_FLAG_PATH.write_text(time.strftime("%Y-%m-%d %H:%M:%S"), encoding="utf-8")
        _dbg(f"wakeup flag 생성: {WAKEUP_FLAG_PATH}")
    except Exception as e:
        print(f"[WAKEUP][WARN] wakeup flag 생성 실패: {e}")


def _data_dir_candidates() -> list[Path]:
    candidates = [
        DATA_DIR,
        FILE_DIR / "data",
        Path("/app/data"),
    ]
    uniq: list[Path] = []
    seen: set[str] = set()
    for p in candidates:
        k = str(p)
        if k not in seen:
            uniq.append(p)
            seen.add(k)
    return uniq


def _admin_state_candidates() -> list[Path]:
    return [d / "admin_state.json" for d in _data_dir_candidates()]


def _resolved_admin_state_path() -> Path:
    for p in _admin_state_candidates():
        if p.exists():
            return p
    return ADMIN_STATE_PATH


def _load_admin_state() -> dict:
    for p in _admin_state_candidates():
        if p.exists():
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
                _trace("admin_state_load", path=str(p), sessions=len(data.get("sessions") or []), history=len(data.get("history") or []))
                return data
            except Exception:
                pass
    return {"sessions": [], "history": []}


def _save_admin_state(state: dict) -> None:
    p = _resolved_admin_state_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _link_matches_kvan_session_id(link: str, session_id: str) -> bool:
    """
    kvan_link 와 session_id(KEY… 또는 변형) 대조. 여러 방식 중 하나만 통과하면 True.
    (부분 문자열, parse_qs, KEY~&type=KEYED 앞 추출, /p/KEY…, 전체 KEY 토큰, 접두어 생략 변형)
    """
    if not link or not session_id:
        return False
    sid = str(session_id).strip()
    link_raw = link.strip()
    if not sid or not link_raw:
        return False
    sid_l = sid.lower()
    link_l = link_raw.lower()

    def _eq(a: str, b: str) -> bool:
        aa = unquote(str(a).strip())
        bb = unquote(str(b).strip())
        return aa == bb or aa.lower() == bb.lower()

    if sid in link_raw or sid_l in link_l:
        return True

    try:
        q = parse_qs(urlparse(link_raw).query)
        for v in q.get("sessionId") or []:
            if _eq(v, sid):
                return True
    except Exception:
        pass

    for pat in (
        r"sessionId=(KEY[^&]+?)&type=KEYED",
        r"sessionid=(KEY[^&]+?)&type=KEYED",
        r"sessionId=(KEY[^&]+?)(?:&|$)",
    ):
        m = re.search(pat, link_raw, re.IGNORECASE)
        if m and _eq(m.group(1), sid):
            return True

    m = re.search(r"/p/(KEY[A-Za-z0-9]+)", link_raw, re.IGNORECASE)
    if m and _eq(m.group(1), sid):
        return True

    for tok in re.findall(r"KEY[A-Za-z0-9]+", link_raw, re.IGNORECASE):
        if _eq(tok, sid):
            return True

    if sid_l.startswith("key") and len(sid_l) > 3:
        suffix = sid_l[3:]
        for tok in re.findall(r"KEY([A-Za-z0-9]+)", link_raw, re.IGNORECASE):
            if tok.lower() == suffix:
                return True
    elif not sid_l.startswith("key") and re.match(r"^[A-Za-z0-9]+$", sid):
        for tok in re.findall(r"KEY([A-Za-z0-9]+)", link_raw, re.IGNORECASE):
            if tok.lower() == sid_l:
                return True

    return False


def _get_agency_id_for_session(session_id: str) -> Optional[str]:
    """
    admin_state 에서 session_id 에 해당하는 agency_id 반환.
    내부 id 정확 일치 후, 진행 세션 kvan_link 매칭 → 히스토리 (auto_kvan 과 동일).
    """
    if not session_id:
        return None
    try:
        st = _load_admin_state()
        sid = str(session_id).strip()
        sessions = st.get("sessions") or []
        history = st.get("history") or []

        def _aid_from(s: dict) -> Optional[str]:
            a = (s.get("agency_id") or "").strip()
            return a or None

        for s in sessions + history:
            if str(s.get("id") or "").strip() == sid:
                return _aid_from(s)
        for s in sessions:
            link = (s.get("kvan_link") or "").strip()
            if link and _link_matches_kvan_session_id(link, sid):
                return _aid_from(s)
        for s in history:
            link = (s.get("kvan_link") or "").strip()
            if link and _link_matches_kvan_session_id(link, sid):
                return _aid_from(s)
        return None
    except Exception:
        return None


def _extract_primary_kvan_key_from_tx_raw(raw_text: str) -> Optional[str]:
    rt = (raw_text or "").strip()
    if not rt:
        return None
    for pat in (
        r"sessionId[=:]\s*(KEY[0-9A-Za-z]+)",
        r"sessionid[=:]\s*(KEY[0-9A-Za-z]+)",
    ):
        m = re.search(pat, rt, re.I)
        if m:
            return m.group(1)
    m = re.search(r"/p/(KEY[0-9A-Za-z]+)", rt, re.I)
    if m:
        return m.group(1)
    keys = re.findall(r"(KEY[0-9A-Za-z]+)", rt, re.I)
    if not keys:
        return None
    uniq: list[str] = []
    for k in keys:
        if k not in uniq:
            uniq.append(k)
    return max(uniq, key=len)


def _resolve_agency_id_for_kvan_tx_row(raw_text: str, cur) -> tuple[Optional[str], str]:
    key = _extract_primary_kvan_key_from_tx_raw(raw_text)
    if not key:
        return None, ""
    aid = _get_agency_id_for_session(key)
    if aid:
        return aid, key
    try:
        cur.execute(
            """
            SELECT agency_id FROM kvan_links
            WHERE kvan_link LIKE %s OR kvan_session_id LIKE %s
               OR internal_session_id = %s
            """,
            (f"%{key}%", f"%{key}%", key),
        )
        rows = cur.fetchall() or []
        if len(rows) == 1:
            db_ag = str(rows[0].get("agency_id") or "").strip()
            return (db_ag if db_ag else None), key
        ags = [
            str(r.get("agency_id") or "").strip()
            for r in rows
            if str(r.get("agency_id") or "").strip()
        ]
        if ags and len(set(ags)) == 1:
            return ags[0], key
    except Exception:
        pass
    return (aid or None), key


def _load_valid_agency_ids(cur) -> set[str]:
    try:
        cur.execute("SELECT id FROM agencies")
        rows = cur.fetchall() or []
        return {str(r.get("id") or "").strip() for r in rows if str(r.get("id") or "").strip()}
    except Exception:
        return set()


def _sanitize_agency_id_for_fk(
    agency_id: Optional[str],
    valid_agency_ids: set[str],
    *,
    stage: str,
    hint: str = "",
) -> Optional[str]:
    ag = str(agency_id or "").strip()
    if not ag:
        return None
    if ag in valid_agency_ids:
        return ag
    _trace("sync_invalid_agency", stage=stage, hint=hint, agency_id=ag)
    return None


def _resolve_agency_id_by_kvan_key_db(cur, session_key: str) -> Optional[str]:
    key = (session_key or "").strip()
    if not key:
        return None
    try:
        cur.execute(
            """
            SELECT agency_id
            FROM kvan_links
            WHERE kvan_session_id = %s
               OR kvan_link LIKE %s
               OR internal_session_id = %s
            ORDER BY captured_at DESC
            LIMIT 20
            """,
            (key, f"%{key}%", key),
        )
        rows = cur.fetchall() or []
        ids = [
            str(r.get("agency_id") or "").strip()
            for r in rows
            if str(r.get("agency_id") or "").strip()
        ]
        ids = list(dict.fromkeys(ids))
        if len(ids) == 1:
            return ids[0]
    except Exception:
        pass
    return None


def _extract_session_id_from_tx_message(message: str) -> str:
    msg = str(message or "").strip()
    if not msg:
        return ""
    m = re.search(r"세션ID=([^,\)\s]+)", msg)
    if m:
        return str(m.group(1) or "").strip()
    m2 = re.search(r"(KEY[0-9A-Za-z]+)", msg, re.I)
    if m2:
        return str(m2.group(1) or "").strip()
    return ""


def _guess_open_session_id_for_success(
    amount: int,
    agency_id: Optional[str],
    reg_date: str = "",
) -> str:
    """
    transactions.message에 세션ID 힌트가 없을 때, admin_state의 '결제중' 세션에서
    금액/소유자(본사·대행사) 기준으로 단일 후보를 찾아 완료 처리에 사용한다.
    """
    try:
        st = _load_admin_state()
        sessions = st.get("sessions") or []
        target_amount = int(amount or 0)
        target_agency = str(agency_id or "").strip()
        candidates: list[str] = []
        all_open: list[str] = []
        relaxed_agency_candidates: list[str] = []
        for s in sessions:
            if not isinstance(s, dict):
                continue
            if _session_considered_terminal(s):
                continue
            if str(s.get("status") or "").strip() != "결제중":
                continue
            sid = str(s.get("id") or "").strip()
            if sid:
                all_open.append(sid)
            try:
                s_amt = int(str(s.get("amount") or "0").replace(",", "").strip() or "0")
            except Exception:
                s_amt = 0
            if target_amount > 0 and s_amt != target_amount:
                continue
            s_ag = str(s.get("agency_id") or "").strip()
            if target_agency and s_ag != target_agency:
                # agency_id가 잘못 매핑된 경우를 대비해 amount/date만 맞는 후보를 별도 수집
                if reg_date:
                    s_dt = _parse_session_datetime(s.get("created_at"))
                    if s_dt is not None and s_dt.strftime("%Y-%m-%d") > reg_date:
                        continue
                if sid:
                    relaxed_agency_candidates.append(sid)
                continue
            if not target_agency and s_ag:
                continue
            if reg_date:
                s_dt = _parse_session_datetime(s.get("created_at"))
                if s_dt is not None and s_dt.strftime("%Y-%m-%d") > reg_date:
                    continue
            if sid:
                candidates.append(sid)
        uniq = list(dict.fromkeys(candidates))
        if len(uniq) == 1:
            return uniq[0]
        # 2차 완화: agency_id 일치 조건만 완화했을 때 단일 후보면 채택
        relaxed = list(dict.fromkeys(relaxed_agency_candidates))
        if len(relaxed) == 1:
            return relaxed[0]
        # 3차 완화: 결제중 세션이 딱 1개면 해당 세션으로 확정
        all_open_uniq = list(dict.fromkeys(all_open))
        if len(all_open_uniq) == 1:
            return all_open_uniq[0]
    except Exception:
        pass
    return ""


def _lookup_internal_session_id_for_kvan_key(kvan_key: str) -> str:
    kk = (kvan_key or "").strip()
    if not kk:
        return ""
    try:
        st = _load_admin_state()
        for s in (st.get("sessions") or []) + (st.get("history") or []):
            link = (s.get("kvan_link") or "").strip()
            if link and _link_matches_kvan_session_id(link, kk):
                return str(s.get("id") or "").strip()
        return ""
    except Exception:
        return ""


def _normalize_session_id_for_admin_state(session_id: str) -> tuple[str, str]:
    """
    admin_state 세션 id는 내부세션(숫자)일 수 있고, K-VAN 화면은 KEY 토큰만 줄 수 있다.
    반환: (admin_state 조회용 session_id, kvan_key)
    """
    raw = (session_id or "").strip()
    if not raw:
        return "", ""
    key = ""
    m = re.search(r"(KEY[0-9A-Za-z]+)", raw, re.I)
    if m:
        key = m.group(1)
    if not key:
        return raw, ""
    internal = _lookup_internal_session_id_for_kvan_key(key)
    if internal:
        return internal, key
    return key, key


def _session_order_path_candidates(session_id: str) -> list[Path]:
    candidates = [d / "sessions" / "orders" / f"{session_id}.json" for d in _data_dir_candidates()]
    uniq: list[Path] = []
    seen: set[str] = set()
    for p in candidates:
        k = str(p)
        if k not in seen:
            uniq.append(p)
            seen.add(k)
    return uniq


def _upsert_history_by_session_id(history: list[dict], entry: dict) -> list[dict]:
    sid = str(entry.get("id") or "").strip()
    if not sid:
        return history

    merged_history: list[dict] = []
    merged_target: Optional[dict] = None

    for h in history or []:
        if str(h.get("id") or "").strip() == sid:
            if merged_target is None:
                merged_target = dict(h)
            else:
                merged_target.update(h)
        else:
            merged_history.append(h)

    if merged_target is None:
        merged_target = {}
    merged_target.update(entry)
    merged_history.append(merged_target)
    return merged_history


def _mark_session_checked(session_id: str, title: str, has_approval: bool) -> None:
    if not session_id:
        return
    try:
        target_session_id, kvan_key = _normalize_session_id_for_admin_state(session_id)
        session_lookup = target_session_id or (session_id or "")
        st = _load_admin_state()
        sessions = list(st.get("sessions") or [])
        history = list(st.get("history") or [])
        now_iso = datetime.utcnow().isoformat()

        if has_approval:
            remaining_sessions: list[dict] = []
            moved_session: Optional[dict] = None

            for s in sessions:
                if str(s.get("id") or "") == str(session_lookup):
                    moved_session = dict(s)
                else:
                    remaining_sessions.append(s)

            if moved_session is None:
                moved_session = {"id": session_lookup}
            if kvan_key:
                moved_session["kvan_session_id"] = kvan_key

            moved_session["status"] = "결제완료"
            moved_session["has_approval"] = True
            moved_session["checked_title"] = title
            moved_session["finished_at"] = moved_session.get("finished_at") or now_iso
            history = _upsert_history_by_session_id(history, moved_session)
            st["sessions"] = remaining_sessions
        else:
            st["sessions"] = sessions

        st["history"] = history
        _save_admin_state(st)
    except Exception as e:
        print(f"[WARN] _mark_session_checked 실패: {e}")


def _mark_session_deleted(session_id: str, title: str) -> None:
    try:
        target_session_id, kvan_key = _normalize_session_id_for_admin_state(session_id)
        session_lookup = target_session_id or (session_id or "")
        st = _load_admin_state()
        sessions = list(st.get("sessions") or [])
        history = list(st.get("history") or [])
        now_iso = datetime.utcnow().isoformat()

        remaining_sessions: list[dict] = []
        removed_session: Optional[dict] = None

        for s in sessions:
            if str(s.get("id") or "") == str(session_lookup):
                removed_session = dict(s)
            else:
                remaining_sessions.append(s)

        if removed_session is None:
            removed_session = {"id": session_lookup}
        if kvan_key:
            removed_session["kvan_session_id"] = kvan_key

        removed_session["status"] = "만료"
        removed_session["deleted"] = True
        removed_session["deleted_in_kvan"] = True
        removed_session["checked_title"] = title
        removed_session["deleted_at"] = now_iso
        removed_session["finished_at"] = removed_session.get("finished_at") or now_iso

        old_msg = str(removed_session.get("result_message") or "").strip()
        mark_msg = "만료 감지로 K-VAN 링크가 삭제되었습니다."
        removed_session["result_message"] = f"{old_msg}\n{mark_msg}".strip() if old_msg else mark_msg

        history = _upsert_history_by_session_id(history, removed_session)
        st["sessions"] = remaining_sessions
        st["history"] = history
        _save_admin_state(st)
        _trace(
            "mark_session_deleted",
            input_session_id=session_id,
            normalized_session_id=session_lookup,
            kvan_key=kvan_key,
        )
    except Exception as e:
        print(f"[WARN] _mark_session_deleted 실패: {e}")


def _mark_session_expired_with_transactions(session_id: str, title: str) -> None:
    """
    만료+거래있음: admin_state history 반영 + HQ 알림용 expired_with_transactions.json 기록.
    (auto_kvan.py 와 동일. 이전에는 크롤러가 JSON을 쓰지 않아 본사 어드민 목록이 항상 비었음.)
    """
    try:
        target_session_id, kvan_key = _normalize_session_id_for_admin_state(session_id)
        session_lookup = target_session_id or (session_id or "")
        st = _load_admin_state()
        sessions = list(st.get("sessions") or [])
        history = list(st.get("history") or [])
        now_iso = datetime.utcnow().isoformat()

        remaining_sessions: list[dict] = []
        moved: Optional[dict] = None

        for s in sessions:
            if str(s.get("id") or "") == str(session_lookup):
                moved = dict(s)
            else:
                remaining_sessions.append(s)

        if moved is None:
            moved = {"id": session_lookup}
        if kvan_key:
            moved["kvan_session_id"] = kvan_key

        agency_id = str(moved.get("agency_id") or "").strip()
        if not agency_id:
            try:
                agency_id = str(
                    _get_agency_id_for_session(kvan_key or session_lookup) or ""
                ).strip()
            except Exception:
                agency_id = ""

        moved["status"] = "만료"
        moved["has_transaction"] = True
        moved["deleted"] = False
        moved["deleted_in_kvan"] = False
        moved["checked_title"] = title
        moved["finished_at"] = moved.get("finished_at") or now_iso

        history = _upsert_history_by_session_id(history, moved)
        st["sessions"] = remaining_sessions
        st["history"] = history
        _save_admin_state(st)

        try:
            items: list[dict] = []
            if EXPIRED_WITH_TRANSACTIONS_PATH.exists():
                try:
                    items = json.loads(
                        EXPIRED_WITH_TRANSACTIONS_PATH.read_text(encoding="utf-8")
                    )
                except Exception:
                    items = []
            if not isinstance(items, list):
                items = []
            sid_key = str(session_id or "").strip()
            if sid_key:
                items = [
                    x
                    for x in items
                    if str(x.get("session_id") or "").strip() != sid_key
                ]
            items.append(
                {
                    "session_id": session_lookup,
                    "kvan_session_id": kvan_key,
                    "title": (title or "")[:200],
                    "agency_id": agency_id,
                    "finished_at": now_iso,
                    "seen": False,
                }
            )
            items = items[-200:]
            EXPIRED_WITH_TRANSACTIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
            EXPIRED_WITH_TRANSACTIONS_PATH.write_text(
                json.dumps(items, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            _append_admin_log(
                "CRAWLER",
                f"만료+거래있음 세션 기록 session_id={session_lookup}, key={kvan_key or '-'} (어드민 알림 JSON)",
            )
            _trace(
                "mark_session_expired_with_tx",
                input_session_id=session_id,
                normalized_session_id=session_lookup,
                kvan_key=kvan_key,
                agency_id=agency_id,
            )
        except Exception as e_json:
            print(f"[WARN] 만료+거래있음 목록(JSON) 저장 실패: {e_json}")
    except Exception as e:
        print(f"[WARN] _mark_session_expired_with_transactions 실패: {e}")


def _is_session_already_processed(session_id: str) -> bool:
    if not session_id:
        return False
    try:
        st = _load_admin_state()
        history = st.get("history") or []
        for h in history:
            if str(h.get("id")) == str(session_id):
                if h.get("has_approval") or h.get("deleted"):
                    return True
        return False
    except Exception as e:
        print(f"[WARN] _is_session_already_processed 실패: {e}")
        return False


def _parse_session_datetime(ts) -> datetime | None:
    """admin_state 시각 문자열 → naive UTC (utcnow 와 비교용)."""
    if not ts:
        return None
    try:
        s = str(ts).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except Exception:
        return None


def _session_considered_terminal(s: dict) -> bool:
    """완료·만료 등으로 '빠른 폴링' 대상이 아닌 세션."""
    if not isinstance(s, dict):
        return True
    if s.get("deleted") or s.get("has_approval"):
        return True
    st = str(s.get("status") or "").strip()
    if not st:
        return False
    low = st.lower()
    for token in (
        "완료",
        "만료",
        "취소",
        "실패",
        "종료",
        "삭제",
        "expired",
        "취소완료",
        "결제완료",
    ):
        if token in st or token in low:
            return True
    return False


def _has_active_sessions(window_minutes: int = 10) -> bool:
    """
    admin_state.json 기준 '지금은 자주 돌아야 하는가'.

    주의(서버 이슈 원인이었음):
    - status 미설정을 '결제중'으로 보면 세션이 하나만 있어도 영구적으로 active=True → 2초 폴링.
    - 생성 시각만 보고 10분 동안 active면 트래픽이 과도해짐.

    규칙:
    - 명시적으로 status == '결제중' 이고 종료 상태가 아닐 때만 True.
    - 그 외에는 created_at 이 window 이내이되, 종료/승인 처리된 세션은 제외.
    - history 는 명시적 '결제중' + 최근 생성일 때만 True.
    """
    try:
        st = _load_admin_state()
        sessions = st.get("sessions") or []
        history = st.get("history") or []
        cutoff = datetime.utcnow() - timedelta(minutes=window_minutes)

        for s in sessions:
            if _session_considered_terminal(s):
                continue
            if str(s.get("status") or "").strip() == "결제중":
                dt = _parse_session_datetime(s.get("created_at"))
                if dt is not None and dt >= cutoff:
                    return True

        for s in sessions:
            if _session_considered_terminal(s):
                continue
            dt = _parse_session_datetime(s.get("created_at"))
            if dt is None:
                continue
            if dt >= cutoff:
                return True

        for h in history:
            if _session_considered_terminal(h):
                continue
            if str(h.get("status") or "").strip() != "결제중":
                continue
            dt = _parse_session_datetime(h.get("created_at"))
            if dt is None:
                continue
            if dt >= cutoff:
                return True

        return False
    except Exception as e:
        print(f"[WARN] _has_active_sessions 실패: {e}")
        return False


def _has_any_admin_sessions() -> bool:
    try:
        st = _load_admin_state()
        return bool((st.get("sessions") or []) or (st.get("history") or []))
    except Exception:
        return False


def _count_open_sessions() -> int:
    try:
        st = _load_admin_state()
        sessions = st.get("sessions") or []
        cnt = 0
        for s in sessions:
            if not isinstance(s, dict):
                continue
            if _session_considered_terminal(s):
                continue
            if str(s.get("status") or "").strip() == "결제중":
                cnt += 1
        return cnt
    except Exception:
        return 0


def _backfill_admin_state_from_kvan_links(store: "KVStore", max_rows: int = 300) -> None:
    """
    admin_state 가 비어 있을 때 kvan_links DB 스냅샷으로 최소 세션 정보를 복원한다.
    서버 재시작/파일 유실 후에도 KEY-세션 매핑이 끊기지 않도록 보강.
    """
    try:
        st = _load_admin_state()
        if (st.get("sessions") or []) or (st.get("history") or []):
            return
        rows = store.load_kvan_links(limit=max_rows) or []
        if not rows:
            return

        sessions: list[dict] = []
        history: list[dict] = []
        seen_ids: set[str] = set()
        for r in rows:
            sid = str(r.get("internal_session_id") or "").strip() or str(
                r.get("kvan_session_id") or ""
            ).strip()
            if not sid or sid in seen_ids:
                continue
            seen_ids.add(sid)
            created_at = (
                str(r.get("link_created_at") or "").strip()
                or str(r.get("captured_at") or "").strip()
                or datetime.utcnow().isoformat()
            )
            entry = {
                "id": sid,
                "amount": int(r.get("amount") or 0),
                "installment": "",
                "status": "결제중",
                "created_at": created_at,
                "agency_id": str(r.get("agency_id") or "").strip(),
                "kvan_link": str(r.get("kvan_link") or "").strip(),
                "kvan_session_id": str(r.get("kvan_session_id") or "").strip(),
                "title": str(r.get("title") or "").strip(),
            }
            status_text = str(r.get("status") or "").strip()
            if _is_expired_link_status(status_text):
                entry["status"] = "만료"
                entry["deleted_in_kvan"] = True
                entry["finished_at"] = created_at
                history.append(entry)
            else:
                sessions.append(entry)
        if sessions or history:
            _save_admin_state({"sessions": sessions, "history": history})
            _trace(
                "admin_state_backfilled_from_kvan_links",
                sessions=len(sessions),
                history=len(history),
            )
    except Exception as e:
        _trace("admin_state_backfill_error", error=str(e)[:220])


# =========================================================
# 저장소
# =========================================================

def _json_table_path(table_name: str) -> Path:
    return LOCAL_DB_DIR / f"{table_name}.json"


def _json_load_rows(table_name: str) -> list[dict]:
    path = _json_table_path(table_name)
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _json_save_rows(table_name: str, rows: list[dict]) -> None:
    path = _json_table_path(table_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")


def _json_ensure_table(table_name: str) -> None:
    if not _json_table_path(table_name).exists():
        _json_save_rows(table_name, [])


def _json_delete_where(table_name: str, predicate) -> int:
    rows = _json_load_rows(table_name)
    kept: list[dict] = []
    removed = 0
    for r in rows:
        try:
            if predicate(r):
                removed += 1
            else:
                kept.append(r)
        except Exception:
            kept.append(r)
    _json_save_rows(table_name, kept)
    return removed


def _parse_mysql_url(db_url: str) -> dict:
    p = urlparse(db_url)
    if p.scheme not in ("mysql", "mysql+pymysql"):
        raise ValueError(f"지원하지 않는 DB URL 스킴: {p.scheme}")
    return {
        "host": p.hostname or "localhost",
        "port": int(p.port or 3306),
        "user": unquote(p.username or "root"),
        "password": unquote(p.password or ""),
        "database": (p.path or "/railway").lstrip("/") or "railway",
    }


def get_db():
    cfg = _parse_mysql_url(DATABASE_URL)
    connect_timeout = int(os.environ.get("MYSQL_CONNECT_TIMEOUT", "3"))
    read_timeout = int(os.environ.get("MYSQL_READ_TIMEOUT", "6"))
    write_timeout = int(os.environ.get("MYSQL_WRITE_TIMEOUT", "6"))
    return pymysql.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
        write_timeout=write_timeout,
    )


def _is_retryable_db_error(exc: Exception) -> bool:
    msg = str(exc or "").lower()
    return any(
        k in msg
        for k in (
            "2013",
            "2006",
            "lost connection",
            "server has gone away",
            "connection reset",
            "connection refused",
            "broken pipe",
        )
    )


def _get_db_with_retry(max_attempts: int = 3, delay_sec: float = 0.8):
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            conn = get_db()
            try:
                conn.ping(reconnect=True)
            except Exception:
                pass
            return conn
        except Exception as e:
            last_exc = e
            if attempt >= max_attempts or not _is_retryable_db_error(e):
                raise
            _append_admin_log("AUTO", f"[WARN] DB 재연결 재시도 {attempt}/{max_attempts}: {e}")
            time.sleep(delay_sec * attempt)
    if last_exc:
        raise last_exc
    raise RuntimeError("DB connection retry failed")


def _get_db_dashboard_quick():
    """
    대시보드 요약은 보조 정보이므로 짧은 타임아웃 연결을 사용한다.
    (여기서 오래 막혀 결제링크/거래 크롤 본 루프가 지연되는 것을 방지)
    """
    cfg = _parse_mysql_url(DATABASE_URL)
    connect_timeout = int(os.environ.get("K_VAN_DASH_DB_CONNECT_TIMEOUT", "2"))
    read_timeout = int(os.environ.get("K_VAN_DASH_DB_READ_TIMEOUT", "3"))
    write_timeout = int(os.environ.get("K_VAN_DASH_DB_WRITE_TIMEOUT", "3"))
    return pymysql.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
        write_timeout=write_timeout,
    )


class KVStore:
    def __init__(self) -> None:
        self.use_json = _use_json_store()
        try:
            self.ensure_tables()
        except Exception as e:
            if self.use_json:
                raise
            # 서버에서는 DB 실패를 즉시 드러내고 종료(어드민 DB와 분리되는 JSON 폴백 방지).
            if _is_server_env():
                _alog(f"[FATAL] 서버 DB 초기화 실패: {e}")
                raise
            # 로컬 개발 환경에서는 JSON 폴백 허용.
            _alog(f"[ERROR] DB 초기화 실패 - JSON 폴백 전환: {e}")
            print(f"[WARN] DB 초기화 실패 - JSON 폴백 전환: {e}")
            self.use_json = True
            self.ensure_tables()

    def ensure_tables(self) -> None:
        if self.use_json:
            for name in ("kvan_links", "kvan_transactions", "transactions", "agencies"):
                _json_ensure_table(name)
            return

        conn = _get_db_with_retry()
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS kvan_links (
                  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                  captured_at DATETIME NOT NULL,
                  link_created_at DATETIME NULL DEFAULT NULL,
                  title VARCHAR(255) DEFAULT '',
                  amount BIGINT DEFAULT 0,
                  ttl_label VARCHAR(100) DEFAULT '',
                  status VARCHAR(100) DEFAULT '',
                  kvan_link VARCHAR(512) DEFAULT '',
                  mid VARCHAR(100) DEFAULT '',
                  kvan_session_id VARCHAR(100) DEFAULT '',
                  agency_id VARCHAR(64) DEFAULT '',
                  internal_session_id VARCHAR(64) DEFAULT '',
                  raw_text TEXT
                ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
                """
            )
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
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS transactions (
                  id VARCHAR(32) NOT NULL PRIMARY KEY,
                  created_at DATETIME NOT NULL,
                  agency_id VARCHAR(64) DEFAULT '',
                  amount BIGINT DEFAULT 0,
                  customer_name VARCHAR(255) DEFAULT '',
                  phone_number VARCHAR(64) DEFAULT '',
                  card_type VARCHAR(32) DEFAULT '',
                  resident_front VARCHAR(64) DEFAULT '',
                  card_prefix4 VARCHAR(8) DEFAULT '',
                  status VARCHAR(32) DEFAULT '',
                  message TEXT,
                  settlement_status VARCHAR(32) DEFAULT '미정산',
                  settled_at DATETIME NULL,
                  kvan_mid VARCHAR(100) DEFAULT '',
                  kvan_approval_no VARCHAR(100) DEFAULT '',
                  kvan_tx_type VARCHAR(100) DEFAULT '',
                  kvan_registered_at VARCHAR(50) DEFAULT ''
                ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS agencies (
                  id VARCHAR(64) NOT NULL PRIMARY KEY,
                  kvan_mid VARCHAR(100) DEFAULT ''
                ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
                """
            )

            # 기존에 생성된 transactions 테이블에 card_prefix4 컬럼이 없을 수 있어,
            # 실행 중 ALTER TABLE로 컬럼을 보강한다.
            try:
                cur.execute(
                    """
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'transactions'
                      AND COLUMN_NAME = 'card_prefix4'
                    """
                )
                cols = cur.fetchall() or []
                if not cols:
                    cur.execute("ALTER TABLE transactions ADD COLUMN card_prefix4 VARCHAR(8) DEFAULT ''")
            except Exception:
                # 컬럼 추가가 실패해도 기존 로직이 최소 동작은 하도록 무시
                pass
            try:
                cur.execute(
                    """
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'kvan_links'
                      AND COLUMN_NAME = 'agency_id'
                    """
                )
                if not (cur.fetchall() or []):
                    cur.execute(
                        "ALTER TABLE kvan_links ADD COLUMN agency_id VARCHAR(64) DEFAULT ''"
                    )
            except Exception:
                pass
            try:
                cur.execute(
                    """
                    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'kvan_links'
                      AND COLUMN_NAME = 'internal_session_id'
                    """
                )
                if not (cur.fetchall() or []):
                    cur.execute(
                        "ALTER TABLE kvan_links ADD COLUMN internal_session_id VARCHAR(64) DEFAULT ''"
                    )
            except Exception:
                pass
            try:
                cur.execute(
                    """
                    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'kvan_links'
                      AND COLUMN_NAME = 'link_created_at'
                    """
                )
                if not (cur.fetchall() or []):
                    cur.execute(
                        "ALTER TABLE kvan_links ADD COLUMN link_created_at DATETIME NULL DEFAULT NULL"
                    )
            except Exception:
                pass
            try:
                cur.execute(
                    "UPDATE kvan_links SET link_created_at = captured_at WHERE link_created_at IS NULL"
                )
            except Exception:
                pass
        conn.commit()
        conn.close()

    def replace_kvan_links(self, rows: list[dict]) -> None:
        if self.use_json:
            _json_save_rows("kvan_links", rows)
            return

        if not rows:
            print(
                "[INFO] /payment-link 스냅샷 0건 - kvan_links 는 건드리지 않습니다(빈 응답으로 DB 초기화 방지)."
            )
            return

        new_urls: list[str] = []
        for r in rows:
            u = (r.get("kvan_link") or "").strip()
            if u:
                new_urls.append(u)
        if not new_urls:
            print("[WARN] kvan_links 병합: 유효한 kvan_link 가 없어 DB 를 변경하지 않습니다.")
            return

        preserved = load_kvan_link_preserved_by_url(new_urls)
        conn = _get_db_with_retry()
        ensure_kvan_links_internal_session_column(conn)
        ensure_kvan_links_link_created_at(conn)
        with conn.cursor() as cur:
            ph = ",".join(["%s"] * len(new_urls))
            cur.execute(
                f"DELETE FROM kvan_links WHERE kvan_link NOT IN ({ph})",
                tuple(new_urls),
            )
            for row in rows:
                link = (row.get("kvan_link") or "").strip()
                if not link:
                    continue
                prev = preserved.get(link, {})
                agency_id = (row.get("agency_id") or "").strip() or (
                    prev.get("agency_id") or ""
                ).strip()
                internal_session_id = (row.get("internal_session_id") or "").strip() or (
                    prev.get("internal_session_id") or ""
                ).strip()
                title = (row.get("title") or "").strip() or (prev.get("title") or "").strip()
                try:
                    parsed_ui = parse_kvan_link_ui_created_at(
                        str(row.get("raw_text") or "")
                    )
                except Exception:
                    parsed_ui = None
                link_created_at = prev.get("link_created_at") or parsed_ui
                amount = row.get("amount", 0)
                try:
                    ai = int(amount)
                except (TypeError, ValueError):
                    ai = 0
                if ai <= 0:
                    try:
                        pi = int(prev.get("amount") or 0)
                        if pi > 0:
                            ai = pi
                    except (TypeError, ValueError):
                        pass
                cur.execute("DELETE FROM kvan_links WHERE kvan_link = %s", (link,))
                cur.execute(
                    """
                    INSERT INTO kvan_links (
                      captured_at, link_created_at, title, amount, ttl_label, status,
                      kvan_link, mid, kvan_session_id, agency_id, internal_session_id, raw_text
                    )
                    VALUES (NOW(), IFNULL(%s, NOW()), %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        link_created_at,
                        title,
                        ai,
                        row.get("ttl_label", ""),
                        row.get("status", ""),
                        link,
                        row.get("mid", ""),
                        row.get("kvan_session_id", ""),
                        agency_id,
                        internal_session_id,
                        row.get("raw_text", ""),
                    ),
                )
        conn.commit()
        conn.close()

    def load_kvan_links(self, limit: int | None = None) -> list[dict]:
        if self.use_json:
            rows = _json_load_rows("kvan_links")
            if isinstance(limit, int) and limit > 0:
                return rows[:limit]
            return rows

        conn = get_db()
        with conn.cursor() as cur:
            sql = """
                SELECT id, captured_at, link_created_at, title, amount, ttl_label, status,
                       kvan_link, mid, kvan_session_id, agency_id, internal_session_id, raw_text
                FROM kvan_links
                ORDER BY id DESC
            """
            if isinstance(limit, int) and limit > 0:
                sql += " LIMIT %s"
                cur.execute(sql, (int(limit),))
            else:
                cur.execute(sql)
            rows = cur.fetchall() or []
        conn.close()
        return rows

    def delete_kvan_links_by_urls(self, urls: set[str]) -> None:
        if not urls:
            return
        if self.use_json:
            _json_delete_where("kvan_links", lambda r: (r.get("kvan_link") or "").strip() in urls)
            return

        conn = get_db()
        with conn.cursor() as cur:
            placeholders = ",".join(["%s"] * len(urls))
            cur.execute(f"DELETE FROM kvan_links WHERE kvan_link IN ({placeholders})", tuple(urls))
        conn.commit()
        conn.close()

    def replace_kvan_transactions(
        self, rows: list[dict], *, force_empty: bool = False
    ) -> None:
        """
        force_empty=False 이고 rows 가 비면 TRUNCATE 하지 않는다(타임아웃·파싱 실패로 DB 전량 삭제 방지).
        화면에 거래 0건이 확실할 때만 force_empty=True 로 빈 스냅샷을 반영한다.
        """
        if self.use_json:
            if not rows and not force_empty:
                print("[INFO] kvan_transactions(json): 빈 스냅샷 무시, 이전 파일 유지")
                _trace("replace_kvan_tx_skip", mode="json", rows=0, force_empty=False)
                return
            _json_save_rows("kvan_transactions", rows)
            _trace("replace_kvan_tx_saved", mode="json", rows=len(rows), force_empty=force_empty)
            return

        if not rows and not force_empty:
            print("[INFO] kvan_transactions(MySQL): 빈 스냅샷 - TRUNCATE 생략(기존 행 유지)")
            _trace("replace_kvan_tx_skip", mode="mysql", rows=0, force_empty=False)
            return

        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE kvan_transactions")
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO kvan_transactions (
                      captured_at, merchant_name, pg_name, mid, fee_rate, tx_type,
                      amount, cancel_amount, payable_amount, card_company, card_number,
                      installment, approval_no, registered_at, raw_text
                    )
                    VALUES (NOW(), %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        row.get("merchant_name", ""),
                        row.get("pg_name", ""),
                        row.get("mid", ""),
                        row.get("fee_rate", ""),
                        row.get("tx_type", ""),
                        row.get("amount", 0),
                        row.get("cancel_amount", 0),
                        row.get("payable_amount", 0),
                        row.get("card_company", ""),
                        row.get("card_number", ""),
                        row.get("installment", ""),
                        row.get("approval_no", ""),
                        row.get("registered_at", ""),
                        row.get("raw_text", ""),
                    ),
                )
        conn.commit()
        conn.close()
        _trace("replace_kvan_tx_saved", mode="mysql", rows=len(rows), force_empty=force_empty)

    def load_recent_kvan_transactions(self, limit: int = 200) -> list[dict]:
        if self.use_json:
            rows = _json_load_rows("kvan_transactions")
            rows.sort(key=lambda x: x.get("captured_at", ""), reverse=True)
            return rows[:limit]

        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, captured_at, merchant_name, mid, tx_type,
                       amount, approval_no, card_number, registered_at, raw_text
                FROM kvan_transactions
                ORDER BY captured_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall() or []
        conn.close()
        return rows

    def upsert_popup_transaction(
        self,
        session_id: str,
        amount: int,
        approval_no: str,
        card_number: str,
        registered_at: str,
        customer_name: str,
    ) -> None:
        approval_no = (approval_no or "").strip()
        if not approval_no or not amount:
            return

        prefix4 = _card_prefix4(card_number)
        agency_id = _get_agency_id_for_session(session_id)
        agency_id_from_state = (agency_id or "").strip()

        if self.use_json:
            tx_rows = _json_load_rows("transactions")
            found = None
            for tx in tx_rows:
                if (tx.get("kvan_approval_no") or "").strip() == approval_no:
                    found = tx
                    break

            if found:
                found["amount"] = found.get("amount") or amount
                found["customer_name"] = found.get("customer_name") or (customer_name or "")
                found["status"] = "success"
                found["kvan_registered_at"] = registered_at
                if not found.get("card_prefix4") and prefix4:
                    found["card_prefix4"] = prefix4
                if not found.get("agency_id") and agency_id:
                    found["agency_id"] = agency_id
            else:
                new_tx_id = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")[-18:]
                tx_rows.append(
                    {
                        "id": new_tx_id,
                        "created_at": datetime.utcnow().isoformat(),
                        "agency_id": agency_id or "",
                        "amount": amount,
                        "customer_name": customer_name or "",
                        "phone_number": "",
                        "card_type": "",
                        "resident_front": "",
                        "card_prefix4": prefix4,
                        "status": "success",
                        "message": f"K-VAN 결제 승인 (세션ID={session_id}, 승인번호={approval_no}, 카드={card_number})",
                        "settlement_status": "미정산",
                        "settled_at": None,
                        "kvan_mid": "",
                        "kvan_approval_no": approval_no,
                        "kvan_tx_type": "결제 승인",
                        "kvan_registered_at": registered_at,
                    }
                )
                append_payment_notification(
                    PAYMENT_NOTIFICATIONS_PATH,
                    agency_id=agency_id or "",
                    amount=amount,
                    tx_id=new_tx_id,
                    customer_name=customer_name or "",
                )
            _json_save_rows("transactions", tx_rows)
            return

        conn = get_db()
        with conn.cursor() as cur:
            valid_agency_ids = _load_valid_agency_ids(cur)
            db_agency_by_key = _resolve_agency_id_by_kvan_key_db(cur, session_id)
            if (not agency_id) and db_agency_by_key:
                agency_id = db_agency_by_key
            safe_agency_id = _sanitize_agency_id_for_fk(
                agency_id,
                valid_agency_ids,
                stage="popup_upsert",
                hint=approval_no,
            )
            _trace(
                "popup_upsert_resolved",
                session_id=session_id,
                approval_no=approval_no,
                agency_from_state=agency_id_from_state,
                agency_from_kvan_links=(db_agency_by_key or ""),
                agency_final=(safe_agency_id or ""),
            )
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
                    SET created_at = COALESCE(created_at, NOW()),
                        amount = COALESCE(amount, %s),
                        customer_name = COALESCE(customer_name, %s),
                        card_prefix4 = COALESCE(NULLIF(card_prefix4, ''), %s),
                        status = 'success',
                        kvan_registered_at = %s,
                        agency_id = COALESCE(NULLIF(agency_id, ''), %s)
                    WHERE id = %s
                    """,
                    (amount, customer_name or "", prefix4, registered_at, safe_agency_id, tx_id),
                )
            else:
                new_tx_id = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")[-18:]
                message = f"K-VAN 결제 승인 (세션ID={session_id}, 승인번호={approval_no}, 카드={card_number})"
                cur.execute(
                    """
                    INSERT INTO transactions (
                      id, created_at, agency_id, amount, customer_name, phone_number,
                      card_type, resident_front, card_prefix4, status, message,
                      settlement_status, settled_at, kvan_mid, kvan_approval_no,
                      kvan_tx_type, kvan_registered_at
                    )
                    VALUES (
                      %s, NOW(), %s, %s, %s, '', '', %s,
                      'success', %s, '미정산', NULL, '', %s, '결제 승인', %s
                    )
                    """,
                    (new_tx_id, safe_agency_id, amount, customer_name or "", prefix4, message, approval_no, registered_at),
                )
                append_payment_notification(
                    PAYMENT_NOTIFICATIONS_PATH,
                    agency_id=safe_agency_id or "",
                    amount=amount,
                    tx_id=new_tx_id,
                    customer_name=customer_name or "",
                )
        conn.commit()
        conn.close()

    def sync_kvan_to_transactions(self) -> bool:
        updated = 0
        inserted = 0
        skipped_no_amount = 0
        synthetic_approval = 0
        notified = 0

        try:
            krows = self.load_recent_kvan_transactions(limit=200)
            _trace("sync_start", use_json=self.use_json, krows=len(krows))

            if self.use_json:
                tx_rows = _json_load_rows("transactions")

                for kr in krows:
                    amt = kr.get("amount") or 0
                    approval_raw = (kr.get("approval_no") or "").strip()
                    approval = approval_raw
                    mid = (kr.get("mid") or "").strip()
                    tx_type = (kr.get("tx_type") or "").strip()
                    reg = (kr.get("registered_at") or "").strip()
                    card_number = (kr.get("card_number") or "").strip()
                    prefix4 = _card_prefix4(card_number)

                    if not amt:
                        skipped_no_amount += 1
                        continue

                    tx_status = (
                        "success"
                        if "승인" in tx_type
                        else "fail"
                        if ("취소" in tx_type or "실패" in tx_type or "오류" in tx_type)
                        else "other"
                    )
                    if tx_status != "success":
                        continue
                    reg_date = reg.split(" ")[0] if reg else ""
                    raw_tx = (kr.get("raw_text") or "").strip()
                    sid_hint_for_msg = _extract_primary_kvan_key_from_tx_raw(raw_tx) or ""
                    resolved_agency_id = (
                        str(_get_agency_id_for_session(sid_hint_for_msg) or "").strip()
                        if sid_hint_for_msg
                        else ""
                    )

                    found = None
                    if approval_raw:
                        for tx in tx_rows:
                            if (tx.get("kvan_approval_no") or "").strip() == approval_raw:
                                found = tx
                                break
                    if (not found) and sid_hint_for_msg:
                        for tx in tx_rows:
                            msg = str(tx.get("message") or "")
                            if sid_hint_for_msg and sid_hint_for_msg in msg:
                                found = tx
                                break

                    if not found and not (approval_raw or sid_hint_for_msg):
                        # KEY/승인번호 근거가 없으면 대행사/본사 구분을 위해 동기화를 건너뛴다.
                        continue

                    if found:
                        found["amount"] = found.get("amount") or amt
                        found["status"] = tx_status
                        found["kvan_mid"] = mid
                        found["kvan_approval_no"] = approval_raw
                        found["kvan_tx_type"] = tx_type
                        found["kvan_registered_at"] = reg
                        if (not (found.get("agency_id") or "").strip()) and resolved_agency_id:
                            found["agency_id"] = resolved_agency_id
                        if prefix4 and not (found.get("card_prefix4") or "").strip():
                            found["card_prefix4"] = prefix4
                        updated += 1
                    else:
                        tx_rows.append(
                            {
                                "id": datetime.utcnow().strftime("%Y%m%d%H%M%S%f")[-18:],
                                "created_at": datetime.utcnow().isoformat(),
                                "agency_id": resolved_agency_id,
                                "amount": amt,
                                "customer_name": "",
                                "phone_number": "",
                                "card_type": "",
                                "resident_front": "",
                                "card_prefix4": prefix4,
                                "status": tx_status,
                                "message": (
                                    f"K-VAN {tx_type or '거래'} 자동 연동 "
                                    f"(승인번호={approval_raw or '없음'}"
                                    + (f", 세션ID={sid_hint_for_msg}" if sid_hint_for_msg else "")
                                    + ")"
                                ),
                                "settlement_status": "미정산",
                                "settled_at": None,
                                "kvan_mid": mid,
                                "kvan_approval_no": approval_raw,
                                "kvan_tx_type": tx_type,
                                "kvan_registered_at": reg,
                            }
                        )
                        inserted += 1

                _json_save_rows("transactions", tx_rows)

            else:
                conn = _get_db_with_retry()
                with conn.cursor() as cur:
                    try:
                        st_probe = _load_admin_state()
                        s_cnt = len(st_probe.get("sessions") or [])
                        h_cnt = len(st_probe.get("history") or [])
                        open_cnt = _count_open_sessions()
                        if s_cnt == 0 and h_cnt == 0 and krows:
                            _trace("sync_mapping_source_empty", sessions=s_cnt, history=h_cnt, krows=len(krows))
                        _trace(
                            "sync_mapping_source_state",
                            sessions=s_cnt,
                            history=h_cnt,
                            open_sessions=open_cnt,
                            krows=len(krows),
                        )
                    except Exception:
                        pass
                    valid_agency_ids = _load_valid_agency_ids(cur)
                    for kr in krows:
                        amt = kr.get("amount") or 0
                        approval_raw = (kr.get("approval_no") or "").strip()
                        approval = approval_raw
                        mid = (kr.get("mid") or "").strip()
                        tx_type = (kr.get("tx_type") or "").strip()
                        reg = (kr.get("registered_at") or "").strip()
                        card_number = (kr.get("card_number") or "").strip()
                        prefix4 = _card_prefix4(card_number)

                        if not amt:
                            skipped_no_amount += 1
                            continue

                        reg_date = reg.split(" ")[0] if reg else ""
                        tx_status = (
                            "success"
                            if "승인" in tx_type
                            else "fail"
                            if ("취소" in tx_type or "실패" in tx_type or "오류" in tx_type)
                            else "other"
                        )
                        if tx_status != "success":
                            _trace(
                                "sync_skip_non_success",
                                tx_type=tx_type,
                                amount=amt,
                                reg_date=reg_date,
                            )
                            continue

                        agency_source = "none"
                        resolved_agency_id = ""
                        resolved_agency_id = _sanitize_agency_id_for_fk(
                            resolved_agency_id,
                            valid_agency_ids,
                            stage="map_from_key_only",
                            hint=approval_raw or approval,
                        )

                        raw_tx = (kr.get("raw_text") or "").strip()
                        key_agency, kkey = _resolve_agency_id_for_kvan_tx_row(raw_tx, cur)
                        _trace(
                            "sync_key_probe_raw",
                            approval=approval_raw,
                            extracted_key=kkey or "",
                            key_agency=(key_agency or ""),
                        )
                        if kkey:
                            resolved_agency_id = _sanitize_agency_id_for_fk(
                                key_agency,
                                valid_agency_ids,
                                stage="map_from_key",
                                hint=approval_raw or approval,
                            )
                            if resolved_agency_id:
                                agency_source = "map_from_key"
                            print(
                                f"[KVAN-TX-SYNC][crawler] approval={approval_raw or approval} key={kkey} "
                                f"agency_id={(resolved_agency_id or '')!r}"
                            )
                        _trace(
                            "sync_key_probe_after_raw",
                            approval=approval_raw,
                            source=agency_source,
                            resolved_agency_id=resolved_agency_id or "",
                        )
                        _trace(
                            "sync_row_mapping",
                            approval=approval,
                            amount=amt,
                            mid=mid,
                            reg_date=reg_date,
                            source=agency_source,
                            resolved_agency_id=resolved_agency_id or "",
                        )

                        tx = None
                        if approval_raw:
                            cur.execute(
                                """
                                SELECT id, message, agency_id
                                FROM transactions
                                WHERE kvan_approval_no = %s
                                LIMIT 1
                                """,
                                (approval_raw,),
                            )
                            tx = cur.fetchone()
                        if (not tx) and kkey:
                            cur.execute(
                                """
                                SELECT id, message, agency_id
                                FROM transactions
                                WHERE message LIKE %s
                                ORDER BY created_at DESC
                                LIMIT 1
                                """,
                                (f"%{kkey}%",),
                            )
                            tx = cur.fetchone()

                        if tx:
                            tx_id = tx["id"]
                            existing_agency_id = str(tx.get("agency_id") or "").strip()
                            tx_msg = str(tx.get("message") or "")
                            msg_sid = _extract_session_id_from_tx_message(tx_msg)
                            msg_key_agency = None
                            if not resolved_agency_id and msg_sid:
                                msg_key_agency = _get_agency_id_for_session(msg_sid)
                                if not msg_key_agency:
                                    try:
                                        msg_key_agency = _resolve_agency_id_by_kvan_key_db(cur, msg_sid)
                                    except Exception:
                                        msg_key_agency = None
                                resolved_agency_id = _sanitize_agency_id_for_fk(
                                    msg_key_agency,
                                    valid_agency_ids,
                                    stage="map_from_existing_message_key",
                                    hint=approval_raw or approval,
                                )
                                if resolved_agency_id:
                                    agency_source = "map_from_existing_message_key"
                            _trace(
                                "sync_key_probe_existing_tx_message",
                                approval=approval_raw,
                                tx_id=tx_id,
                                message_sid=msg_sid or "",
                                message_agency=(msg_key_agency or ""),
                                source=agency_source,
                                resolved_agency_id=resolved_agency_id or "",
                            )
                            final_agency_id = resolved_agency_id
                            # 기존 agency_id가 있으면 기본적으로 보존한다.
                            # 단, KEY 기반으로 강하게 확정된 경우에만 교정 허용.
                            if existing_agency_id:
                                if (
                                    resolved_agency_id
                                    and resolved_agency_id != existing_agency_id
                                    and agency_source == "map_from_key"
                                ):
                                    final_agency_id = resolved_agency_id
                                    _trace(
                                        "sync_agency_corrected_by_key",
                                        approval=approval,
                                        before=existing_agency_id,
                                        after=resolved_agency_id,
                                    )
                                else:
                                    final_agency_id = existing_agency_id
                            cur.execute(
                                """
                                UPDATE transactions
                                SET created_at = COALESCE(created_at, NOW()),
                                    amount = COALESCE(amount, %s),
                                    status = %s,
                                    kvan_mid = %s,
                                    kvan_approval_no = %s,
                                    kvan_tx_type = %s,
                                    kvan_registered_at = %s,
                                    card_prefix4 = COALESCE(NULLIF(card_prefix4, ''), %s),
                                    agency_id = %s
                                WHERE id = %s
                                """,
                                (
                                    amt,
                                    tx_status,
                                    mid,
                                    approval_raw,
                                    tx_type,
                                    reg,
                                    prefix4,
                                    final_agency_id,
                                    tx_id,
                                ),
                            )
                            sid_hint = _extract_session_id_from_tx_message(
                                tx.get("message") or ""
                            ) or kkey
                            if not sid_hint:
                                sid_hint = _guess_open_session_id_for_success(
                                    amount=int(amt or 0),
                                    agency_id=resolved_agency_id,
                                    reg_date=reg_date,
                                )
                            if sid_hint:
                                _mark_session_checked(
                                    sid_hint,
                                    title="거래내역 자동 동기화",
                                    has_approval=True,
                                )
                                _trace(
                                    "sync_mark_checked",
                                    approval=approval_raw,
                                    session_id=sid_hint,
                                    path="approval_or_key_match",
                                )
                            else:
                                open_cnt = _count_open_sessions()
                                if open_cnt <= 0:
                                    _trace(
                                        "sync_mark_skipped_no_open_session",
                                        approval=approval_raw,
                                        amount=amt,
                                        agency_id=resolved_agency_id or "",
                                        reg_date=reg_date,
                                        path="approval_or_key_match",
                                    )
                                else:
                                    _trace(
                                        "sync_mark_unresolved",
                                        approval=approval_raw,
                                        amount=amt,
                                        agency_id=resolved_agency_id or "",
                                        reg_date=reg_date,
                                        open_sessions=open_cnt,
                                        path="approval_or_key_match",
                                    )
                            updated += 1
                            continue

                        if not (approval_raw or kkey):
                            _trace(
                                "sync_skip_insert_missing_key_and_approval",
                                amount=amt,
                                mid=mid,
                                reg_date=reg_date,
                            )
                            continue

                        new_tx_id = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")[-18:]
                        sid_hint_for_msg = _extract_primary_kvan_key_from_tx_raw(raw_tx) or ""
                        message = (
                            f"K-VAN {tx_type or '거래'} 자동 연동 "
                            f"(승인번호={approval_raw or '없음'}"
                            + (f", 세션ID={sid_hint_for_msg}" if sid_hint_for_msg else "")
                            + ")"
                        )

                        cur.execute(
                            """
                            INSERT INTO transactions (
                              id, created_at, agency_id, amount, customer_name, phone_number,
                              card_type, resident_front, card_prefix4, status, message,
                              settlement_status, settled_at, kvan_mid, kvan_approval_no,
                              kvan_tx_type, kvan_registered_at
                            )
                            VALUES (
                              %s, NOW(), %s, %s, '', '', '', '', %s,
                              %s, %s, '미정산', NULL, %s, %s, %s, %s
                            )
                            """,
                            (
                                new_tx_id,
                                resolved_agency_id,
                                amt,
                                prefix4,
                                tx_status,
                                message,
                                mid,
                                approval_raw,
                                tx_type,
                                reg,
                            ),
                        )
                        inserted += 1
                        if tx_status == "success" and int(amt or 0) > 0:
                            append_payment_notification(
                                PAYMENT_NOTIFICATIONS_PATH,
                                agency_id=resolved_agency_id or "",
                                amount=int(amt),
                                tx_id=new_tx_id,
                                customer_name="",
                            )
                            notified += 1

                conn.commit()
                conn.close()

            if updated or inserted:
                print(
                    f"[INFO] K-VAN → transactions 동기화 완료 (updated={updated}, inserted={inserted}, json={self.use_json})"
                )
            _trace(
                "sync_done",
                updated=updated,
                inserted=inserted,
                skipped_no_amount=skipped_no_amount,
                synthetic_approval=synthetic_approval,
                notified=notified,
                use_json=self.use_json,
            )
            # 크롤러 대기: 매 사이클마다 동일 행 UPDATE 만 일어나면 had_new 로 보지 않는다.
            # (서버 MySQL에서 매 루프 sync 가 True 가 되어 2초 폴링에 고착되던 원인 제거)
            return bool(inserted)

        except Exception as e:
            print(f"[WARN] K-VAN ↔ transactions 동기화 오류: {e}")
            _trace("sync_error", error=str(e)[:300])
            return False


# =========================================================
# 셀렉터 / 빠른 동작 상수
# =========================================================

SIGN_IN_URL = "https://store.k-van.app/"
PAYMENT_LINK_URL = "https://store.k-van.app/payment-link"

FAST_POLL = float(os.environ.get("K_VAN_FAST_POLL", "0.05"))
FAST_UI_WAIT = float(os.environ.get("K_VAN_FAST_UI_WAIT", "1.2"))
FAST_NAV_WAIT = float(os.environ.get("K_VAN_FAST_NAV_WAIT", "2.2"))
FAST_NAV_RETRIES = int(os.environ.get("K_VAN_FAST_NAV_RETRIES", "3"))
FAST_DELETE_PER_PASS = int(os.environ.get("K_VAN_FAST_DELETE_PER_PASS", "50"))

SIGN_IN_SELECTORS = {
    "id_primary": (By.XPATH, "//label[normalize-space(text())='아이디']/following::input[1]"),
    "id_placeholder": (By.XPATH, "//input[contains(@placeholder,'아이디')]"),
    "id_fallback": (By.XPATH, "(//input[@type='text' or not(@type)])[1]"),
    "password_primary": (By.XPATH, "//label[normalize-space(text())='비밀번호']/following::input[1]"),
    "password_fallback": (By.XPATH, "(//input[@type='password'])[1]"),
    "submit_primary": (By.XPATH, "//button[contains(normalize-space(.), '로그인')]"),
}

PIN_POPUP_SELECTORS = {
    "input": (
        By.XPATH,
        "//*[contains(text(), 'PIN') and contains(text(), '입력')]/ancestor::div[1]//input",
    ),
    "confirm": (By.XPATH, "//button[contains(normalize-space(.), '확인')]"),
}

TX_BUTTON_XPATH = (
    "//button[@title='거래 내역']"
    " | //button[contains(normalize-space(.),'거래 내역')]"
    " | //button[contains(normalize-space(.),'거래내역')]"
    " | //button[.//svg[contains(@class,'lucide-receipt')]]"
    " | //button[contains(@aria-label,'거래') or contains(@aria-label,'내역')]"
)

TRASH_BUTTON_REL_XPATH = (
    ".//button[@title='삭제']"
    " | .//button[contains(@aria-label,'삭제')]"
    " | .//button[contains(@aria-label,'휴지통')]"
    " | .//button[.//svg[contains(@class,'trash') or contains(@class,'lucide-trash') or contains(@class,'lucide-trash-2')]]"
)

CONFIRM_DELETE_XPATH = (
    # UI 텍스트에 공백/개행/자식요소가 섞여도 매칭되도록 exact match 대신 contains 사용
    "//div[@role='alertdialog']//button[contains(normalize-space(.),'삭제') and not(contains(normalize-space(.),'취소'))]"
    " | //button[contains(normalize-space(.),'삭제') and not(contains(normalize-space(.),'취소'))]"
)

CLOSE_DIALOG_XPATH = (
    ".//button[@data-slot='dialog-close']"
    " | .//button[contains(@aria-label,'닫기')]"
    " | .//button[contains(normalize-space(.),'닫기')]"
)

CREATE_BUTTON_XPATH = (
    "//button[contains(normalize-space(.),'생성')]"
    " | //a[contains(normalize-space(.),'생성')]"
)

LINK_CREATE_CONFIRM_XPATH = (
    "//div[@role='dialog']//button[contains(normalize-space(.),'생성하기')]"
    " | //button[contains(normalize-space(.),'생성하기')]"
)

LINK_COPY_BUTTON_XPATH = (
    "//button[contains(normalize-space(.),'링크 복사')]"
    " | //button[.//svg[contains(@class,'lucide-copy') or contains(@class,'copy')]]"
)

NO_TX_TEXTS = (
    "거래내역이 없습니다",
    "거래 내역이 없습니다",
    "조회된 거래내역이 없습니다",
    "거래 내역 없음",
)


# =========================================================
# 드라이버
# =========================================================

def _get_chromedriver_path() -> Optional[str]:
    env_path = os.environ.get("CHROMEDRIVER_PATH", "").strip()
    if env_path and Path(env_path).exists():
        return env_path
    base = FILE_DIR / "tool"
    for name in ("chromedriver.exe", "chromedriver"):
        p = base / name
        if p.exists():
            return str(p)
    return None


def create_driver(headless: Optional[bool] = None) -> webdriver.Chrome:
    if headless is None:
        headless = _is_server_env()

    options = webdriver.ChromeOptions()
    options.page_load_strategy = "eager"

    if headless:
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-setuid-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
    else:
        options.add_argument("--start-maximized")

    options.add_argument("--disable-notifications")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--disable-features=Translate,OptimizationHints")
    options.add_experimental_option("excludeSwitches", ["enable-logging"])

    prefs = {
        "profile.default_content_setting_values.notifications": 2,
        "profile.managed_default_content_settings.images": 2,
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False,
    }
    options.add_experimental_option("prefs", prefs)

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
    driver.implicitly_wait(0)
    driver.set_page_load_timeout(12)
    driver.set_script_timeout(12)
    return driver


# =========================================================
# 공통 유틸
# =========================================================

def _brief_sleep(sec: float = 0.05) -> None:
    time.sleep(sec)


def _safe_text(el) -> str:
    try:
        return " ".join((el.text or "").split())
    except Exception:
        return ""


def _parse_amount(text: str) -> int:
    return parse_amount_won(text or "")


def _card_prefix4(card_number: str) -> str:
    """
    카드번호 문자열에서 숫자만 추출한 뒤 앞 4자리만 반환.
    (예: '1234-5678-...' -> '1234')
    """
    digits = re.sub(r"[^\d]", "", str(card_number or ""))
    return digits[:4] if digits else ""


def _normalized_approval_for_sync(approval: str, kr: dict) -> str:
    """
    승인번호가 비어도 /transactions 행을 내부 transactions 로 동기화하기 위한 대체 키.
    (MID/거래일시/카드/금액/유형 기반으로 안정적인 해시 생성)
    """
    ap = (approval or "").strip()
    if ap:
        return ap
    raw = "|".join(
        [
            str(kr.get("mid") or "").strip(),
            str(kr.get("registered_at") or "").strip(),
            str(kr.get("card_number") or "").strip(),
            str(int(kr.get("amount") or 0)),
            str(kr.get("tx_type") or "").strip(),
            str(kr.get("raw_text") or "").strip()[:80],
        ]
    )
    digest = hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()[:12]
    return f"NOAPP-{digest}"


def _poll_until(fn, timeout: float, interval: float = FAST_POLL):
    end = time.time() + timeout
    last = None
    while time.time() < end:
        try:
            last = fn()
            if last:
                return last
        except Exception:
            pass
        time.sleep(interval)
    return last


def _find_first_visible(driver: webdriver.Chrome, xpaths: list[str], timeout: float = FAST_UI_WAIT):
    def _inner():
        for xp in xpaths:
            try:
                els = driver.find_elements(By.XPATH, xp)
            except Exception:
                continue
            for el in els:
                try:
                    if el.is_displayed():
                        return el
                except Exception:
                    continue
        return None

    return _poll_until(_inner, timeout=timeout)


def _fast_click(driver: webdriver.Chrome, el) -> bool:
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center', inline:'center'});", el)
    except Exception:
        pass

    try:
        el.click()
        return True
    except Exception:
        try:
            driver.execute_script("arguments[0].click();", el)
            return True
        except Exception:
            return False


def _find_input_quick(driver: webdriver.Chrome, css_list: list[str], max_wait: float = 1.5):
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
        time.sleep(0.05)
    return None


def _find_card_container(el):
    xps = [
        "ancestor::tr[1]",
        "ancestor::*[@role='row'][1]",
        "ancestor::li[1]",
        "ancestor::div[contains(@class,'rounded')][1]",
        "ancestor::div[contains(@class,'border')][1]",
        "ancestor::div[contains(@class,'card')][1]",
        "ancestor::div[contains(@class,'row')][1]",
    ]
    for xp in xps:
        try:
            row = el.find_element(By.XPATH, xp)
            if row.is_displayed():
                return row
        except Exception:
            continue
    return None


def _get_session_id_from_text(text: str) -> str:
    m = re.search(r"(KEY[0-9A-Za-z]+)", text or "")
    return m.group(1) if m else ""


def _kvan_now() -> datetime:
    try:
        offset_hours = int(os.environ.get("K_VAN_TZ_OFFSET_HOURS", "9"))
    except Exception:
        offset_hours = 9
    return datetime.utcnow() + timedelta(hours=offset_hours)


def _extract_expire_at_from_lines(lines: list[str]) -> Optional[datetime]:
    for raw in lines or []:
        ln = str(raw or "").strip()
        if "만료일" not in ln:
            continue
        m = re.search(r"(20\d{2}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})", ln)
        if not m:
            continue
        ts = m.group(1).strip()
        try:
            return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
    return None


def _extract_status_from_link_lines(lines: list[str]) -> str:
    if not lines:
        return ""
    header_markers = (
        "생성일시",
        "만료일시",
        "세션ID",
        "작업",
        "상호명",
        "상품명",
        "유효시간",
        "본인인증",
        "결제 방식",
        "PG사",
        "MID",
    )
    exact_statuses = {
        "사용",
        "사용중",
        "사용 중",
        "대기",
        "완료",
        "만료",
        "취소",
        "취소됨",
        "취소 완료",
        "취소완료",
    }
    compact_set = {x.replace(" ", "") for x in exact_statuses}
    for raw in lines:
        ln = str(raw or "").strip()
        if not ln:
            continue
        if "취소 가능" in ln or "취소가능" in ln:
            continue
        if any(h in ln for h in header_markers):
            continue
        compact = ln.replace(" ", "")
        if ln in exact_statuses or compact in compact_set:
            return ln
        if "상태" in ln and any(k in ln for k in ("사용", "대기", "완료", "만료", "취소")):
            return ln
    return ""


def _is_expired_link_status(status_text: str) -> bool:
    s = str(status_text or "").strip()
    if not s:
        return False
    if "만료일시" in s:
        return False
    if "취소 가능" in s or "취소가능" in s:
        return False
    if "만료" in s:
        return True
    if s in ("취소", "취소됨", "취소 완료", "취소완료"):
        return True
    return False


def _popup_has_no_history(text: str) -> bool:
    t = (text or "").replace(" ", "")
    return any(x.replace(" ", "") in t for x in NO_TX_TEXTS)


# =========================================================
# 로그인 / 네비게이션
# =========================================================

def _click_notice_if_present(driver: webdriver.Chrome) -> None:
    try:
        btn = WebDriverWait(driver, 1).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(normalize-space(.),'확인 후 로그인')]"))
        )
        _fast_click(driver, btn)
        _brief_sleep(0.08)
    except TimeoutException:
        pass
    except Exception:
        pass


def sign_in(driver: webdriver.Chrome, row: PaymentRow) -> None:
    _dbg(f"_simple_sign_in 시작 (URL={SIGN_IN_URL}, id={row.login_id})")
    driver.get(SIGN_IN_URL)
    _click_notice_if_present(driver)

    id_input = _find_input_quick(
        driver,
        ["input[placeholder*='아이디']", "input[name*='id']", "input[type='text']"],
        max_wait=1.2,
    )
    if not id_input:
        for loc in (
            SIGN_IN_SELECTORS["id_primary"],
            SIGN_IN_SELECTORS["id_placeholder"],
            SIGN_IN_SELECTORS["id_fallback"],
        ):
            try:
                id_input = WebDriverWait(driver, 0.5).until(EC.visibility_of_element_located(loc))
                break
            except TimeoutException:
                continue
    if not id_input:
        raise RuntimeError("아이디 입력창을 찾지 못했습니다.")

    pw_input = _find_input_quick(
        driver,
        ["input[type='password']", "input[placeholder*='비밀번호']"],
        max_wait=1.2,
    )
    if not pw_input:
        for loc in (SIGN_IN_SELECTORS["password_primary"], SIGN_IN_SELECTORS["password_fallback"]):
            try:
                pw_input = WebDriverWait(driver, 0.5).until(EC.visibility_of_element_located(loc))
                break
            except TimeoutException:
                continue
    if not pw_input:
        raise RuntimeError("비밀번호 입력창을 찾지 못했습니다.")

    driver.execute_script(
        """
arguments[0].value = arguments[2];
arguments[1].value = arguments[3];
arguments[0].dispatchEvent(new Event('input', {bubbles:true}));
arguments[1].dispatchEvent(new Event('input', {bubbles:true}));
""",
        id_input,
        pw_input,
        row.login_id,
        row.login_password,
    )

    submit_btn = WebDriverWait(driver, 1.5).until(
        EC.element_to_be_clickable(SIGN_IN_SELECTORS["submit_primary"])
    )
    _fast_click(driver, submit_btn)

    try:
        pin_input = WebDriverWait(driver, 1.0).until(EC.visibility_of_element_located(PIN_POPUP_SELECTORS["input"]))
        pin_input.clear()
        pin_input.send_keys(row.login_pin)
        confirm_btn = driver.find_element(*PIN_POPUP_SELECTORS["confirm"])
        _fast_click(driver, confirm_btn)
    except Exception:
        pass

    def _store_ready():
        cur = driver.current_url or ""
        return "store.k-van.app" in cur and "sso.oneque.net" not in cur

    _poll_until(_store_ready, timeout=6.0, interval=0.08)
    _dbg(f"_simple_sign_in 전체 완료 (url={driver.current_url})")


def _wait_payment_link_page_ready(driver: webdriver.Chrome, timeout: float = FAST_NAV_WAIT) -> bool:
    def _ready():
        cur = driver.current_url or ""
        if "payment-link" not in cur:
            return False

        try:
            tx_icons = driver.find_elements(By.XPATH, TX_BUTTON_XPATH)
            if any(el.is_displayed() for el in tx_icons):
                return True
        except Exception:
            pass

        try:
            key_tokens = driver.find_elements(By.XPATH, "//*[contains(normalize-space(.),'KEY20')]")
            if any(el.is_displayed() for el in key_tokens):
                return True
        except Exception:
            pass

        try:
            cards = driver.find_elements(
                By.XPATH,
                "//div[contains(@class,'rounded') and contains(@class,'border')][.//*[contains(.,'KEY')]]"
            )
            if any(el.is_displayed() for el in cards):
                return True
        except Exception:
            pass

        # empty 문구는 마지막에 본다. (초기 로딩 플래시 오탐 방지)
        try:
            empty_msgs = driver.find_elements(By.XPATH, "//*[contains(normalize-space(.),'생성된 결제 링크가 없습니다')]")
            if any(el.is_displayed() for el in empty_msgs):
                return True
        except Exception:
            pass

        return False

    ok = bool(_poll_until(_ready, timeout=timeout, interval=FAST_POLL))
    if ok:
        print("[PAGE_READY] 결제링크 관리 페이지 로드 완료.")
    else:
        print("[WARN] 결제링크 관리 페이지 준비 대기 타임아웃 - 현재 상태로 진행.")
    return ok


def _has_payment_links_quick(driver: webdriver.Chrome, retries: int = 3, delay: float = 0.12) -> bool:
    for attempt in range(retries):
        try:
            icons = driver.find_elements(By.XPATH, TX_BUTTON_XPATH)
            if any(el.is_displayed() for el in icons):
                print(f"[EMPTY_CHECK] 거래 내역 아이콘 감지 → 링크 존재 (attempt={attempt}, count={len(icons)})")
                return True

            key_tokens = driver.find_elements(By.XPATH, "//*[contains(normalize-space(.),'KEY20')]")
            if any(el.is_displayed() for el in key_tokens):
                print(f"[EMPTY_CHECK] KEY 세션ID 감지 → 링크 존재 (attempt={attempt}, count={len(key_tokens)})")
                return True

            card_containers = driver.find_elements(
                By.XPATH,
                "//div[contains(@class,'rounded') and contains(@class,'border')][.//*[contains(.,'KEY') or contains(.,'KEY20')]]"
            )
            if any(el.is_displayed() for el in card_containers):
                print(f"[EMPTY_CHECK] 결제링크 카드 컨테이너 감지 → 링크 존재 (attempt={attempt}, count={len(card_containers)})")
                return True

            empty_msgs = driver.find_elements(By.XPATH, "//*[contains(normalize-space(.),'생성된 결제 링크가 없습니다')]")
            if any(el.is_displayed() for el in empty_msgs):
                # 아이콘/KEY/카드가 없을 때만 빈 화면으로 인정
                print(f"[EMPTY_CHECK] 결제링크 없음 문구 감지 (attempt={attempt})")
                return False
        except Exception as e:
            print(f"[EMPTY_CHECK] 링크 존재 여부 확인 중 예외 (attempt={attempt}): {e}")

        time.sleep(delay)

    print("[EMPTY_CHECK] 여러 번 확인했으나 링크를 찾지 못했습니다 (빈 화면으로 간주).")
    return False


def _go_to_payment_link(driver: webdriver.Chrome, max_attempts: int = FAST_NAV_RETRIES) -> bool:
    cur = driver.current_url or ""
    if "payment-link" in cur and _wait_payment_link_page_ready(driver, timeout=1.0):
        return True

    for attempt in range(max_attempts):
        cur = driver.current_url or ""
        print(f"[NAV] /payment-link 진입 시도 (attempt={attempt}, current_url={cur})")
        try:
            driver.get(PAYMENT_LINK_URL)
        except Exception as e:
            print(f"[NAV] driver.get({PAYMENT_LINK_URL}) 중 예외: {e}")

        if _wait_payment_link_page_ready(driver, timeout=FAST_NAV_WAIT):
            print(f"[NAV] URL 기반 /payment-link 진입 성공 (attempt={attempt}, url={driver.current_url})")
            return True

        try:
            nav_btn = _find_first_visible(
                driver,
                [
                    "//a[contains(@href,'payment-link')]",
                    "//button[contains(@href,'payment-link')]",
                    "//a[contains(normalize-space(.),'결제링크')]",
                    "//a[contains(normalize-space(.),'결제 링크')]",
                    "//button[contains(normalize-space(.),'결제링크')]",
                    "//button[contains(normalize-space(.),'결제 링크')]",
                ],
                timeout=0.8,
            )
            if nav_btn and _fast_click(driver, nav_btn):
                if _wait_payment_link_page_ready(driver, timeout=FAST_NAV_WAIT):
                    print(f"[NAV] 메뉴 클릭으로 /payment-link 진입 성공 (attempt={attempt}, url={driver.current_url})")
                    return True
        except Exception as e_nav:
            print(f"[NAV] 메뉴 기반 /payment-link 진입 예외: {e_nav}")

        _brief_sleep(0.08)

    print("[NAV][ERROR] 여러 차례 시도했으나 /payment-link 로 진입하지 못했습니다.")
    return False


# =========================================================
# 링크 생성
# =========================================================

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
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"{path} 파일을 찾을 수 없습니다.")

    raw = json.loads(p.read_text(encoding="utf-8"))

    if "amount" not in raw or raw.get("amount") in ("", None, "0"):
        raise ValueError("JSON 데이터에 결제 금액(amount)이 없습니다.")

    if not str(raw.get("product_name") or "").strip():
        raw["product_name"] = "SISA 결제링크"

    if not str(raw.get("login_id") or "").strip():
        raw["login_id"] = os.environ.get("K_VAN_ID", "m3313")
    if not str(raw.get("login_password") or "").strip():
        raw["login_password"] = os.environ.get("K_VAN_PW", "1234")
    if not str(raw.get("login_pin") or "").strip():
        raw["login_pin"] = os.environ.get("K_VAN_PIN", "2424")
    if not str(raw.get("card_type") or "").strip():
        raw["card_type"] = "personal"
    if not str(raw.get("installment_months") or "").strip():
        raw["installment_months"] = "일시불"

    for opt in ("card_number", "expiry_mm", "expiry_yy", "card_password", "phone_number", "customer_name", "resident_front"):
        if opt not in raw or raw[opt] is None:
            raw[opt] = ""

    amount_int = int(str(raw["amount"]).replace(",", "").strip())
    if amount_int <= 0:
        raise ValueError(f"amount 값이 0 이하입니다: {amount_int}")

    card_type = str(raw.get("card_type", "personal")).strip().lower()
    if card_type not in ("personal", "business"):
        card_type = "personal"

    return PaymentRow(
        login_id=str(raw["login_id"]).strip(),
        login_password=str(raw["login_password"]).strip(),
        login_pin=str(raw["login_pin"]).strip(),
        card_type=card_type,
        card_number=str(raw.get("card_number", "")).strip(),
        expiry_mm=str(raw.get("expiry_mm", "")).strip(),
        expiry_yy=str(raw.get("expiry_yy", "")).strip(),
        card_password=str(raw.get("card_password", "")).strip(),
        installment_months=str(raw.get("installment_months", "일시불")).strip(),
        phone_number=str(raw.get("phone_number", "")).strip(),
        customer_name=str(raw.get("customer_name", "")).strip(),
        resident_front=str(raw.get("resident_front", "")).strip(),
        amount=amount_int,
        product_name=str(raw["product_name"]).strip(),
    )


def _load_order_with_session_fallback(session_id: str = "") -> PaymentRow:
    if session_id:
        candidates = _session_order_path_candidates(session_id)
        for p in candidates:
            if p.exists():
                return load_order_from_json(str(p))

        st = _load_admin_state()
        amount_val = 0
        installment_val = "일시불"
        for s in st.get("sessions") or []:
            if str(s.get("id")) == str(session_id):
                amount_val = int(str(s.get("amount") or "0").replace(",", "").strip() or "0")
                installment_val = str(s.get("installment") or "일시불")
                break

        if amount_val <= 0:
            raise FileNotFoundError(f"세션 주문 JSON / admin_state 에서 amount 를 찾지 못했습니다. session_id={session_id}")

        return PaymentRow(
            login_id=os.environ.get("K_VAN_ID", "m3313"),
            login_password=os.environ.get("K_VAN_PW", "1234"),
            login_pin=os.environ.get("K_VAN_PIN", "2424"),
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

    return load_order_from_json(str(ORDER_JSON_PATH))


def _choose_product_name_for_amount(amount: int) -> str:
    return f"SISA 세션 결제 {amount:,}원"


def _go_to_create_link_page(driver: webdriver.Chrome) -> bool:
    if not _go_to_payment_link(driver):
        return False

    for _ in range(12):
        try:
            btn = _find_first_visible(driver, [CREATE_BUTTON_XPATH], timeout=0.4)
            if btn and _fast_click(driver, btn):
                _brief_sleep(0.12)
                return True
        except Exception:
            pass
        time.sleep(0.1)

    try:
        clicked = driver.execute_script(
            """
const els = Array.from(document.querySelectorAll('button,a,[role="button"]')).filter(el => el.offsetParent !== null);
for (const el of els) {
  const t = (el.innerText || '').trim().replace(/\\s+/g,'');
  const a = (el.getAttribute('aria-label') || '').trim().replace(/\\s+/g,'');
  if (t.includes('생성') || a.includes('생성')) {
    el.click();
    return true;
  }
}
return false;
"""
        )
        if clicked:
            _brief_sleep(0.12)
            return True
    except Exception:
        pass

    return False


def _set_session_ttl_fast(driver: webdriver.Chrome) -> None:
    try:
        trigger = driver.find_element(By.ID, "session-ttl")
        _fast_click(driver, trigger)
        _brief_sleep(0.06)

        for txt in ("5분", "60분"):
            try:
                opt = _find_first_visible(driver, [f"//*[contains(normalize-space(.),'{txt}')]"], timeout=0.4)
                if opt and _fast_click(driver, opt):
                    _brief_sleep(0.06)
                    return
            except Exception:
                continue
    except Exception:
        pass


def _fill_payment_link_form_and_get_url(driver: webdriver.Chrome, row: PaymentRow, session_id: str) -> Optional[str]:
    amount_input = _find_first_visible(
        driver,
        [
            "//*[contains(normalize-space(.),'금액')]/following::input[1]",
            "//input[@type='number' or @inputmode='decimal']",
        ],
        timeout=1.2,
    )
    if amount_input:
        try:
            amount_input.clear()
        except Exception:
            pass
        amount_input.send_keys(str(row.amount))

    product_name = row.product_name or _choose_product_name_for_amount(row.amount)
    name_input = _find_first_visible(
        driver,
        [
            "//*[contains(normalize-space(.),'상품명')]/following::input[1]",
            "//input[contains(@placeholder,'상품명')]",
        ],
        timeout=0.8,
    )
    if name_input:
        try:
            name_input.clear()
        except Exception:
            pass
        name_input.send_keys(product_name)

    desc_input = _find_first_visible(
        driver,
        [
            "//*[contains(normalize-space(.),'상품설명') or contains(normalize-space(.),'상품 설명')]/following::textarea[1]",
            "//*[contains(normalize-space(.),'상품설명') or contains(normalize-space(.),'상품 설명')]/following::input[1]",
        ],
        timeout=0.6,
    )
    if desc_input:
        try:
            desc_input.clear()
        except Exception:
            pass
        desc_input.send_keys("글로벌 중고명품 경매사이트 구매 대행 서비스 즉시구매 결제 및 예치금")

    try:
        driver.execute_script("window.scrollBy(0, 1200);")
    except Exception:
        pass
    _brief_sleep(0.08)

    _set_session_ttl_fast(driver)

    create_btn = _find_first_visible(driver, ["//button[contains(normalize-space(.),'링크 생성하기')]"], timeout=1.5)
    if not create_btn:
        return None
    if not _fast_click(driver, create_btn):
        return None
    _brief_sleep(0.12)

    confirm_btn = _find_first_visible(driver, [LINK_CREATE_CONFIRM_XPATH], timeout=1.5)
    if confirm_btn:
        _fast_click(driver, confirm_btn)
        _brief_sleep(0.12)

    def _read_url():
        try:
            url_input = driver.find_element(By.XPATH, "//input[@readonly and contains(@value,'https://store.k-van.app')]")
            val = (url_input.get_attribute("value") or "").strip()
            return val if "https://store.k-van.app" in val else None
        except Exception:
            return None

    link_text = _poll_until(_read_url, timeout=3.0, interval=0.06)
    if not link_text:
        return None

    try:
        copy_btn = _find_first_visible(driver, [LINK_COPY_BUTTON_XPATH], timeout=0.8)
        if copy_btn:
            _fast_click(driver, copy_btn)
    except Exception:
        pass

    try:
        driver.execute_script(
            """
const text = arguments[0] || '';
try {
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(text);
  } else {
    const ta = document.createElement('textarea');
    ta.value = text;
    document.body.appendChild(ta);
    ta.select();
    document.execCommand('copy');
    document.body.removeChild(ta);
  }
} catch(e) {}
""",
            link_text,
        )
    except Exception:
        pass

    if session_id:
        _store_kvan_link_for_session(session_id, link_text)

    return link_text


def _store_kvan_link_for_session(session_id: str, link: str) -> None:
    if not session_id or not link:
        return
    session_blob: Optional[dict] = None
    try:
        state = _load_admin_state()
        updated = False
        for s in state.get("sessions") or []:
            if str(s.get("id")) == str(session_id):
                s["kvan_link"] = link
                updated = True
                session_blob = dict(s)
                break
        if updated:
            _save_admin_state(state)
            _append_admin_log("AUTO", f"admin_state 링크 저장 완료 session_id={session_id}")
            try:
                upsert_kvan_link_creation_seed(
                    link,
                    str(session_id),
                    session_blob or {},
                    skip_db=LOCAL_TEST,
                )
            except Exception as e_seed:
                _append_admin_log("AUTO", f"[WARN] kvan_links 시드 실패: {e_seed}")
    except Exception as e:
        print(f"[WARN] admin_state 링크 저장 실패: {e}")


def save_result_to_json(path: str, status: str, message: str, link: str = "") -> None:
    payload = {"status": status, "message": message, "link": link}
    try:
        Path(path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


# =========================================================
# 크롤링 / 파싱
# =========================================================

def _extract_link_from_card(card) -> str:
    try:
        anchors = card.find_elements(By.XPATH, ".//a[contains(@href,'store.k-van.app')]")
        for a in anchors:
            href = (a.get_attribute("href") or "").strip()
            if href:
                return href
    except Exception:
        pass

    try:
        inputs = card.find_elements(By.XPATH, ".//input[contains(@value,'https://store.k-van.app')]")
        for inp in inputs:
            val = (inp.get_attribute("value") or "").strip()
            if val:
                return val
    except Exception:
        pass

    text = _safe_text(card)
    m = re.search(r"(https://store\.k-van\.app[^\s]+)", text)
    return m.group(1) if m else ""


def _parse_link_card(card) -> Optional[dict]:
    card_text = (card.text or "").strip()
    if not card_text:
        return None

    lines = [ln.strip() for ln in card_text.splitlines() if ln.strip()]
    session_id = _get_session_id_from_text(card_text)

    title = ""
    for ln in lines:
        if "상호명" in ln:
            title = ln
            break
    if not title:
        for ln in lines:
            if "상품명" in ln:
                title = ln
                break
    if not title:
        title = lines[0] if lines else ""
    amount = 0
    ttl_label = ""
    status = ""
    mid = ""
    kvan_link = _extract_link_from_card(card)
    if not session_id and kvan_link:
        session_id = extract_kvan_session_key_from_url(kvan_link)
    if not session_id:
        _trace("link_card_skip_no_session", text_preview=card_text[:120])
        return None

    for ln in lines:
        if not amount and "원" in ln:
            amount = _parse_amount(ln)
        if not ttl_label and "분" in ln:
            ttl_label = ln
        if not mid and "MID" in ln.upper():
            mid = ln

    badge_texts = []
    try:
        badges = card.find_elements(By.XPATH, ".//span[@data-slot='badge']")
        badge_texts = [_safe_text(b) for b in badges if _safe_text(b)]
    except Exception:
        pass

    status = _extract_status_from_link_lines(lines)
    if not status:
        for b in badge_texts:
            if _is_expired_link_status(b) or "사용" in b or "대기" in b or "완료" in b:
                status = b
                break

    if "만료" not in (status or ""):
        expire_at = _extract_expire_at_from_lines(lines)
        if expire_at is not None and expire_at < _kvan_now():
            status = "만료"

    if not kvan_link and session_id:
        kvan_link = f"https://store.k-van.app/p/{session_id}?sessionId={session_id}&type=KEYED"

    aid = ""
    try:
        got = _get_agency_id_for_session(session_id)
        aid = (got or "").strip()
    except Exception:
        aid = ""
    internal_sid = _lookup_internal_session_id_for_kvan_key(session_id)

    return {
        "captured_at": datetime.utcnow().isoformat(),
        "title": title,
        "amount": amount,
        "ttl_label": ttl_label,
        "status": status,
        "kvan_link": kvan_link,
        "mid": mid,
        "kvan_session_id": session_id,
        "agency_id": aid,
        "internal_session_id": internal_sid,
        "raw_text": card_text,
    }


def _scrape_payment_links_and_store(driver: webdriver.Chrome, store: KVStore) -> int:
    if not _go_to_payment_link(driver):
        raise RuntimeError("[NAV] /payment-link 로 진입하지 못했습니다.")

    try:
        driver.refresh()
    except Exception:
        pass

    _wait_payment_link_page_ready(driver, timeout=FAST_NAV_WAIT)

    rows: list[dict] = []
    seen: set[str] = set()

    try:
        tx_buttons = driver.find_elements(By.XPATH, TX_BUTTON_XPATH)
    except Exception:
        tx_buttons = []

    for btn in tx_buttons:
        try:
            card = _find_card_container(btn)
            if not card:
                continue
            parsed = _parse_link_card(card)
            if not parsed:
                continue
            key = parsed.get("kvan_session_id") or parsed.get("kvan_link")
            if key and key not in seen:
                seen.add(key)
                rows.append(parsed)
        except Exception:
            continue

    if not rows:
        try:
            cards = driver.find_elements(
                By.XPATH,
                "//*[contains(normalize-space(.),'KEY20')]/ancestor::tr[1]"
                " | //*[contains(normalize-space(.),'KEY20')]/ancestor::*[@role='row'][1]"
                " | //*[contains(normalize-space(.),'KEY20')]/ancestor::div[contains(@class,'rounded')][1]"
            )
        except Exception:
            cards = []

        for card in cards:
            try:
                parsed = _parse_link_card(card)
                if not parsed:
                    continue
                key = parsed.get("kvan_session_id") or parsed.get("kvan_link")
                if key and key not in seen:
                    seen.add(key)
                    rows.append(parsed)
            except Exception:
                continue

    store.replace_kvan_links(rows)
    print(f"[INFO] /payment-link 에서 {len(rows)}건 저장 완료 (json={store.use_json})")
    return len(rows)


def _scrape_transactions_and_store(driver: webdriver.Chrome, store: KVStore) -> int:
    """
    /transactions 스크랩 - `kvan_tx_table_scrape` (구버전 단순 헤더 + infer 폴백).
    """
    _trace(
        "tx_scrape_start",
        current_url=(driver.current_url or "")[:120],
        use_json=store.use_json,
    )
    snapshot_rows, body_rows, used_label, _attempts, h_tr1 = (
        extract_kvan_transactions_from_page(driver, navigate=True)
    )
    _trace(
        "tx_scrape_result",
        used_label=used_label or "(none)",
        body_rows=len(body_rows),
        snapshot_rows=len(snapshot_rows),
        header_attempts=len(_attempts),
        h_tr1_preview=(h_tr1[:8] if h_tr1 else []),
    )
    if used_label == "timeout":
        print(
            "[WARN] /transactions 테이블 로딩 타임아웃 - kvan_transactions 는 비우지 않고 유지합니다."
        )
        _alog("/transactions tbody 로딩 타임아웃")
        _trace("tx_scrape_timeout")
        return 0

    if snapshot_rows:
        used_headers = next(
            (h for lab, h in _attempts if lab == used_label),
            [],
        )
        if used_headers:
            preview = used_headers[: min(12, len(used_headers))]
            print(
                f"[INFO] /transactions 헤더 소스={used_label}, {len(used_headers)}컬럼 → "
                f"스냅샷 {len(snapshot_rows)}건 | {preview}"
                f"{'…' if len(used_headers) > 12 else ''}"
            )

    if not snapshot_rows and body_rows:
        print(
            f"[WARN] /transactions tbody {len(body_rows)}행이 있으나 유효 파싱 0건 - "
            "헤더·열 불일치 가능. kvan_transactions DB 유지."
        )
        try:
            print(f"[DEBUG] 첫 행 셀 {len(body_rows[0])}개: {body_rows[0][:10]}")
            print(f"[DEBUG] tr1 헤더: {h_tr1[:14]!s}")
        except Exception:
            pass
        infer_n = sum(1 for lab, _ in _attempts if lab.startswith("infer_"))
        _alog(
            f"[WARN] /transactions 파싱 0건 tbody_rows={len(body_rows)} infer_tries={infer_n}"
        )
        _trace(
            "tx_scrape_zero_snapshot",
            first_body_row=(body_rows[0][:10] if body_rows else []),
            infer_tries=infer_n,
        )
        return 0

    if not snapshot_rows and not body_rows:
        store.replace_kvan_transactions([], force_empty=True)
        print("[INFO] /transactions 거래 행 0건 - kvan_transactions 비움")
        _trace("tx_scrape_empty_body")
        return 0

    store.replace_kvan_transactions(snapshot_rows)
    print(
        f"[INFO] /transactions 에서 {len(snapshot_rows)}건 저장 완료 "
        f"(json={store.use_json}, header={used_label})"
    )
    _trace(
        "tx_scrape_saved",
        saved_rows=len(snapshot_rows),
        header=used_label,
        first_snapshot=(snapshot_rows[0] if snapshot_rows else {}),
    )
    return len(snapshot_rows)


def _scrape_dashboard_and_store(driver: webdriver.Chrome) -> None:
    """
    가맹점 홈(대시보드)에서 월/전일 매출·정산·크레딧 요약을 읽어 kvan_dashboard 테이블에 INSERT.
    (구 auto_kvan.py 에만 있던 로직 - 링크 생성 전용 main() 에서는 호출되지 않아 크롤러로 이전)
    """
    if _use_json_store():
        print("[INFO] kvan_dashboard: JSON 저장소 모드 - 대시보드 DB 스킵")
        return
    try:
        started = time.perf_counter()
        time.sleep(0.2)

        max_wait = float(os.environ.get("K_VAN_DASHBOARD_WAIT_SEC", "1.2"))
        poll = float(os.environ.get("K_VAN_DASHBOARD_POLL_SEC", "0.12"))

        labels = {
            "monthly": "월 매출",
            "yesterday": "전일 매출",
            "settlement": "정산 예정 금액",
            "credit": "나의 크레딧",
        }
        found: dict[str, Any] = {k: None for k in labels}
        end_ts = time.time() + max(0.4, max_wait)
        while time.time() <= end_ts:
            progressed = False
            for key, label_text in labels.items():
                if found.get(key) is not None:
                    continue
                try:
                    els = driver.find_elements(
                        By.XPATH, f"//*[normalize-space(text())='{label_text}']"
                    )
                    if els:
                        found[key] = els[0].find_element(By.XPATH, "./ancestor::div[1]")
                        progressed = True
                except Exception:
                    continue
            if all(found.values()):
                break
            if not progressed:
                time.sleep(poll)
        # 경계 타이밍에 라벨이 뜬 경우를 한 번 더 보정
        for key, label_text in labels.items():
            if found.get(key) is not None:
                continue
            try:
                els = driver.find_elements(
                    By.XPATH, f"//*[normalize-space(text())='{label_text}']"
                )
                if els:
                    found[key] = els[0].find_element(By.XPATH, "./ancestor::div[1]")
            except Exception:
                continue
        monthly_block = found.get("monthly")
        yesterday_block = found.get("yesterday")
        settlement_block = found.get("settlement")
        credit_block = found.get("credit")

        monthly_sales = monthly_approved_cnt = monthly_approved_amt = 0
        monthly_canceled_cnt = monthly_canceled_amt = 0

        if monthly_block:
            try:
                amt_el = monthly_block.find_element(By.XPATH, ".//*[contains(text(),'원')]")
                monthly_sales = _parse_amount(amt_el.text)
            except Exception:
                pass
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
            try:
                today_el = settlement_block.find_element(
                    By.XPATH,
                    ".//*[contains(normalize-space(text()),'금일 정산 예정금')]/following::div[1]",
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

        recent_summary = ""
        try:
            recent_container = driver.find_element(
                By.XPATH,
                "//*[contains(normalize-space(text()),'최근 거래 내역')]/ancestor::section[1]",
            )
            recent_summary = recent_container.text.strip()
        except Exception:
            pass

        conn = _get_db_dashboard_quick()
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
        elapsed = time.perf_counter() - started
        print(f"[INFO] 대시보드 요약을 kvan_dashboard 에 저장했습니다. ({elapsed:.2f}s)")
        _alog(f"대시보드 요약 kvan_dashboard 저장 ({elapsed:.2f}s)")
    except Exception as e:
        print(f"[WARN] 대시보드 크롤링/DB 저장 실패: {e}")
        _alog(f"[WARN] 대시보드 크롤링 실패: {e}")


def _dashboard_home_and_scrape(driver: webdriver.Chrome) -> None:
    """스토어 루트로 이동 후 대시보드 스크랩. 이후 호출측에서 /payment-link 로 복귀."""
    try:
        t0 = time.perf_counter()
        # 이미 dashboard/루트에 있으면 불필요한 재이동을 줄인다.
        cur = (driver.current_url or "").lower()
        if "dashboard" not in cur and "store.k-van.app" not in cur:
            driver.get(SIGN_IN_URL)
            time.sleep(0.25)
        _scrape_dashboard_and_store(driver)
        _alog(f"대시보드 홈 스크랩 완료 ({time.perf_counter() - t0:.2f}s)")
    except Exception as e:
        _alog(f"[WARN] 대시보드 홈 이동/스크랩: {e}")


# =========================================================
# 팝업 / 삭제
# =========================================================

def _wait_tx_dialog_fast(driver: webdriver.Chrome, timeout: float = FAST_UI_WAIT):
    return _find_first_visible(
        driver,
        [
            "//div[@role='dialog' and .//h2[contains(normalize-space(.),'거래 내역')]]",
            "//div[@role='dialog' and contains(normalize-space(.),'거래 내역')]",
            "//div[contains(@class,'dialog') and contains(normalize-space(.),'거래 내역')]",
            "//div[contains(@class,'modal') and contains(normalize-space(.),'거래 내역')]",
        ],
        timeout=timeout,
    )


def _close_dialog(driver: webdriver.Chrome, dialog) -> None:
    try:
        try:
            close_btn = dialog.find_element(By.XPATH, CLOSE_DIALOG_XPATH)
            _fast_click(driver, close_btn)
        except Exception:
            try:
                body = driver.find_element(By.TAG_NAME, "body")
                body.send_keys(Keys.ESCAPE)
            except Exception:
                pass

        try:
            WebDriverWait(driver, 0.3).until_not(
                EC.presence_of_element_located((By.XPATH, "//div[@role='dialog']"))
            )
        except TimeoutException:
            pass

        _brief_sleep(0.04)
    except Exception:
        pass


def _click_trash_and_confirm(driver: webdriver.Chrome, card) -> bool:
    try:
        trash_btn = card.find_element(By.XPATH, TRASH_BUTTON_REL_XPATH)
        if not _fast_click(driver, trash_btn):
            return False

        # 팝업이 닫힐 때 DOM이 바로 제거되지 않고 data-state만 바뀌는 UI가 있어,
        # 단순 "presence" 대신 data-state/표시여부로 open->closed 전환을 판정합니다.
        timeout_sec = float(os.environ.get("K_VAN_DELETE_CONFIRM_TIMEOUT", "8"))
        retry_interval = 0.2
        end_ts = time.time() + timeout_sec

        alertdialog_xpath = "//div[@role='alertdialog']"

        def _is_alertdialog_open() -> bool:
            try:
                dialogs = driver.find_elements(By.XPATH, alertdialog_xpath)
                if not dialogs:
                    return False
                # 열린 것만 찾기: data-state="open" 이면서 표시되는 경우
                for d in dialogs:
                    try:
                        if d.is_displayed():
                            ds = (d.get_attribute("data-state") or "").strip().lower()
                            if ds == "open":
                                return True
                            # data-state가 없으면 open/closed 판단 못하므로 보수적으로 표시중이면 open으로 취급
                            if not ds:
                                return True
                    except Exception:
                        continue
            except Exception:
                pass
            return False

        while time.time() < end_ts:
            # 팝업이 이미 닫혔으면 성공
            try:
                if not _is_alertdialog_open():
                    _brief_sleep(0.04)
                    return True
            except Exception:
                pass

            try:
                dialog = driver.find_element(By.XPATH, alertdialog_xpath)
                # 팝업 안에서 '삭제' 버튼만 다시 탐색
                confirm_candidates = dialog.find_elements(By.XPATH, ".//button[contains(normalize-space(.),'삭제') and not(contains(normalize-space(.),'취소'))]")
                confirm_btn = None
                for c in confirm_candidates:
                    try:
                        if c.is_displayed():
                            confirm_btn = c
                            break
                    except Exception:
                        continue

                if confirm_btn is None:
                    time.sleep(retry_interval)
                    continue

                # 1) 일반 클릭
                if _fast_click(driver, confirm_btn):
                    try:
                        WebDriverWait(driver, 0.35).until(lambda _: not _is_alertdialog_open())
                        _brief_sleep(0.04)
                        return True
                    except Exception:
                        pass

                # 2) JS click
                try:
                    driver.execute_script("arguments[0].click();", confirm_btn)
                    try:
                        WebDriverWait(driver, 0.35).until(lambda _: not _is_alertdialog_open())
                        _brief_sleep(0.04)
                        return True
                    except TimeoutException:
                        pass
                except Exception:
                    pass

                # 3) disabled/pointer-events 스타일이 남아있어도 트리거를 만들기 위한 강제 시도
                try:
                    driver.execute_script(
                        """
                        try { arguments[0].disabled = false; } catch(e) {}
                        try { arguments[0].removeAttribute('disabled'); } catch(e) {}
                        arguments[0].click();
                        """,
                        confirm_btn,
                    )
                    try:
                        WebDriverWait(driver, 0.35).until(lambda _: not _is_alertdialog_open())
                        _brief_sleep(0.04)
                        return True
                    except TimeoutException:
                        pass
                except Exception:
                    pass
            except Exception:
                # 팝업 DOM이 잠깐 렌더링되는 타이밍일 수 있음
                pass

            time.sleep(retry_interval)

        print("[WARN] 휴지통 클릭 후 삭제 팝업이 닫히지 않았습니다.")
        return False
    except Exception as e:
        print(f"[WARN] 휴지통/삭제 처리 오류: {e}")
        return False


def _is_card_expired(card) -> bool:
    try:
        badges = card.find_elements(By.XPATH, ".//span[@data-slot='badge']")
        badge_texts = [(_safe_text(b) or "").replace(" ", "") for b in badges]
        for b in badge_texts:
            if b in ("만료", "취소", "취소됨", "취소완료"):
                return True
    except Exception:
        pass

    lines = [ln.strip() for ln in (card.text or "").splitlines() if ln.strip()]
    status = _extract_status_from_link_lines(lines)
    if _is_expired_link_status(status):
        return True

    expire_at = _extract_expire_at_from_lines(lines)
    if expire_at and expire_at <= _kvan_now():
        return True

    return False


def _delete_expired_no_tx_links_fast(driver: webdriver.Chrome, store: KVStore, max_delete: int = FAST_DELETE_PER_PASS) -> int:
    if not _go_to_payment_link(driver):
        return 0

    _wait_payment_link_page_ready(driver, timeout=FAST_NAV_WAIT)
    deleted = 0

    max_elapsed = float(os.environ.get("K_VAN_DELETE_PASS_MAX_SEC", "12"))
    started = time.perf_counter()
    while deleted < max_delete:
        if (time.perf_counter() - started) >= max(3.0, max_elapsed):
            _dbg(
                f"[DELETE] pass time budget reached ({time.perf_counter() - started:.2f}s) "
                f"deleted={deleted}"
            )
            break
        try:
            tx_buttons = driver.find_elements(By.XPATH, TX_BUTTON_XPATH)
        except Exception:
            tx_buttons = []

        if not tx_buttons:
            break

        deleted_this_round = False

        for btn in tx_buttons:
            try:
                card = _find_card_container(btn)
                if not card:
                    continue

                card_text = _safe_text(card)
                if not card_text:
                    continue

                session_id = _get_session_id_from_text(card_text)
                title = card_text.split("\n")[0].strip() if "\n" in card_text else card_text[:80]

                if not _is_card_expired(card):
                    continue

                if not _fast_click(driver, btn):
                    continue

                dialog = _wait_tx_dialog_fast(driver, timeout=1.2)
                if not dialog:
                    continue

                popup_text = _safe_text(dialog)
                no_history = _popup_has_no_history(popup_text)
                popup_rows_unknown = False

                rows = []
                try:
                    rows = dialog.find_elements(By.XPATH, ".//table//tbody//tr")
                except Exception:
                    rows = []

                if not no_history and len(rows) == 0:
                    # 팝업 로딩 지연으로 tbody 행이 아직 없을 수 있다.
                    # 이 경우를 "거래없음"으로 간주하면 거래가 있는 링크를 오삭제할 수 있으므로
                    # 즉시 삭제를 건너뛰고 다음 사이클에서 재검증한다.
                    popup_rows_unknown = True

                if not no_history and rows:
                    first_row_text = (rows[0].text or "").strip()
                    if "없습니다" in first_row_text or "없음" in first_row_text:
                        no_history = True

                if popup_rows_unknown:
                    _trace(
                        "expired_skip_delete_unknown_popup",
                        session_id=session_id or "-",
                        title=title[:60],
                    )
                    _close_dialog(driver, dialog)
                    continue

                if no_history:
                    _close_dialog(driver, dialog)
                    if _click_trash_and_confirm(driver, card):
                        if session_id:
                            _mark_session_deleted(session_id, title)
                        _trace("expired_delete_no_tx", session_id=session_id or "-", title=title[:60])
                        deleted += 1
                        deleted_this_round = True
                        _append_admin_log("AUTO", f"즉시 삭제 완료 session_id={session_id or '-'}")
                        _brief_sleep(0.08)
                        break
                    continue

                # 만료 + 거래있음
                try:
                    if rows:
                        row = rows[0]
                        cells = row.find_elements(By.XPATH, ".//td")
                        if len(cells) >= 7:
                            tx_type = _safe_text(cells[1]) if len(cells) > 1 else ""
                            amount_text = _safe_text(cells[2]) if len(cells) > 2 else ""
                            approval_no = _safe_text(cells[3]) if len(cells) > 3 else ""
                            customer_name = _safe_text(cells[4]) if len(cells) > 4 else ""
                            card_number = _safe_text(cells[5]) if len(cells) > 5 else ""
                            registered_at = _safe_text(cells[6]) if len(cells) > 6 else ""
                            amt = _parse_amount(amount_text)

                            if "결제 승인" in tx_type and amt:
                                store.upsert_popup_transaction(
                                    session_id=session_id,
                                    amount=amt,
                                    approval_no=approval_no,
                                    card_number=card_number,
                                    registered_at=registered_at,
                                    customer_name=customer_name,
                                )
                except Exception as e_parse:
                    print(f"[WARN] 만료+거래있음 팝업 파싱 실패: {e_parse}")

                _mark_session_expired_with_transactions(session_id, title)
                _trace("expired_keep_with_tx", session_id=session_id or "-", title=title[:60], rows=len(rows))
                _close_dialog(driver, dialog)

            except StaleElementReferenceException:
                continue
            except Exception as e:
                print(f"[WARN] 즉시 삭제 처리 중 오류: {e}")
                continue

        if not deleted_this_round:
            break

        _wait_payment_link_page_ready(driver, timeout=1.0)

    return deleted


def _scan_payment_link_popups_and_sync(
    driver: webdriver.Chrome,
    store: KVStore,
    allow_popup_for_non_expired: bool = True,
) -> bool:
    changed = False

    if not _go_to_payment_link(driver):
        return False

    try:
        tx_buttons = driver.find_elements(By.XPATH, TX_BUTTON_XPATH)
    except Exception:
        tx_buttons = []

    if not tx_buttons:
        return changed

    seen_session_ids: set[str] = set()

    for btn in tx_buttons:
        try:
            card = _find_card_container(btn)
            if not card:
                continue

            card_text = (card.text or "").strip()
            if not card_text:
                continue

            session_id = _get_session_id_from_text(card_text)
            if not session_id:
                continue

            if session_id in seen_session_ids:
                continue
            seen_session_ids.add(session_id)

            title = card_text.split("\n")[0].strip() if "\n" in card_text else card_text[:80]
            is_expired = _is_card_expired(card)

            if is_expired:
                continue

            if _is_session_already_processed(session_id):
                continue

            if not allow_popup_for_non_expired:
                _mark_session_checked(session_id, title, has_approval=False)
                continue

            if not _fast_click(driver, btn):
                continue

            dialog = _wait_tx_dialog_fast(driver, timeout=1.2)
            if not dialog:
                continue

            popup_text = dialog.text or ""

            if _popup_has_no_history(popup_text):
                _close_dialog(driver, dialog)
                _mark_session_checked(session_id, title, has_approval=False)
                continue

            try:
                rows = dialog.find_elements(By.XPATH, ".//table//tbody//tr")
                if not rows:
                    _mark_session_checked(session_id, title, has_approval=False)
                    _close_dialog(driver, dialog)
                    continue

                row = rows[0]
                cells = row.find_elements(By.XPATH, ".//td")
                if len(cells) < 7:
                    _mark_session_checked(session_id, title, has_approval=False)
                    _close_dialog(driver, dialog)
                    continue

                tx_type = _safe_text(cells[1]) if len(cells) > 1 else ""
                amount_text = _safe_text(cells[2]) if len(cells) > 2 else ""
                approval_no = _safe_text(cells[3]) if len(cells) > 3 else ""
                customer_name = _safe_text(cells[4]) if len(cells) > 4 else ""
                card_number = _safe_text(cells[5]) if len(cells) > 5 else ""
                registered_at = _safe_text(cells[6]) if len(cells) > 6 else ""
                amt = _parse_amount(amount_text)

                if "결제 승인" in tx_type and amt:
                    store.upsert_popup_transaction(
                        session_id=session_id,
                        amount=amt,
                        approval_no=approval_no,
                        card_number=card_number,
                        registered_at=registered_at,
                        customer_name=customer_name,
                    )
                    _mark_session_checked(session_id, title, has_approval=True)
                    changed = True
                else:
                    _mark_session_checked(session_id, title, has_approval=False)

            except Exception as e_row:
                print(f"[WARN] 거래내역 팝업 파싱 오류: {e_row}")
                _mark_session_checked(session_id, title, has_approval=False)
            finally:
                _close_dialog(driver, dialog)

        except StaleElementReferenceException:
            continue
        except Exception as e_card:
            print(f"[WARN] 결제링크 카드 처리 오류: {e_card}")
            continue

    return changed


def mark_expired_sessions_from_kvan_links(store: KVStore) -> None:
    try:
        expired_urls: set[str] = set()
        rows = store.load_kvan_links()

        for row in rows:
            url = (row.get("kvan_link") or "").strip()
            status_text = str(row.get("status") or "").strip()
            if url and _is_expired_link_status(status_text):
                expired_urls.add(url)

        if not expired_urls:
            return

        st = _load_admin_state()
        sessions = list(st.get("sessions") or [])
        history = list(st.get("history") or [])
        remaining_sessions: list[dict] = []
        removed_count = 0
        now_iso = datetime.utcnow().isoformat()

        for s in sessions:
            link = (s.get("kvan_link") or "").strip()
            if link and link in expired_urls:
                removed_count += 1
                sid = str(s.get("id") or "")
                s["status"] = "만료"
                s["deleted"] = True
                s["deleted_in_kvan"] = True
                s["deleted_at"] = now_iso
                s["finished_at"] = s.get("finished_at") or now_iso
                old_msg = str(s.get("result_message") or "").strip()
                mark_msg = "만료 감지로 K-VAN 링크가 삭제되었습니다."
                s["result_message"] = f"{old_msg}\n{mark_msg}".strip() if old_msg else mark_msg
                history = _upsert_history_by_session_id(history, dict(s))
                _append_admin_log("AUTO", f"만료 링크 세션 정리 session_id={sid}")
            else:
                remaining_sessions.append(s)

        st["sessions"] = remaining_sessions
        st["history"] = history
        _save_admin_state(st)
        store.delete_kvan_links_by_urls(expired_urls)

        if removed_count:
            _append_admin_log("AUTO", f"만료/취소 링크 정리 완료 (세션 {removed_count}건, 링크 {len(expired_urls)}건)")
    except Exception as e:
        print(f"[WARN] 링크 만료 세션 반영 실패: {e}")


# =========================================================
# 실행
# =========================================================

def run_create(session_id: str = "") -> int:
    row = _load_order_with_session_fallback(session_id=session_id)
    result_json_path = (SESSION_RESULT_DIR / f"{session_id}.json") if session_id else RESULT_JSON_PATH

    driver = create_driver()
    try:
        _append_admin_log("AUTO", f"K-VAN 로그인 시작 session_id={session_id or '-'}")
        sign_in(driver, row)

        _append_admin_log("AUTO", "로그인 완료, /payment-link 즉시 진입")
        if not _go_to_payment_link(driver):
            msg = "/payment-link 진입 실패"
            print(f"[ERROR] {msg}")
            save_result_to_json(str(result_json_path), "error", msg)
            return 1

        if not _go_to_create_link_page(driver):
            msg = "결제링크 생성 페이지 진입 실패(+ 생성 버튼 미동작)"
            print(f"[ERROR] {msg}")
            save_result_to_json(str(result_json_path), "error", msg)
            return 1

        link_url = _fill_payment_link_form_and_get_url(driver, row, session_id)
        if not link_url:
            msg = "결제 링크 생성 실패 또는 링크 미발견"
            print(f"[ERROR] {msg}")
            save_result_to_json(str(result_json_path), "error", msg)
            return 1

        _append_admin_log("AUTO", f"결제 링크 생성 완료 session_id={session_id or '-'} link={link_url}")
        save_result_to_json(
            str(result_json_path),
            "link_created",
            "결제 링크가 생성되었습니다.",
            link=link_url,
        )
        signal_crawler_wakeup()
        print(f"[INFO] 생성된 결제 링크: {link_url}")
        return 0
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def _wait_with_wakeup(total_delay: int) -> None:
    waited = 0
    while waited < total_delay:
        _touch_heartbeat()
        step = min(1, total_delay - waited)
        time.sleep(step)
        waited += step
        try:
            if WAKEUP_FLAG_PATH.exists():
                try:
                    WAKEUP_FLAG_PATH.unlink()
                except Exception:
                    pass
                return
        except Exception:
            pass


def run_crawler_loop(max_cycles: int = 0, max_runtime_sec: int = 0) -> int:
    store = KVStore()
    driver = create_driver(headless=_is_server_env())
    _trace(
        "crawler_boot",
        data_dir=str(DATA_DIR),
        use_json=store.use_json,
        server_env=_is_server_env(),
        database_url=(DATABASE_URL or "")[:120],
    )

    try:
        print("[crawler] K-VAN 로그인 시작")
        _alog("K-VAN 로그인 시작")

        env_row = PaymentRow(
            login_id=os.environ.get("K_VAN_ID", "m3313"),
            login_password=os.environ.get("K_VAN_PW", "1234"),
            login_pin=os.environ.get("K_VAN_PIN", "2424"),
            card_type="personal",
            card_number="",
            expiry_mm="",
            expiry_yy="",
            card_password="",
            installment_months="일시불",
            phone_number="",
            customer_name="",
            resident_front="",
            amount=0,
            product_name="",
        )
        sign_in(driver, env_row)
        print("[crawler] 로그인 완료. 주기 크롤링 루프 시작.")
        _alog("로그인 완료. 주기 크롤링 루프 시작")

        # 로그인 직후 1회: 대시보드 매출 요약 → kvan_dashboard (본사 HQ 스키마와 동일)
        _dashboard_home_and_scrape(driver)

        if _go_to_payment_link(driver):
            try:
                deleted_boot = _delete_expired_no_tx_links_fast(driver, store, max_delete=FAST_DELETE_PER_PASS)
                _dbg(f"로그인 직후 즉시 삭제={deleted_boot}")
            except Exception as e0:
                _dbg(f"로그인 직후 삭제 루틴 오류: {e0}")

        backup_interval = int(os.environ.get("K_VAN_CRAWL_INTERVAL", "600"))
        startup_fast_cycles = int(os.environ.get("K_VAN_STARTUP_FAST_CYCLES", "3"))
        # 로컬/서버 동일 기본 스케줄 (필요 시 환경변수로만 조절)
        sleep_idle = int(os.environ.get("K_VAN_IDLE_SLEEP_SEC", "180"))
        sleep_active = int(os.environ.get("K_VAN_ACTIVE_SLEEP_SEC", "2"))
        sleep_medium = int(os.environ.get("K_VAN_MEDIUM_SLEEP_SEC", "30"))
        sleep_startup = int(os.environ.get("K_VAN_STARTUP_SLEEP_SEC", "2"))
        active_win = int(os.environ.get("K_VAN_ACTIVE_SESSION_WINDOW_MINUTES", "3"))
        popup_win = int(os.environ.get("K_VAN_POPUP_SESSION_WINDOW_MINUTES", "30"))
        last_backup_ts = 0.0
        cycle = 0
        empty_cycles = 0
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

            had_new = False
            deleted_any = False

            try:
                if not _has_any_admin_sessions():
                    _backfill_admin_state_from_kvan_links(store)
                # 1) 가장 먼저 즉시 삭제
                deleted = _delete_expired_no_tx_links_fast(driver, store, max_delete=FAST_DELETE_PER_PASS)
                if deleted > 0:
                    deleted_any = True
                    had_new = True

                # 2) 링크 스냅샷
                link_count = _scrape_payment_links_and_store(driver, store)
                mark_expired_sessions_from_kvan_links(store)

                has_links = link_count > 0 or _has_payment_links_quick(driver)

                # 3) 팝업 동기화
                active_for_popup = _has_active_sessions(window_minutes=popup_win)
                source_empty = not _has_any_admin_sessions()
                if source_empty:
                    # admin_state 기반 매핑 소스가 비면 팝업 파싱을 유지해
                    # 승인번호/카드정보를 transactions에 먼저 반영하고 kvan_links 기반 매핑을 복구한다.
                    active_for_popup = True
                    _trace("popup_scan_forced", reason="admin_state_empty")
                if has_links:
                    if _scan_payment_link_popups_and_sync(driver, store, allow_popup_for_non_expired=active_for_popup):
                        had_new = True

                # 4) 거래내역 스냅샷
                _scrape_transactions_and_store(driver, store)

                # 5) K-VAN -> transactions 동기화
                if store.sync_kvan_to_transactions():
                    had_new = True

                # 6) 마지막 링크 존재 여부 확인
                if _go_to_payment_link(driver):
                    try:
                        driver.refresh()
                    except Exception:
                        pass
                    _wait_payment_link_page_ready(driver, timeout=1.5)
                    has_links_end = _has_payment_links_quick(driver)
                    if has_links_end:
                        empty_cycles = 0
                        _dbg("사이클 종료: 결제링크가 하나 이상 존재 → empty_cycles=0")
                    else:
                        empty_cycles += 1
                        _dbg(f"사이클 종료: 결제링크 없음 → empty_cycles={empty_cycles}")

            except Exception as e:
                print(f"[crawler][WARN] 크롤링 오류: {e}")
                _alog(f"[WARN] 크롤링 오류: {e}")
                try:
                    emsg = str(e or "").lower()
                    driver_dead = any(
                        k in emsg
                        for k in (
                            "invalid session id",
                            "failed to establish a new connection",
                            "max retries exceeded",
                            "session deleted because of page crash",
                        )
                    )
                    if driver_dead:
                        try:
                            driver.quit()
                        except Exception:
                            pass
                        driver = create_driver(headless=_is_server_env())
                        _alog("[WARN] webdriver 세션 장애 감지 → driver 재생성")
                    sign_in(driver, env_row)
                except Exception as e2:
                    print(f"[crawler][ERROR] 재로그인 오류: {e2}")
                    _alog(f"[ERROR] 재로그인 오류: {e2}")

            cycle += 1

            if max_cycles > 0 and cycle >= max_cycles:
                msg = f"테스트 종료: 최대 사이클 도달 ({cycle}/{max_cycles})"
                print(f"[crawler] {msg}")
                _alog(msg)
                break

            active = _has_active_sessions(window_minutes=active_win)

            delay = sleep_startup
            delay_reason = "startup"
            if cycle >= startup_fast_cycles:
                # 삭제가 발생했거나(=DB 상태 변화 가능) / 신규가 없고(=바로 크롤링할 게 없음)
                # 이 경우에는 추가로 결제/취소 페이지와 결제링크 생성페이지(payment-link)를 한 번 더 확인 후 장시간 대기
                no_new_work = (not had_new and not active and empty_cycles >= 1)
                if deleted_any or no_new_work:
                    try:
                        _dbg(
                            f"[NO_WORK/DELETE] 추가 확인 수행 (deleted_any={deleted_any}, no_new_work={no_new_work})"
                        )
                    except Exception:
                        pass

                    # 1) 결제 및 취소(/transactions) 페이지 확인(스냅샷 반영)
                    try:
                        _scrape_transactions_and_store(driver, store)
                    except Exception as _e_tx:
                        _dbg(f"[NO_WORK/DELETE] /transactions 확인 실패(무시): {_e_tx!r}")

                    # 2) 결제링크 생성페이지 확인(=payment-link) : 새로고침 후 준비 상태 확인
                    try:
                        if _go_to_payment_link(driver):
                            try:
                                driver.refresh()
                            except Exception:
                                pass
                            _wait_payment_link_page_ready(driver, timeout=1.8)
                            # 생성 버튼이 보이는지 정도만 체크(클릭/수정은 하지 않음)
                            try:
                                _find_first_visible(driver, [CREATE_BUTTON_XPATH], timeout=1.2)
                            except Exception:
                                pass
                    except Exception as _e_pl:
                        _dbg(f"[NO_WORK/DELETE] payment-link 확인 실패(무시): {_e_pl!r}")

                    delay = sleep_idle
                    delay_reason = "idle_after_check"
                elif active or had_new:
                    delay = sleep_active
                    delay_reason = "active_or_new"
                elif empty_cycles >= 3:
                    delay = sleep_idle
                    delay_reason = "empty_links_stable"
                else:
                    # 로컬/서버 구분 없이 동일 (구 로컬 5초는 K_VAN_MEDIUM_SLEEP_SEC=5 로 설정 가능)
                    delay = sleep_medium
                    delay_reason = "medium"

            print(
                f"[crawler] 다음 크롤링까지 {delay}초 대기 "
                f"({delay_reason}, active={active}, had_new={had_new}, empty_cycles={empty_cycles}, "
                f"active_win={active_win}m)"
            )
            _alog(
                f"다음 크롤링까지 {delay}초 ({delay_reason}, active={active}, had_new={had_new}, "
                f"empty_cycles={empty_cycles})"
            )
            _wait_with_wakeup(delay)

            now_ts = time.time()
            if backup_interval > 0 and now_ts - last_backup_ts >= backup_interval:
                print(f"[crawler] 백업 주기({backup_interval}s) 도달 - 정상 동작 확인")
                _alog(f"백업 주기({backup_interval}s) 도달 - 정상 동작")
                last_backup_ts = now_ts
                try:
                    _dashboard_home_and_scrape(driver)
                    if not _go_to_payment_link(driver):
                        _alog("[WARN] 백업 주기 후 /payment-link 복귀 실패")
                except Exception as e_dash:
                    _alog(f"[WARN] 백업 주기 대시보드 스크랩: {e_dash}")

        return 0

    finally:
        _alog("크롤러 종료 (driver.quit)")
        _touch_heartbeat()
        try:
            driver.quit()
        except Exception:
            pass


# =========================================================
# CLI
# =========================================================

def _parse_args():
    p = argparse.ArgumentParser(description="K-VAN 통합 실행 파일")
    p.add_argument("--mode", choices=["create", "crawl"], default="crawl", help="create=링크생성 / crawl=크롤러")
    p.add_argument("--session-id", default="", help="세션 주문 JSON용 session_id")
    p.add_argument("--max-cycles", type=int, default=int(os.environ.get("K_VAN_CRAWLER_MAX_CYCLES", "0")))
    p.add_argument("--max-seconds", type=int, default=int(os.environ.get("K_VAN_CRAWLER_MAX_SECONDS", "0")))
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if args.mode == "create":
        raise SystemExit(run_create(session_id=args.session_id.strip()))
    raise SystemExit(run_crawler_loop(max_cycles=max(0, args.max_cycles), max_runtime_sec=max(0, args.max_seconds)))