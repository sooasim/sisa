# -*- coding: utf-8 -*-
"""
K-VAN 통합 실행 파일
- 로컬: JSON 저장소 사용
- 서버: Railway MySQL 사용
- 기본 실행 모드: crawl
- create 모드: 결제 링크 생성
- crawl 모드: 로그인 → /payment-link → 만료+거래없음 즉시 삭제 → 링크/거래 동기화

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
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs, unquote
from typing import Optional, List, Dict, Any

import pymysql
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

_raw_data_dir = os.environ.get("SISA_DATA_DIR", "").strip()
if _raw_data_dir:
    DATA_DIR = Path(_raw_data_dir)
else:
    app_data = Path("/app/data")
    DATA_DIR = app_data if app_data.exists() else (FILE_DIR / "data")

DATA_DIR.mkdir(parents=True, exist_ok=True)

ADMIN_LOG_PATH = DATA_DIR / "hq_logs.log"
WAKEUP_FLAG_PATH = DATA_DIR / "crawler_wakeup.flag"
HEARTBEAT_PATH = DATA_DIR / "kvan_crawler.heartbeat"

ORDER_JSON_PATH = DATA_DIR / "current_order.json"
RESULT_JSON_PATH = DATA_DIR / "last_result.json"

SESSION_ORDER_DIR = DATA_DIR / "sessions" / "orders"
SESSION_RESULT_DIR = DATA_DIR / "sessions" / "results"
SESSION_ORDER_DIR.mkdir(parents=True, exist_ok=True)
SESSION_RESULT_DIR.mkdir(parents=True, exist_ok=True)

ADMIN_STATE_PATH = DATA_DIR / "admin_state.json"
LOCAL_DB_DIR = DATA_DIR / "local_db"
LOCAL_DB_DIR.mkdir(parents=True, exist_ok=True)

DEBUG_CRAWLER = os.environ.get("K_VAN_DEBUG", "1") == "1"

MYSQL_DEFAULT_URL = (
    "mysql://root:mzLCEjeoFjOCfqdHQjOVevFJbgaZunnZ@mysql.railway.internal:3306/railway"
)
DATABASE_URL = os.environ.get("DATABASE_URL") or os.environ.get("MYSQL_URL") or MYSQL_DEFAULT_URL


def _is_server_env() -> bool:
    return bool(os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("RUN_HEADLESS"))


_local_flag = os.environ.get("SISA_LOCAL_TEST")
if _local_flag is None:
    LOCAL_TEST = not _is_server_env()
else:
    LOCAL_TEST = _local_flag.strip().lower() in ("1", "true", "yes", "y")


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
                return json.loads(p.read_text(encoding="utf-8"))
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
    kvan_link 와는 _link_matches_kvan_session_id 로 여러 방식 중 하나만 통과하면 일치.
    """
    if not session_id:
        return None
    try:
        st = _load_admin_state()
        for s in (st.get("sessions") or []) + (st.get("history") or []):
            if str(s.get("id") or "") == str(session_id):
                aid = (s.get("agency_id") or "").strip()
                return aid or None
            link = (s.get("kvan_link") or "").strip()
            if link and _link_matches_kvan_session_id(link, session_id):
                aid = (s.get("agency_id") or "").strip()
                return aid or None
        return None
    except Exception:
        return None


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
        st = _load_admin_state()
        sessions = list(st.get("sessions") or [])
        history = list(st.get("history") or [])
        now_iso = datetime.utcnow().isoformat()

        if has_approval:
            remaining_sessions: list[dict] = []
            moved_session: Optional[dict] = None

            for s in sessions:
                if str(s.get("id") or "") == str(session_id):
                    moved_session = dict(s)
                else:
                    remaining_sessions.append(s)

            if moved_session is None:
                moved_session = {"id": session_id}

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
        st = _load_admin_state()
        sessions = list(st.get("sessions") or [])
        history = list(st.get("history") or [])
        now_iso = datetime.utcnow().isoformat()

        remaining_sessions: list[dict] = []
        removed_session: Optional[dict] = None

        for s in sessions:
            if str(s.get("id") or "") == str(session_id):
                removed_session = dict(s)
            else:
                remaining_sessions.append(s)

        if removed_session is None:
            removed_session = {"id": session_id}

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
    except Exception as e:
        print(f"[WARN] _mark_session_deleted 실패: {e}")


def _mark_session_expired_with_transactions(session_id: str, title: str) -> None:
    try:
        st = _load_admin_state()
        sessions = list(st.get("sessions") or [])
        history = list(st.get("history") or [])
        now_iso = datetime.utcnow().isoformat()

        remaining_sessions: list[dict] = []
        moved: Optional[dict] = None

        for s in sessions:
            if str(s.get("id") or "") == str(session_id):
                moved = dict(s)
            else:
                remaining_sessions.append(s)

        if moved is None:
            moved = {"id": session_id}

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


def _has_active_sessions(window_minutes: int = 10) -> bool:
    try:
        st = _load_admin_state()
        sessions = st.get("sessions") or []
        history = st.get("history") or []
        cutoff = datetime.utcnow() - timedelta(minutes=window_minutes)

        for s in sessions:
            if str(s.get("status") or "결제중") == "결제중":
                return True

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
        print(f"[WARN] _has_active_sessions 실패: {e}")
        return False


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
    return pymysql.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
        connect_timeout=5,
        read_timeout=10,
        write_timeout=10,
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


class KVStore:
    def __init__(self) -> None:
        self.use_json = _use_json_store()
        self.ensure_tables()

    def ensure_tables(self) -> None:
        if self.use_json:
            for name in ("kvan_links", "kvan_transactions", "transactions", "agencies"):
                _json_ensure_table(name)
            return

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
        conn.commit()
        conn.close()

    def replace_kvan_links(self, rows: list[dict]) -> None:
        if self.use_json:
            _json_save_rows("kvan_links", rows)
            return

        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE kvan_links")
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO kvan_links (
                      captured_at, title, amount, ttl_label, status,
                      kvan_link, mid, kvan_session_id, raw_text
                    )
                    VALUES (NOW(), %s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        row.get("title", ""),
                        row.get("amount", 0),
                        row.get("ttl_label", ""),
                        row.get("status", ""),
                        row.get("kvan_link", ""),
                        row.get("mid", ""),
                        row.get("kvan_session_id", ""),
                        row.get("raw_text", ""),
                    ),
                )
        conn.commit()
        conn.close()

    def load_kvan_links(self) -> list[dict]:
        if self.use_json:
            return _json_load_rows("kvan_links")

        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, captured_at, title, amount, ttl_label, status,
                       kvan_link, mid, kvan_session_id, raw_text
                FROM kvan_links
                ORDER BY id DESC
                """
            )
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

    def replace_kvan_transactions(self, rows: list[dict]) -> None:
        if self.use_json:
            _json_save_rows("kvan_transactions", rows)
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
                       amount, approval_no, card_number, registered_at
                FROM kvan_transactions
                ORDER BY captured_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall() or []
        conn.close()
        return rows

    def load_agency_mid_map(self) -> dict[str, str]:
        out: dict[str, str] = {}
        if self.use_json:
            for ag in _json_load_rows("agencies"):
                m = (ag.get("kvan_mid") or "").strip()
                if m:
                    out[m] = str(ag.get("id") or "")
            return out

        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("SELECT id, kvan_mid FROM agencies")
            for ag in cur.fetchall() or []:
                m = (ag.get("kvan_mid") or "").strip()
                if m:
                    out[m] = str(ag.get("id") or "")
        conn.close()
        return out

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
            _json_save_rows("transactions", tx_rows)
            return

        conn = get_db()
        with conn.cursor() as cur:
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
                        card_prefix4 = COALESCE(NULLIF(card_prefix4, ''), %s),
                        status = 'success',
                        kvan_registered_at = %s,
                        agency_id = COALESCE(NULLIF(agency_id, ''), %s)
                    WHERE id = %s
                    """,
                    (amount, customer_name or "", prefix4, registered_at, agency_id, tx_id),
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
                    (new_tx_id, agency_id, amount, customer_name or "", prefix4, message, approval_no, registered_at),
                )
        conn.commit()
        conn.close()

    def sync_kvan_to_transactions(self) -> bool:
        updated = 0
        inserted = 0

        try:
            krows = self.load_recent_kvan_transactions(limit=200)

            if self.use_json:
                tx_rows = _json_load_rows("transactions")

                approval_to_agency: dict[str, str] = {}
                prefix_to_agency: dict[str, str] = {}
                for tx in tx_rows or []:
                    ag = (tx.get("agency_id") or "").strip()
                    if not ag:
                        continue
                    a = (tx.get("kvan_approval_no") or "").strip()
                    if a and a not in approval_to_agency:
                        approval_to_agency[a] = ag
                    p = (tx.get("card_prefix4") or "").strip()
                    if p and p not in prefix_to_agency:
                        prefix_to_agency[p] = ag

                for kr in krows:
                    amt = kr.get("amount") or 0
                    approval = (kr.get("approval_no") or "").strip()
                    mid = (kr.get("mid") or "").strip()
                    tx_type = (kr.get("tx_type") or "").strip()
                    reg = (kr.get("registered_at") or "").strip()
                    card_number = (kr.get("card_number") or "").strip()
                    prefix4 = _card_prefix4(card_number)

                    if not amt or not approval:
                        continue

                    tx_status = (
                        "success"
                        if "승인" in tx_type
                        else "fail"
                        if ("취소" in tx_type or "실패" in tx_type or "오류" in tx_type)
                        else "other"
                    )
                    reg_date = reg.split(" ")[0] if reg else ""

                    resolved_agency_id = approval_to_agency.get(approval) or (
                        prefix4 and prefix_to_agency.get(prefix4)
                    ) or ""

                    found = None
                    for tx in tx_rows:
                        if (tx.get("kvan_approval_no") or "").strip() == approval:
                            found = tx
                            break

                    if not found:
                        for tx in tx_rows:
                            same_amount = int(tx.get("amount") or 0) == int(amt)
                            if resolved_agency_id:
                                same_agency = (tx.get("agency_id") or "") == resolved_agency_id
                            else:
                                same_agency = True
                            created_at = str(tx.get("created_at") or "")
                            same_date = (not reg_date) or created_at.startswith(reg_date)
                            if same_amount and same_agency and same_date:
                                found = tx
                                break

                    if found:
                        found["amount"] = found.get("amount") or amt
                        found["status"] = tx_status
                        found["kvan_mid"] = mid
                        found["kvan_approval_no"] = approval
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
                                "message": f"K-VAN {tx_type or '거래'} 자동 연동 (승인번호={approval})",
                                "settlement_status": "미정산",
                                "settled_at": None,
                                "kvan_mid": mid,
                                "kvan_approval_no": approval,
                                "kvan_tx_type": tx_type,
                                "kvan_registered_at": reg,
                            }
                        )
                        # 신규건도 maps 반영(추후 매칭 정확도 향상)
                        if resolved_agency_id:
                            approval_to_agency.setdefault(approval, resolved_agency_id)
                            if prefix4:
                                prefix_to_agency.setdefault(prefix4, resolved_agency_id)
                        inserted += 1

                _json_save_rows("transactions", tx_rows)

            else:
                conn = get_db()
                with conn.cursor() as cur:
                    # kvan_transactions(원천)에서 찾으려는 승인번호/카드 prefix 목록
                    approvals = []
                    prefixes = []
                    for kr in krows:
                        a = (kr.get("approval_no") or "").strip()
                        if a:
                            approvals.append(a)
                        p4 = _card_prefix4(kr.get("card_number") or "")
                        if p4:
                            prefixes.append(p4)
                    approvals = list(dict.fromkeys(approvals))
                    prefixes = list(dict.fromkeys(prefixes))

                    approval_to_agency: dict[str, str] = {}
                    prefix_to_agency: dict[str, str] = {}

                    if approvals:
                        placeholders = ",".join(["%s"] * len(approvals))
                        cur.execute(
                            f"""
                            SELECT kvan_approval_no, agency_id
                            FROM transactions
                            WHERE kvan_approval_no IN ({placeholders})
                              AND agency_id IS NOT NULL AND agency_id <> ''
                            """,
                            tuple(approvals),
                        )
                        for r in cur.fetchall() or []:
                            a = (r.get("kvan_approval_no") or "").strip()
                            ag = (r.get("agency_id") or "").strip()
                            if a and ag and a not in approval_to_agency:
                                approval_to_agency[a] = ag

                    if prefixes:
                        placeholders = ",".join(["%s"] * len(prefixes))
                        cur.execute(
                            f"""
                            SELECT card_prefix4, agency_id
                            FROM transactions
                            WHERE card_prefix4 IN ({placeholders})
                              AND agency_id IS NOT NULL AND agency_id <> ''
                            """,
                            tuple(prefixes),
                        )
                        for r in cur.fetchall() or []:
                            p4 = (r.get("card_prefix4") or "").strip()
                            ag = (r.get("agency_id") or "").strip()
                            if p4 and ag and p4 not in prefix_to_agency:
                                prefix_to_agency[p4] = ag

                    for kr in krows:
                        amt = kr.get("amount") or 0
                        approval = (kr.get("approval_no") or "").strip()
                        mid = (kr.get("mid") or "").strip()
                        tx_type = (kr.get("tx_type") or "").strip()
                        reg = (kr.get("registered_at") or "").strip()
                        card_number = (kr.get("card_number") or "").strip()
                        prefix4 = _card_prefix4(card_number)

                        if not amt or not approval:
                            continue

                        reg_date = reg.split(" ")[0] if reg else ""
                        tx_status = (
                            "success"
                            if "승인" in tx_type
                            else "fail"
                            if ("취소" in tx_type or "실패" in tx_type or "오류" in tx_type)
                            else "other"
                        )

                        resolved_agency_id = approval_to_agency.get(approval) or (
                            prefix4 and prefix_to_agency.get(prefix4)
                        ) or ""

                        cur.execute(
                            """
                            SELECT id
                            FROM transactions
                            WHERE kvan_approval_no = %s
                            LIMIT 1
                            """,
                            (approval,),
                        )
                        tx = cur.fetchone()

                        if tx:
                            tx_id = tx["id"]
                            cur.execute(
                                """
                                UPDATE transactions
                                SET amount = COALESCE(amount, %s),
                                    status = %s,
                                    kvan_mid = %s,
                                    kvan_approval_no = %s,
                                    kvan_tx_type = %s,
                                    kvan_registered_at = %s,
                                    card_prefix4 = COALESCE(NULLIF(card_prefix4, ''), %s),
                                    agency_id = COALESCE(NULLIF(agency_id, ''), %s)
                                WHERE id = %s
                                """,
                                (
                                    amt,
                                    tx_status,
                                    mid,
                                    approval,
                                    tx_type,
                                    reg,
                                    prefix4,
                                    resolved_agency_id,
                                    tx_id,
                                ),
                            )
                            updated += 1
                            continue

                        # 승인번호로 매칭이 안 되면, (amount + 날짜)로 기존 거래를 찾아보되
                        # 가능한 경우 resolved_agency_id로 좁힌다.
                        params = [amt, reg_date, reg_date, resolved_agency_id]
                        sql = """
                            SELECT id
                            FROM transactions
                            WHERE amount = %s
                              AND (%s = '' OR DATE(created_at) = %s)
                        """
                        if resolved_agency_id:
                            sql += " AND agency_id = %s"
                        sql += " ORDER BY created_at DESC LIMIT 1"
                        if resolved_agency_id:
                            cur.execute(sql, tuple(params))
                        else:
                            cur.execute(
                                """
                                SELECT id
                                FROM transactions
                                WHERE amount = %s
                                  AND (%s = '' OR DATE(created_at) = %s)
                                ORDER BY created_at DESC
                                LIMIT 1
                                """,
                                (amt, reg_date, reg_date),
                            )
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
                                    kvan_registered_at = %s,
                                    card_prefix4 = COALESCE(NULLIF(card_prefix4, ''), %s),
                                    agency_id = COALESCE(NULLIF(agency_id, ''), %s)
                                WHERE id = %s
                                """,
                                (
                                    tx_status,
                                    mid,
                                    approval,
                                    tx_type,
                                    reg,
                                    prefix4,
                                    resolved_agency_id,
                                    tx_id,
                                ),
                            )
                            updated += 1
                            continue

                        new_tx_id = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")[-18:]
                        message = f"K-VAN {tx_type or '거래'} 자동 연동 (승인번호={approval})"

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
                                approval,
                                tx_type,
                                reg,
                            ),
                        )
                        if resolved_agency_id:
                            approval_to_agency.setdefault(approval, resolved_agency_id)
                            if prefix4:
                                prefix_to_agency.setdefault(prefix4, resolved_agency_id)
                        inserted += 1

                conn.commit()
                conn.close()

            if updated or inserted:
                print(
                    f"[INFO] K-VAN → transactions 동기화 완료 (updated={updated}, inserted={inserted}, json={self.use_json})"
                )
            return bool(updated or inserted)

        except Exception as e:
            print(f"[WARN] K-VAN ↔ transactions 동기화 오류: {e}")
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
    text = (text or "").replace("원", "").replace(",", "").strip()
    digits = re.sub(r"[^\d-]", "", text)
    try:
        return int(digits) if digits else 0
    except Exception:
        return 0


def _card_prefix4(card_number: str) -> str:
    """
    카드번호 문자열에서 숫자만 추출한 뒤 앞 4자리만 반환.
    (예: '1234-5678-...' -> '1234')
    """
    digits = re.sub(r"[^\d]", "", str(card_number or ""))
    return digits[:4] if digits else ""


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
    try:
        state = _load_admin_state()
        updated = False
        for s in state.get("sessions") or []:
            if str(s.get("id")) == str(session_id):
                s["kvan_link"] = link
                updated = True
                break
        if updated:
            _save_admin_state(state)
            _append_admin_log("AUTO", f"admin_state 링크 저장 완료 session_id={session_id}")
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
    if not session_id:
        return None

    title = lines[0] if lines else ""
    amount = 0
    ttl_label = ""
    status = ""
    mid = ""
    kvan_link = _extract_link_from_card(card)

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

    return {
        "captured_at": datetime.utcnow().isoformat(),
        "title": title,
        "amount": amount,
        "ttl_label": ttl_label,
        "status": status,
        "kvan_link": kvan_link,
        "mid": mid,
        "kvan_session_id": session_id,
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
    if "transactions" in (driver.current_url or ""):
        try:
            driver.refresh()
        except Exception:
            pass
    else:
        driver.get("https://store.k-van.app/transactions")

    try:
        WebDriverWait(driver, 6).until(EC.presence_of_element_located((By.XPATH, "//table//tbody//tr")))
    except TimeoutException:
        print("[INFO] /transactions 테이블이 비어 있거나 아직 렌더링되지 않았습니다.")
        store.replace_kvan_transactions([])
        return 0

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

    snapshot_rows: list[dict] = []
    rows = driver.find_elements(By.XPATH, "//table//tbody//tr")

    for i, tr in enumerate(rows, start=1):
        try:
            cells = tr.find_elements(By.XPATH, ".//td")
            texts = [c.text.strip() for c in cells]
            if not any(texts):
                continue

            def get(i_: int) -> str:
                return texts[i_] if 0 <= i_ < len(texts) else ""

            snapshot_rows.append(
                {
                    "id": i,
                    "captured_at": datetime.utcnow().isoformat(),
                    "merchant_name": get(idx_merchant),
                    "pg_name": get(idx_pg),
                    "mid": get(idx_mid),
                    "fee_rate": get(idx_fee),
                    "tx_type": get(idx_type),
                    "amount": _parse_amount(get(idx_amt)),
                    "cancel_amount": _parse_amount(get(idx_cancel)),
                    "payable_amount": _parse_amount(get(idx_payable)),
                    "card_company": get(idx_cardco),
                    "card_number": get(idx_cardno),
                    "installment": get(idx_inst),
                    "approval_no": get(idx_approval),
                    "registered_at": get(idx_reg),
                    "raw_text": " | ".join(texts),
                }
            )
        except Exception as e_row:
            print(f"[WARN] 거래내역 파싱 오류: {e_row}")

    store.replace_kvan_transactions(snapshot_rows)
    print(f"[INFO] /transactions 에서 {len(snapshot_rows)}건 저장 완료 (json={store.use_json})")
    return len(snapshot_rows)


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

    while deleted < max_delete:
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

                rows = []
                try:
                    rows = dialog.find_elements(By.XPATH, ".//table//tbody//tr")
                except Exception:
                    rows = []

                if not no_history and len(rows) == 0:
                    no_history = True

                if not no_history and rows:
                    first_row_text = (rows[0].text or "").strip()
                    if "없습니다" in first_row_text or "없음" in first_row_text:
                        no_history = True

                if no_history:
                    _close_dialog(driver, dialog)
                    if _click_trash_and_confirm(driver, card):
                        if session_id:
                            _mark_session_deleted(session_id, title)
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

        if _go_to_payment_link(driver):
            try:
                deleted_boot = _delete_expired_no_tx_links_fast(driver, store, max_delete=FAST_DELETE_PER_PASS)
                _dbg(f"로그인 직후 즉시 삭제={deleted_boot}")
            except Exception as e0:
                _dbg(f"로그인 직후 삭제 루틴 오류: {e0}")

        backup_interval = int(os.environ.get("K_VAN_CRAWL_INTERVAL", "600"))
        startup_fast_cycles = int(os.environ.get("K_VAN_STARTUP_FAST_CYCLES", "3"))
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
                active_for_popup = _has_active_sessions(window_minutes=30)
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

            active = _has_active_sessions(window_minutes=10)

            delay = 2
            if cycle >= startup_fast_cycles:
                # 삭제가 발생했거나(=DB 상태 변화 가능) / 신규가 없고(=바로 크롤링할 게 없음)
                # 이 경우에는 추가로 결제/취소 페이지와 결제링크 생성페이지(payment-link)를 한 번 더 확인 후 3분 대기
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

                    delay = 180
                elif empty_cycles >= 3:
                    delay = 30 if LOCAL_TEST else 600
                elif active or had_new:
                    delay = 2
                else:
                    delay = 5 if LOCAL_TEST else 30

            print(
                f"[crawler] 다음 크롤링까지 {delay}초 대기 "
                f"(active_sessions={active}, had_new={had_new}, empty_cycles={empty_cycles})"
            )
            _alog(
                f"다음 크롤링까지 {delay}초 대기 "
                f"(active_sessions={active}, had_new={had_new}, empty_cycles={empty_cycles})"
            )
            _wait_with_wakeup(delay)

            now_ts = time.time()
            if backup_interval > 0 and now_ts - last_backup_ts >= backup_interval:
                print(f"[crawler] 백업 주기({backup_interval}s) 도달 - 정상 동작 확인")
                _alog(f"백업 주기({backup_interval}s) 도달 - 정상 동작")
                last_backup_ts = now_ts

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