# -*- coding: utf-8 -*-
"""
K-VAN 결제링크 DB 공통: URL에서 KEY 추출, 금액 파싱, 링크 최초 생성 시 kvan_links 시드.
auto_kvan.py / kvan_crawler.py / web_form.py 가 동일 규칙을 쓰도록 분리.
"""
from __future__ import annotations

import os
import re
from datetime import datetime
from urllib.parse import parse_qs, urlparse

import pymysql
from pymysql.cursors import DictCursor

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
DB_CONNECT_TIMEOUT = int(os.environ.get("MYSQL_CONNECT_TIMEOUT", "5"))
DB_READ_TIMEOUT = int(os.environ.get("MYSQL_READ_TIMEOUT", "10"))
DB_WRITE_TIMEOUT = int(os.environ.get("MYSQL_WRITE_TIMEOUT", "10"))


def kvan_db_connect():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset="utf8mb4",
        cursorclass=DictCursor,
        autocommit=False,
        connect_timeout=DB_CONNECT_TIMEOUT,
        read_timeout=DB_READ_TIMEOUT,
        write_timeout=DB_WRITE_TIMEOUT,
    )


def extract_kvan_session_key_from_url(link: str) -> str:
    """K-VAN 결제 URL에서 KEY… 세션 토큰 추출."""
    u = (link or "").strip()
    if not u:
        return ""
    try:
        q = parse_qs(urlparse(u).query)
        for key in ("sessionId", "sessionid"):
            for v in q.get(key) or []:
                vv = (v or "").strip()
                if vv.startswith("KEY"):
                    return vv
    except Exception:
        pass
    m = re.search(r"/p/(KEY[0-9A-Za-z]+)", u, re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r"(KEY[0-9A-Za-z]+)", u)
    return m.group(1) if m else ""


def parse_kvan_link_ui_created_at(text: str) -> datetime | None:
    """
    K-VAN 결제링크 카드/목록에 표시되는 '생성·등록' 일시를 raw_text 에서 추출.
    (DB 최초 INSERT 시각과 다를 수 있어, 화면에 나온 실제 생성 시각 표시용)
    """
    raw = (text or "").strip()
    if not raw:
        return None
    blob = raw.replace("\r\n", "\n")
    lines = [ln.strip() for ln in blob.split("\n") if ln.strip()]

    def _build_dt(y: int, mo: int, d: int, h: int, mi: int, se: int) -> datetime | None:
        try:
            if not (2000 <= y <= 2100 and 1 <= mo <= 12 and 1 <= d <= 31):
                return None
            return datetime(y, mo, d, min(h, 23), min(mi, 59), min(se, 59))
        except (TypeError, ValueError):
            return None

    def _ampm_adjust(window: str, hour: int) -> int:
        if "오후" in window and 1 <= hour <= 11:
            return hour + 12
        if "오전" in window and hour == 12:
            return 0
        return hour

    # 1) '생성/등록' 라벨 근처 윈도에서만 탐색 (만료일시 단독 라벨은 제외)
    candidates: list[datetime] = []
    for i, ln in enumerate(lines):
        if re.search(r"만료일시|만료\s*일", ln) and not re.search(
            r"생성|등록", ln, re.I
        ):
            continue
        if not re.search(
            r"(?:링크\s*)?생성|생성(?:일시|시간|일)|등록(?:일시|일|시간)|만들어진\s*시각",
            ln,
            re.I,
        ):
            continue
        window = "\n".join(lines[i : min(i + 6, len(lines))])
        # ISO / 숫자 형
        for m in re.finditer(
            r"(\d{4})\s*[-/.년]\s*(\d{1,2})\s*[-/.월]\s*(\d{1,2})(?:일)?"
            r'(?:\s+|[Tt])\s*(\d{1,2})\s*:\s*(\d{2})(?::\s*(\d{2}))?',
            window,
        ):
            y, mo, d, h, mi, se = (
                int(m.group(1)),
                int(m.group(2)),
                int(m.group(3)),
                int(m.group(4)),
                int(m.group(5)),
                int(m.group(6) or 0),
            )
            h = _ampm_adjust(window, h)
            dt = _build_dt(y, mo, d, h, mi, se)
            if dt:
                candidates.append(dt)
        for m in re.finditer(
            r"(\d{4})\s*[-/.]\s*(\d{1,2})\s*[-/.]\s*(\d{1,2})\s+"
            r"(?:오전|오후)?\s*(\d{1,2})\s*:\s*(\d{2})(?::\s*(\d{2}))?",
            window,
        ):
            y, mo, d, h, mi, se = (
                int(m.group(1)),
                int(m.group(2)),
                int(m.group(3)),
                int(m.group(4)),
                int(m.group(5)),
                int(m.group(6) or 0),
            )
            h = _ampm_adjust(window, h)
            dt = _build_dt(y, mo, d, h, mi, se)
            if dt:
                candidates.append(dt)

    if candidates:
        return min(candidates)

    # 2) 라벨 없이 카드 첫머리에 날짜만 있는 경우 (약한 휴리스틱: 첫 번째 유효 패턴만)
    head = "\n".join(lines[:8])
    m = re.search(
        r"(\d{4})\s*[-/.년]\s*(\d{1,2})\s*[-/.월]\s*(\d{1,2})(?:일)?"
        r"\s+(\d{1,2})\s*:\s*(\d{2})(?::\s*(\d{2}))?",
        head,
    )
    if m:
        y, mo, d, h, mi, se = (
            int(m.group(1)),
            int(m.group(2)),
            int(m.group(3)),
            int(m.group(4)),
            int(m.group(5)),
            int(m.group(6) or 0),
        )
        h = _ampm_adjust(head, h)
        return _build_dt(y, mo, d, h, mi, se)
    return None


def parse_amount_won(text: str) -> int:
    """
    '결제금액 1,234,567원' 등에서 금액 추출.
    비정상적으로 큰 값(파싱 오류)은 제외하고 합리적 후보만 사용.
    """
    raw = text or ""
    # 라벨 뒤 금액 우선
    for pat in (
        r"결제\s*금액\s*[:：]?\s*([\d,，\s]+)\s*원",
        r"판매\s*가격\s*[:：]?\s*([\d,，\s]+)\s*원",
        r"금액\s*[:：]?\s*([\d,，\s]+)\s*원",
    ):
        m = re.search(pat, raw)
        if m:
            try:
                v = int(re.sub(r"[^\d]", "", m.group(1)))
                if 0 < v <= 1_000_000_000:
                    return v
            except ValueError:
                continue
    candidates: list[int] = []
    for m in re.finditer(r"([\d,，\s]{1,18})\s*원", raw):
        try:
            v = int(re.sub(r"[^\d]", "", m.group(1)))
            if 0 < v <= 1_000_000_000:
                candidates.append(v)
        except ValueError:
            continue
    if not candidates:
        stripped = raw.replace("원", "").replace(",", "").replace("，", "").strip()
        try:
            v = int(re.sub(r"[^\d]", "", stripped))
            return v if 0 < v <= 1_000_000_000 else 0
        except ValueError:
            return 0
    # 여러 개면 '결제 금액'에 가까운 보통가(너무 큰 단일 오탐 제거)
    reasonable = [c for c in candidates if c <= 100_000_000]
    return max(reasonable) if reasonable else max(candidates)


def _norm_kvan_header(h: str) -> str:
    return re.sub(r"\s+", " ", (h or "").replace("\n", " ").strip())


def infer_kvan_transaction_header_cell_label(inner_html: str) -> str:
    """
    K-VAN 결제/취소 내역 thead: 라벨이 <span>이 아니라 input placeholder / select 첫 옵션에만 있는 경우가 많음.
    Selenium .text 는 이런 셀에서 빈 문자열이 되어 열 개수가 tbody 와 맞지 않는 원인이 된다.
    """
    raw = inner_html or ""

    def _from_placeholder(html: str) -> str:
        for m in re.finditer(r'placeholder\s*=\s*"([^"]+)"', html, re.I):
            lab = (m.group(1) or "").strip()
            if lab and lab != "~":
                return lab
        for m in re.finditer(r"placeholder\s*=\s*'([^']+)'", html, re.I):
            lab = (m.group(1) or "").strip()
            if lab and lab != "~":
                return lab
        return ""

    lab = _from_placeholder(raw)
    if lab:
        return _norm_kvan_header(lab)

    m = re.search(
        r"<option[^>]*\svalue\s*=\s*(?:\"\"|'')[^>]*>([^<]*)</option>",
        raw,
        re.I,
    )
    if not m:
        m = re.search(
            r"<option[^>]*value\s*=\s*(?:\"\"|'')\s*>([^<]*)</option>",
            raw,
            re.I,
        )
    if m:
        lab = (m.group(1) or "").strip()
        if lab:
            return _norm_kvan_header(lab)

    for sp in re.findall(r"<span[^>]*>([^<]*)</span>", raw, re.I):
        lab = re.sub(r"\s+", " ", sp).strip()
        if lab and lab != "~":
            return _norm_kvan_header(lab)

    text = re.sub(r"<[^>]+>", " ", raw)
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"^~\s*", "", text).strip()
    return _norm_kvan_header(text)


def kvan_transactions_header_indices(headers: list[str]) -> dict[str, int]:
    """
    K-VAN '결제 및 취소내역' 테이블 헤더 → 컬럼 인덱스.
    UI 변경(거래 유형 / 거래일시 등)에 대응해 복수 별칭을 둔다.
    """
    hnorm = [_norm_kvan_header(x) for x in headers]

    def find(*subs: str) -> int:
        for sub in subs:
            s = sub.strip()
            for i, h in enumerate(hnorm):
                if s in h:
                    return i
        return -1

    return {
        "merchant": find("가맹점명", "가맹점", "상점명"),
        "pg": find("PG사", "PG"),
        "mid": find("MID"),
        "fee": find("수수료율", "수수료"),
        "tx_trade": find("거래 유형", "거래유형"),
        "tx_pay": find("결제 유형", "결제유형"),
        "amount": find("결제 금액", "결제금액"),
        "cancel": find("취소 금액", "취소금액"),
        "payable": find("지급예정금액", "지급 예정금액"),
        "cardco": find("카드사"),
        "cardno": find("카드번호"),
        "inst": find("할부"),
        "approval": find("승인번호", "승인 번호"),
        "registered": find(
            "거래일시",
            "거래 일시",
            "등록일",
            "등록 일",
            "거래일",
        ),
    }


def parse_kvan_transactions_cell_amount(text: str) -> int:
    """테이블 셀의 '20,000' / '-' / '1,000원' 형태."""
    t = (text or "").strip()
    if not t or t == "-":
        return 0
    if "원" in t:
        return parse_amount_won(t)
    digits = re.sub(r"[^\d]", "", t.replace(",", "").replace("，", ""))
    if not digits:
        return 0
    try:
        v = int(digits)
        return v if 0 <= v <= 1_000_000_000_000 else 0
    except ValueError:
        return 0


def kvan_transactions_row_to_snapshot(
    headers: list[str],
    cell_texts: list[str],
    row_num: int,
    *,
    captured_iso: str,
) -> dict | None:
    """
    한 행 → kvan_transactions / 크롤러 스냅샷 dict.
    승인번호가 있거나 결제금액>0 인 행만 유효.
    """
    ix = kvan_transactions_header_indices(headers)

    def getc(key: str) -> str:
        i = ix.get(key, -1)
        if i is None or i < 0:
            return ""
        return cell_texts[i].strip() if i < len(cell_texts) else ""

    if not any((x or "").strip() for x in cell_texts):
        return None

    tx_trade = getc("tx_trade")
    tx_pay = getc("tx_pay")
    tx_type = (tx_trade if tx_trade and tx_trade != "-" else tx_pay) or ""

    approval = getc("approval")
    amount = parse_kvan_transactions_cell_amount(getc("amount"))

    if not approval and amount <= 0:
        return None

    return {
        "id": row_num,
        "captured_at": captured_iso,
        "merchant_name": getc("merchant"),
        "pg_name": getc("pg"),
        "mid": getc("mid"),
        "fee_rate": getc("fee"),
        "tx_type": tx_type,
        "amount": amount,
        "cancel_amount": parse_kvan_transactions_cell_amount(getc("cancel")),
        "payable_amount": parse_kvan_transactions_cell_amount(getc("payable")),
        "card_company": getc("cardco"),
        "card_number": getc("cardno"),
        "installment": getc("inst"),
        "approval_no": approval,
        "registered_at": getc("registered"),
        "raw_text": " | ".join(cell_texts),
    }


def build_kvan_transactions_snapshots(
    headers: list[str],
    body_cell_rows: list[list[str]],
    *,
    captured_iso: str,
) -> list[dict]:
    out: list[dict] = []
    for i, cells in enumerate(body_cell_rows, start=1):
        rec = kvan_transactions_row_to_snapshot(
            headers, cells, i, captured_iso=captured_iso
        )
        if rec:
            out.append(rec)
    return out


def fetch_agency_company_name(agency_id: str) -> str:
    """agencies.company_name 조회. 없거나 본사면 본사."""
    aid = (agency_id or "").strip()
    if not aid:
        return "본사"
    try:
        conn = kvan_db_connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT company_name FROM agencies WHERE id = %s LIMIT 1",
                (aid,),
            )
            row = cur.fetchone()
        conn.close()
        if row and (row.get("company_name") or "").strip():
            return str(row["company_name"]).strip()
    except Exception:
        pass
    return aid


def ensure_kvan_links_link_created_at(conn) -> None:
    """링크 최초 등록 시각(크롤링마다 바뀌는 captured_at 과 구분). 컬럼 추가 + 기존 행 백필."""
    try:
        with conn.cursor() as cur:
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
            cur.execute(
                "UPDATE kvan_links SET link_created_at = captured_at WHERE link_created_at IS NULL"
            )
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass


def ensure_kvan_links_internal_session_column(conn) -> None:
    try:
        with conn.cursor() as cur:
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
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass


def upsert_kvan_link_creation_seed(
    link: str,
    internal_session_id: str,
    session_blob: dict | None,
    *,
    skip_db: bool = False,
) -> None:
    """
    매크로가 링크를 만든 직후: KEY·내부 세션·agency_id·금액을 kvan_links 에 시드.
    이후 크롤러가 동일 kvan_link 로 스크랩하면 agency_id / internal_session_id 를 병합 유지한다.
    """
    if skip_db:
        return
    link = (link or "").strip()
    internal_session_id = (internal_session_id or "").strip()
    session_blob = session_blob or {}
    if not link or "store.k-van.app" not in link:
        return
    kkey = extract_kvan_session_key_from_url(link)
    if not kkey:
        return
    agency_id = str(session_blob.get("agency_id") or "").strip()
    owner = fetch_agency_company_name(agency_id)
    amt_raw = str(session_blob.get("amount") or "").strip()
    amount = parse_amount_won(amt_raw + ("원" if amt_raw and "원" not in amt_raw else ""))
    if amount <= 0:
        try:
            amount = int(re.sub(r"[^\d]", "", amt_raw) or "0")
        except ValueError:
            amount = 0
    title = f"{owner} · 내부세션 {internal_session_id}"
    conn = None
    try:
        conn = kvan_db_connect()
        ensure_kvan_links_internal_session_column(conn)
        ensure_kvan_links_link_created_at(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, agency_id, internal_session_id FROM kvan_links WHERE kvan_link = %s LIMIT 1",
                (link,),
            )
            prev = cur.fetchone()
            if prev:
                cur.execute(
                    """
                    UPDATE kvan_links SET
                      kvan_session_id = %s,
                      agency_id = COALESCE(NULLIF(TRIM(%s), ''), agency_id),
                      internal_session_id = COALESCE(NULLIF(TRIM(%s), ''), internal_session_id),
                      link_created_at = COALESCE(link_created_at, NOW()),
                      title = CASE
                        WHEN title IS NULL OR TRIM(title) = '' THEN %s
                        ELSE title END,
                      amount = CASE WHEN amount IS NULL OR amount = 0 THEN %s ELSE amount END,
                      raw_text = %s
                    WHERE kvan_link = %s
                    """,
                    (
                        kkey,
                        agency_id,
                        internal_session_id,
                        title,
                        amount,
                        f"seed:internal={internal_session_id}",
                        link,
                    ),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO kvan_links (
                      captured_at, link_created_at, title, amount, ttl_label, status,
                      kvan_link, mid, kvan_session_id, agency_id, internal_session_id, raw_text
                    )
                    VALUES (NOW(), NOW(), %s, %s, '', %s, %s, '', %s, %s, %s, %s)
                    """,
                    (
                        title,
                        amount,
                        "링크생성됨",
                        link,
                        kkey,
                        agency_id,
                        internal_session_id,
                        f"seed:internal={internal_session_id}",
                    ),
                )
        conn.commit()
    except Exception:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def load_kvan_link_preserved_by_url(urls: list[str]) -> dict[str, dict]:
    """병합용: 기존 행의 agency_id, internal_session_id, title(선택)."""
    out: dict[str, dict] = {}
    urls = [u.strip() for u in urls if (u or "").strip()]
    if not urls:
        return out
    try:
        conn = kvan_db_connect()
        ensure_kvan_links_internal_session_column(conn)
        ensure_kvan_links_link_created_at(conn)
        with conn.cursor() as cur:
            ph = ",".join(["%s"] * len(urls))
            cur.execute(
                f"""
                SELECT kvan_link, agency_id, internal_session_id, title, link_created_at, captured_at
                FROM kvan_links WHERE kvan_link IN ({ph})
                """,
                tuple(urls),
            )
            for row in cur.fetchall() or []:
                k = (row.get("kvan_link") or "").strip()
                if k:
                    out[k] = dict(row)
        conn.close()
    except Exception:
        pass
    return out
