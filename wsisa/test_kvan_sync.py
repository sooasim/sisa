import json
from datetime import datetime

from web_form import get_db


def _print_title(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def dump_kvan_transactions(cur, limit: int = 10) -> None:
    _print_title(f"[1] K-VAN 결제/취소 내역 (최근 {limit}건) - kvan_transactions")
    cur.execute(
        """
        SELECT captured_at, tx_type, amount, approval_no, card_company, card_number,
               registered_at
        FROM kvan_transactions
        ORDER BY captured_at DESC
        LIMIT %s
        """,
        (limit,),
    )
    rows = cur.fetchall()
    if not rows:
        print("데이터가 없습니다.")
        return
    for r in rows:
        print(
            f"{r['captured_at']} | {r['tx_type']} | {r['amount']}원 | "
            f"승인번호={r['approval_no']} | 카드사={r['card_company']} | "
            f"카드번호={r['card_number']} | 등록일={r['registered_at']}"
        )


def dump_kvan_links(cur, limit: int = 10) -> None:
    _print_title(f"[2] K-VAN 결제링크 관리 (최근 {limit}건) - kvan_links")
    cur.execute(
        """
        SELECT captured_at, title, amount, ttl_label, status, kvan_link, kvan_session_id
        FROM kvan_links
        ORDER BY captured_at DESC
        LIMIT %s
        """,
        (limit,),
    )
    rows = cur.fetchall()
    if not rows:
        print("데이터가 없습니다.")
        return
    for r in rows:
        print(
            f"{r['captured_at']} | {r['title']} | {r['amount']}원 | "
            f"{r['ttl_label']} | {r['status']} | 세션ID={r['kvan_session_id']} | "
            f"링크={r['kvan_link']}"
        )


def dump_internal_transactions(cur, limit: int = 10) -> None:
    _print_title(f"[3] 내부 거래 내역 (최근 {limit}건) - transactions")
    cur.execute(
        """
        SELECT created_at,
               agency_id,
               amount,
               customer_name,
               status,
               settlement_status,
               kvan_approval_no,
               kvan_registered_at
        FROM transactions
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (limit,),
    )
    rows = cur.fetchall()
    if not rows:
        print("데이터가 없습니다.")
        return
    for r in rows:
        print(
            f"{r['created_at']} | agency={r['agency_id']} | {r['amount']}원 | "
            f"고객={r['customer_name']} | 상태={r['status']} / 정산={r['settlement_status']} | "
            f"K-VAN 승인번호={r['kvan_approval_no']} | 등록일={r['kvan_registered_at']}"
        )


def check_latest_approval_pair(cur) -> None:
    """
    가장 최근 'success' 거래 1건과, 그 승인번호로 kvan_transactions 를 찾아
    금액/승인번호/시간이 일치하는지 요약해 보여준다.
    """
    _print_title("[4] 최신 승인 거래 1건 매핑 검증")
    cur.execute(
        """
        SELECT id, created_at, amount, customer_name, kvan_approval_no
        FROM transactions
        WHERE status = 'success'
          AND kvan_approval_no IS NOT NULL
          AND kvan_approval_no <> ''
        ORDER BY created_at DESC
        LIMIT 1
        """
    )
    tx = cur.fetchone()
    if not tx:
        print("status='success' 이고 kvan_approval_no 가 있는 거래가 없습니다.")
        return

    approval = tx["kvan_approval_no"]
    amount = tx["amount"]
    print(
        f"- 내부 거래: id={tx['id']} | {tx['created_at']} | 금액={amount}원 | "
        f"고객={tx['customer_name']} | 승인번호={approval}"
    )

    cur.execute(
        """
        SELECT captured_at, tx_type, amount, approval_no, registered_at
        FROM kvan_transactions
        WHERE approval_no = %s
        ORDER BY captured_at DESC
        LIMIT 1
        """,
        (approval,),
    )
    kr = cur.fetchone()
    if not kr:
        print("- K-VAN 거래: 해당 승인번호를 가진 행이 없습니다.")
        return

    print(
        f"- K-VAN 거래: {kr['captured_at']} | 유형={kr['tx_type']} | "
        f"금액={kr['amount']}원 | 승인번호={kr['approval_no']} | 등록일={kr['registered_at']}"
    )

    if kr["amount"] == amount:
        print("→ 금액이 일치합니다.")
    else:
        print("→ [주의] 금액이 일치하지 않습니다.")


def main() -> None:
    print("K-VAN ↔ 내부 DB 동기화 상태를 점검합니다.")
    conn = get_db()
    try:
        with conn.cursor() as cur:
            dump_kvan_transactions(cur)
            dump_kvan_links(cur)
            dump_internal_transactions(cur)
            check_latest_approval_pair(cur)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

