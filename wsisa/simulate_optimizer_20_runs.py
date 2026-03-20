from __future__ import annotations

import json
from pathlib import Path


def simulate_dashboard_collect(
    appear_at: dict[str, float],
    *,
    max_wait: float = 1.2,
    poll: float = 0.12,
) -> dict:
    """
    _scrape_dashboard_and_store 의 빠른 블록 탐색 루프를 시간축으로 시뮬레이션.
    """
    labels = ["monthly", "yesterday", "settlement", "credit"]
    found = {k: False for k in labels}
    t = 0.0
    steps = 0
    while t <= max_wait:
        progressed = False
        for k in labels:
            if found[k]:
                continue
            if appear_at.get(k, 999.0) <= t:
                found[k] = True
                progressed = True
        if all(found.values()):
            break
        if not progressed:
            t += poll
        steps += 1
        if steps > 200:
            break
    # 실코드의 경계 시점 보정 1회 반영
    for k in labels:
        if found[k]:
            continue
        if appear_at.get(k, 999.0) <= max_wait:
            found[k] = True
    return {
        "found_all": all(found.values()),
        "found": found,
        "elapsed_sim": round(min(t, max_wait), 3),
        "iterations": steps,
    }


def simulate_expired_policy(
    *,
    is_expired: bool,
    popup_text: str,
    row_count: int,
    first_row_text: str,
) -> str:
    """
    _delete_expired_no_tx_links_fast 분기 시뮬레이션:
    - 만료 + 거래없음 => delete
    - 만료 + 거래있음 => keep_and_record
    - 만료 아님 => skip
    """
    if not is_expired:
        return "skip"

    no_history = ("없습니다" in popup_text) or ("없음" in popup_text)
    if not no_history and row_count == 0:
        no_history = True
    if not no_history and row_count > 0 and (("없습니다" in first_row_text) or ("없음" in first_row_text)):
        no_history = True
    return "delete" if no_history else "keep_and_record"


def run() -> int:
    results: list[dict] = []

    # 1~10: 대시보드 탐색 지연 시뮬레이션
    dashboard_cases = [
        ("01_dash_all_fast", {"monthly": 0.0, "yesterday": 0.0, "settlement": 0.0, "credit": 0.0}, True),
        ("02_dash_partial_fast", {"monthly": 0.0, "yesterday": 0.2, "settlement": 0.3, "credit": 0.3}, True),
        ("03_dash_credit_late", {"monthly": 0.0, "yesterday": 0.2, "settlement": 0.3, "credit": 1.1}, True),
        ("04_dash_one_missing", {"monthly": 0.0, "yesterday": 0.1, "settlement": 0.2, "credit": 9.9}, False),
        ("05_dash_all_missing", {"monthly": 9.9, "yesterday": 9.9, "settlement": 9.9, "credit": 9.9}, False),
        ("06_dash_slow_poll", {"monthly": 0.7, "yesterday": 0.8, "settlement": 0.9, "credit": 1.0}, True),
        ("07_dash_staggered", {"monthly": 0.1, "yesterday": 0.4, "settlement": 0.7, "credit": 1.0}, True),
        ("08_dash_near_timeout", {"monthly": 1.15, "yesterday": 1.15, "settlement": 1.15, "credit": 1.15}, True),
        ("09_dash_mid_missing", {"monthly": 0.1, "yesterday": 9.9, "settlement": 0.2, "credit": 0.3}, False),
        ("10_dash_jitter", {"monthly": 0.24, "yesterday": 0.36, "settlement": 0.72, "credit": 0.84}, True),
    ]
    for name, ap, expect_all in dashboard_cases:
        out = simulate_dashboard_collect(ap)
        ok = out["found_all"] == expect_all
        results.append(
            {
                "name": name,
                "type": "dashboard",
                "ok": ok,
                "expect_found_all": expect_all,
                "found_all": out["found_all"],
                "elapsed_sim": out["elapsed_sim"],
                "cause": "대시보드 블록 탐색 지연/누락",
                "action": "빠른 폴링 루프 + max_wait 제한",
            }
        )

    # 11~20: 만료 링크 분기 시뮬레이션
    expired_cases = [
        ("11_expired_no_history_text", True, "거래 내역이 없습니다", 0, "", "delete"),
        ("12_expired_empty_rows", True, "거래 내역", 0, "", "delete"),
        ("13_expired_rows_with_data", True, "거래 내역", 2, "결제 승인 10000", "keep_and_record"),
        ("14_expired_first_row_none", True, "거래 내역", 1, "거래 내역이 없습니다", "delete"),
        ("15_not_expired", False, "거래 내역", 2, "결제 승인 12000", "skip"),
        ("16_expired_cancel_data", True, "거래 내역", 1, "결제 취소 12000", "keep_and_record"),
        ("17_expired_popup_none", True, "없음", 0, "", "delete"),
        ("18_expired_popup_has_hist", True, "거래 내역", 3, "승인번호 123", "keep_and_record"),
        ("19_expired_row_text_none", True, "거래 내역", 3, "없습니다", "delete"),
        ("20_expired_large_rows", True, "거래 내역", 9, "결제 승인", "keep_and_record"),
    ]
    for name, is_expired, popup_text, row_count, first_row_text, expect in expired_cases:
        actual = simulate_expired_policy(
            is_expired=is_expired,
            popup_text=popup_text,
            row_count=row_count,
            first_row_text=first_row_text,
        )
        ok = actual == expect
        results.append(
            {
                "name": name,
                "type": "expired_policy",
                "ok": ok,
                "expect": expect,
                "actual": actual,
                "cause": "만료 링크의 거래내역 유무 분기",
                "action": "거래없음 delete / 거래있음 keep_and_record",
            }
        )

    pass_cnt = sum(1 for r in results if r["ok"])
    out_dir = Path(__file__).resolve().parent.parent / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "kvan_optimizer_20_results.json"
    md_path = out_dir / "kvan_optimizer_20_results.md"
    json_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = ["# K-VAN 최적화 시뮬레이션 20회", ""]
    for r in results:
        lines.append(
            f"- {r['name']}: {'PASS' if r['ok'] else 'FAIL'} | type={r['type']} | "
            f"cause={r['cause']} | action={r['action']}"
        )
    lines += ["", f"PASS {pass_cnt}/20"]
    md_path.write_text("\n".join(lines), encoding="utf-8")

    print(f"result_json={json_path}")
    print(f"result_md={md_path}")
    print(f"pass={pass_cnt}/20")
    return 0 if pass_cnt == 20 else 1


if __name__ == "__main__":
    raise SystemExit(run())

