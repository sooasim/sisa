from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from kvan_crawler import _normalized_approval_for_sync
from kvan_link_common import build_kvan_transactions_snapshots


def _make_row(amount: str = "10,000", approval: str = "A12345") -> list[str]:
    return [
        "테스트상점",   # 가맹점명
        "KG",          # PG사
        "MID001",      # MID
        "2.9%",        # 수수료율
        "결제 승인",   # 거래/결제 유형
        amount,        # 결제 금액
        "0",           # 취소 금액
        "9,000",       # 지급예정금액
        "신한",        # 카드사
        "1234-****",   # 카드번호
        "일시불",      # 할부
        approval,      # 승인번호
        "2026-03-20 10:11:12",  # 거래일시/등록일
    ]


def run() -> int:
    scenarios: list[dict] = [
        {
            "name": "01_full_headers",
            "headers": ["가맹점명", "PG사", "MID", "수수료율", "거래 유형", "결제 금액", "취소 금액", "지급예정금액", "카드사", "카드번호", "할부", "승인번호", "거래일시"],
            "rows": [_make_row()],
            "expect_min": 1,
            "cause": "정상 기준 케이스",
        },
        {
            "name": "02_pay_type_alias",
            "headers": ["가맹점명", "PG사", "MID", "수수료율", "결제 유형", "결제 금액", "취소 금액", "지급예정금액", "카드사", "카드번호", "할부", "승인번호", "등록일"],
            "rows": [_make_row()],
            "expect_min": 1,
            "cause": "거래 유형 대신 결제 유형만 있는 UI",
        },
        {
            "name": "03_spaced_headers",
            "headers": ["가맹점 명", "PG 사", "MID", "수수료 율", "거래유형", "결제금액", "취소금액", "지급 예정금액", "카드 사", "카드 번호", "할부", "승인 번호", "등록 일"],
            "rows": [_make_row()],
            "expect_min": 1,
            "cause": "헤더 띄어쓰기/붙여쓰기 변형",
        },
        {
            "name": "04_missing_approval",
            "headers": ["가맹점명", "PG사", "MID", "수수료율", "결제 유형", "결제 금액", "취소 금액", "지급예정금액", "카드사", "카드번호", "할부", "승인번호", "등록일"],
            "rows": [_make_row(approval="")],
            "expect_min": 1,
            "cause": "승인번호 공백 케이스 (기존 sync 스킵 원인)",
        },
        {
            "name": "05_missing_amount",
            "headers": ["가맹점명", "PG사", "MID", "수수료율", "결제 유형", "결제 금액", "취소 금액", "지급예정금액", "카드사", "카드번호", "할부", "승인번호", "등록일"],
            "rows": [_make_row(amount="0")],
            "expect_min": 0,
            "cause": "금액 0은 내부 거래 생성 불가",
        },
        {
            "name": "06_two_rows",
            "headers": ["가맹점명", "PG사", "MID", "수수료율", "거래 유형", "결제 금액", "취소 금액", "지급예정금액", "카드사", "카드번호", "할부", "승인번호", "거래일시"],
            "rows": [_make_row(approval="AP1"), _make_row(amount="22,000", approval="AP2")],
            "expect_min": 2,
            "cause": "복수 행 파싱",
        },
        {
            "name": "07_cancel_tx",
            "headers": ["가맹점명", "PG사", "MID", "수수료율", "거래 유형", "결제 금액", "취소 금액", "지급예정금액", "카드사", "카드번호", "할부", "승인번호", "거래일시"],
            "rows": [[*_make_row(amount="10,000", approval="CXL1")[:4], "결제 취소", "10,000", "10,000", "0", *_make_row() [8:]]],
            "expect_min": 1,
            "cause": "취소 유형도 스냅샷 대상",
        },
        {
            "name": "08_header_noise",
            "headers": ["선택", "가맹점명", "PG사", "MID", "수수료율", "결제 유형", "결제 금액", "취소 금액", "지급예정금액", "카드사", "카드번호", "할부", "승인번호", "등록일", "비고"],
            "rows": [["", *_make_row(), ""]],
            "expect_min": 1,
            "cause": "불필요 컬럼 포함",
        },
        {
            "name": "09_registered_alias",
            "headers": ["가맹점명", "PG사", "MID", "수수료율", "거래 유형", "결제 금액", "취소 금액", "지급예정금액", "카드사", "카드번호", "할부", "승인번호", "거래일"],
            "rows": [_make_row()],
            "expect_min": 1,
            "cause": "거래일 컬럼명 변형",
        },
        {
            "name": "10_empty_body",
            "headers": ["가맹점명", "PG사", "MID", "수수료율", "거래 유형", "결제 금액", "취소 금액", "지급예정금액", "카드사", "카드번호", "할부", "승인번호", "거래일시"],
            "rows": [],
            "expect_min": 0,
            "cause": "tbody가 비어있는 실제 케이스",
        },
    ]

    # 11~20: 승인번호 누락 + 값 변화 조합
    for i in range(11, 21):
        scenarios.append(
            {
                "name": f"{i:02d}_missing_approval_variant",
                "headers": ["가맹점명", "PG사", "MID", "수수료율", "결제 유형", "결제 금액", "취소 금액", "지급예정금액", "카드사", "카드번호", "할부", "승인번호", "등록일"],
                "rows": [[
                    "테스트상점",
                    "KG",
                    f"MID{i}",
                    "2.9%",
                    "결제 승인" if i % 2 else "결제 취소",
                    f"{10000 + i * 37:,}",
                    "0",
                    "9000",
                    "신한",
                    f"{1000+i}-****",
                    "일시불",
                    "",
                    f"2026-03-20 10:{i:02d}:12",
                ]],
                "expect_min": 1,
                "cause": "승인번호 누락 반복 케이스",
            }
        )

    results: list[dict] = []
    for sc in scenarios:
        snaps = build_kvan_transactions_snapshots(
            sc["headers"],
            sc["rows"],
            captured_iso=datetime.utcnow().isoformat(),
        )
        ok = len(snaps) >= sc["expect_min"]
        synthetic = ""
        if snaps:
            synthetic = _normalized_approval_for_sync(
                str(snaps[0].get("approval_no") or ""),
                snaps[0],
            )
        results.append(
            {
                "name": sc["name"],
                "expected_min_rows": sc["expect_min"],
                "actual_rows": len(snaps),
                "ok": ok,
                "cause": sc["cause"],
                "approval_raw": (snaps[0].get("approval_no") if snaps else ""),
                "approval_normalized": synthetic,
                "note": (
                    "승인번호 누락 보정키 생성됨"
                    if (snaps and not (snaps[0].get("approval_no") or "").strip() and synthetic.startswith("NOAPP-"))
                    else ""
                ),
            }
        )

    out_dir = Path(__file__).resolve().parent.parent / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "kvan_simulation_20_results.json"
    md_path = out_dir / "kvan_simulation_20_results.md"
    json_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = ["# K-VAN 시뮬레이션 20회 결과", ""]
    for r in results:
        lines.append(
            f"- {r['name']}: {'PASS' if r['ok'] else 'FAIL'} | "
            f"rows={r['actual_rows']} (expect>={r['expected_min_rows']}) | "
            f"cause={r['cause']} | approval_norm={r['approval_normalized'] or '-'} "
            f"{('| ' + r['note']) if r['note'] else ''}"
        )
    pass_cnt = sum(1 for r in results if r["ok"])
    lines += ["", f"PASS {pass_cnt}/20"]
    md_path.write_text("\n".join(lines), encoding="utf-8")

    print(f"result_json={json_path}")
    print(f"result_md={md_path}")
    print(f"pass={pass_cnt}/20")
    return 0 if pass_cnt == 20 else 1


if __name__ == "__main__":
    raise SystemExit(run())

