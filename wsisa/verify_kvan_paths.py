#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
K-VAN 데이터 경로·플래그 일치 여부 빠른 점검 (DB 연결 없음).

  python wsisa/verify_kvan_paths.py

- web_form 과 동일 규칙으로 DATA_DIR 후보
- kvan_crawler.py 가 현재 사용하는 DATA_DIR (임포트 시 계산)
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# wsisa 를 패스에 넣어 kvan_crawler 의 DATA_DIR 읽기
WSISA = Path(__file__).resolve().parent
ROOT = WSISA.parent
if str(WSISA) not in sys.path:
    sys.path.insert(0, str(WSISA))

os.chdir(str(WSISA))


def web_form_style_data_dir() -> Path:
    return Path(os.environ.get("SISA_DATA_DIR") or (ROOT / "data"))


def main() -> int:
    web_dd = web_form_style_data_dir()
    try:
        import kvan_crawler as kc  # noqa: PLC0415

        crawler_dd = kc.DATA_DIR
    except Exception as e:  # noqa: BLE001
        print(f"[FAIL] kvan_crawler 임포트 실패: {e}")
        return 1

    print("=== K-VAN 경로 검증 ===")
    print(f"SISA_DATA_DIR env: {os.environ.get('SISA_DATA_DIR')!r}")
    print(f"web_form 스타일 DATA_DIR: {web_dd}")
    print(f"kvan_crawler DATA_DIR:    {crawler_dd}")

    same = web_dd.resolve() == crawler_dd.resolve()
    print(f"경로 일치: {'OK' if same else 'FAIL — wakeup 플래그가 서로 다른 폴더에 쌓일 수 있음'}")

    for name in (
        "crawler_wakeup.flag",
        "kvan_crawler.lock",
        "kvan_crawler.heartbeat",
        "admin_state.json",
        "hq_logs.log",
    ):
        p = crawler_dd / name
        print(f"  [{name}] {'exists' if p.exists() else 'missing'}  {p}")

    return 0 if same else 2


if __name__ == "__main__":
    raise SystemExit(main())
