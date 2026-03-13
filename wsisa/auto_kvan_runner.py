"""
auto_kvan_runner.py - K-VAN 링크 생성 직렬 큐 실행기

K-VAN 은 동일 계정으로 동시 로그인이 불가하다.
web_form.py 의 trigger_auto_kvan_async() 가 이 스크립트를 단 한 번만 띄우고,
이 스크립트가 큐를 소진할 때까지 순서대로 auto_kvan.main() 을 호출한다.

사용법:
    python auto_kvan_runner.py <queue_file> <lock_file>
"""
from __future__ import annotations

import json
import os
import sys
import time
import subprocess
from pathlib import Path
from datetime import datetime

_SELF_DIR = Path(__file__).resolve().parent
_AUTO_KVAN = _SELF_DIR / "auto_kvan.py"
_BASE_DIR = _SELF_DIR.parent
_DATA_DIR = Path(os.environ.get("SISA_DATA_DIR", "").strip() or str(_BASE_DIR / "data"))
_LOG_PATH = _DATA_DIR / "hq_logs.log"


def _log(msg: str) -> None:
    try:
        _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(f"{datetime.utcnow().isoformat()} [RUNNER] {msg}\n")
    except Exception:
        pass
    print(f"[RUNNER] {msg}")


def _read_queue(queue_path: Path) -> list:
    try:
        if not queue_path.exists():
            return []
        return json.loads(queue_path.read_text(encoding="utf-8"))
    except Exception:
        return []


def _write_queue(queue_path: Path, queue: list) -> None:
    try:
        queue_path.write_text(json.dumps(queue), encoding="utf-8")
    except Exception as e:
        _log(f"[WARN] 큐 파일 쓰기 실패: {e}")


def _pop_session(queue_path: Path):
    queue = _read_queue(queue_path)
    if not queue:
        return None
    sid = queue.pop(0)
    _write_queue(queue_path, queue)
    return sid


def main() -> None:
    if len(sys.argv) < 3:
        print("사용법: auto_kvan_runner.py <queue_file> <lock_file>")
        sys.exit(1)

    queue_path = Path(sys.argv[1])
    lock_path = Path(sys.argv[2])

    try:
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path.write_text(str(os.getpid()), encoding="utf-8")
    except Exception as e:
        _log(f"[ERROR] 락 파일 생성 실패: {e}")
        sys.exit(1)

    _log(f"runner 시작 pid={os.getpid()}")

    try:
        while True:
            sid = _pop_session(queue_path)
            if sid is None:
                time.sleep(5)
                sid = _pop_session(queue_path)
                if sid is None:
                    _log("큐 비어있음 – runner 종료")
                    break

            _log(f"세션 처리 시작 session_id={sid}")
            try:
                result = subprocess.run(
                    [sys.executable, str(_AUTO_KVAN), sid],
                    timeout=600,
                )
                _log(f"세션 처리 완료 session_id={sid} exit={result.returncode}")
            except subprocess.TimeoutExpired:
                _log(f"[ERROR] 세션 처리 타임아웃(10분) session_id={sid}")
            except Exception as e:
                _log(f"[ERROR] 세션 처리 실패 session_id={sid}: {e}")

            time.sleep(3)

    finally:
        try:
            lock_path.unlink(missing_ok=True)
        except Exception:
            pass
        _log("runner 종료 – 락 파일 제거 완료")


if __name__ == "__main__":
    main()
