# K-VAN 연동 데이터 흐름 (시뮬레이션)

동일 **`SISA_DATA_DIR`**(또는 Docker `/app/data`, 로컬에서는 리포지토리 루트 **`./data`**) + 동일 **MySQL `MYSQL*`** 환경변수가  
`web_form.py`, `kvan_crawler.py`, `auto_kvan.py` 에 공유되어야 표시/저장·`crawler_wakeup.flag` 가 일치합니다.

## 1) 본사 `/admin` — 세션·링크 생성 (웹)

| 단계 | 기록 내용 | 저장 위치 |
|------|-----------|-----------|
| 결제 요청 생성 | `sessions[]` 항목 (id, amount, installment, agency_id 비어 있으면 본사) | `admin_state.json` |
| 주문 JSON | 금액·할부 등 | `data/sessions/orders/{session_id}.json` |
| 매크로 트리거 | `trigger_auto_kvan_async(session_id)` | 서브프로세스 `auto_kvan.py` |

**표시:** `admin_state`의 본사 세션 + MySQL `kvan_links`(만료/취소 제외, agency_id 빈 링크) 요약.

## 2) 매크로 `auto_kvan.py` — 링크 생성

| 단계 | 기록 내용 | 저장 위치 |
|------|-----------|-----------|
| 로그인·폼 작성 | K-VAN 결제 URL | (브라우저) |
| 링크 확보 | 세션에 `kvan_link` 저장 | `admin_state.json` (웹과 동일 경로 후보) |
| 시드 | KEY·agency·금액 | MySQL `kvan_links` (`upsert_kvan_link_creation_seed`) |
| 깨우기 | 크롤러 주기 단축 | `data/crawler_wakeup.flag` |
| (서버 옵션) | `/transactions` 스크랩·동기화 | `kvan_transactions` → `transactions`, `K_VAN_AFTER_LINK_SCRAPE_TX` |

**결제 승인 팝업 동기화(매크로 내):** `transactions` INSERT/UPDATE + `payment_notifications.json` (공통 `append_payment_notification`).

## 3) 크롤러 `kvan_crawler.py` — 주기 루프 (`python kvan_crawler.py` 기본 `--mode crawl`)

| 순서 | 동작 | DB/파일 |
|------|------|---------|
| 0 | 로그인 직후 + `K_VAN_CRAWL_INTERVAL` 백업 주기마다 스토어 루트 대시보드 스크랩 | MySQL `kvan_dashboard` |
| 1 | 만료·거래없음 링크 즉시 삭제 | `admin_state`, `kvan_links` |
| 1b | 만료+거래있음 | `admin_state` history + **`expired_with_transactions.json`** |
| 2 | `/payment-link` 스크랩 | MySQL `kvan_links` |
| 2b | 만료 링크 정리 | `mark_expired_sessions_from_kvan_links` |
| 3 | 결제링크 카드 **거래내역 팝업** | `upsert_popup_transaction` → `transactions` + **알림 JSON** |
| 4 | `/transactions` 테이블 스크랩 | `kvan_transactions` (`kvan_tx_table_scrape.py`: tbody 우선 대기 → 구버전 thead `.text` → infer 폴백) |
| 5 | `sync_kvan_to_transactions` | `transactions` 매핑/INSERT, **신규 승인 건 알림 JSON** |

## 4) 본사 `/hq-admin`

| 소스 | 용도 |
|------|------|
| MySQL `transactions` | 전체 거래 내역 표 |
| MySQL `kvan_links`, `kvan_transactions` | K-VAN 원천·삭제 등 |
| `expired_with_transactions.json` + **history 병합** | 만료+거래 있음 목록 |
| `payment_notifications.json` | 미확인 결제 알림 배지 |
| `hq_logs.log` | 로그 tail |

**페이지 새로고침(F5)과 크롤:** HQ/대행사/레거시 `/admin` 을 **GET**으로 열 때 `maybe_trigger_kvan_crawler_on_page_view()` 가  
`crawler_wakeup.flag` + 필요 시 `kvan_crawler.py` 기동을 요청한다(기본 **45초**에 한 번, `KVAN_PAGE_REFRESH_CRAWL_SEC` 로 조절).  
「결제 및 취소 내역」「결제 링크 관리」는 K-VAN **웹 URL**이 아니라 **DB에 반영된 스냅샷**이므로, 크롤 한 사이클이 끝난 뒤 다시 새로고침하면 최신에 가깝게 보인다.

**「새로고침」버튼(수동):** `action=refresh_kvan` POST → `trigger_kvan_crawler_refresh()` → 로그에 사이클 완료가 찍히면  
`/api/crawler-refresh-status` 폴링이 끝난 뒤 **브라우저가 GET으로 같은 페이지를 다시 로드**해 `transactions` / `kvan_links` 조회 결과를 갱신한다.

## 5) 대행사 `/agency-admin`

| 소스 | 용도 |
|------|------|
| `admin_state` (agency_id 필터) | 세션/히스토리 |
| MySQL `transactions` WHERE `agency_id` | 거래 표 (본사와 동일 DB) |
| `payment_notifications.json` (agency_id 필터) | 알림 |

## 6) 고객 `/pay/<session_id>` (본사 결제 페이지)

| 소스 | 용도 |
|------|------|
| `admin_state` **sessions + history** | 금액·할부 고정 표시 |
| `sessions/orders/{id}.json` | 주문 저장 후 매크로 |

## 최근 보완 요약

- 크롤러가 만료+거래 시 **`expired_with_transactions.json`** 에도 기록 (어드민과 일치).
- 크롤러가 팝업/`sync`로 **신규 `transactions` INSERT 시 `payment_notifications.json`** 기록 (매크로와 동일).
- `/pay/<id>` 가 **history** 에만 남은 세션도 금액 조회 가능.
