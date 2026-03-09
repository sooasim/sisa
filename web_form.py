from __future__ import annotations

import json
from typing import List
from pathlib import Path
from datetime import datetime
import os

from flask import (
    Flask,
    render_template_string,
    redirect,
    url_for,
    request,
    flash,
    jsonify,
    session,
)

ORDER_JSON_PATH = "current_order.json"
RESULT_JSON_PATH = "last_result.json"
ADMIN_STATE_PATH = "admin_state.json"
HQ_STATE_PATH = "hq_state.json"
SESSION_ORDER_DIR = Path("sessions") / "orders"
SESSION_RESULT_DIR = Path("sessions") / "results"

HEADERS: List[str] = [
    "login_id",
    "login_password",
    "login_pin",
    "card_type",  # personal / business
    "card_number",
    "expiry_mm",
    "expiry_yy",
    "card_password",
    "installment_months",
    "phone_number",
    "customer_name",
    "resident_front",
    "amount",
    "product_name",
]


app = Flask(__name__)
app.secret_key = "worldsisa-form-secret"

# 약관 파일 경로 (프로젝트 루트의 terms.html)
BASE_DIR = Path(__file__).resolve().parent
TERMS_FILE = BASE_DIR / "terms.html"


FORM_TEMPLATE = """
<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <title>구매대행 계약서 및 청구서</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0" id="viewport-meta" />
  <script>
    if (screen.width < 1280) {
      var vp = document.getElementById('viewport-meta');
      if (vp) vp.setAttribute('content', 'width=1280');
    }
  </script>
  <!-- 폰트 / 아이콘 / Tailwind -->
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;900&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
  <script src="https://cdn.tailwindcss.com"></script>
  <script>
    tailwind.config = {
      theme: {
        extend: {
          fontFamily: {
            sans: ['Inter', 'sans-serif'],
          },
          colors: {
            brand: {
              blue: '#2f4b9f',
              dark: '#1e326b',
              accent: '#e6edf7'
            }
          }
        }
      }
    }
  </script>
  <style>
    ::-webkit-scrollbar { width: 8px; height: 8px; }
    ::-webkit-scrollbar-track { background: rgba(255, 255, 255, 0.05); }
    ::-webkit-scrollbar-thumb { background: rgba(255, 255, 255, 0.2); border-radius: 4px; }
    ::-webkit-scrollbar-thumb:hover { background: rgba(255, 255, 255, 0.4); }

    .glass-card {
      background: rgba(255,255,255,0.08);
      backdrop-filter: blur(14px);
      -webkit-backdrop-filter: blur(14px);
      border: 1px solid rgba(255,255,255,0.2);
    }

    /* 기존 폼 스타일을 카드 안에서만 사용 */
    .kv-container { max-width: 720px; margin: 0 auto; }
    .kv-inner { background:#ffffff; color:#111827; border-radius:1.5rem; padding:20px 18px 18px; box-shadow:0 18px 45px rgba(15,23,42,0.35); }
    h1 { margin-top:0; font-size:22px; letter-spacing:-0.02em; }
    .grid { display:grid; grid-template-columns: repeat(auto-fit,minmax(220px,1fr)); gap:16px 24px; align-items:flex-start; }
    label { display:block; font-size:13px; font-weight:600; color:#4b5563; margin-bottom:4px; }
    input, select { width:100%; padding:10px 12px; border-radius:8px; border:1px solid #d1d5db; font-size:14px; box-sizing:border-box; background-color:#f9fafb; transition:border-color .15s, box-shadow .15s, background-color .15s; }
    input:focus, select:focus { outline:none; border-color:#2563eb; box-shadow:0 0 0 1px #2563eb33; background-color:#ffffff; }
    .section-title { margin-top:16px; margin-bottom:8px; font-size:15px; font-weight:700; color:#111827; border-bottom:1px solid #e5e7eb; padding-bottom:4px; }
    .card-box { margin-top:8px; padding:18px 16px 16px; border-radius:14px; border:1px solid #e5e7eb; background:linear-gradient(135deg,#f9fafb,#eef2ff); box-shadow:0 10px 28px rgba(15,23,42,0.12); }
    .field-row { display:flex; gap:12px; flex-wrap:wrap; }
    .field-row > div { flex:1; min-width:0; }
    .field-row .field-sm { flex:0 0 130px; }
    .field-row .field-md { flex:0 0 140px; }
    .field-row .field-pass { flex:0 0 150px; }
    .card-segments { display:flex; gap:8px; }
    .card-segments input { max-width:65px; text-align:center; letter-spacing:2px; }
    .amount-wrap { display:flex; align-items:center; gap:8px; }
    .amount-wrap input { max-width:110px; text-align:right; }
    .amount-suffix { font-size:14px; color:#4b5563; }
    .buyer-grid { max-width:360px; margin:0 auto; display:grid; grid-template-columns:1fr; gap:12px; }
    .phone-wrap { display:flex; align-items:center; gap:8px; }
    .phone-prefix { padding:9px 10px; border-radius:8px; border:1px solid #d1d5db; background:#f9fafb; font-size:14px; color:#374151; }
    .phone-segments { display:flex; gap:8px; flex:1; }
    .phone-segments input { max-width:70px; text-align:center; }
    .card-type-group { display:flex; flex-wrap:nowrap; align-items:center; gap:12px; border:1px solid #d1d5db; border-radius:999px; padding:6px 10px; background:#f9fafb; }
    .card-type-option { display:flex; align-items:center; gap:4px; white-space:nowrap; }
    .actions { margin-top:24px; display:flex; justify-content:flex-end; gap:12px; }
    .btn-pill { border:none; border-radius:999px; padding:10px 20px; font-size:14px; font-weight:600; cursor:pointer; }
    .btn-primary { background:#2563eb; color:white; }
    .btn-primary:hover { background:#1d4ed8; }
    .btn-secondary { background:white; color:#374151; border:1px solid #d1d5db; }
    .btn-secondary:hover { background:#f3f4f6; }
    .help { font-size:12px; color:#6b7280; margin-top:2px; }
    .flash { margin-bottom:12px; padding:8px 10px; border-radius:8px; font-size:13px; }
    .flash-success { background:#ecfdf3; color:#166534; border:1px solid #bbf7d0; }
    .flash-error { background:#fef2f2; color:#b91c1c; border:1px solid #fecaca; }

    /* 결과 모달 (기존 유지) */
    .result-backdrop {
      position: fixed;
      inset: 0;
      background: rgba(15,23,42,0.65);
      display: flex;
      align-items: center;
      justify-content: center;
      z-index: 999;
    }
    .result-card {
      width: 100%;
      max-width: 360px;
      background: #ffffff;
      border-radius: 16px;
      box-shadow: 0 22px 55px rgba(15,23,42,0.75);
      padding: 20px 20px 16px;
      text-align: center;
      box-sizing: border-box;
      animation: fade-in-up .22s ease-out;
    }
    .result-icon {
      width: 52px;
      height: 52px;
      border-radius: 999px;
      display:flex;
      align-items:center;
      justify-content:center;
      margin: 0 auto 8px;
      font-size:28px;
    }
    .result-icon.success { background:#ecfdf3; color:#16a34a; }
    .result-icon.fail { background:#fef2f2; color:#ef4444; }
    .result-title {
      font-size:18px;
      font-weight:700;
      margin-bottom:6px;
      color:#111827;
    }
    .result-message {
      font-size:13px;
      color:#4b5563;
      white-space:pre-line;
      margin-bottom:14px;
    }
    .result-actions {
      display:flex;
      justify-content:center;
      gap:10px;
      margin-top:4px;
    }
    .result-btn {
      min-width:90px;
      border-radius:999px;
      padding:8px 16px;
      font-size:13px;
      font-weight:600;
      cursor:pointer;
      border:none;
    }
    .result-btn.primary {
      background:#2563eb;
      color:#ffffff;
    }
    .result-btn.primary:hover { background:#1d4ed8; }
    .result-badge {
      display:inline-flex;
      align-items:center;
      gap:4px;
      padding:2px 8px;
      border-radius:999px;
      font-size:11px;
      font-weight:600;
      margin-bottom:4px;
    }
    .result-badge.success { background:#ecfdf3; color:#15803d; }
    .result-badge.fail { background:#fef2f2; color:#b91c1c; }
    @keyframes fade-in-up {
      from { opacity:0; transform: translateY(6px); }
      to { opacity:1; transform: translateY(0); }
    }
  </style>
</head>
<body class="bg-brand-blue text-white font-sans overflow-x-hidden antialiased flex flex-col min-h-screen">
  <main class="flex-grow pt-10 pb-10 px-4">
    <div class="kv-container">
      <div class="glass-card rounded-[2rem] border border-white/20 shadow-2xl">
        <div class="kv-inner">
          <h1 class="mb-1">구매대행 계약서 및 청구서</h1>
          <p class="text-xs text-gray-500 mb-4">
            아래 정보는 SISA 해외 경매 구매대행 계약 및 대면 결제 청구서 작성에 사용됩니다.
          </p>

          {% if last_result and last_result.status in ['success', 'fail'] %}
            {% set _status = last_result.status %}
            {% set _is_success = (_status == 'success') %}
            <div class="result-backdrop" id="result-modal">
              <div class="result-card">
                <div class="result-icon {{ 'success' if _is_success else 'fail' }}">
                  {% if _is_success %}✓{% else %}!{% endif %}
                </div>
                <div class="result-badge {{ 'success' if _is_success else 'fail' }}">
                  {% if _is_success %}결제 성공{% else %}결제 실패{% endif %}
                </div>
                <div class="result-title">
                  {% if _is_success %}결제가 완료되었습니다.{% else %}결제가 실패했습니다.{% endif %}
                </div>
                <div class="result-message">
                  {{ last_result.message }}
                </div>
                <div class="result-actions">
                  <button type="button" class="result-btn primary" onclick="window.__closeResultModal && window.__closeResultModal();">
                    확인
                  </button>
                </div>
              </div>
            </div>
          {% endif %}

          {% with messages = get_flashed_messages(with_categories=true) %}
            {% if messages %}
              {% for category, msg in messages %}
                <div class="flash flash-{{category}}">{{ msg }}</div>
              {% endfor %}
            {% endif %}
          {% endwith %}

          <form method="post" action="{{ url_for('index') }}">
            <!-- 로그인 정보는 폼에 보이지 않게 hidden 으로 처리 -->
            <input type="hidden" name="login_id" value="{{ defaults.login_id }}" />
            <input type="hidden" name="login_password" value="{{ defaults.login_password }}" />
            <input type="hidden" name="login_pin" value="{{ defaults.login_pin }}" />

            <div class="section-title">결제 / 카드 정보</div>
            <div class="field-row">
              <div style="flex:1.4">
                <label>카드 구분</label>
                <div class="card-type-group mt-1">
                  <label class="card-type-option text-sm text-gray-700">
                    <input type="radio" name="card_type" value="personal" {% if defaults.card_type == 'personal' %}checked{% endif %}/>
                    <span>개인카드</span>
                  </label>
                  <label class="card-type-option text-sm text-gray-700">
                    <input type="radio" name="card_type" value="business" {% if defaults.card_type == 'business' %}checked{% endif %}/>
                    <span>사업자(법인)카드</span>
                  </label>
                </div>
              </div>
              <div style="flex:1.0">
                <label for="product_name">상품명</label>
                <input id="product_name" name="product_name" placeholder="기본값: 잡화" value="{{ defaults.product_name }}" />
              </div>
            </div>
            <div class="card-box">
              <div>
                <label>카드번호 (4자리씩 입력)</label>
                <div class="card-segments">
                  <input id="card_number_1" maxlength="4" inputmode="numeric" value="{{ defaults.card_number_1 }}" />
                  <input id="card_number_2" maxlength="4" inputmode="numeric" value="{{ defaults.card_number_2 }}" />
                  <input id="card_number_3" maxlength="4" inputmode="numeric" value="{{ defaults.card_number_3 }}" />
                  <input id="card_number_4" maxlength="4" inputmode="numeric" value="{{ defaults.card_number_4 }}" />
                </div>
                <input type="hidden" id="card_number" name="card_number" value="{{ defaults.card_number }}" />
              </div>
              <div class="field-row" style="margin-top:14px;">
                <div class="field-md">
                  <label for="expiry_mm">유효기간 MM</label>
                  <select id="expiry_mm" name="expiry_mm" required>
                    <option value="">선택</option>
                    {% for m in range(1,13) %}
                      <option value="{{ m }}" {% if defaults.expiry_mm|string == m|string %}selected{% endif %}>{{ "%02d"|format(m) }}</option>
                    {% endfor %}
                  </select>
                </div>
                <div class="field-md">
                  <label for="expiry_yy">유효기간 YY (연도)</label>
                  <select id="expiry_yy" name="expiry_yy" required>
                    <option value="">선택</option>
                    {% for y in range(2026, 2037) %}
                      <option value="{{ y }}" {% if defaults.expiry_yy|string == y|string %}selected{% endif %}>{{ y }}</option>
                    {% endfor %}
                  </select>
                </div>
              </div>
              <div class="field-row" style="margin-top:14px;">
                <div class="field-pass">
                  <label for="card_password">카드 비밀번호 앞 2자리</label>
                  <input id="card_password" name="card_password" type="password" maxlength="2" required value="{{ defaults.card_password }}" autocomplete="off" />
                </div>
                <div class="field-md">
                  <label for="installment_months">할부개월</label>
                  <select id="installment_months" name="installment_months" required>
                    <option value="일시불" {% if defaults.installment_months in ['', None, '일시불'] %}selected{% endif %}>일시불</option>
                    {% for m in range(2,7) %}
                      <option value="{{ m }}" {% if defaults.installment_months|string == m|string %}selected{% endif %}>{{ m }}개월</option>
                    {% endfor %}
                  </select>
                </div>
                <div style="flex:1.4">
                  <label for="amount_unit">결제 금액 (만원 단위)</label>
                  <div class="amount-wrap">
                    <input id="amount_unit" name="amount_unit" inputmode="numeric" value="{{ defaults.amount_unit }}" {% if fixed_amount %}readonly{% endif %} />
                    <span class="amount-suffix">만원</span>
                  </div>
                  <div class="help" style="text-align:right;">= <span id="amount_preview">{{ defaults.amount_preview }}</span></div>
                  <input type="hidden" id="amount" name="amount" value="{{ defaults.amount }}" />
                </div>
              </div>
            </div>

            <div class="section-title">구매자 정보</div>
            <div class="buyer-grid">
              <div>
                <label>연락처</label>
                <div class="phone-wrap">
                  <span class="phone-prefix">010</span>
                  <div class="phone-segments">
                    <input id="phone1" maxlength="4" inputmode="numeric" value="{{ defaults.phone1 }}" />
                    <input id="phone2" maxlength="4" inputmode="numeric" value="{{ defaults.phone2 }}" />
                  </div>
                </div>
                <input type="hidden" id="phone_number" name="phone_number" value="{{ defaults.phone_number }}" />
              </div>
              <div>
                <label for="customer_name">이름</label>
                <input id="customer_name" name="customer_name" required value="{{ defaults.customer_name }}" />
              </div>
              <div>
                <label for="resident_front">주민번호 앞자리 (YYMMDD)</label>
                <input id="resident_front" name="resident_front" maxlength="6" required value="{{ defaults.resident_front }}" />
              </div>
            </div>

            <div class="section-title">결제 전 필수 동의</div>
            <div class="mt-2 rounded-2xl bg-gradient-to-br from-brand-dark via-brand-blue/90 to-brand-dark text-white p-4 md:p-5 border border-white/10 shadow-md space-y-2">
              <p class="text-xs text-white/80">
                고객님, 안전한 경매 대행 서비스를 위해 아래 사항에 모두 동의해 주셔야 입찰 및 결제가 진행됩니다.
              </p>
              <div class="space-y-2 text-sm">
                <label class="flex items-start gap-3 p-2 bg-white/5 rounded-xl border border-white/10 cursor-pointer">
                  <input id="agree_service" type="checkbox" class="mt-1 h-4 w-4 rounded border-white/40 bg-white/10 accent-blue-400" />
                  <span><strong class="text-blue-300 mr-1">[필수]</strong> SISA 서비스 이용약관 동의</span>
                </label>
                <label class="flex items-start gap-3 p-2 bg-white/5 rounded-xl border border-white/10 cursor-pointer">
                  <input id="agree_law" type="checkbox" class="mt-1 h-4 w-4 rounded border-white/40 bg-white/10 accent-blue-400" />
                  <span><strong class="text-blue-300 mr-1">[필수]</strong> 해외 경매 입찰의 법적 구속력(민법 제527조) 및 원칙적 취소 불가 원칙에 대해 이해하였습니다.</span>
                </label>
                <label class="flex items-start gap-3 p-2 bg-white/5 rounded-xl border border-white/10 cursor-pointer">
                  <input id="agree_penalty" type="checkbox" class="mt-1 h-4 w-4 rounded border-white/40 bg-white/10 accent-blue-400" />
                  <span><strong class="text-blue-300 mr-1">[필수]</strong> 정당한 사유 없는 취소 시 발생하는 위약금 규정 및 낙찰 권리/소유권 이전 규정에 동의합니다.</span>
                </label>
                <label class="flex items-start gap-3 p-2 bg-white/5 rounded-xl border border-white/10 cursor-pointer">
                  <input id="agree_realname" type="checkbox" class="mt-1 h-4 w-4 rounded border-white/40 bg-white/10 accent-blue-400" />
                  <span><strong class="text-blue-300 mr-1">[필수]</strong> 반드시 본인 명의의 결제수단을 사용하며, 부정거래(카드깡 등) 시 형사 고발 조치될 수 있음에 서약합니다.</span>
                </label>
                <label class="flex items-start gap-3 p-2 bg-white/5 rounded-xl border border-white/10 cursor-pointer">
                  <input id="agree_privacy" type="checkbox" class="mt-1 h-4 w-4 rounded border-white/40 bg-white/10 accent-blue-400" />
                  <span><strong class="text-blue-300 mr-1">[필수]</strong> 개인정보 수집 및 이용 동의</span>
                </label>
                <label class="flex items-start gap-3 p-2 bg-white/5 rounded-xl border border-dashed border-white/20 cursor-pointer">
                  <input id="agree_marketing" type="checkbox" class="mt-1 h-4 w-4 rounded border-white/40 bg-white/10 accent-blue-400" />
                  <span><strong class="text-gray-300 mr-1">[선택]</strong> 마케팅 및 글로벌 경매 동향 정보 수신 동의</span>
                </label>
              </div>
              <p class="text-[11px] text-white/70 pt-1">
                위의 필수 항목에 모두 체크하고 <strong>주문 데이터 저장</strong> 버튼을 누르는 경우, 상기 내용에 모두 동의하고 구매대행 계약 및 결제를 진행하는 것에 동의한 것으로 간주됩니다.
              </p>
            </div>

            <div class="mt-4">
              <label class="text-xs font-semibold text-gray-700 mb-1 block">이용약관 전문</label>
              <div class="border border-gray-200 rounded-lg h-40 overflow-hidden bg-gray-50">
                <iframe src="{{ url_for('terms') }}" class="w-full h-full border-0 bg-white"></iframe>
              </div>
            </div>

            <div class="actions">
              <button type="reset" class="btn-pill btn-secondary">초기화</button>
              <button type="submit" class="btn-pill btn-primary">주문 데이터 저장</button>
            </div>
          </form>
        </div>
      </div>
    </div>
  </main>

  <script>
    (function() {
      function digitsOnly(v) {
        return (v || "").replace(/\\D/g, "");
      }

      // 카드번호 4칸 -> 숨겨진 card_number 로 합치기
      var segIds = ["card_number_1", "card_number_2", "card_number_3", "card_number_4"];
      var segInputs = segIds.map(function(id) { return document.getElementById(id); }).filter(Boolean);
      var hiddenCard = document.getElementById("card_number");

      function updateCardNumber() {
        var parts = segInputs.map(function(input) {
          var v = digitsOnly(input.value).slice(0, 4);
          input.value = v;
          return v;
        });
        var joined = parts.join("");
        if (hiddenCard) hiddenCard.value = joined;
      }

      segInputs.forEach(function(input, idx) {
        input.addEventListener("input", function(e) {
          e.target.value = digitsOnly(e.target.value).slice(0, 4);
          if (e.target.value.length === 4 && idx < segInputs.length - 1) {
            segInputs[idx + 1].focus();
          }
          updateCardNumber();
        });
      });
      updateCardNumber();

      // 금액: 만원 단위 -> 전체 금액 / 미리보기
      var unitInput = document.getElementById("amount_unit");
      var hiddenAmount = document.getElementById("amount");
      var preview = document.getElementById("amount_preview");

      function updateAmount() {
        if (!unitInput) return;
        var unit = parseInt(digitsOnly(unitInput.value) || "0", 10);
        var full = unit * 10000;
        if (hiddenAmount) hiddenAmount.value = full || "";
        if (preview) {
          preview.textContent = full ? full.toLocaleString("ko-KR") + " 원" : "0 원";
        }
      }

      if (unitInput) {
        unitInput.addEventListener("input", function(e) {
          e.target.value = digitsOnly(e.target.value);
          updateAmount();
        });
        updateAmount();
      }

      // 연락처: 010 고정 + 4자리 + 4자리 -> 숨겨진 phone_number 로 저장 (뒷 8자리만)
      var phone1 = document.getElementById("phone1");
      var phone2 = document.getElementById("phone2");
      var hiddenPhone = document.getElementById("phone_number");

      function updatePhone() {
        if (!phone1 || !phone2 || !hiddenPhone) return;
        phone1.value = digitsOnly(phone1.value).slice(0, 4);
        phone2.value = digitsOnly(phone2.value).slice(0, 4);
        hiddenPhone.value = (phone1.value || "") + (phone2.value || "");
      }

      if (phone1 && phone2) {
        phone1.addEventListener("input", function(e) {
          e.target.value = digitsOnly(e.target.value).slice(0, 4);
          if (e.target.value.length === 4) {
            phone2.focus();
          }
          updatePhone();
        });
        phone2.addEventListener("input", function(e) {
          e.target.value = digitsOnly(e.target.value).slice(0, 4);
          updatePhone();
        });
        updatePhone();
      }

      var form = document.querySelector("form");
      if (form) {
        form.addEventListener("submit", function(e) {
          // 필수 동의 체크
          var requiredIds = ["agree_service", "agree_law", "agree_penalty", "agree_realname", "agree_privacy"];
          var allOk = true;
          for (var i = 0; i < requiredIds.length; i++) {
            var el = document.getElementById(requiredIds[i]);
            if (el && !el.checked) {
              allOk = false;
              break;
            }
          }
          if (!allOk) {
            e.preventDefault();
            alert("모든 [필수] 동의 항목에 체크해 주세요.");
            return;
          }
          updateCardNumber();
          updateAmount();
          updatePhone();
        });
      }

      // 결과 모달 닫기 핸들러
      var modal = document.getElementById("result-modal");
      window.__closeResultModal = function () {
        if (modal) {
          modal.style.display = "none";
        }
      };
      if (modal) {
        // 배경 클릭 시도 시 닫기
        modal.addEventListener("click", function(e) {
          if (e.target === modal) {
            window.__closeResultModal();
          }
        });
        // 몇 초 후 자동으로 닫기 (원하지 않으면 시간 늘리거나 제거)
        setTimeout(function () {
          window.__closeResultModal();
        }, 6000);
      }

      // 자동 결과 확인 폴링: 상태가 "진행중" 이면 5초 대기 후 2초 간격으로 /last-result 확인
      var lastStatus = "{{ last_result.status if last_result else '' }}";
      var currentSessionId = "{{ session_id if session_id is defined else '' }}";
      if (lastStatus === "진행중") {
        setTimeout(function () {
          var timerId = setInterval(function () {
            var url = "{{ url_for('last_result_api') }}";
            if (currentSessionId) {
              url += "?session_id=" + encodeURIComponent(currentSessionId);
            }
            fetch(url, { cache: "no-store" })
              .then(function (res) { return res.json(); })
              .then(function (data) {
                if (!data || !data.status) return;
                if (data.status === "진행중" || data.status === "unknown") {
                  return;
                }
                clearInterval(timerId);
                // 완료/실패 등 최종 상태가 되면 페이지를 새로고침하여 모달을 띄운다
                window.location.reload();
              })
              .catch(function () { /* 네트워크 오류는 무시 */ });
          }, 2000);
        }, 5000);
      }
    })();
  </script>
</body>
</html>
"""


@app.route("/", methods=["GET", "POST"])
def index():
    defaults = {
        "login_id": "m3313",
        "login_password": "k2255",
        "login_pin": "2424",
        "card_type": "personal",
    }

    # auto_kvan.py 가 남긴 마지막 결제 결과가 있으면 먼저 읽어온다
    last_result: dict | None = None
    if Path(RESULT_JSON_PATH).exists():
        try:
            with open(RESULT_JSON_PATH, "r", encoding="utf-8") as f:
                payload = json.load(f)
            status = str(payload.get("status", "unknown"))
            message = str(payload.get("message", "") or "")
            last_result = {"status": status, "message": message}
        except Exception:
            last_result = None

    # 마지막 결과가 성공이 아닌 경우에만, 이전 주문 데이터를 기본값으로 사용 (간이 자동완성)
    if not (last_result and last_result.get("status") == "success"):
        if Path(ORDER_JSON_PATH).exists():
            try:
                with open(ORDER_JSON_PATH, "r", encoding="utf-8") as f:
                    saved = json.load(f)
                for key in HEADERS:
                    if key in saved and saved[key] not in (None, ""):
                        defaults[key] = str(saved[key])
            except Exception:
                # 손상된 JSON 이 있어도 폼은 기본값으로 표시
                pass

    # 카드번호 4칸 분리용 기본값
    card_number = defaults.get("card_number", "")
    defaults["card_number_1"] = card_number[0:4]
    defaults["card_number_2"] = card_number[4:8]
    defaults["card_number_3"] = card_number[8:12]
    defaults["card_number_4"] = card_number[12:16]

    # 금액: 전체 금액 -> 만원 단위 / 미리보기
    amount_str = defaults.get("amount", "") or "0"
    try:
        amount_int = int(amount_str)
    except ValueError:
        amount_int = 0
    defaults["amount_unit"] = str(amount_int // 10000) if amount_int else ""
    defaults["amount_preview"] = f"{amount_int:,} 원" if amount_int else "0 원"

    # 연락처: 저장된 뒷 8자리를 4-4 로 분할
    phone_suffix = (defaults.get("phone_number") or "").strip()
    phone_digits = "".join(ch for ch in phone_suffix if ch.isdigit())
    phone_digits = phone_digits[-8:] if len(phone_digits) >= 8 else phone_digits.rjust(8, "0")
    defaults["phone1"] = phone_digits[0:4] if len(phone_digits) >= 4 else ""
    defaults["phone2"] = phone_digits[4:8] if len(phone_digits) >= 8 else ""

    if request.method == "POST":
        form = request.form
        try:
            data = {h: form.get(h, "").strip() for h in HEADERS}
            if not data["product_name"]:
                data["product_name"] = "잡화"
            with open(ORDER_JSON_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            # 결과 상태 JSON 을 "진행중" 으로 초기화
            with open(RESULT_JSON_PATH, "w", encoding="utf-8") as f:
                json.dump(
                    {"status": "진행중", "message": "자동 결제를 대기 중입니다."},
                    f,
                    ensure_ascii=False,
                    indent=2,
                )
        except Exception as e:  # noqa: BLE001
            flash(f"데이터 저장 중 오류가 발생했습니다: {e}", "error")
        else:
            flash("주문 데이터가 성공적으로 저장되었습니다.", "success")
        return redirect(url_for("index"))

    return render_template_string(FORM_TEMPLATE, defaults=defaults, last_result=last_result)


@app.route("/pay/<session_id>", methods=["GET", "POST"])
def pay(session_id: str):
    """관리자가 생성한 단일 결제 링크용 폼 (금액/할부를 고정해서 노출)."""
    defaults = {
        "login_id": "m3313",
        "login_password": "k2255",
        "login_pin": "2424",
        "card_type": "personal",
    }

    # 세션별 주문/결과 파일 경로
    SESSION_ORDER_DIR.mkdir(parents=True, exist_ok=True)
    SESSION_RESULT_DIR.mkdir(parents=True, exist_ok=True)
    order_path = SESSION_ORDER_DIR / f"{session_id}.json"
    result_path = SESSION_RESULT_DIR / f"{session_id}.json"

    # 관리자 상태에서 현재 세션 정보 읽기 (금액/할부 고정용)
    fixed_amount = False
    if Path(ADMIN_STATE_PATH).exists():
        try:
            with open(ADMIN_STATE_PATH, "r", encoding="utf-8") as f:
                admin_state = json.load(f)
            sessions = admin_state.get("sessions") or []
            for s in sessions:
                if str(s.get("id")) == str(session_id):
                    amount_str = str(s.get("amount", "") or "")
                    if amount_str:
                        defaults["amount"] = amount_str
                        fixed_amount = True
                    installment = str(s.get("installment", "") or "")
                    if installment:
                        defaults["installment_months"] = installment
                    break
        except Exception:
            pass

    # 세션별 마지막 결과 읽기
    last_result: dict | None = None
    if result_path.exists():
        try:
            with open(result_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            status = str(payload.get("status", "unknown"))
            message = str(payload.get("message", "") or "")
            last_result = {"status": status, "message": message}
        except Exception:
            last_result = None

    # 기본 파생 값들 구성 (카드번호 분리, 금액 unit, 전화번호 분리)
    card_number = defaults.get("card_number", "")
    defaults["card_number_1"] = card_number[0:4]
    defaults["card_number_2"] = card_number[4:8]
    defaults["card_number_3"] = card_number[8:12]
    defaults["card_number_4"] = card_number[12:16]

    amount_str = defaults.get("amount", "") or "0"
    try:
        amount_int = int(amount_str)
    except ValueError:
        amount_int = 0
    defaults["amount_unit"] = str(amount_int // 10000) if amount_int else ""
    defaults["amount_preview"] = f"{amount_int:,} 원" if amount_int else "0 원"

    phone_suffix = (defaults.get("phone_number") or "").strip()
    phone_digits = "".join(ch for ch in phone_suffix if ch.isdigit())
    phone_digits = phone_digits[-8:] if len(phone_digits) >= 8 else phone_digits.rjust(8, "0")
    defaults["phone1"] = phone_digits[0:4] if len(phone_digits) >= 4 else ""
    defaults["phone2"] = phone_digits[4:8] if len(phone_digits) >= 8 else ""

    if request.method == "POST":
        form = request.form
        try:
            data = {h: form.get(h, "").strip() for h in HEADERS}
            if not data["product_name"]:
                data["product_name"] = "잡화"
            with open(order_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            # 결과 상태 JSON 을 "진행중" 으로 초기화
            with open(result_path, "w", encoding="utf-8") as f:
                json.dump(
                    {"status": "진행중", "message": "자동 결제를 대기 중입니다."},
                    f,
                    ensure_ascii=False,
                    indent=2,
                )
        except Exception as e:  # noqa: BLE001
            flash(f"데이터 저장 중 오류가 발생했습니다: {e}", "error")
        else:
            flash("주문 데이터가 성공적으로 저장되었습니다.", "success")
        return redirect(url_for("pay", session_id=session_id))

    return render_template_string(
        FORM_TEMPLATE,
        defaults=defaults,
        last_result=last_result,
        fixed_amount=fixed_amount,
        session_id=session_id,
    )


@app.route("/last-result", methods=["GET"])
def last_result_api():
    """자동 결제 결과를 JSON 으로 반환 (폼에서 폴링용)."""
    payload = {"status": "unknown", "message": ""}
    session_id = request.args.get("session_id", "").strip()
    if session_id:
        path = SESSION_RESULT_DIR / f"{session_id}.json"
    else:
        path = Path(RESULT_JSON_PATH)

    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                payload["status"] = str(data.get("status", "unknown"))
                payload["message"] = str(data.get("message", "") or "")
        except Exception:
            pass
    return jsonify(payload)


@app.route("/health", methods=["GET"])
def health():
    """간단 헬스 체크 엔드포인트."""
    return jsonify({"status": "ok"}), 200


def _load_hq_state() -> dict:
    """본사 어드민 상태(hq_state.json)를 로드."""
    state = {"applications": [], "agencies": [], "transactions": []}
    path = Path(HQ_STATE_PATH)
    if path.exists():
        try:
            with path.open("r", encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                state.update({k: loaded.get(k, state[k]) for k in state.keys()})
        except Exception:
            pass
    return state


def _save_hq_state(state: dict) -> None:
    try:
        with Path(HQ_STATE_PATH).open("w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


@app.route("/agency-apply", methods=["POST"])
def agency_apply():
    """대행사 등록 신청 폼 수신 엔드포인트 (agency-register.html 에서 POST)."""
    form = request.form
    company_name = form.get("업체명", "").strip()
    domain = form.get("도메인(영문)", "").strip()
    phone = form.get("전화번호", "").strip()
    bank_name = form.get("은행명", "").strip()
    account_number = form.get("계좌번호", "").strip()
    email_or_sheet = form.get("이메일_또는_구글시트", "").strip()
    agency_login_id = form.get("대행사아이디", "").strip()
    agency_login_pw = form.get("대행사비밀번호", "").strip()

    app_id = datetime.utcnow().strftime("AG%Y%m%d%H%M%S%f")
    state = _load_hq_state()
    applications = state.get("applications") or []
    applications.append(
        {
            "id": app_id,
            "company_name": company_name,
            "domain": domain,
            "phone": phone,
            "bank_name": bank_name,
            "account_number": account_number,
            "email_or_sheet": email_or_sheet,
            "login_id": agency_login_id,
            "login_password": agency_login_pw,
            "created_at": datetime.utcnow().isoformat(),
            "status": "pending",
        }
    )
    state["applications"] = applications
    _save_hq_state(state)

    # 간단한 접수 완료 페이지 반환 (SISA 스타일)
    return """
<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <title>대행사 등록 신청 완료</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap" rel="stylesheet">
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-[#2f4b9f] text-white font-[Inter] flex items-center justify-center min-h-screen">
  <div class="bg-white/10 border border-white/20 rounded-2xl px-8 py-10 max-w-md w-full text-center shadow-2xl">
    <div class="w-12 h-12 rounded-full bg-emerald-400/20 border border-emerald-300/50 flex items-center justify-center mx-auto mb-4">
      <span class="text-2xl text-emerald-300">✓</span>
    </div>
    <h1 class="text-2xl font-bold mb-2">대행사 등록 신청이 접수되었습니다.</h1>
    <p class="text-sm text-white/70 mb-4 leading-relaxed">
      SISA 본사 어드민에서 신청 내용을 검토한 후,<br/>
      담당자가 개별적으로 연락을 드립니다.
    </p>
    <p class="text-[11px] text-white/60">
      이 창은 닫으셔도 됩니다. 추가 문의는 본사 이메일로 연락해 주세요.
    </p>
  </div>
</body>
</html>
"""


@app.route("/terms", methods=["GET"])
def terms():
    """이용약관 HTML 파일을 iframe 으로 표시하기 위한 라우트."""
    if TERMS_FILE.exists():
        try:
            return TERMS_FILE.read_text(encoding="utf-8")
        except Exception:
            pass
    return (
        "<!doctype html><html><body><p>이용약관 파일을 불러올 수 없습니다.</p></body></html>",
        200,
        {"Content-Type": "text/html; charset=utf-8"},
    )

@app.route("/admin", methods=["GET", "POST"])
def admin():
    """어드민: 최대 5개까지 결제 세션 생성/조회 및 간단 관리."""
    base_url = request.url_root.rstrip("/")

    # 기존 상태 로드 (sessions 리스트 기반)
    sessions: list[dict] = []
    history: list[dict] = []
    message = ""
    if Path(ADMIN_STATE_PATH).exists():
        try:
            with open(ADMIN_STATE_PATH, "r", encoding="utf-8") as f:
                saved = json.load(f)
            if isinstance(saved, dict):
                if isinstance(saved.get("sessions"), list):
                    sessions = saved["sessions"]
                if isinstance(saved.get("history"), list):
                    history = saved["history"]
                # 이전 단일 세션 포맷에서 마이그레이션
                if saved.get("current_session_id") and not sessions:
                    sessions = [
                        {
                            "id": str(saved.get("current_session_id")),
                            "amount": str(saved.get("amount", "") or ""),
                            "installment": str(saved.get("installment", "") or "일시불"),
                            "status": "결제중",
                            "created_at": saved.get("created_at")
                            or datetime.utcnow().isoformat(),
                        }
                    ]
        except Exception:
            sessions = []

    if request.method == "POST":
        action = request.form.get("action", "create").strip()

        if action == "create":
            amount = request.form.get("admin_amount", "").strip()
            installment = request.form.get("admin_installment", "일시불").strip()

            # 현재 진행 중(결제중) 세션 수 확인
            active_count = sum(
                1 for s in sessions if s.get("status", "결제중") == "결제중"
            )
            if active_count >= 5:
                message = "동시에 진행할 수 있는 세션은 최대 5개입니다."
            else:
                # 새 세션 ID 생성
                session_id = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")[-12:]
                session = {
                    "id": session_id,
                    "amount": amount,  # 비어 있으면 '고정 안 됨' 으로 동작
                    "installment": installment or "",
                    "status": "결제중",
                    "created_at": datetime.utcnow().isoformat(),
                }
                sessions.append(session)
                admin_state = {"sessions": sessions, "history": history}
                try:
                    with open(ADMIN_STATE_PATH, "w", encoding="utf-8") as f:
                        json.dump(admin_state, f, ensure_ascii=False, indent=2)
                except Exception as e:  # noqa: BLE001
                    message = f"상태 저장 중 오류가 발생했습니다: {e}"
                else:
                    if amount:
                        message = "결제요청 페이지 링크가 생성되었습니다. 링크를 복사하여 고객에게 전달하세요."
                    else:
                        message = "금액이 고정되지 않은 결제요청 링크가 생성되었습니다. 링크를 복사하여 고객에게 전달하세요."

        elif action == "close_session":
            sid = request.form.get("session_id", "").strip()
            memo = request.form.get("memo", "").strip()
            new_sessions: list[dict] = []
            for s in sessions:
                if str(s.get("id")) == sid:
                    entry = {
                        "id": sid,
                        "amount": str(s.get("amount", "") or ""),
                        "installment": str(s.get("installment", "") or "일시불"),
                        "status": "관리자종료",
                        "created_at": s.get("created_at") or datetime.utcnow().isoformat(),
                        "finished_at": datetime.utcnow().isoformat(),
                        "result_message": memo or "관리자가 세션을 종료했습니다.",
                        "customer_name": "",
                        "phone_number": "",
                        "product_name": "",
                        "settled": "정산전",
                    }
                    history.append(entry)
                else:
                    new_sessions.append(s)
            sessions = new_sessions
            admin_state = {"sessions": sessions, "history": history}
            try:
                with open(ADMIN_STATE_PATH, "w", encoding="utf-8") as f:
                    json.dump(admin_state, f, ensure_ascii=False, indent=2)
            except Exception as e:  # noqa: BLE001
                message = f"세션 종료 중 오류가 발생했습니다: {e}"

        elif action == "toggle_settle":
            sid = request.form.get("session_id", "").strip()
            for h in history:
                if str(h.get("id")) == sid:
                    h["settled"] = "정산완료" if h.get("settled") != "정산완료" else "정산전"
                    break
            admin_state = {"sessions": sessions, "history": history}
            try:
                with open(ADMIN_STATE_PATH, "w", encoding="utf-8") as f:
                    json.dump(admin_state, f, ensure_ascii=False, indent=2)
            except Exception as e:  # noqa: BLE001
                message = f"정산 상태 변경 중 오류가 발생했습니다: {e}"

        elif action == "delete_history":
            sid = request.form.get("session_id", "").strip()
            history = [h for h in history if str(h.get("id")) != sid]
            admin_state = {"sessions": sessions, "history": history}
            try:
                with open(ADMIN_STATE_PATH, "w", encoding="utf-8") as f:
                    json.dump(admin_state, f, ensure_ascii=False, indent=2)
            except Exception as e:  # noqa: BLE001
                message = f"기록 삭제 중 오류가 발생했습니다: {e}"

    ADMIN_TEMPLATE = """
    <!DOCTYPE html>
    <html lang="ko">
    <head>
      <meta charset="UTF-8" />
      <title>SISA K-VAN 결제 어드민</title>
      <meta name="viewport" content="width=device-width, initial-scale=1.0" id="viewport-meta" />
      <script>
        if (screen.width < 1280) {
          var vp = document.getElementById('viewport-meta');
          if (vp) vp.setAttribute('content', 'width=1280');
        }
        // 5분마다 자동 새로고침
        setInterval(function() { window.location.reload(); }, 300000);
      </script>
      <!-- 폰트 / 아이콘 / Tailwind -->
      <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;900&display=swap" rel="stylesheet">
      <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
      <script src="https://cdn.tailwindcss.com"></script>
      <script>
        tailwind.config = {
          theme: {
            extend: {
              fontFamily: {
                sans: ['Inter', 'sans-serif'],
              },
              colors: {
                brand: {
                  blue: '#2f4b9f',
                  dark: '#1e326b',
                  accent: '#e6edf7'
                }
              }
            }
          }
        }
      </script>
      <style>
        ::-webkit-scrollbar { width: 8px; height: 8px; }
        ::-webkit-scrollbar-track { background: rgba(255, 255, 255, 0.05); }
        ::-webkit-scrollbar-thumb { background: rgba(255, 255, 255, 0.2); border-radius: 4px; }
        ::-webkit-scrollbar-thumb:hover { background: rgba(255, 255, 255, 0.4); }

        .glass-card {
          background: rgba(255, 255, 255, 0.06);
          backdrop-filter: blur(14px);
          -webkit-backdrop-filter: blur(14px);
          border: 1px solid rgba(255, 255, 255, 0.22);
        }

        .admin-card-inner {
          background: rgba(15,23,42,0.92);
          border-radius: 1.5rem;
          padding: 18px 18px 20px;
          box-shadow: 0 22px 60px rgba(15,23,42,0.9);
          border: 1px solid #1f2937;
        }

        label { display:block; font-size:13px; font-weight:600; color:#9ca3af; margin-bottom:4px; }
        input, select { width:100%; padding:10px 12px; border-radius:10px; border:1px solid #374151; background:#020617; color:#e5e7eb; box-sizing:border-box; font-size:14px; }
        input:focus, select:focus { outline:none; border-color:#3b82f6; box-shadow:0 0 0 1px #3b82f6; }
        .grid { display:grid; grid-template-columns:2fr 1.5fr; gap:16px; margin-top:8px; }
        .actions { margin-top:16px; display:flex; gap:10px; align-items:center; flex-wrap:wrap; }
        .btn-pill { border:none; border-radius:999px; padding:10px 18px; font-size:14px; font-weight:600; cursor:pointer; }
        .btn-primary { background:#3b82f6; color:white; }
        .btn-primary:hover { background:#2563eb; }
        .btn-secondary { background:transparent; color:#e5e7eb; border:1px solid #4b5563; }
        .btn-secondary:hover { background:#111827; }
        .hint { font-size:12px; color:#9ca3af; margin-top:4px; }
        .status-card { margin-top:18px; padding:14px 12px; border-radius:16px; background:#020617; border:1px dashed #374151; font-size:13px; }
        .status-title { font-size:13px; font-weight:600; color:#9ca3af; margin-bottom:6px; display:flex; align-items:center; gap:6px; }
        .status-row { display:flex; justify-content:space-between; margin-bottom:4px; gap:8px; }
        .status-label { color:#9ca3af; font-size:12px; }
        .status-value { color:#e5e7eb; font-size:12px; text-align:right; }
        .link-box { margin-top:8px; padding:8px 10px; border-radius:12px; background:#020617; border:1px solid #1f2937; display:flex; gap:8px; align-items:center; }
        .link-text { flex:1; font-size:12px; color:#e5e7eb; word-break:break-all; }
        .msg { margin-top:10px; font-size:12px; color:#a5b4fc; }
        .pill-btn { border-radius:999px; padding:6px 10px; font-size:11px; border:none; cursor:pointer; }
        .pill-danger { background:#b91c1c; color:#fef2f2; }
        .pill-muted { background:#111827; color:#e5e7eb; border:1px solid #4b5563; }
        .small-input { width:100%; padding:6px 8px; border-radius:8px; border:1px solid #374151; background:#020617; color:#e5e7eb; font-size:12px; box-sizing:border-box; }
      </style>
    </head>
    <body class="bg-brand-blue text-white font-sans overflow-x-hidden antialiased flex flex-col min-h-screen">
      <!-- 헤더 -->
      <header class="fixed top-0 left-0 right-0 z-30 glass-card border-b border-white/10">
        <div class="max-w-5xl mx-auto px-4 py-3 flex items-center justify-between">
          <div class="flex items-center gap-2">
            <i class="fa-solid fa-globe text-white text-xl drop-shadow-sm"></i>
            <div class="flex flex-col leading-tight">
              <span class="text-xs font-semibold tracking-[0.18em] uppercase text-white/70">SISA</span>
              <span class="text-xs text-white/80">K-VAN Payment Admin</span>
            </div>
          </div>
          <div class="hidden sm:flex items-center gap-2 text-[11px] text-white/70">
            <span class="px-2 py-1 rounded-full bg-black/20 border border-white/20">실시간 결제 세션 관리</span>
          </div>
        </div>
      </header>

      <main class="flex-grow pt-24 pb-12 px-3 sm:px-4">
        <div class="max-w-4xl mx-auto">
          <div class="glass-card rounded-[2rem] border border-white/20 shadow-2xl">
            <div class="admin-card-inner">
              <div class="flex items-center justify-between mb-4">
                <div>
                  <h1 class="text-xl font-semibold text-white mb-1">World SISA 대면결제 세션 어드민</h1>
                  <p class="text-xs text-slate-300">
                    고객에게 보낼 결제 링크를 생성하고, 진행 중인 결제와 완료된 결제를 한 곳에서 확인합니다.
                  </p>
                </div>
              </div>

              <form method="post" action="{{ url_for('admin') }}">
                <div class="grid">
                  <div>
                    <label for="admin_amount">결제 금액 (원 단위)</label>
                    <input id="admin_amount" name="admin_amount" inputmode="numeric" placeholder="예: 20000" />
                    <div class="hint">비워두면 금액이 고정되지 않은 결제 요청 링크가 생성됩니다.</div>
                  </div>
                  <div>
                    <label for="admin_installment">할부개월</label>
                    <select id="admin_installment" name="admin_installment">
                      <option value="일시불" selected>일시불</option>
                      {% for m in range(2,7) %}
                        <option value="{{ m }}">{{ m }}개월</option>
                      {% endfor %}
                    </select>
                  </div>
                </div>
                <div class="actions">
                  <input type="hidden" name="action" value="create" />
                  <button type="submit" class="btn-pill btn-primary">결제창 생성</button>
                  <span class="hint">버튼을 누르면 새로운 결제 요청 링크가 만들어집니다. (동시 최대 5개)</span>
                </div>
              </form>

              <div class="status-card">
                <div class="status-title">
                  <i class="fa-solid fa-circle-play text-emerald-400 text-xs"></i>
                  진행 중인 결제 세션 (최대 5개)
                </div>
                {% if sessions %}
                  {% for s in sessions %}
                    <div style="margin:8px 0; padding:10px 11px; border-radius:12px; background:#020617; border:1px solid #111827;">
                      <div class="status-row">
                        <span class="status-label">세션 ID</span>
                        <span class="status-value">{{ s.id }}</span>
                      </div>
                      <div class="status-row">
                        <span class="status-label">결제금액</span>
                        <span class="status-value">{{ s.amount or '고정 안 됨' }}</span>
                      </div>
                      <div class="status-row">
                        <span class="status-label">할부개월</span>
                        <span class="status-value">{{ s.installment }}</span>
                      </div>
                      <div class="status-row">
                        <span class="status-label">상태</span>
                        <span class="status-value">{{ s.status or '결제중' }}</span>
                      </div>
                      <div class="status-title" style="margin-top:6px;">
                        <i class="fa-solid fa-link text-blue-400 text-xs"></i>
                        결제 요청 링크
                      </div>
                      <div class="link-box">
                        <div class="link-text" id="pay-link-{{ loop.index }}">{{ base_url }}{{ url_for('pay', session_id=s.id) }}</div>
                        <button type="button" class="btn-pill btn-secondary" onclick="copyPayLink('pay-link-{{ loop.index }}')">복사</button>
                      </div>
                      <form method="post" action="{{ url_for('admin') }}" style="margin-top:6px; display:flex; gap:6px; align-items:center; flex-wrap:wrap;">
                        <input type="hidden" name="action" value="close_session" />
                        <input type="hidden" name="session_id" value="{{ s.id }}" />
                        <input class="small-input" name="memo" placeholder="종료 메모 (선택)" />
                        <button type="submit" class="pill-btn pill-danger">강제종료</button>
                      </form>
                    </div>
                  {% endfor %}
                {% else %}
                  <div class="hint">아직 생성된 결제 요청 링크가 없습니다.</div>
                {% endif %}
              </div>

              <div class="status-card">
                <div class="status-title">
                  <i class="fa-solid fa-clipboard-list text-indigo-300 text-xs"></i>
                  결제관리 (완료/종료된 세션)
                </div>
                {% if history %}
                  {% for h in history %}
                    <div style="margin:8px 0; padding:10px 11px; border-radius:12px; background:#020617; border:1px solid #1f2937;">
                      <div class="status-row">
                        <span class="status-label">세션 ID</span>
                        <span class="status-value">{{ h.id }}</span>
                      </div>
                      <div class="status-row">
                        <span class="status-label">이름</span>
                        <span class="status-value">{{ h.customer_name or '-' }}</span>
                      </div>
                      <div class="status-row">
                        <span class="status-label">전화번호</span>
                        <span class="status-value">{{ h.phone_number or '-' }}</span>
                      </div>
                      <div class="status-row">
                        <span class="status-label">금액</span>
                        <span class="status-value">{{ h.amount or '-' }}</span>
                      </div>
                      <div class="status-row">
                        <span class="status-label">상태</span>
                        <span class="status-value">{{ h.status }}</span>
                      </div>
                      <div class="status-row">
                        <span class="status-label">정산</span>
                        <span class="status-value">{{ h.settled or '정산전' }}</span>
                      </div>
                      <div class="status-row">
                        <span class="status-label">완료시간</span>
                        <span class="status-value" style="font-size:11px;">{{ h.finished_at or '-' }}</span>
                      </div>
                      <div class="status-title" style="margin-top:6px;">메모 / 실패사유</div>
                      <div style="font-size:12px; color:#e5e7eb; white-space:pre-line; margin-bottom:6px;">
                        {{ h.result_message or '-' }}
                      </div>
                      <div class="status-row" style="gap:6px; margin-top:4px; flex-wrap:wrap;">
                        <form method="post" action="{{ url_for('admin') }}">
                          <input type="hidden" name="action" value="toggle_settle" />
                          <input type="hidden" name="session_id" value="{{ h.id }}" />
                          <button type="submit" class="pill-btn pill-muted">
                            {% if h.settled == '정산완료' %}정산취소{% else %}정산완료{% endif %}
                          </button>
                        </form>
                        <button type="button" class="pill-btn pill-muted" onclick="copyHistory('{{ h.customer_name or '' }}','{{ h.phone_number or '' }}','{{ h.amount or '' }}','{{ h.status or '' }}','{{ (h.result_message or '').replace('\\n',' ') }}')">복사</button>
                        <form method="post" action="{{ url_for('admin') }}">
                          <input type="hidden" name="action" value="delete_history" />
                          <input type="hidden" name="session_id" value="{{ h.id }}" />
                          <button type="submit" class="pill-btn pill-danger">삭제</button>
                        </form>
                      </div>
                    </div>
                  {% endfor %}
                {% else %}
                  <div class="hint">아직 완료/종료된 결제 기록이 없습니다.</div>
                {% endif %}
              </div>

              {% if message %}
                <div class="msg">{{ message }}</div>
              {% endif %}
            </div>
          </div>
        </div>
      </main>

      <script>
        function copyPayLink(id) {
          var el = document.getElementById(id);
          if (!el) return;
          var text = el.textContent || el.innerText || "";
          if (!navigator.clipboard) {
            var ta = document.createElement("textarea");
            ta.value = text;
            document.body.appendChild(ta);
            ta.select();
            document.execCommand("copy");
            document.body.removeChild(ta);
          } else {
            navigator.clipboard.writeText(text).catch(function() {});
          }
          alert("결제요청 페이지 링크가 복사되었습니다.");
        }

        function copyHistory(name, phone, amount, status, memo) {
          var parts = [
            "이름: " + (name || ""),
            "전화: 0" + (phone || ""),
            "금액: " + (amount || ""),
            "상태: " + (status || ""),
            "메모: " + (memo || "")
          ];
          var text = parts.join("\\t");
          if (!navigator.clipboard) {
            var ta = document.createElement("textarea");
            ta.value = text;
            document.body.appendChild(ta);
            ta.select();
            document.execCommand("copy");
            document.body.removeChild(ta);
          } else {
            navigator.clipboard.writeText(text).catch(function () {});
          }
          alert("결제 실폐/완료 정보가 복사되었습니다.");
        }
      </script>
    </body>
    </html>
    """

    return render_template_string(
        ADMIN_TEMPLATE, sessions=sessions, history=history, message=message, base_url=base_url
    )


@app.route("/hq-login", methods=["GET", "POST"])
def hq_login():
    """본사 메인 어드민 로그인 (admin / admin1234 기본값)."""
    error = ""
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()
        admin_user = os.environ.get("HQ_ADMIN_USER", "admin")
        admin_pw = os.environ.get("HQ_ADMIN_PASSWORD", "admin1234")
        if username == admin_user and password == admin_pw:
            session["hq_logged_in"] = True
            return redirect(url_for("hq_admin"))
        error = "아이디 또는 비밀번호가 올바르지 않습니다."

    template = """
    <!DOCTYPE html>
    <html lang="ko">
    <head>
      <meta charset="UTF-8" />
      <title>SISA HQ 어드민 로그인</title>
      <meta name="viewport" content="width=device-width, initial-scale=1.0" />
      <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap" rel="stylesheet">
      <script src="https://cdn.tailwindcss.com"></script>
    </head>
    <body class="bg-[#2f4b9f] text-white font-[Inter] min-h-screen flex items-center justify-center">
      <div class="bg-white/10 border border-white/20 rounded-2xl px-8 py-10 max-w-sm w-full shadow-2xl">
        <h1 class="text-xl font-bold mb-2 text-center">SISA HQ Admin</h1>
        <p class="text-xs text-white/70 text-center mb-6">본사 전용 어드민 로그인</p>
        <form method="post" class="space-y-4">
          <div>
            <label class="block text-xs font-semibold text-white/70 mb-1">아이디</label>
            <input name="username" type="text" required class="w-full bg-black/20 border border-white/20 rounded-lg py-2.5 px-3 text-sm text-white placeholder-white/40 focus:outline-none focus:border-blue-300" placeholder="admin" />
          </div>
          <div>
            <label class="block text-xs font-semibold text-white/70 mb-1">비밀번호</label>
            <input name="password" type="password" required class="w-full bg-black/20 border border-white/20 rounded-lg py-2.5 px-3 text-sm text-white placeholder-white/40 focus:outline-none focus:border-blue-300" placeholder="********" />
          </div>
          {% if error %}
          <p class="text-xs text-red-200">{{ error }}</p>
          {% endif %}
          <button type="submit" class="w-full mt-2 bg-white text-brand-blue font-bold py-2.5 rounded-lg hover:bg-brand-accent transition">
            로그인
          </button>
        </form>
      </div>
    </body>
    </html>
    """
    return render_template_string(template, error=error)


@app.route("/hq-admin", methods=["GET", "POST"])
def hq_admin():
    """본사 메인 어드민 대시보드."""
    if not session.get("hq_logged_in"):
        return redirect(url_for("hq_login"))

    state = _load_hq_state()
    applications = state.get("applications") or []
    agencies = state.get("agencies") or []
    transactions = state.get("transactions") or []
    message = ""

    if request.method == "POST":
        action = request.form.get("action", "").strip()
        if action == "approve_application":
            app_id = request.form.get("application_id", "").strip()
            found = None
            for a in applications:
                if str(a.get("id")) == app_id:
                    found = a
                    break
            if found:
                found["status"] = "approved"
                agency_id = datetime.utcnow().strftime("AGY%Y%m%d%H%M%S%f")
                agency = {
                    "id": agency_id,
                    "company_name": found.get("company_name", ""),
                    "domain": found.get("domain", ""),
                    "phone": found.get("phone", ""),
                    "bank_name": found.get("bank_name", ""),
                    "account_number": found.get("account_number", ""),
                    "email_or_sheet": found.get("email_or_sheet", ""),
                    "login_id": found.get("login_id", ""),
                    "login_password": found.get("login_password", ""),
                    "fee_percent": 10,
                    "created_at": datetime.utcnow().isoformat(),
                    "status": "active",
                }
                agencies.append(agency)
                state["applications"] = applications
                state["agencies"] = agencies
                _save_hq_state(state)
                message = f"대행사 '{agency['company_name']}' 가 생성되었습니다."
        # 다른 액션(수수료 변경, 정산 상태 변경 등)은 추후 확장

    template = """
    <!DOCTYPE html>
    <html lang="ko">
    <head>
      <meta charset="UTF-8" />
      <title>SISA HQ Admin</title>
      <meta name="viewport" content="width=device-width, initial-scale=1.0" />
      <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
      <script src="https://cdn.tailwindcss.com"></script>
      <script>
        tailwind.config = {
          theme: {
            extend: {
              fontFamily: { sans: ['Inter', 'sans-serif'] },
              colors: {
                brand: { blue: '#2f4b9f', dark: '#1e326b', accent: '#e6edf7' }
              }
            }
          }
        }
      </script>
    </head>
    <body class="bg-brand-blue text-white font-sans overflow-x-hidden antialiased min-h-screen flex flex-col">
      <header class="fixed top-0 left-0 right-0 z-30 bg-brand-dark/80 backdrop-blur border-b border-white/10">
        <div class="max-w-6xl mx-auto px-4 py-3 flex items-center justify-between">
          <div class="flex items-center gap-2">
            <i class="fa-solid fa-shield-halved text-white text-xl"></i>
            <div class="flex flex-col leading-tight">
              <span class="text-sm font-semibold tracking-[0.16em] uppercase text-white/70">SISA HQ</span>
              <span class="text-xs text-white/80">Global Agency & Settlement Admin</span>
            </div>
          </div>
          <div class="text-[11px] text-white/70">
            대행사 신청 URL:
            <span class="font-mono bg-white/10 px-2 py-1 rounded-full border border-white/20">
              https://worldsisa.com/agency-register.html
            </span>
          </div>
        </div>
      </header>
      <main class="flex-grow pt-20 pb-10 px-3 sm:px-4">
        <div class="max-w-6xl mx-auto space-y-8">
          {% if message %}
          <div class="bg-emerald-500/10 border border-emerald-400/40 text-emerald-100 text-sm px-4 py-3 rounded-xl">
            {{ message }}
          </div>
          {% endif %}

          <!-- 1. 대행사 신청 현황 -->
          <section class="glass-card rounded-2xl border border-white/20 shadow-xl p-5">
            <div class="flex items-center justify-between mb-3">
              <h2 class="text-lg font-semibold flex items-center gap-2">
                <i class="fa-solid fa-file-pen text-brand-accent"></i> 대행사 신청 현황
              </h2>
              <p class="text-[11px] text-white/60">신청서 양식과 동일한 정보가 리스트로 표시됩니다.</p>
            </div>
            {% if applications %}
            <div class="overflow-x-auto">
              <table class="min-w-full text-sm border-separate border-spacing-y-2">
                <thead class="text-xs text-white/70">
                  <tr>
                    <th class="px-3 py-1 text-left">신청일</th>
                    <th class="px-3 py-1 text-left">업체명</th>
                    <th class="px-3 py-1 text-left">도메인(영문)</th>
                    <th class="px-3 py-1 text-left">전화번호</th>
                    <th class="px-3 py-1 text-left">은행/계좌</th>
                    <th class="px-3 py-1 text-left">이메일/구글시트</th>
                    <th class="px-3 py-1 text-left">아이디</th>
                    <th class="px-3 py-1 text-left">비밀번호</th>
                    <th class="px-3 py-1 text-center">상태</th>
                    <th class="px-3 py-1 text-center">승인 및 생성</th>
                  </tr>
                </thead>
                <tbody>
                  {% for a in applications %}
                  <tr class="bg-black/20 hover:bg-black/30 transition">
                    <td class="px-3 py-2 text-[11px] text-white/70">{{ a.created_at or '' }}</td>
                    <td class="px-3 py-2 font-semibold">{{ a.company_name }}</td>
                    <td class="px-3 py-2 text-[11px] text-white/80">{{ a.domain }}</td>
                    <td class="px-3 py-2 text-[11px] text-white/80">{{ a.phone }}</td>
                    <td class="px-3 py-2 text-[11px] text-white/80">{{ a.bank_name }} / {{ a.account_number }}</td>
                    <td class="px-3 py-2 text-[11px] text-white/70 max-w-[160px] truncate">{{ a.email_or_sheet }}</td>
                    <td class="px-3 py-2 text-[11px] font-mono text-blue-200">{{ a.login_id }}</td>
                    <td class="px-3 py-2 text-[11px] text-white/60">••••••</td>
                    <td class="px-3 py-2 text-center text-[11px]">
                      {% if a.status == 'approved' %}
                        <span class="px-2 py-1 rounded-full bg-emerald-500/20 text-emerald-200 border border-emerald-500/40 text-[10px]">승인됨</span>
                      {% else %}
                        <span class="px-2 py-1 rounded-full bg-yellow-500/20 text-yellow-200 border border-yellow-500/40 text-[10px]">대기</span>
                      {% endif %}
                    </td>
                    <td class="px-3 py-2 text-center">
                      {% if a.status != 'approved' %}
                      <form method="post" action="{{ url_for('hq_admin') }}">
                        <input type="hidden" name="action" value="approve_application" />
                        <input type="hidden" name="application_id" value="{{ a.id }}" />
                        <button type="submit" class="px-3 py-1 rounded-full bg-brand-accent text-brand-blue text-[11px] font-semibold hover:bg-white transition">
                          승인 및 생성
                        </button>
                      </form>
                      {% else %}
                        <span class="text-[10px] text-white/40">생성 완료</span>
                      {% endif %}
                    </td>
                  </tr>
                  {% endfor %}
                </tbody>
              </table>
            </div>
            {% else %}
              <p class="text-xs text-white/60">접수된 대행사 신청이 아직 없습니다.</p>
            {% endif %}
          </section>

          <!-- 2. 전체 거래 내역 (요약 구조만 준비) -->
          <section class="glass-card rounded-2xl border border-white/20 shadow-xl p-5">
            <div class="flex items-center justify-between mb-3">
              <h2 class="text-lg font-semibold flex items-center gap-2">
                <i class="fa-solid fa-list-ul text-brand-accent"></i> 전체 거래 내역 (요약)
              </h2>
              <p class="text-[11px] text-white/60">시간순으로 성사된 주문 결제가 여기에 누적됩니다.</p>
            </div>
            {% if transactions %}
              <!-- 이후 결제 시스템과 연동 시 채워질 영역 -->
              <p class="text-xs text-white/60">거래 데이터 연동 후 상세 리스트가 표시됩니다.</p>
            {% else %}
              <p class="text-xs text-white/60">아직 집계된 거래 내역이 없습니다.</p>
            {% endif %}
          </section>

          <!-- 3. 대행사 목록 (기본 정보 및 추후 정산용) -->
          <section class="glass-card rounded-2xl border border-white/20 shadow-xl p-5">
            <div class="flex items-center justify-between mb-3">
              <h2 class="text-lg font-semibold flex items-center gap-2">
                <i class="fa-solid fa-building text-brand-accent"></i> 등록된 대행사 목록
              </h2>
              <p class="text-[11px] text-white/60">수수료 % 및 정산 시스템은 이후 이 리스트를 기반으로 확장됩니다.</p>
            </div>
            {% if agencies %}
            <div class="overflow-x-auto">
              <table class="min-w-full text-sm border-separate border-spacing-y-2">
                <thead class="text-xs text-white/70">
                  <tr>
                    <th class="px-3 py-1 text-left">업체명</th>
                    <th class="px-3 py-1 text-left">도메인</th>
                    <th class="px-3 py-1 text-left">아이디</th>
                    <th class="px-3 py-1 text-center">수수료%</th>
                    <th class="px-3 py-1 text-center">상태</th>
                  </tr>
                </thead>
                <tbody>
                  {% for ag in agencies %}
                  <tr class="bg-black/20 hover:bg-black/30 transition">
                    <td class="px-3 py-2 font-semibold">{{ ag.company_name }}</td>
                    <td class="px-3 py-2 text-[11px] text-white/80">{{ ag.domain }}</td>
                    <td class="px-3 py-2 text-[11px] font-mono text-blue-200">{{ ag.login_id }}</td>
                    <td class="px-3 py-2 text-center text-[11px] text-white/80">{{ ag.fee_percent }}%</td>
                    <td class="px-3 py-2 text-center text-[11px]">
                      {% if ag.status == 'active' %}
                        <span class="px-2 py-1 rounded-full bg-emerald-500/20 text-emerald-200 border border-emerald-500/40 text-[10px]">활성</span>
                      {% else %}
                        <span class="px-2 py-1 rounded-full bg-gray-500/20 text-gray-200 border border-gray-500/40 text-[10px]">중지</span>
                      {% endif %}
                    </td>
                  </tr>
                  {% endfor %}
                </tbody>
              </table>
            </div>
            {% else %}
              <p class="text-xs text-white/60">아직 승인된 대행사가 없습니다.</p>
            {% endif %}
          </section>
        </div>
      </main>
    </body>
    </html>
    """
    return render_template_string(
        template,
        applications=applications,
        agencies=agencies,
        transactions=transactions,
        message=message,
    )

if __name__ == "__main__":
    # 개발용/배포용 서버 실행 (Railway 등)
    port = int(os.environ.get("PORT", "5000"))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)

