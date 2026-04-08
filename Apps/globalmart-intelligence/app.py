# GlobalMart Data Intelligence Platform — Streamlit App
# Production version: IBM Plex Sans/Mono fonts, Genie Space API for NL queries

import streamlit as st
import pandas as pd
import requests
import time
from databricks import sql
from databricks.sdk import WorkspaceClient

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG — update GENIE_SPACE_ID with your actual Genie Space ID
# ─────────────────────────────────────────────────────────────────────────────
DATABRICKS_HOST  = "https://dbc-6a69ae44-27a9.cloud.databricks.com"
DATABRICKS_TOKEN = "dapica8c7f394cf5c6e3f442a8e3250c9e1e"
WAREHOUSE_PATH   = "/sql/1.0/warehouses/d282a86664f1647d"
GENIE_SPACE_ID   = "01f1293ebc4a14ee9d4d151d5e7272d8"   

DASH_UC1 = "https://dbc-6a69ae44-27a9.cloud.databricks.com/dashboardsv3/01f1294001bc185f8d1d4c51ec158dd6/published?o=7474648625671937"
DASH_UC2 = "https://dbc-6a69ae44-27a9.cloud.databricks.com/dashboardsv3/01f1296aa7291feaab0abe745cc2ae88/published?o=7474648625671937"
DASH_UC3 = "https://dbc-6a69ae44-27a9.cloud.databricks.com/dashboardsv3/01f129412aa01e6cabf57e87ae2bcab4/published?o=7474648625671937"
DASH_UC4 = "https://dbc-6a69ae44-27a9.cloud.databricks.com/dashboardsv3/01f1296d184c1f3db178f7f4a2dc8b71/published?o=7474648625671937"

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG — must be the first Streamlit call
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="GlobalMart Intelligence",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─────────────────────────────────────────────────────────────────────────────
# NULL-SAFE HELPERS
# FIX 1: SQL SUM(CASE WHEN ...) returns NULL (not 0) when no rows match.
# r.get('key', 0) only uses the default when the key is absent — if the key
# exists but holds None, it returns None, and int(None) / float(None) crash.
# _i() and _f() handle None, empty strings, and wrong types gracefully.
# ─────────────────────────────────────────────────────────────────────────────

def _i(val, default: int = 0) -> int:
    """Safely convert val to int, returning default on None / empty / error."""
    if val is None:
        return default
    try:
        return int(val)
    except (TypeError, ValueError):
        return default

def _f(val, default: float = 0.0) -> float:
    """Safely convert val to float, returning default on None / empty / error."""
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


# ─────────────────────────────────────────────────────────────────────────────
# CSS — IBM Plex Sans + IBM Plex Mono
# FIX 2: Sliding tab nav styles added (.nav-tab, .nav-tab-active)
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@300;400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap');

/* ── VARIABLES ─────────────────────────────────────────────────────────── */
:root {
    --bg:     #0a0c10;
    --surf:   #121519;
    --surf2:  #181c22;
    --bdr:    #21262f;
    --txt:    #e2e8f0;
    --muted:  #64748b;
    --blue:   #3b82f6;
    --orange: #f97316;
    --green:  #22c55e;
    --red:    #ef4444;
    --yellow: #eab308;
    --purple: #8b5cf6;
    --font:   'IBM Plex Sans', sans-serif;
    --mono:   'IBM Plex Mono', monospace;
}

/* ── BASE ──────────────────────────────────────────────────────────────── */
html, body,
[data-testid="stAppViewContainer"],
[data-testid="stMain"] {
    background-color: var(--bg) !important;
    color: var(--txt);
    font-family: var(--font);
    font-size: 14px;
    -webkit-font-smoothing: antialiased;
}
[data-testid="stSidebar"] {
    background-color: var(--surf) !important;
    border-right: 1px solid var(--bdr);
}
[data-testid="stSidebar"] * { color: var(--txt) !important; }

h1, h2, h3, h4 {
    font-family: var(--font) !important;
    font-weight: 700 !important;
    color: var(--txt) !important;
    letter-spacing: -0.3px;
}

/* ── SLIDING TAB NAVIGATION (FIX 2) ───────────────────────────────────── */
/* Wrapper resets Streamlit's default button padding/margin */
.nav-tab > button,
.nav-tab-active > button {
    width: 100% !important;
    text-align: left !important;
    background: transparent !important;
    border: none !important;
    border-left: 3px solid transparent !important;
    border-radius: 0 6px 6px 0 !important;
    padding: 9px 14px 9px 14px !important;
    font-family: var(--font) !important;
    font-size: 13px !important;
    font-weight: 400 !important;
    color: var(--muted) !important;
    cursor: pointer !important;
    transition: background .12s, color .12s !important;
    margin-bottom: 2px !important;
}
.nav-tab > button:hover {
    background: rgba(255,255,255,.04) !important;
    color: var(--txt) !important;
}
/* Active tab: blue left border + tinted background + bold text */
.nav-tab-active > button {
    border-left: 3px solid var(--blue) !important;
    background: rgba(59,130,246,.10) !important;
    color: var(--txt) !important;
    font-weight: 600 !important;
}

/* ── METRIC CARDS ──────────────────────────────────────────────────────── */
[data-testid="stMetric"] {
    background: var(--surf) !important;
    border: 1px solid var(--bdr) !important;
    border-radius: 8px !important;
    padding: 16px 14px 14px 16px !important;
    box-sizing: border-box !important;
    min-width: 0 !important;
    overflow: hidden !important;
}
[data-testid="stMetricLabel"] > div,
[data-testid="stMetricLabel"] p {
    font-family: var(--font) !important;
    font-size: 10px !important;
    font-weight: 600 !important;
    letter-spacing: 1.2px !important;
    text-transform: uppercase !important;
    color: var(--muted) !important;
    white-space: nowrap !important;
    overflow: hidden !important;
    text-overflow: ellipsis !important;
    margin: 0 0 6px 0 !important;
}
[data-testid="stMetricValue"] {
    background: transparent !important;
    overflow: hidden !important;
}
[data-testid="stMetricValue"] > div {
    font-family: var(--font) !important;
    font-size: 22px !important;
    font-weight: 700 !important;
    color: var(--txt) !important;
    line-height: 1.15 !important;
    white-space: nowrap !important;
    overflow: hidden !important;
    text-overflow: ellipsis !important;
    letter-spacing: -0.5px !important;
    display: block !important;
    width: 100% !important;
}
[data-testid="stMetricDelta"] { display: none !important; }

/* ── SECTION DIVIDER ───────────────────────────────────────────────────── */
.sdiv {
    font-family: var(--font);
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 1.8px;
    text-transform: uppercase;
    color: var(--muted);
    border-bottom: 1px solid var(--bdr);
    padding-bottom: 8px;
    margin: 32px 0 18px 0;
}

/* ── DASHBOARD BUTTON ──────────────────────────────────────────────────── */
.dbtn { display: inline-block; margin-bottom: 14px; }
.dbtn a {
    display: inline-flex;
    align-items: center;
    gap: 7px;
    background: var(--blue);
    color: #fff !important;
    text-decoration: none !important;
    font-family: var(--font);
    font-weight: 600;
    font-size: 13px;
    padding: 9px 22px;
    border-radius: 6px;
    transition: opacity .15s;
    letter-spacing: 0.1px;
}
.dbtn a:hover { opacity: .85; }

/* ── AI RESULT BOX ─────────────────────────────────────────────────────── */
.aibox {
    background: rgba(59,130,246,.05);
    border: 1px solid rgba(59,130,246,.18);
    border-radius: 8px;
    padding: 18px 22px;
    font-family: var(--font);
    font-size: 14px;
    line-height: 1.85;
    color: var(--txt);
}

/* ── GENIE PANEL ───────────────────────────────────────────────────────── */
.genie-panel {
    background: var(--surf2);
    border: 1px solid var(--bdr);
    border-radius: 12px;
    padding: 26px 28px 22px;
    margin: 4px 0 20px;
    position: relative;
    overflow: hidden;
}
.genie-panel::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, var(--blue), var(--purple), var(--green));
}
.genie-title {
    font-family: var(--font);
    font-size: 20px;
    font-weight: 700;
    color: var(--txt);
    margin-bottom: 6px;
    letter-spacing: -0.3px;
}
.genie-sub {
    font-family: var(--font);
    font-size: 13px;
    color: var(--muted);
    line-height: 1.7;
    margin-bottom: 0;
}
.genie-sub code {
    background: rgba(59,130,246,.1);
    border: 1px solid rgba(59,130,246,.2);
    border-radius: 4px;
    padding: 1px 7px;
    font-family: var(--mono);
    font-size: 11px;
    color: var(--blue);
}
.genie-badge {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    background: rgba(34,197,94,.08);
    border: 1px solid rgba(34,197,94,.2);
    border-radius: 20px;
    padding: 3px 10px;
    font-family: var(--font);
    font-size: 11px;
    font-weight: 600;
    color: var(--green);
    margin-top: 12px;
    letter-spacing: 0.3px;
}
.genie-badge .dot {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: var(--green);
}

/* ── GENIE RESULT ──────────────────────────────────────────────────────── */
.genie-answer {
    background: var(--surf);
    border: 1px solid var(--bdr);
    border-radius: 8px;
    overflow: hidden;
    margin-top: 16px;
}
.genie-answer-header {
    background: rgba(255,255,255,.03);
    border-bottom: 1px solid var(--bdr);
    padding: 8px 16px;
    font-family: var(--font);
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 1.5px;
    text-transform: uppercase;
    color: var(--muted);
    display: flex;
    align-items: center;
    gap: 8px;
}
.genie-answer-header .dot-g {
    width: 7px; height: 7px;
    border-radius: 50%;
    background: var(--green);
    flex-shrink: 0;
}
.genie-answer-body {
    padding: 16px 20px;
    font-family: var(--font);
    font-size: 14px;
    line-height: 1.8;
    color: var(--txt);
}
.genie-sql {
    padding: 14px 18px;
    font-family: var(--mono);
    font-size: 12px;
    color: #93c5fd;
    white-space: pre-wrap;
    word-break: break-all;
    line-height: 1.6;
    background: #070911;
    border-top: 1px solid var(--bdr);
}

/* ── CARDS ─────────────────────────────────────────────────────────────── */
.prob {
    border-radius: 0 8px 8px 0;
    padding: 14px 18px;
    font-family: var(--font);
}
.prob .pt { font-weight: 600; font-size: 14px; margin-bottom: 5px; }
.prob .ps { font-size: 12px; color: var(--muted); line-height: 1.6; }

.arch-box {
    background: var(--surf);
    border: 1px solid var(--bdr);
    border-radius: 8px;
    padding: 12px 8px;
    text-align: center;
    font-family: var(--font);
}
.arch-box .an { font-size: 14px; font-weight: 700; }
.arch-box .ad { font-size: 10px; color: var(--muted); margin-top: 4px; line-height: 1.5; }

.uccard {
    background: var(--surf);
    border: 1px solid var(--bdr);
    border-radius: 10px;
    padding: 20px 18px;
    box-sizing: border-box;
    font-family: var(--font);
}
.uccard .ucn {
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 1.5px;
    text-transform: uppercase;
    margin-bottom: 6px;
}
.uccard .uct { font-size: 15px; font-weight: 700; margin-bottom: 8px; }
.uccard .ucd { font-size: 12px; color: var(--muted); line-height: 1.65; margin-bottom: 12px; }

/* ── MISC ──────────────────────────────────────────────────────────────── */
[data-testid="stDataFrame"] { border-radius: 8px; overflow: hidden; }
[data-testid="stTextInput"] > div > div {
    background: var(--surf) !important;
    border: 1px solid var(--bdr) !important;
    border-radius: 6px !important;
    color: var(--txt) !important;
    font-family: var(--font) !important;
    font-size: 14px !important;
}
[data-testid="stTabs"] [role="tab"] {
    color: var(--muted) !important;
    font-family: var(--font) !important;
    font-size: 14px !important;
}
[data-testid="stTabs"] [role="tab"][aria-selected="true"] {
    color: var(--txt) !important;
    font-weight: 600 !important;
}
[data-testid="stButton"] > button[kind="primary"] {
    background: var(--blue) !important;
    border: none !important;
    border-radius: 6px !important;
    font-family: var(--font) !important;
    font-weight: 600 !important;
    font-size: 14px !important;
    color: #fff !important;
    padding: 10px 24px !important;
    letter-spacing: 0.1px !important;
}
[data-testid="stButton"] > button[kind="primary"]:hover { opacity: .88 !important; }
[data-testid="stSelectbox"] > div > div {
    background: var(--surf) !important;
    border-color: var(--bdr) !important;
    color: var(--txt) !important;
    font-family: var(--font) !important;
}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# CONNECTIONS
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_resource(show_spinner=False)
def _conn():
    return sql.connect(
        server_hostname="dbc-2d78c719-5ef2.cloud.databricks.com",
        http_path=WAREHOUSE_PATH,
        access_token=DATABRICKS_TOKEN
    )

@st.cache_resource(show_spinner=False)
def _ws():
    """Databricks WorkspaceClient — used for Genie API calls."""
    return WorkspaceClient(
        host=DATABRICKS_HOST
    )

@st.cache_data(ttl=120, show_spinner=False)
def q(query: str) -> pd.DataFrame:
    try:
        with _conn().cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        return pd.DataFrame({"_error": [str(e)]})

def ok(df: pd.DataFrame) -> bool:
    return "_error" not in df.columns and len(df) > 0


# ─────────────────────────────────────────────────────────────────────────────
# GENIE SPACE QUERY
# ─────────────────────────────────────────────────────────────────────────────

def genie_ask(question: str) -> dict:
    result = {"answer_text": "", "sql": None, "rows": None, "columns": None, "error": None}
    
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    base = DATABRICKS_HOST.rstrip("/")

    try:
        # 1. Start conversation
        r = requests.post(
            f"{base}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/start-conversation",
            headers=headers,
            json={"content": question}
        )
        r.raise_for_status()
        data = r.json()
        conversation_id = data["conversation"]["id"]
        message_id      = data["message"]["id"]

        # 2. Poll until COMPLETED or FAILED (max 3 min)
        for _ in range(36):
            time.sleep(5)
            poll = requests.get(
                f"{base}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{conversation_id}/messages/{message_id}",
                headers=headers
            )
            poll.raise_for_status()
            msg = poll.json()
            status = msg.get("status")
            if status == "COMPLETED":
                break
            if status in ("FAILED", "CANCELLED"):
                result["error"] = f"Genie message status: {status} — {msg.get('error','no detail')}"
                return result

        # 3. Parse attachments
        for att in (msg.get("attachments") or []):
            if att.get("text"):
                result["answer_text"] = att["text"].get("content", "")
            if att.get("query"):
                result["sql"] = att["query"].get("query", "")
                att_id = att.get("attachment_id") or att.get("id")
                if att_id:
                    qr = requests.get(
                        f"{base}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{conversation_id}"
                        f"/messages/{message_id}/attachments/{att_id}/query-result",
                        headers=headers
                    )
                    if qr.ok:
                        qdata = qr.json()
                        try:
                            cols = [c["name"] for c in qdata["statement_response"]["manifest"]["schema"]["columns"]]
                            rows = qdata["statement_response"]["result"]["data_array"]
                            result["columns"] = cols
                            result["rows"]    = rows
                        except (KeyError, TypeError):
                            pass

    except Exception as e:
        result["error"] = str(e)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# UI HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def kcards(pairs):
    cols = st.columns(len(pairs), gap="small")
    for col, (v, l) in zip(cols, pairs):
        with col:
            st.metric(label=l, value=str(v))

def dash_button(url: str, label: str):
    st.markdown(
        f'<div class="dbtn"><a href="{url}" target="_blank">&#8599;&nbsp;{label}</a></div>',
        unsafe_allow_html=True)

def section(title: str):
    st.markdown(f'<div class="sdiv">{title}</div>', unsafe_allow_html=True)

def page_header(tag: str, color: str, title: str, desc: str):
    st.markdown(f"""
    <div style='padding:26px 0 8px 0;'>
        <div style='font-family:var(--font,IBM Plex Sans,sans-serif);font-size:10px;
                    font-weight:600;letter-spacing:2px;text-transform:uppercase;
                    color:{color};margin-bottom:10px;'>{tag}</div>
        <div style='font-family:var(--font,IBM Plex Sans,sans-serif);font-size:30px;
                    font-weight:700;letter-spacing:-0.5px;color:#e2e8f0;'>{title}</div>
        <div style='font-family:var(--font,IBM Plex Sans,sans-serif);font-size:13px;
                    color:#64748b;margin-top:10px;max-width:620px;line-height:1.75;'>{desc}</div>
    </div>""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR — FIX 2: Sliding tab nav replaces st.radio
# Each nav item is a st.button wrapped in a CSS div that styles active state.
# Active page stored in st.session_state["page"]; clicking reruns the app.
# ─────────────────────────────────────────────────────────────────────────────

NAV_ITEMS = [
    ("🏠", "Home"),
    ("🔍", "UC1 — Data Quality"),
    ("🚨", "UC2 — Fraud Investigator"),
    ("📦", "UC3 — Product Intelligence"),
    ("📊", "UC4 — Executive BI & Leads"),
]

if "page" not in st.session_state:
    st.session_state["page"] = "Home"

with st.sidebar:
    st.markdown("""
    <div style='padding:14px 0 22px 0;font-family:IBM Plex Sans,sans-serif;'>
        <div style='font-size:18px;font-weight:700;color:#e2e8f0;letter-spacing:-0.3px;'>
            GlobalMart</div>
        <div style='font-size:10px;font-weight:600;letter-spacing:2px;text-transform:uppercase;
                    color:#64748b;margin-top:3px;'>Data Intelligence Platform</div>
    </div>""", unsafe_allow_html=True)

    st.markdown("<div style='margin-bottom:6px;'></div>", unsafe_allow_html=True)

    for icon, label in NAV_ITEMS:
        is_active = st.session_state["page"] == label
        css_class = "nav-tab-active" if is_active else "nav-tab"
        # Wrap the button in a div with the appropriate CSS class
        st.markdown(f"<div class='{css_class}'>", unsafe_allow_html=True)
        if st.button(f"{icon}  {label}", key=f"nav_{label}", use_container_width=True):
            st.session_state["page"] = label
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("""
    <div style='padding-top:28px;border-top:1px solid #21262f;font-family:IBM Plex Sans,sans-serif;
                font-size:10px;color:#64748b;line-height:2.1;margin-top:28px;'>
        Databricks Free Edition<br>
        Delta Lake · Unity Catalog<br>
        Genie Space NL Query<br>
        Raw → Bronze → Silver → MDM → Gold 
    </div>""", unsafe_allow_html=True)

# Read active page from session state
page = st.session_state["page"]


# ═══════════════════════════════════════════════════════════════════════════
# HOME
# ═══════════════════════════════════════════════════════════════════════════
if page == "Home":

    st.markdown("""
    <div style='padding:36px 0 4px 0;font-family:IBM Plex Sans,sans-serif;'>
        <div style='font-size:10px;font-weight:600;letter-spacing:2.5px;text-transform:uppercase;
                    color:#3b82f6;margin-bottom:14px;'>Databricks GenAI Hackathon 2026</div>
        <div style='font-size:42px;font-weight:700;letter-spacing:-1px;
                    line-height:1.1;color:#e2e8f0;'>GlobalMart<br>Data Intelligence</div>
        <div style='font-size:14px;color:#64748b;margin-top:14px;
                    max-width:520px;line-height:1.8;'>
            Six disconnected regional systems unified into one intelligent platform.
            Three core business failures resolved. Four GenAI use cases in production.
            Built entirely on Databricks Free Edition.
        </div>
    </div>""", unsafe_allow_html=True)

    section("Three Business Problems Solved")
    c1, c2, c3 = st.columns(3, gap="medium")
    for col, color, title, stat in [
        (c1, "#3b82f6", "Revenue Audit Failure",
         "~9% revenue overstatement · 3-day manual reconciliation · 30% error rate"),
        (c2, "#ef4444", "Returns Fraud Exposure",
         "$2.3M annual loss · No unified return history · No automated detection"),
        (c3, "#22c55e", "Inventory Blindspot",
         "12–18% revenue loss · Unsold in one region, sold out in another"),
    ]:
        with col:
            st.markdown(
                f'<div class="prob" style="background:#121519;border-left:3px solid {color};">'
                f'<div class="pt">{title}</div><div class="ps">{stat}</div></div>',
                unsafe_allow_html=True)

    section("Medallion Architecture")
    ac = st.columns([4,1,4,1,4,1,4,1,4])
    for col, (name, desc) in zip([ac[i] for i in [0,2,4,6,8]], [
        ("Raw","Raw data files"),
        ("Bronze",  "Auto Loader\nSchema evolution"),
        ("Silver",  "LLM mapping\nDQ · quarantine"),
        ("MDM",     "Golden records\nDedup · survivorship"),
        ("Gold",    "Star schema\nMaterialized Views"),
 
    ]):
        with col:
            st.markdown(
                f'<div class="arch-box"><div class="an">{name}</div>'
                f'<div class="ad">{desc}</div></div>',
                unsafe_allow_html=True)
    for col in [ac[i] for i in [1,3,5,7]]:
        with col:
            st.markdown(
                "<div style='text-align:center;padding-top:14px;"
                "color:#3b82f6;font-size:16px;'>→</div>",
                unsafe_allow_html=True)

    section("Four GenAI Use Cases — Click to Open Dashboard")
    u1, u2, u3, u4 = st.columns(4, gap="medium")
    for col, color, uc, title, desc, url, btn in [
        (u1, "#3b82f6", "UC1", "AI Data Quality Reporter",
         "Silver quarantine → plain-English audit explanations. Anomaly detection on rejection spikes.",
         DASH_UC1, "Open UC1 Dashboard"),
        (u2, "#f97316", "UC2", "Fraud Investigator",
         "8 rules score every customer 0–100. LLM writes investigation briefs for flagged cases.",
         DASH_UC2, "Open UC2 Dashboard"),
        (u3, "#22c55e", "UC3", "Product Intelligence",
         "1,774 products in FAISS. Local embeddings. Grounded catalog Q&A. Genie Space.",
         DASH_UC3, "Open UC3 Dashboard"),
        (u4, "#8b5cf6", "UC4", "Executive BI + Leads",
         "Root-cause synthesis on 3 domains. AI Lead Scoring + SHAP via ai_query() SQL.",
         DASH_UC4, "Open UC4 Dashboard"),
    ]:
        with col:
            st.markdown(
                f'<div class="uccard"><div class="ucn" style="color:{color};">{uc}</div>'
                f'<div class="uct">{title}</div><div class="ucd">{desc}</div></div>',
                unsafe_allow_html=True)
            st.markdown(
                f'<div class="dbtn" style="margin-top:8px;">'
                f'<a href="{url}" target="_blank" style="background:{color};">'
                f'&#8599;&nbsp;{btn}</a></div>',
                unsafe_allow_html=True)

    section("Pipeline Health")
    run_df = q("""SELECT notebook, status, records_processed,
                         llm_calls_made, outputs_flagged, run_timestamp
                  FROM globalmart.gold.pipeline_run_log
                  ORDER BY run_timestamp DESC LIMIT 8""")
    if ok(run_df):
        latest = run_df.iloc[0]
        kcards([
            (latest.get("status", "—"),                               "Last Run Status"),
            (f"{_i(latest.get('records_processed')):,}",              "Records Processed"),
            (f"{_i(latest.get('llm_calls_made')):,}",                 "LLM Calls Made"),
            (f"{_i(latest.get('outputs_flagged')):,}",                "Outputs Flagged"),
        ])
        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
        st.dataframe(run_df, use_container_width=True, hide_index=True)
    else:
        st.info("Run any UC notebook to see pipeline health here.")

    section("Data at a Glance")
    stats = q("""SELECT
        (SELECT COUNT(DISTINCT customer_id) FROM globalmart.gold.dim_customers) AS customers,
        (SELECT COUNT(*)                    FROM globalmart.gold.fact_orders)    AS orders,
        (SELECT COUNT(*)                    FROM globalmart.gold.fact_returns)   AS returns,
        (SELECT COUNT(DISTINCT product_id)  FROM globalmart.gold.dim_products)  AS products,
        (SELECT COUNT(DISTINCT vendor_id)   FROM globalmart.gold.dim_vendors)   AS vendors,
        (SELECT COUNT(*)                    FROM globalmart.gold.fact_sales)     AS transactions""")
    if ok(stats):
        r = stats.iloc[0]
        kcards([
            (f"{_i(r.get('customers')):,}",    "Customers"),
            (f"{_i(r.get('orders')):,}",       "Orders"),
            (f"{_i(r.get('returns')):,}",      "Returns"),
            (f"{_i(r.get('products')):,}",     "Products"),
            (f"{_i(r.get('vendors')):,}",      "Vendors"),
            (f"{_i(r.get('transactions')):,}", "Transactions"),
        ])

    # ── GENIE SPACE — Natural Language Query ─────────────────────────────────
    section("Ask the Data — Powered by Genie Space")

    st.markdown("""
    <div class="genie-panel">
        <div class="genie-title">Ask anything about GlobalMart data</div>
        <div class="genie-sub">
            Powered by <strong style="color:#e2e8f0;">Databricks Genie Space</strong> —
            a native AI layer trained on your Gold schema. Ask in plain English.
            Genie generates the SQL, executes it on <code>globalmart.gold.*</code>,
            and returns the answer with the data.
        </div>
        <div class="genie-badge">
            <span class="dot"></span> Live · Genie Space connected
        </div>
    </div>""", unsafe_allow_html=True)

    EXAMPLES = [
        "Which region has the highest total sales?",
        "Which vendor has the highest return rate?",
        "How many customers are flagged for fraud this run?",
        "Top 10 slow-moving products by days since last sale?",
        "How many HOT lead customers are in the West region?",
        "Total refund amount grouped by return reason?",
        "Which customer segment generates the most profit?",
        "Show me the 5 customers with the highest anomaly score",
    ]

    if "genie_q" not in st.session_state:
        st.session_state["genie_q"] = ""
    if "genie_last_pick" not in st.session_state:
        st.session_state["genie_last_pick"] = ""

    st.markdown("""<div style='font-family:IBM Plex Sans,sans-serif;font-size:10px;
        font-weight:600;letter-spacing:1.5px;text-transform:uppercase;
        color:#64748b;margin:14px 0 7px;'>Try an example</div>""",
        unsafe_allow_html=True)

    picked = st.selectbox(
        "ex",
        ["— select an example question —"] + EXAMPLES,
        label_visibility="collapsed",
        key="genie_ex_pick"
    )
    if (picked
            and picked != "— select an example question —"
            and picked != st.session_state["genie_last_pick"]):
        st.session_state["genie_q"]         = picked
        st.session_state["genie_last_pick"] = picked

    st.markdown("""<div style='font-family:IBM Plex Sans,sans-serif;font-size:10px;
        font-weight:600;letter-spacing:1.5px;text-transform:uppercase;
        color:#64748b;margin:14px 0 7px;'>Or type your own question</div>""",
        unsafe_allow_html=True)

    user_q = st.text_input(
        "question",
        value=st.session_state.get("genie_q", ""),
        placeholder="e.g.  Which region has the highest revenue this year?",
        label_visibility="collapsed",
        key="genie_input"
    )
    if user_q != st.session_state["genie_q"]:
        st.session_state["genie_q"]         = user_q
        st.session_state["genie_last_pick"] = ""

    run = st.button("Ask Genie  →", type="primary", use_container_width=True)

    if run and user_q.strip():
        with st.spinner("Genie is thinking..."):
            result = genie_ask(user_q.strip())

        if result["answer_text"]:
            st.markdown(f"""
            <div class="genie-answer">
                <div class="genie-answer-header">
                    <span class="dot-g"></span> Genie Answer
                </div>
                <div class="genie-answer-body">{result["answer_text"]}</div>
            </div>""", unsafe_allow_html=True)

        if result["sql"]:
            st.markdown(f"""
            <div class="genie-answer" style="margin-top:10px;">
                <div class="genie-answer-header">
                    <span class="dot-g"></span> Generated SQL
                </div>
                <div class="genie-sql">{result["sql"]}</div>
            </div>""", unsafe_allow_html=True)

        if result["rows"] is not None and result["columns"]:
            df_result = pd.DataFrame(result["rows"], columns=result["columns"])
            st.markdown(
                f"<div style='margin:14px 0 6px;font-family:IBM Plex Sans,sans-serif;"
                f"font-size:10px;font-weight:600;letter-spacing:1.5px;"
                f"text-transform:uppercase;color:#64748b;'>"
                f"Query Result — {len(df_result):,} row{'s' if len(df_result)!=1 else ''}"
                f"</div>",
                unsafe_allow_html=True)
            st.dataframe(df_result, use_container_width=True, hide_index=True)

        if result["error"] and not result["answer_text"] and not result["sql"]:
            st.error(f"Genie error: {result['error']}")
            st.caption(
                "Check that GENIE_SPACE_ID is set correctly at the top of the file "
                "and that the Genie Space is configured for the globalmart.gold schema."
            )

    elif run:
        st.warning("Type a question or select an example first.")


# ═══════════════════════════════════════════════════════════════════════════
# UC1 — DATA QUALITY
# ═══════════════════════════════════════════════════════════════════════════
elif page == "UC1 — Data Quality":
    page_header("Use Case 1 · AI Data Quality Reporter", "#3b82f6",
        "AI Data Quality Reporter",
        "Silver quarantine records grouped by rule and entity. One LLM call per group "
        "generates plain-English audit explanations for the finance team. "
        "Anomaly detection flags entities whose rejection volume spiked vs the last run.")
    dash_button(DASH_UC1, "Open UC1 Full Dashboard in Databricks")

    kpi = q("""SELECT COUNT(DISTINCT entity) AS entities,
                      SUM(rejected_count) AS total_rejected,
                      SUM(CASE WHEN severity='CRITICAL' THEN rejected_count END) AS critical,
                      SUM(CASE WHEN severity='HIGH'     THEN rejected_count END) AS high,
                      SUM(CASE WHEN llm_check='PASS'    THEN 1 ELSE 0 END)       AS llm_passed
               FROM globalmart.gold.dq_audit_report""")
    if ok(kpi):
        r = kpi.iloc[0]
        kcards([
            (_i(r.get("entities")),                  "Entities Audited"),
            (f"{_i(r.get('total_rejected')):,}",     "Total Rejected"),
            (f"{_i(r.get('critical')):,}",           "CRITICAL Issues"),
            (f"{_i(r.get('high')):,}",               "HIGH Issues"),
            (f"{_i(r.get('llm_passed')):,}",         "LLM Quality PASS"),
        ])

    section("Rejected Records by Entity")
    chart = q("SELECT entity, SUM(rejected_count) AS cnt FROM globalmart.gold.dq_audit_report GROUP BY entity ORDER BY cnt DESC")
    if ok(chart): st.bar_chart(chart.set_index("entity")["cnt"])

    section("Latest Audit Report — Top Issues")
    audit = q("""SELECT entity, field_affected, issue_type, rejected_count,
                        severity, ai_explanation, llm_check, generated_at
                 FROM globalmart.gold.dq_audit_report
                 ORDER BY CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 ELSE 3 END,
                          rejected_count DESC LIMIT 30""")
    if ok(audit):
        def sc(v):
            c = {"CRITICAL":"#ef4444","HIGH":"#f97316","MEDIUM":"#eab308"}.get(v,"#64748b")
            return f"color:{c};font-weight:600;"
        st.dataframe(audit.style.applymap(sc, subset=["severity"]),
                     use_container_width=True, hide_index=True)
        st.markdown("**Read AI explanation**")
        opts = audit.apply(
            lambda r: f"{r['entity']} — {r['field_affected']} ({r['severity']})", axis=1
        ).tolist()
        sel = st.selectbox("Issue", opts, label_visibility="collapsed")
        if sel:
            st.markdown(
                f'<div class="aibox">{audit.iloc[opts.index(sel)]["ai_explanation"]}</div>',
                unsafe_allow_html=True)
    else:
        st.info("No audit data yet — run the UC1 notebook first.")

    section("Anomaly Alerts")
    anom = q("""SELECT entity, previous_count, current_count, delta_pct,
                       severity_tag, ai_alert, detected_at
                FROM globalmart.gold.dq_anomaly_alerts
                ORDER BY delta_pct DESC LIMIT 10""")
    if ok(anom):
        st.dataframe(
            anom[["entity","previous_count","current_count","delta_pct","severity_tag","detected_at"]],
            use_container_width=True, hide_index=True)
        for _, row in anom.iterrows():
            with st.expander(f"{row['entity']} — +{row['delta_pct']}% ({row['severity_tag']})"):
                st.markdown(f'<div class="aibox">{row["ai_alert"]}</div>', unsafe_allow_html=True)
    else:
        st.info("No anomalies on the last run.")


# ═══════════════════════════════════════════════════════════════════════════
# UC2 — FRAUD INVESTIGATOR
# ═══════════════════════════════════════════════════════════════════════════
elif page == "UC2 — Fraud Investigator":
    page_header("Use Case 2 · Returns Fraud Investigator", "#f97316",
        "Returns Fraud Investigator",
        "Eight deterministic rules score every customer 0–100. Customers above 40 pts "
        "are flagged. no_matching_order (phantom returns) carries the highest single-rule "
        "signal at 15 pts. The LLM writes the investigation brief.")
    dash_button(DASH_UC2, "Open UC2 Full Dashboard in Databricks")

    kpi = q("""SELECT COUNT(*) AS flagged,
                      ROUND(AVG(anomaly_score),1) AS avg_score,
                      MAX(anomaly_score) AS top_score,
                      ROUND(SUM(total_refund_inflation),2) AS total_inflation,
                      SUM(CASE WHEN anomaly_score>=80 THEN 1 END) AS extreme,
                      SUM(CASE WHEN anomaly_score BETWEEN 65 AND 79 THEN 1 END) AS high_risk
               FROM globalmart.gold.flagged_return_customers
               WHERE scored_at=(SELECT MAX(scored_at) FROM globalmart.gold.flagged_return_customers)""")
    if ok(kpi):
        r = kpi.iloc[0]
        # FIX 1 applied: all SUM(CASE WHEN...) columns use _i() to handle NULL→None
        kcards([
            (f"{_i(r.get('flagged')):,}",                           "Customers Flagged"),
            (str(r.get("avg_score", "—")),                          "Avg Score / 100"),
            (str(r.get("top_score", "—")),                          "Highest Score"),
            (f"${_f(r.get('total_inflation')):,.0f}",               "Refund Inflation ($)"),
            (f"{_i(r.get('extreme')):,}",                           "EXTREME (≥80)"),
            (f"{_i(r.get('high_risk')):,}",                         "HIGH (65–79)"),
        ])

    section("Flagged by Region")
    reg = q("""SELECT
                CASE region WHEN 'W' THEN 'West' WHEN 'S' THEN 'South'
                            WHEN 'E' THEN 'East'  WHEN 'N' THEN 'North' ELSE region END AS region,
                COUNT(*) AS flagged
               FROM globalmart.gold.flagged_return_customers
               WHERE scored_at=(SELECT MAX(scored_at) FROM globalmart.gold.flagged_return_customers)
               GROUP BY region ORDER BY flagged DESC""")
    if ok(reg): st.bar_chart(reg.set_index("region")["flagged"])

    section("Investigation Queue")
    fraud = q("""SELECT customer_id, customer_name, segment,
                        CASE region WHEN 'W' THEN 'West' WHEN 'S' THEN 'South'
                                    WHEN 'E' THEN 'East'  WHEN 'N' THEN 'North'
                                    ELSE region END AS region,
                        total_returns, total_orders,
                        ROUND(order_return_rate*100,1)  AS return_rate_pct,
                        ROUND(total_refund_inflation,2) AS refund_inflation,
                        no_matching_order_count,
                        anomaly_score,
                        CASE WHEN anomaly_score>=80 THEN 'EXTREME'
                             WHEN anomaly_score>=65 THEN 'HIGH'
                             WHEN anomaly_score>=50 THEN 'MODERATE'
                             ELSE 'BORDERLINE' END AS priority,
                        rules_violated, investigation_brief, llm_check
                 FROM globalmart.gold.flagged_return_customers
                 WHERE scored_at=(SELECT MAX(scored_at) FROM globalmart.gold.flagged_return_customers)
                 ORDER BY anomaly_score DESC LIMIT 50""")
    if ok(fraud):
        def sc(v):
            if not isinstance(v, (int, float)): return ""
            if v >= 80: return "color:#ef4444;font-weight:700;"
            if v >= 65: return "color:#f97316;font-weight:600;"
            if v >= 50: return "color:#eab308;"
            return "color:#64748b;"
        disp = [c for c in fraud.columns if c != "investigation_brief"]
        st.dataframe(fraud[disp].style.applymap(sc, subset=["anomaly_score"]),
                     use_container_width=True, hide_index=True)
        section("Investigation Brief")
        sel = st.selectbox("Customer", fraud["customer_name"].tolist(), label_visibility="collapsed")
        if sel:
            row = fraud[fraud["customer_name"]==sel].iloc[0]
            st.markdown(
                f"Score **{row['anomaly_score']}**/100 · "
                f"Priority: **{row['priority']}** · "
                f"LLM: `{row['llm_check']}`")
            st.markdown(f'<div class="aibox">{row["investigation_brief"]}</div>',
                        unsafe_allow_html=True)
    else:
        st.info("No flagged customers yet — run the UC2 notebook first.")


# ═══════════════════════════════════════════════════════════════════════════
# UC3 — PRODUCT INTELLIGENCE
# ═══════════════════════════════════════════════════════════════════════════
elif page == "UC3 — Product Intelligence":
    page_header("Use Case 3 · Product Intelligence Assistant", "#22c55e",
        "Product Intelligence Assistant",
        "1,774 products embedded locally with all-MiniLM-L6-v2. FAISS index persisted "
        "to Databricks Volumes. Grounded catalog Q&A — the LLM cannot hallucinate. "
        "Genie Space for SQL-native NL queries over all Gold tables.")
    dash_button(DASH_UC3, "Open UC3 Full Dashboard in Databricks")

    kpi = q("""SELECT COUNT(*) AS tq,
                      SUM(CASE WHEN llm_check='PASS'   THEN 1 ELSE 0 END) AS p,
                      SUM(CASE WHEN llm_check='REVIEW' THEN 1 ELSE 0 END) AS r
               FROM globalmart.gold.rag_query_history""")
    if ok(kpi):
        r = kpi.iloc[0]
        kcards([
            (f"{_i(r.get('tq')):,}", "Queries Answered"),
            (f"{_i(r.get('p')):,}",  "Quality PASS"),
            (f"{_i(r.get('r')):,}",  "Flagged REVIEW"),
        ])

    section("RAG Query History")
    rag = q("""SELECT question, answer, llm_check, llm_check_detail, queried_at
               FROM globalmart.gold.rag_query_history
               ORDER BY queried_at DESC LIMIT 20""")
    if ok(rag):
        st.dataframe(rag[["question","llm_check","llm_check_detail","queried_at"]],
                     use_container_width=True, hide_index=True)
        section("Read a Full Answer")
        sel = st.selectbox("Question", rag["question"].tolist(), label_visibility="collapsed")
        if sel:
            st.markdown(
                f'<div class="aibox">{rag[rag["question"]==sel].iloc[0]["answer"]}</div>',
                unsafe_allow_html=True)
    else:
        st.info("No query history yet — run the UC3 notebook first.")

    section("Slow-Moving Products — Top 20")
    slow = q("""SELECT product_name, brand, region, total_quantity_sold,
                       ROUND(total_sales,2) AS total_sales,
                       days_since_last_sale, avg_daily_quantity
                FROM globalmart.gold.mv_slow_moving_products
                ORDER BY days_since_last_sale DESC LIMIT 20""")
    if ok(slow): st.dataframe(slow, use_container_width=True, hide_index=True)
    else:        st.info("Slow-moving MV not yet populated.")


# ═══════════════════════════════════════════════════════════════════════════
# UC4 — EXECUTIVE BI + LEAD SCORING
# ═══════════════════════════════════════════════════════════════════════════
elif page == "UC4 — Executive BI & Leads":
    page_header("Use Case 4 · Executive BI + AI Lead Scoring", "#8b5cf6",
        "Executive BI + AI Lead Scoring",
        "KPIs aggregated from Gold MVs — never raw rows. LLM synthesises root-cause "
        "narratives for executives. Lead Scoring: 4-component model, HOT/WARM/COLD "
        "labels, SHAP-style explanations generated via ai_query() inside SQL.")
    dash_button(DASH_UC4, "Open UC4 Full Dashboard in Databricks")

    tab1, tab2 = st.tabs(["Executive Summaries", "AI Lead Scores"])

    with tab1:
        ins = q("""SELECT insight_type, ai_summary, llm_check, generated_at
                   FROM globalmart.gold.ai_business_insights
                   WHERE generated_at=(SELECT MAX(generated_at)
                                       FROM globalmart.gold.ai_business_insights)
                   ORDER BY insight_type""")
        labels = {
            "revenue_performance":   ("Revenue Performance",   "#3b82f6"),
            "vendor_return_rate":    ("Vendor Return Rates",   "#f97316"),
            "slow_moving_inventory": ("Slow-Moving Inventory", "#22c55e"),
        }
        if ok(ins):
            for _, row in ins.iterrows():
                title, color = labels.get(row["insight_type"], (row["insight_type"], "#8b5cf6"))
                st.markdown(
                    f'<div style="margin-top:22px;margin-bottom:8px;">'
                    f'<span style="font-family:IBM Plex Sans,sans-serif;font-size:18px;'
                    f'font-weight:700;">{title}</span>'
                    f'<span style="margin-left:10px;font-size:10px;font-weight:600;'
                    f'letter-spacing:1.5px;text-transform:uppercase;color:{color};">'
                    f'{row["llm_check"]}</span>'
                    f'<span style="margin-left:10px;font-size:11px;color:#64748b;">'
                    f'{str(row["generated_at"])[:19]}</span></div>',
                    unsafe_allow_html=True)
                st.markdown(f'<div class="aibox">{row["ai_summary"]}</div>',
                            unsafe_allow_html=True)
        else:
            st.info("No executive summaries yet — run the UC4 notebook first.")

    with tab2:
        lkpi = q("""SELECT COUNT(*) AS total,
                           SUM(CASE WHEN lead_label='HOT'  THEN 1 END) AS hot,
                           SUM(CASE WHEN lead_label='WARM' THEN 1 END) AS warm,
                           SUM(CASE WHEN lead_label='COLD' THEN 1 END) AS cold,
                           ROUND(AVG(lead_score),1) AS avg
                    FROM globalmart.gold.customer_lead_scores""")
        if ok(lkpi):
            r = lkpi.iloc[0]
            # FIX 1 applied: HOT/WARM/COLD counts use _i() — NULL when no rows match that label
            kcards([
                (f"{_i(r.get('total')):,}", "Customers Scored"),
                (f"{_i(r.get('hot')):,}",   "HOT  ≥65"),
                (f"{_i(r.get('warm')):,}",  "WARM 35–64"),
                (f"{_i(r.get('cold')):,}",  "COLD <35"),
                (str(r.get("avg", "—")),    "Avg Score / 100"),
            ])

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        lf = st.selectbox("Filter", ["All","HOT","WARM","COLD"], label_visibility="collapsed")
        wh = f"WHERE lead_label='{lf}'" if lf != "All" else ""
        leads = q(f"""SELECT customer_name, segment, region,
                             ROUND(total_spent,2) AS total_spent, total_orders,
                             return_rate_pct, purchase_value_score, frequency_score,
                             recency_score, return_penalty,
                             ROUND(lead_score,1) AS lead_score, lead_label, shap_explanation
                      FROM globalmart.gold.customer_lead_scores
                      {wh} ORDER BY lead_score DESC LIMIT 100""")
        if ok(leads):
            disp = [c for c in leads.columns if c != "shap_explanation"]
            def lc(v):
                c = {"HOT":"#f97316","WARM":"#eab308","COLD":"#64748b"}.get(v,"")
                return f"color:{c};font-weight:700;" if c else ""
            def ls(v):
                if not isinstance(v,(int,float)): return ""
                if v >= 65: return "color:#f97316;font-weight:600;"
                if v >= 35: return "color:#eab308;"
                return "color:#64748b;"
            st.dataframe(
                leads[disp].style.applymap(lc, subset=["lead_label"])
                                  .applymap(ls, subset=["lead_score"]),
                use_container_width=True, hide_index=True)
            section("SHAP Explanation")
            sel = st.selectbox("Customer", leads["customer_name"].tolist(),
                               label_visibility="collapsed", key="lead_sel")
            if sel:
                row = leads[leads["customer_name"]==sel].iloc[0]
                kcards([
                    (row["purchase_value_score"], "Purchase Value"),
                    (row["frequency_score"],      "Frequency"),
                    (row["recency_score"],        "Recency"),
                    (f"-{row['return_penalty']}", "Return Penalty"),
                ])
                st.markdown(
                    f'<div class="aibox" style="margin-top:12px;">'
                    f'{row["shap_explanation"]}</div>',
                    unsafe_allow_html=True)
        else:
            st.info("No lead scores yet — run the UC4 notebook first.")
