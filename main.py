"""
braze-mock-server/main.py
=========================
Changes vs previous version:
 [TZ-FIX]       _parse_dt_safe() — ป้องกัน aware vs naive datetime TypeError
 [SENT]          is_sent (0/1) — บันทึกว่า campaign ถึง browser หรือไม่
 [SQL-FIX]       Last_Action ใช้ priority (OPEN > DISMISS > IMPRESSION) แทน MAX
 [NO-SKIP]       ลบ SKIP ออก — เหลือ OPEN, DISMISS, IMPRESSION เท่านั้น
 [LOG]           structured log ทุก event + SENT status
 [SHUTDOWN]      เมื่อ Ctrl+C จะหา Card ที่ is_sent=1 แต่ยังไม่ถูกกด (done_flag=0) เพื่อยิง IMPRESSION เข้า Pub/Sub
 [SCHEMA]        เพิ่ม campaign_id ในการยิงเข้า Pub/Sub
 [FULL-PAYLOAD]  _try_publish() ทุกจุดส่ง title, key_message, cta, link, rank,
                 policy metadata และ context_json เพื่อให้ ingestion ดึงได้ครบ
"""


import os
import json
import sqlite3
import datetime
import csv
import time
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import List, Dict, Any, Optional


# ── Thai timezone UTC+7 ───────────────────────────────────────────
TZ_TH = datetime.timezone(datetime.timedelta(hours=7))


def now_th() -> datetime.datetime:
    return datetime.datetime.now(TZ_TH)


def _parse_dt_safe(s: str) -> datetime.datetime:
    dt = datetime.datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ_TH)
    return dt


# ── Reward map ────────────────────────────────────────────────────
_REWARD_MAP = {"OPEN": 1.0, "DISMISS": -0.5, "IMPRESSION": 0.0}
DEFAULT_WINDOW_SEC = 604800   # 7 days


def _extend_open_windows():
    if not os.path.exists(DB_FILE): return
    new_end = (now_th() + datetime.timedelta(seconds=DEFAULT_WINDOW_SEC)).isoformat()
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("UPDATE decision_log SET reward_window_end = ? WHERE done_flag = FALSE", (new_end,))
    n = c.rowcount
    conn.commit()
    conn.close()
    if n > 0:
        print(f"[SERVER] ↻ Extended window for {n} open campaign(s) → {new_end[:16]}")


# ── Pub/Sub ───────────────────────────────────────────────────────
_HERE = Path(__file__).parent


ENABLE_PUBSUB = True
PUBSUB_CONFIG = {
    "project_id":        "ingestion-streaming",
    "topic_id":          "feedback-event",
    "sa_key_path":       str(_HERE / "GCP_SA" / "ingestion-streaming-ae91d4428672.json"),
    "debug_publish_all": True,
}


_publisher = None


def _get_publisher():
    global _publisher
    if _publisher is None and ENABLE_PUBSUB:
        from pubsub_publisher import PubSubPublisher
        _publisher = PubSubPublisher(**PUBSUB_CONFIG)
    return _publisher


def _try_publish(data: dict):
    if not ENABLE_PUBSUB: return
    try:
        pub = _get_publisher()
        if pub: pub.enqueue(data)
    except Exception as e:
        print(f"[Pub/Sub] ❌ {e}")


# ── Structured logger ─────────────────────────────────────────────
def _log(event: str, **kwargs):
    parts = " | ".join(f"{k}={v}" for k, v in kwargs.items())
    ts    = now_th().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{event:10s}] {parts} | {ts} TH")


# ── Database ──────────────────────────────────────────────────────
DB_FOLDER = "data"
DB_FILE   = os.path.join(DB_FOLDER, "mock_logs.db")


def init_db():
    os.makedirs(DB_FOLDER, exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS decision_log (
        impression_id TEXT PRIMARY KEY,
        episode_id TEXT, step_t INTEGER,
        user_id TEXT, campaign_id TEXT,
        policy_name TEXT, encoder_version TEXT,
        propensity_score REAL, is_explore BOOLEAN,
        decision_reason TEXT,
        context_json TEXT, next_context_json TEXT,
        chosen_action_json TEXT, candidate_actions_json TEXT,
        served_timestamp DATETIME, reward_window_end DATETIME,
        attributed_reward_version TEXT, done_flag BOOLEAN,
        is_sent INTEGER DEFAULT 0,
        ui_payload TEXT
    )''')

    try: c.execute("ALTER TABLE decision_log ADD COLUMN is_sent INTEGER DEFAULT 0")
    except sqlite3.OperationalError: pass

    c.execute('''CREATE TABLE IF NOT EXISTS interaction_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        impression_id TEXT,
        event_type TEXT, reward_value REAL, is_terminal_reward BOOLEAN,
        event_timestamp DATETIME, delay_seconds REAL,
        FOREIGN KEY(impression_id) REFERENCES decision_log(impression_id)
    )''')
    conn.commit()
    conn.close()


# ── Helper: build full publish payload ───────────────────────────
def _build_full_payload(
    *,
    impression_id: str,
    user_id: str,
    campaign_id: str,
    event_type: str,
    reward_score: float,
    event_timestamp: str,
    # content
    title: str = "",
    key_message: str = "",
    cta: str = "",
    link: str = "",
    rank: int = 1,
    # model
    policy_name: str = "",
    encoder_version: str = "",
    bandit_score: float = 0.0,
    propensity_score: float = 0.0,
    is_explore: bool = False,
    decision_reason: str = "",
    # context
    context_json: Optional[dict] = None,
    # bandit arm
    arm_campaign: str = "",
    rec_domain: str = "",
) -> dict:
    """สร้าง dict ที่ครบทุก field สำหรับส่งขึ้น Pub/Sub ผ่าน _try_publish()"""
    return {
        "impression_id":    impression_id,
        "external_id":      user_id,
        "campaign_id":      campaign_id,
        "event_type":       event_type,
        "reward_score":     reward_score,
        "event_timestamp":  event_timestamp,
        # LLM content
        "title":            title,
        "key_message":      key_message,
        "cta":              cta,
        "link":             link,
        "rank":             rank,
        # Model metadata
        "policy_name":      policy_name,
        "encoder_version":  encoder_version,
        "bandit_score":     bandit_score,
        "propensity_score": propensity_score,
        "is_explore":       is_explore,
        "decision_reason":  decision_reason,
        # Context snapshot
        "context_json":     context_json or {},
        # Bandit arm
        "attributes": {
            "arm_campaign": arm_campaign,
            "rec_domain":   rec_domain,
        },
    }


def _publish_impression_on_close():
    if not os.path.exists(DB_FILE): return
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()

    cur.execute("""
        SELECT
            impression_id,
            user_id,
            json_extract(chosen_action_json, '$.arm_campaign'),
            json_extract(chosen_action_json, '$.domain'),
            served_timestamp,
            campaign_id,
            -- LLM content (stored in ui_payload)
            json_extract(ui_payload, '$.title'),
            json_extract(ui_payload, '$.key_message'),
            json_extract(ui_payload, '$.cta'),
            json_extract(ui_payload, '$.link'),
            json_extract(ui_payload, '$.rank'),
            -- model metadata
            policy_name,
            encoder_version,
            json_extract(chosen_action_json, '$.bandit_score'),
            json_extract(chosen_action_json, '$.propensity_score'),
            json_extract(chosen_action_json, '$.is_explore'),
            json_extract(chosen_action_json, '$.decision_reason'),
            -- context
            context_json
        FROM decision_log
        WHERE done_flag = 0 OR done_flag = '0' OR done_flag = 'FALSE'
    """)
    rows = cur.fetchall()
    conn.close()

    if not rows:
        print("[SHUTDOWN] No un-clicked campaigns to send as IMPRESSION.")
        return

    print(f"[SHUTDOWN] Sending IMPRESSION for {len(rows)} un-clicked campaign(s)...")
    for r in rows:
        ctx = {}
        try:
            ctx = json.loads(r[17]) if r[17] else {}
        except Exception:
            pass

        _try_publish(_build_full_payload(
            impression_id    = r[0],
            user_id          = r[1] or "",
            campaign_id      = r[5] or "",
            event_type       = "IMPRESSION",
            reward_score     = 0.0,
            event_timestamp  = r[4] or now_th().isoformat(),
            title            = r[6]  or "",
            key_message      = r[7]  or "",
            cta              = r[8]  or "",
            link             = r[9]  or "",
            rank             = int(r[10] or 1),
            policy_name      = r[11] or "",
            encoder_version  = r[12] or "",
            bandit_score     = float(r[13] or 0.0),
            propensity_score = float(r[14] or 0.0),
            is_explore       = bool(r[15]),
            decision_reason  = r[16] or "",
            context_json     = ctx,
            arm_campaign     = r[2] or "",
            rec_domain       = r[3] or "",
        ))


def auto_export_to_csv():
    try:
        _publish_impression_on_close()

        if _publisher:
            n = _publisher.flush()
            print(f"[Pub/Sub] Final flush: {n} events | {_publisher.stats()}")
            time.sleep(2.0)

        conn = sqlite3.connect(DB_FILE)
        c    = conn.cursor()
        c.execute('''
            SELECT
                d.impression_id                                     AS Impression_ID,
                d.user_id                                           AS TMNID,
                d.campaign_id                                       AS Campaign_ID,
                json_extract(d.chosen_action_json, "$.domain")      AS Domain,
                json_extract(d.chosen_action_json, "$.arm_campaign") AS arm_campaign,
                d.policy_name                                       AS Model_Version,
                d.is_sent                                           AS SENT,
                COALESCE(
                    MAX(CASE WHEN i.event_type = 'OPEN'       THEN 'OPEN'       END),
                    MAX(CASE WHEN i.event_type = 'DISMISS'    THEN 'DISMISS'    END),
                    MAX(CASE WHEN i.event_type = 'IMPRESSION' THEN 'IMPRESSION' END),
                    'NO_ACTION'
                )                                                   AS Last_Action,
                COALESCE(MAX(i.reward_value), 0.0)                 AS Reward_Score,
                d.served_timestamp                                  AS Sent_At
            FROM decision_log d
            LEFT JOIN interaction_log i ON d.impression_id = i.impression_id
            GROUP BY d.impression_id
            ORDER BY d.served_timestamp DESC
        ''')
        rows    = c.fetchall()
        headers = [d[0] for d in c.description]
        if rows:
            for fname in ("feedback_data.csv", "feedback_posterior.csv"):
                with open(fname, "w", encoding="utf-8-sig", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(headers)
                    writer.writerows(rows)
            print(f"\n[EXPORT] ✅ {len(rows)} rows → feedback_data.csv")
        conn.close()
    except Exception as e:
        print(f"\n[EXPORT] ❌ {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    _extend_open_windows()
    print(f"[SERVER] 🚀 Started | {now_th().strftime('%Y-%m-%d %H:%M:%S')} TH")
    if ENABLE_PUBSUB:
        try:
            _get_publisher()
            print(f"[Pub/Sub] ✅ Publisher ready")
        except Exception as e:
            print(f"[Pub/Sub] ❌ Init failed: {e}")
    yield
    print("\n[SERVER] Shutting down — Processing final events & CSV...")
    auto_export_to_csv()


# ── App ───────────────────────────────────────────────────────────
app       = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")


class ConnectionManager:
    def __init__(self): self.active_connections: list[WebSocket] = []
    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active_connections.append(ws)
        print(f"[WS]        browser connected | total={len(self.active_connections)}")
    def disconnect(self, ws: WebSocket):
        self.active_connections.remove(ws)
        print(f"[WS]        browser disconnected | total={len(self.active_connections)}")
    async def broadcast(self, msg: str):
        for ws in self.active_connections:
            await ws.send_text(msg)


manager = ConnectionManager()


class CampaignRequest(BaseModel):
    impression_id:     str
    id:                str
    user_id:           str
    title:             str
    key_message:       str
    cta:               str
    link:              str
    decision_context:  Dict[str, Any] = {}
    chosen_action:     Dict[str, Any] = {}
    candidate_actions: List[Dict[str, Any]] = []
    policy_info:       Dict[str, Any] = {}
    rl_metadata:       Dict[str, Any] = {}


class ActionLog(BaseModel):
    impression_id: str
    action_type:   str
    reward_value:  float


# ── Endpoints ─────────────────────────────────────────────────────


@app.get("/", response_class=HTMLResponse)
async def get_web_ui(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/history")
async def get_history():
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    now_iso = now_th().isoformat()
    c.execute("""SELECT ui_payload FROM decision_log
                 WHERE done_flag = FALSE AND reward_window_end > ?
                 ORDER BY served_timestamp ASC""", (now_iso,))
    rows = c.fetchall()
    conn.close()
    return {"history": [json.loads(r[0]) for r in rows]}


@app.get("/api/pubsub_stats")
async def get_pubsub_stats():
    if _publisher: return {"enabled": True, "stats": _publisher.stats()}
    return {"enabled": False}


@app.get("/api/status")
async def get_status():
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    c.execute("SELECT COUNT(*) FROM decision_log")
    n_camps = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM interaction_log")
    n_events = c.fetchone()[0]
    conn.close()
    return {
        "time_th":        now_th().strftime("%Y-%m-%d %H:%M:%S TH"),
        "ws_connections": len(manager.active_connections),
        "campaigns_sent": n_camps,
        "events_logged":  n_events,
        "pubsub_stats":   _publisher.stats() if _publisher else {},
    }


@app.post("/api/reset")
async def reset_demo():
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    new_end = (now_th() + datetime.timedelta(seconds=DEFAULT_WINDOW_SEC)).isoformat()

    c.execute("UPDATE decision_log SET done_flag = 0, is_sent = 1, reward_window_end = ?", (new_end,))
    n = c.rowcount
    c.execute("DELETE FROM interaction_log WHERE event_type IN ('OPEN', 'DISMISS')")

    conn.commit()
    conn.close()
    _log("RESET", reopened=n, cleared_history="Yes")
    return {"status": "ok", "reopened": n}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True: await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)


@app.post("/api/send_campaign")
async def receive_campaign(campaign: CampaignRequest):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    now  = now_th()

    window_sec        = campaign.rl_metadata.get("reward_window_seconds", DEFAULT_WINDOW_SEC)
    reward_window_end = now + datetime.timedelta(seconds=window_sec)

    n_browsers = len(manager.active_connections)
    is_sent    = 1 if n_browsers > 0 else 0

    arm    = campaign.chosen_action.get("arm_campaign", "?")
    domain = campaign.chosen_action.get("domain", "?")

    ui_payload = {
        "impression_id": campaign.impression_id,
        "id":   campaign.id, "user_id": campaign.user_id,
        "title": campaign.title, "key_message": campaign.key_message,
        "cta":  campaign.cta, "link": campaign.link,
        "rank": campaign.chosen_action.get("rank", 1),
        "metadata": {
            "bandit_score": campaign.chosen_action.get("bandit_score"),
            "domain": domain, "arm_campaign": arm,
        },
    }

    c.execute('''INSERT INTO decision_log
        (impression_id, episode_id, step_t, user_id, campaign_id, policy_name,
         encoder_version, propensity_score, is_explore, decision_reason,
         context_json, next_context_json, chosen_action_json, candidate_actions_json,
         served_timestamp, reward_window_end, attributed_reward_version, done_flag,
         is_sent, ui_payload)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
        (campaign.impression_id, campaign.rl_metadata.get("episode_id", ""),
         campaign.rl_metadata.get("step_t", 1), campaign.user_id, campaign.id,
         campaign.policy_info.get("policy_name", "unknown"), campaign.policy_info.get("encoder_version", "unknown"),
         campaign.chosen_action.get("propensity_score", 1.0), campaign.chosen_action.get("is_explore", False),
         campaign.chosen_action.get("decision_reason", ""), json.dumps(campaign.decision_context), None,
         json.dumps(campaign.chosen_action), json.dumps(campaign.candidate_actions),
         now.isoformat(), reward_window_end.isoformat(), "v1.1", False, is_sent, json.dumps(ui_payload)))

    # บันทึก IMPRESSION ลง Local DB ทันที
    c.execute('''INSERT INTO interaction_log
        (impression_id, event_type, reward_value, is_terminal_reward, event_timestamp, delay_seconds)
        VALUES (?, 'IMPRESSION', NULL, FALSE, ?, 0.0)''',
        (campaign.impression_id, now.isoformat()))

    conn.commit()
    conn.close()

    await manager.broadcast(json.dumps(ui_payload))

    # ── Pub/Sub: IMPRESSION with full payload ────────────────────
    _try_publish(_build_full_payload(
        impression_id    = campaign.impression_id,
        user_id          = campaign.user_id,
        campaign_id      = campaign.id,
        event_type       = "IMPRESSION",
        reward_score     = 0.0,
        event_timestamp  = now.isoformat(),
        title            = campaign.title,
        key_message      = campaign.key_message,
        cta              = campaign.cta,
        link             = campaign.link,
        rank             = campaign.chosen_action.get("rank", 1),
        policy_name      = campaign.policy_info.get("policy_name", ""),
        encoder_version  = campaign.policy_info.get("encoder_version", ""),
        bandit_score     = float(campaign.chosen_action.get("bandit_score") or 0.0),
        propensity_score = float(campaign.chosen_action.get("propensity_score") or 1.0),
        is_explore       = bool(campaign.chosen_action.get("is_explore", False)),
        decision_reason  = campaign.chosen_action.get("decision_reason", ""),
        context_json     = campaign.decision_context,
        arm_campaign     = arm,
        rec_domain       = domain,
    ))

    sent_label = f"📬 SENT to {n_browsers} browser(s)" if is_sent else "📤 QUEUED (no browser)"
    _log("IMPRESSION", user=campaign.user_id, imp=campaign.impression_id[:8] + "…",
         arm=arm, domain=domain, sent=sent_label)

    return {"status": "success"}


@app.post("/api/log_action")
async def log_interaction(action: ActionLog):
    conn = sqlite3.connect(DB_FILE)
    c    = conn.cursor()
    now  = now_th()

    c.execute("""
        SELECT
            served_timestamp, reward_window_end, done_flag,
            chosen_action_json, user_id, campaign_id,
            -- LLM content from ui_payload
            json_extract(ui_payload, '$.title'),
            json_extract(ui_payload, '$.key_message'),
            json_extract(ui_payload, '$.cta'),
            json_extract(ui_payload, '$.link'),
            json_extract(ui_payload, '$.rank'),
            -- model metadata
            policy_name, encoder_version,
            -- context
            context_json
        FROM decision_log WHERE impression_id = ?""",
        (action.impression_id,))
    row = c.fetchone()

    if not row:
        conn.close()
        _log("ERROR", reason="impression_not_found", imp=action.impression_id[:8] + "…")
        return {"status": "error", "message": "Impression not found"}

    served_time       = _parse_dt_safe(row[0])
    reward_window_end = _parse_dt_safe(row[1])
    is_done           = row[2]
    chosen            = json.loads(row[3] or "{}")
    user_id           = row[4] or ""
    campaign_id       = row[5] or ""
    # LLM content
    title             = row[6]  or ""
    key_message       = row[7]  or ""
    cta               = row[8]  or ""
    link              = row[9]  or ""
    rank              = int(row[10] or 1)
    # model
    policy_name       = row[11] or ""
    encoder_version   = row[12] or ""
    # context
    ctx = {}
    try:
        ctx = json.loads(row[13]) if row[13] else {}
    except Exception:
        pass

    if now > reward_window_end or is_done:
        conn.close()
        reason = "window_closed" if now > reward_window_end else "already_terminal"
        _log("IGNORED", reason=reason, imp=action.impression_id[:8] + "…")
        return {"status": "ignored", "message": "Reward window closed"}

    delay           = (now - served_time).total_seconds()
    terminal_events = {"OPEN", "DISMISS", "PURCHASE", "BOUNCE", "TIMEOUT"}
    is_terminal     = action.action_type in terminal_events

    c.execute('''INSERT INTO interaction_log
        (impression_id, event_type, reward_value, is_terminal_reward, event_timestamp, delay_seconds)
        VALUES (?,?,?,?,?,?)''',
        (action.impression_id, action.action_type, action.reward_value,
         is_terminal, now.isoformat(), delay))

    if is_terminal:
        c.execute("UPDATE decision_log SET done_flag = TRUE WHERE impression_id = ?",
                  (action.impression_id,))

    conn.commit()
    conn.close()

    r_sign = f"+{action.reward_value:.2f}" if action.reward_value >= 0 else f"{action.reward_value:.2f}"
    _log(action.action_type, user=user_id, imp=action.impression_id[:8] + "…",
         reward=r_sign, delay=f"{delay:.1f}s", terminal=is_terminal)

    # ── Pub/Sub: OPEN / DISMISS with full payload ────────────────
    _try_publish(_build_full_payload(
        impression_id    = action.impression_id,
        user_id          = user_id,
        campaign_id      = campaign_id,
        event_type       = action.action_type,
        reward_score     = action.reward_value,
        event_timestamp  = now.isoformat(),
        title            = title,
        key_message      = key_message,
        cta              = cta,
        link             = link,
        rank             = rank,
        policy_name      = policy_name,
        encoder_version  = encoder_version,
        bandit_score     = float(chosen.get("bandit_score") or 0.0),
        propensity_score = float(chosen.get("propensity_score") or 1.0),
        is_explore       = bool(chosen.get("is_explore", False)),
        decision_reason  = chosen.get("decision_reason", ""),
        context_json     = ctx,
        arm_campaign     = chosen.get("arm_campaign", ""),
        rec_domain       = chosen.get("domain", ""),
    ))

    return {"status": "success"}


# ── Test endpoints ─────────────────────────────────────────────────


@app.post("/api/test/send_impression")
async def test_impression():
    import uuid
    return await receive_campaign(CampaignRequest(
        impression_id = str(uuid.uuid4()), id = "TEST_CAMP_001",
        user_id = "tmn.10086632327", title = "ทดสอบ Campaign",
        key_message = "Test ingestion", cta = "ลองดู",
        link = "https://example.com",
        chosen_action = {"arm_campaign": "83", "domain": "PMS", "bandit_score": 1.23, "propensity_score": 0.9},
        policy_info = {"policy_name": "NeuralLinear_test", "encoder_version": "v1"},
    ))


@app.post("/api/test/send_action")
async def test_action(impression_id: str, action_type: str = "OPEN"):
    return await log_interaction(ActionLog(
        impression_id = impression_id,
        action_type   = action_type,
        reward_value  = _REWARD_MAP.get(action_type.upper(), 0.0),
    ))