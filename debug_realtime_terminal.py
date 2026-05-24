"""
braze-mock-server/debug_realtime_terminal.py
=============================================
รัน VS Code terminal แยกต่างหากจาก uvicorn — monitor event real-time

รัน:  python debug_realtime_terminal.py
หยุด: Ctrl+C

ดูข้อมูลจาก data/mock_logs.db โดยตรง (อ่านอย่างเดียว)
ไม่กระทบ server ที่รันอยู่

Output format:
  [CAMPAIGN  ] ส่ง campaign ไปยัง browser (SENT=1/0)
  [OPEN      ] user คลิก CTA button
  [DISMISS   ] user กดปิด
  [IMPRESSION] user เห็น campaign แต่ไม่ได้กด
"""

import sqlite3
import time
import os
import sys
import datetime

DB_FILE       = "data/mock_logs.db"
POLL_INTERVAL = 0.5   # วินาที

# ANSI color codes
C = {
    "RESET":      "\033[0m",
    "CYAN":       "\033[96m",
    "GREEN":      "\033[92m",
    "RED":        "\033[91m",
    "BLUE":       "\033[94m",
    "YELLOW":     "\033[93m",
    "GRAY":       "\033[90m",
    "BOLD":       "\033[1m",
    "ORANGE":     "\033[33m",
}

EVENT_COLOR = {
    "OPEN":       C["GREEN"],
    "DISMISS":    C["RED"],
    "IMPRESSION": C["BLUE"],
    "NO_ACTION":  C["GRAY"],
}

def cprint(color: str, text: str):
    print(f"{color}{text}{C['RESET']}")

def fmt_ts(ts_str: str) -> str:
    if not ts_str:
        return "?"
    try:
        dt = datetime.datetime.fromisoformat(ts_str)
        return dt.strftime("%H:%M:%S")
    except Exception:
        return ts_str[:8]

def watch():
    last_decision_rowid    = 0
    last_interaction_id    = 0
    total_impressions      = 0
    total_opens            = 0
    total_dismisses        = 0

    cprint(C["CYAN"] + C["BOLD"], "\n" + "═" * 65)
    cprint(C["CYAN"] + C["BOLD"],  "  Buddy Engine — Real-time Event Monitor")
    cprint(C["CYAN"] + C["BOLD"],  f"  DB: {os.path.abspath(DB_FILE)}")
    cprint(C["CYAN"] + C["BOLD"],  "  Ctrl+C to stop")
    cprint(C["CYAN"] + C["BOLD"],  "═" * 65 + "\n")

    if not os.path.exists(DB_FILE):
        cprint(C["YELLOW"], f"  ⏳ Waiting for DB: {DB_FILE} ...")

    while True:
        try:
            if not os.path.exists(DB_FILE):
                time.sleep(1)
                continue

            conn = sqlite3.connect(f"file:{DB_FILE}?mode=ro", uri=True, timeout=1)
            c    = conn.cursor()

            # ── New campaigns ──────────────────────────────────────────
            c.execute("""
                SELECT rowid, impression_id, user_id, campaign_id, is_sent, served_timestamp
                FROM decision_log WHERE rowid > ?
                ORDER BY rowid ASC
            """, (last_decision_rowid,))
            for row in c.fetchall():
                last_decision_rowid = row[0]
                total_impressions  += 1
                sent_label = (f"{C['GREEN']}📬 SENT to browser{C['RESET']}"
                              if row[4] == 1
                              else f"{C['YELLOW']}📤 QUEUED (no browser){C['RESET']}")
                ts = fmt_ts(row[5])
                print(
                    f"  {C['ORANGE']}[CAMPAIGN  ]{C['RESET']} "
                    f"user={row[2]} | camp={row[3]} | {sent_label} | {ts}"
                )

            # ── New interactions ───────────────────────────────────────
            c.execute("""
                SELECT i.id, i.impression_id, i.event_type, i.reward_value,
                       i.event_timestamp, d.user_id, i.delay_seconds, d.is_sent
                FROM interaction_log i
                JOIN decision_log d ON d.impression_id = i.impression_id
                WHERE i.id > ?
                ORDER BY i.id ASC
            """, (last_interaction_id,))
            for row in c.fetchall():
                last_interaction_id = row[0]
                et      = row[2] or "?"
                reward  = row[3]
                ts      = fmt_ts(row[4])
                user    = row[5] or "?"
                delay   = f"{row[6]:.1f}s" if row[6] else "—"
                is_sent = row[7]

                color   = EVENT_COLOR.get(et, C["GRAY"])
                r_str   = f"+{reward:.2f}" if reward and reward >= 0 else (f"{reward:.2f}" if reward else "0.00")
                sent_m  = "(browser open)" if is_sent else "(no browser)"

                if et == "OPEN":      total_opens     += 1
                elif et == "DISMISS": total_dismisses += 1

                print(
                    f"  {color}[{et:10s}]{C['RESET']} "
                    f"user={user} | imp={row[1][:8]}… | "
                    f"reward={r_str} | delay={delay} | {sent_m} | {ts}"
                )

            conn.close()

        except sqlite3.OperationalError as e:
            if "no such table" not in str(e):
                cprint(C["GRAY"], f"  [DB] {e}")
        except Exception as e:
            cprint(C["YELLOW"], f"  [ERROR] {e}")

        time.sleep(POLL_INTERVAL)

def show_summary():
    """แสดง summary ของ DB ปัจจุบัน — รัน 1 ครั้งแล้วออก"""
    if not os.path.exists(DB_FILE):
        print(f"DB not found: {DB_FILE}")
        return

    conn = sqlite3.connect(f"file:{DB_FILE}?mode=ro", uri=True)
    c    = conn.cursor()

    c.execute("""
        SELECT last_action, is_sent, COUNT(*) AS count
        FROM (
            SELECT
                d.impression_id,
                d.is_sent,
                COALESCE(
                    MAX(CASE WHEN i.event_type = 'OPEN'       THEN 'OPEN'       END),
                    MAX(CASE WHEN i.event_type = 'DISMISS'    THEN 'DISMISS'    END),
                    MAX(CASE WHEN i.event_type = 'IMPRESSION' THEN 'IMPRESSION' END),
                    'NO_ACTION'
                ) AS last_action
            FROM decision_log d
            LEFT JOIN interaction_log i ON d.impression_id = i.impression_id
            GROUP BY d.impression_id, d.is_sent
        )
        GROUP BY last_action, is_sent
        ORDER BY count DESC
    """)

    rows = c.fetchall()
    conn.close()

    cprint(C["CYAN"] + C["BOLD"], "\n  ─── DB Summary ───────────────────────────────────")
    cprint(C["GRAY"], f"  {'Last_Action':<14} {'SENT':>6} {'count':>8}")
    cprint(C["GRAY"],  "  " + "─" * 30)
    for row in rows:
        et, sent, cnt = row
        color  = EVENT_COLOR.get(et, C["GRAY"])
        sent_s = "✅" if sent == 1 else "❌"
        print(f"  {color}{et:<14}{C['RESET']} {sent_s}  {cnt:>8}")
    cprint(C["CYAN"], "  ──────────────────────────────────────────────────\n")


if __name__ == "__main__":
    if "--summary" in sys.argv:
        show_summary()
    else:
        try:
            show_summary()   # show current state first
            watch()
        except KeyboardInterrupt:
            cprint(C["CYAN"], "\n\n  Stopped. Final summary:")
            show_summary()