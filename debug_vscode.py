"""
braze-mock-server/debug_vscode.py   [รัน: python debug_vscode.py]
==================================================================
Preflight check สำหรับ VS Code / local machine

ตรวจ 5 จุด:
  V1  SA key file exists
  V2  GCP auth + list topics
  V3  Topic path ถูกต้อง
  V4  Test publish 1 message
  V5  Pub/Sub publisher init (ENABLE_PUBSUB = True)

รันก่อนเริ่ม uvicorn เสมอ
"""

import json
import os
import sys
import datetime

# ── Config ────────────────────────────────────────────────────────
# แก้ค่า 3 บรรทัดนี้ให้ตรงกับ PUBSUB_CONFIG ใน main.py
GCP_PROJECT_ID    = "ingestion-streaming"
PUBSUB_TOPIC_ID   = "feedback-event"          
SA_KEY_PATH       = "GCP_SA/ingestion-streaming-ae91d4428672.json"


def ok(tag, msg):  print(f"  ✅  [{tag}] {msg}")
def warn(tag, msg): print(f"  ⚠️  [{tag}] {msg}")
def fail(tag, msg): print(f"  ❌  [{tag}] {msg}"); return False


def check_v1_key():
    print("\n[V1] SA Key file")
    if os.path.exists(SA_KEY_PATH):
        size = os.path.getsize(SA_KEY_PATH)
        ok("V1", f"Found: {SA_KEY_PATH} ({size} bytes)")
        try:
            with open(SA_KEY_PATH) as f:
                key_data = json.load(f)
            project = key_data.get("project_id", "?")
            email   = key_data.get("client_email", "?")
            ok("V1", f"project_id={project}, client_email={email}")
            return True
        except Exception as e:
            fail("V1", f"Cannot parse JSON: {e}")
            return False
    else:
        fail("V1", f"NOT FOUND: {SA_KEY_PATH}")
        if SA_KEY_PATH.startswith("GCP_SA/"):
            warn("V1", "Path is relative — use absolute path!")
        return False


def check_v2_auth():
    print("\n[V2] GCP Authentication")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_KEY_PATH
    try:
        from google.cloud import pubsub_v1
        client = pubsub_v1.PublisherClient()
        ok("V2", "PublisherClient initialized ✅")
        return client
    except ImportError:
        fail("V2", "google-cloud-pubsub not installed → pip install google-cloud-pubsub")
        return None
    except Exception as e:
        fail("V2", f"Auth failed: {e}")
        return None


def check_v3_topic(client):
    print("\n[V3] Topic path validation")
    if client is None:
        warn("V3", "Skipped (no client)")
        return None

    # Test both formats
    if PUBSUB_TOPIC_ID.startswith("projects/"):
        fail("V3", f"PUBSUB_TOPIC_ID is full path — should be short name only!")
        fail("V3", f"Change to: \"{PUBSUB_TOPIC_ID.split('/')[-1]}\"")
        topic_path = PUBSUB_TOPIC_ID
    else:
        topic_path = client.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC_ID)
        ok("V3", f"topic_path = {topic_path}")

    # Verify topic exists
    try:
        topic = client.get_topic(request={"topic": topic_path})
        ok("V3", f"Topic exists ✅ | name={topic.name}")
        return topic_path
    except Exception as e:
        fail("V3", f"Topic not found or no permission: {e}")
        warn("V3", "ตรวจสอบว่า SA มี role: roles/pubsub.publisher")
        return None


def check_v4_publish(client, topic_path):
    print("\n[V4] Test publish")
    if client is None or topic_path is None:
        warn("V4", "Skipped")
        return False

    test_msg = {
        "i":  "debug-test-001",
        "u":  "tmn.test",
        "r":  1.0,
        "ts": datetime.datetime.utcnow().isoformat(),
    }
    test_attrs = {"event_type": "DEBUG_TEST", "arm_campaign": "test"}

    try:
        future = client.publish(
            topic_path,
            json.dumps(test_msg, separators=(",", ":")).encode(),
            **test_attrs
        )
        msg_id = future.result(timeout=10)
        ok("V4", f"Published test message ✅ | msg_id={msg_id}")
        ok("V4", f"Payload: {json.dumps(test_msg)} | attrs={test_attrs}")
        return True
    except Exception as e:
        fail("V4", f"Publish failed: {e}")
        return False


def check_v5_publisher_class():
    print("\n[V5] PubSubPublisher class init")
    try:
        sys.path.insert(0, os.path.dirname(__file__))
        from pubsub_publisher import PubSubPublisher
        pub = PubSubPublisher(
            project_id        = GCP_PROJECT_ID,
            topic_id          = PUBSUB_TOPIC_ID,
            sa_key_path       = SA_KEY_PATH,
            debug_publish_all = True,   # ทดสอบ: ให้ IMPRESSION ผ่านด้วย
        )
        ok("V5", f"PubSubPublisher ready | stats={pub.stats()}")

        # Enqueue test event
        pub.enqueue({
            "impression_id":  "debug-class-001",
            "external_id":    "tmn.test",
            "event_type":     "IMPRESSION",   # จะผ่านเพราะ debug_publish_all=True
            "reward_score":    0.0,
            "event_timestamp": datetime.datetime.utcnow().isoformat(),
            "attributes": {"arm_campaign": "test", "rec_domain": "PMS"},
        })
        ok("V5", "Enqueued IMPRESSION event (debug_publish_all=True)")

        n = pub.flush()
        ok("V5", f"Flushed {n} event(s)")
        return True
    except FileNotFoundError as e:
        fail("V5", str(e))
        return False
    except Exception as e:
        fail("V5", f"Error: {e}")
        return False


def run():
    print("=" * 60)
    print("  VS Code Preflight — Pub/Sub Publisher")
    print("=" * 60)

    results = {}
    results["V1"] = check_v1_key()
    client        = check_v2_auth()
    results["V2"] = client is not None
    topic_path    = check_v3_topic(client)
    results["V3"] = topic_path is not None
    results["V4"] = check_v4_publish(client, topic_path)
    results["V5"] = check_v5_publisher_class()

    passed = sum(results.values())
    total  = len(results)
    print(f"\n{'='*60}")
    print(f"  Result: {passed}/{total} passed")

    if passed == total:
        print("  🟢 ผ่านทุก check — พร้อม run uvicorn แล้ว")
        print("\n  Next step:")
        print("    uvicorn main:app --reload --port 8080")
    else:
        failed = [k for k, v in results.items() if not v]
        print(f"  🔴 แก้ไข {failed} ก่อน")
    print("=" * 60)


if __name__ == "__main__":
    run()