"""
braze-mock-server/pubsub_publisher.py
======================================
Changes vs previous version:
 [FULL-PAYLOAD]  _slim() — เพิ่ม LLM content fields (ti, km, ct, lk, rk)
                           และ model metadata (pn, ev, bs, ps, ie, dr, cx)
                           เพื่อให้ run_allfeedback_ingestion.py ดึงได้ครบ
"""

import json
import logging
import os
import threading
import time
from typing import List

logger = logging.getLogger(__name__)

_NO_SIGNAL = {"IMPRESSION", "NO_ACTION"}


def _resolve_topic_path(client, project_id: str, topic_id: str) -> str:
    """รองรับทั้ง short name และ full path"""
    if topic_id.startswith("projects/"):
        return topic_id
    return client.topic_path(project_id, topic_id)


def _slim(data: dict) -> tuple:
    """
    บีบ event dict ให้เป็น (bytes, attrs) ก่อนส่งขึ้น Pub/Sub

    Body keys:
      i   = impression_id       u  = external_id (TMNID)
      c   = campaign_id         d  = rec_domain
      r   = reward_score        ts = event_timestamp
      ── LLM content ──────────────────────────────────
      ti  = title               km = key_message
      ct  = cta                 lk = link
      rk  = rank
      ── Model metadata ───────────────────────────────
      pn  = policy_name         ev = encoder_version
      bs  = bandit_score        ps = propensity_score
      ie  = is_explore          dr = decision_reason
      cx  = context_json (dict)
    """
    attrs_src = data.get("attributes") or {}

    body = {
        # ─── Identity ───────────────────────────────────────────────────
        "i":  data.get("impression_id", ""),
        "u":  data.get("external_id",   ""),
        "c":  data.get("campaign_id",   ""),
        "d":  str(attrs_src.get("rec_domain", "")),
        "r":  float(data.get("reward_score", 0.0)),
        "ts": data.get("event_timestamp", ""),
        # ─── LLM content fields ─────────────────────────────────────────
        "ti": data.get("title",       "") or "",
        "km": data.get("key_message", "") or "",
        "ct": data.get("cta",         "") or "",
        "lk": data.get("link",        "") or "",
        "rk": int(data.get("rank", 1) or 1),
        # ─── Model metadata ─────────────────────────────────────────────
        "pn": data.get("policy_name",      "") or "",
        "ev": data.get("encoder_version",  "") or "",
        "bs": float(data.get("bandit_score",     0.0) or 0.0),
        "ps": float(data.get("propensity_score", 0.0) or 0.0),
        "ie": bool(data.get("is_explore", False)),
        "dr": data.get("decision_reason", "") or "",
        # cx: context snapshot — ส่งเป็น dict ตรงๆ (json.dumps จัดการตอน serialize)
        "cx": data.get("context_json") or {},
    }

    attrs = {
        "event_type":   str(data.get("event_type", "")).upper(),
        "arm_campaign": str(attrs_src.get("arm_campaign", "")),
    }

    return json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8"), attrs


class PubSubPublisher:
    """
    Buffer-based Pub/Sub publisher

    debug_publish_all=True  : publish ทุก event รวม IMPRESSION
                              ใช้ตอนทดสอบ ingestion เพื่อเห็น volume จริง
    debug_publish_all=False : filter IMPRESSION / NO_ACTION ออก (production)
    """

    FLUSH_INTERVAL_SEC = 5    # flush ทุก 5 วิ
    BUFFER_MAX         = 3    # flush ทุก 3 events (demo mode)

    def __init__(
        self,
        project_id: str,
        topic_id: str,
        sa_key_path: str,
        debug_publish_all: bool = False,
    ):
        if not os.path.exists(sa_key_path):
            raise FileNotFoundError(
                f"SA key not found: {sa_key_path}\n"
                f"กรุณาใช้ absolute path เช่น /home/user/.gcp/key.json"
            )

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_key_path
        from google.cloud import pubsub_v1
        self._client           = pubsub_v1.PublisherClient()
        self._topic_path       = _resolve_topic_path(self._client, project_id, topic_id)
        self._debug_all        = debug_publish_all
        self._buffer: List[dict] = []
        self._lock             = threading.Lock()
        self._stats            = {"published": 0, "filtered": 0, "batches": 0, "errors": 0}

        t = threading.Thread(target=self._loop, daemon=True)
        t.start()

        mode = "ALL events (debug)" if debug_publish_all else "signal events only"
        logger.info(f"[Pub/Sub Publisher] ready → {self._topic_path} | mode: {mode}")

    def enqueue(self, event_data: dict) -> bool:
        event_type = str(event_data.get("event_type", "")).upper()

        if not self._debug_all and event_type in _NO_SIGNAL:
            self._stats["filtered"] += 1
            return False

        with self._lock:
            self._buffer.append(event_data)
            full = len(self._buffer) >= self.BUFFER_MAX

        if full:
            self.flush()
        return True

    def flush(self) -> int:
        with self._lock:
            if not self._buffer:
                return 0
            batch, self._buffer = self._buffer[:], []

        futures = []
        for ev in batch:
            try:
                b, attrs = _slim(ev)
                futures.append(self._client.publish(self._topic_path, b, **attrs))
            except Exception as e:
                logger.error(f"[Pub/Sub] prepare error: {e}")
                self._stats["errors"] += 1

        ok = 0
        for f in futures:
            try:
                f.result(timeout=10)
                ok += 1
            except Exception as e:
                logger.error(f"[Pub/Sub] send error: {e}")
                self._stats["errors"] += 1

        self._stats["published"] += ok
        self._stats["batches"]   += 1
        if ok:
            logger.info(
                f"[Pub/Sub] ✅ flushed {ok}/{len(batch)} "
                f"| pub={self._stats['published']} filtered={self._stats['filtered']} err={self._stats['errors']}"
            )
        return ok

    def stats(self) -> dict:
        return dict(self._stats)

    def _loop(self):
        while True:
            time.sleep(self.FLUSH_INTERVAL_SEC)
            try:
                self.flush()
            except Exception as e:
                logger.error(f"[Pub/Sub] auto-flush error: {e}")