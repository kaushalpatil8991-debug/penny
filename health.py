#!/usr/bin/env python3
"""
Unified FastAPI Server - Health Checks + Telegram Webhook (Penny Detector)
Imports fyers.py as a module and runs the detector in a background thread.
"""

import os
import time
import threading
import requests
from datetime import datetime
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Import the detector module directly (unified architecture - no subprocess)
import fyers

# =============================================================================
# APP STATE - Thread-safe shared state between webhook and detector
# =============================================================================

class AppState:
    """Thread-safe shared state between webhook handler and detector thread."""
    def __init__(self):
        # Auth code delivery via threading.Event
        self.auth_code_event = threading.Event()
        self.auth_code_value = None
        self.auth_code_lock = threading.Lock()

        # Force start (bypass market hours)
        self.force_start = False
        self.force_event = threading.Event()

        # Detector reference
        self.detector_ref = None
        self.detector_lock = threading.Lock()

    def set_auth_code(self, code):
        """Called by webhook when auth_code URL is received."""
        with self.auth_code_lock:
            self.auth_code_value = code
            self.auth_code_event.set()

    def wait_for_auth_code(self, timeout=600):
        """Block until auth code arrives or timeout. Called by FyersAuthenticator."""
        got_it = self.auth_code_event.wait(timeout=timeout)
        if got_it:
            with self.auth_code_lock:
                code = self.auth_code_value
                self.auth_code_value = None
                self.auth_code_event.clear()
                return code
        return None

    def set_detector(self, det):
        with self.detector_lock:
            self.detector_ref = det

    def get_detector(self):
        with self.detector_lock:
            return self.detector_ref


# =============================================================================
# GLOBALS
# =============================================================================

app = FastAPI(title="Penny Detector Service", version="3.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

start_time = time.time()
app_state = AppState()
detector_thread = None
health_check_url = "https://penny-00h7.onrender.com/health"
last_self_health_check = time.time()

WEBHOOK_URL = "https://penny-00h7.onrender.com/telegram-webhook"

# =============================================================================
# TELEGRAM HELPERS
# =============================================================================

def send_telegram(text):
    """Send a message to the configured Telegram chat."""
    try:
        url = f"https://api.telegram.org/bot{fyers.TELEGRAM_BOT_TOKEN}/sendMessage"
        response = requests.post(url, data={
            "chat_id": fyers.TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML"
        }, timeout=10)
        result = response.json()
        if not result.get("ok"):
            print(f"[TELEGRAM] Send failed: {result}")
    except Exception as e:
        print(f"[TELEGRAM] Error: {e}")


def setup_telegram_webhook():
    """Register the webhook URL with Telegram Bot API."""
    url = f"https://api.telegram.org/bot{fyers.TELEGRAM_BOT_TOKEN}/setWebhook"
    payload = {
        "url": WEBHOOK_URL,
        "allowed_updates": ["message"],
        "drop_pending_updates": True
    }
    try:
        response = requests.post(url, json=payload, timeout=15)
        result = response.json()
        if result.get("ok"):
            print(f"[WEBHOOK] Telegram webhook set: {WEBHOOK_URL}")
        else:
            print(f"[WEBHOOK] Failed to set webhook: {result}")
    except Exception as e:
        print(f"[WEBHOOK] Error setting webhook: {e}")


def remove_telegram_webhook():
    """Remove the webhook (revert to polling mode for debugging)."""
    url = f"https://api.telegram.org/bot{fyers.TELEGRAM_BOT_TOKEN}/deleteWebhook"
    try:
        response = requests.post(url, json={"drop_pending_updates": True}, timeout=15)
        result = response.json()
        print(f"[WEBHOOK] Webhook removed: {result}")
    except Exception as e:
        print(f"[WEBHOOK] Error removing webhook: {e}")

# =============================================================================
# TELEGRAM WEBHOOK ENDPOINT
# =============================================================================

@app.post("/telegram-webhook")
async def telegram_webhook(request: Request):
    """Receive Telegram webhook updates for auth code delivery."""
    try:
        data = await request.json()
    except Exception:
        return {"ok": False}

    message = data.get("message", {})
    text = message.get("text", "").strip()
    chat_id = str(message.get("chat", {}).get("id", ""))

    # Security: only accept messages from the configured chat
    if chat_id != fyers.TELEGRAM_CHAT_ID:
        return {"ok": True}

    if not text:
        return {"ok": True}

    text_lower = text.lower()

    # Force start - bypass market hours
    if text_lower in ["force", "start", "start now"]:
        app_state.force_start = True
        app_state.force_event.set()
        send_telegram("Force start activated. Bypassing market hours...")
        return {"ok": True}

    # Auth code detection
    if "auth_code=" in text:
        handler = fyers.TelegramHandler()
        auth_code = handler.extract_auth_code(text)
        if auth_code:
            app_state.set_auth_code(auth_code)
            send_telegram("Auth code received! Processing authentication...")
        else:
            send_telegram("Could not extract auth code from that URL. Please try again.")

    return {"ok": True}

# =============================================================================
# HEALTH / STATUS ENDPOINTS
# =============================================================================

@app.get("/")
async def root():
    return {
        "message": "Penny Detector Service running",
        "status": "online",
        "detector_status": "running" if (detector_thread and detector_thread.is_alive()) else "stopped"
    }


@app.get("/health")
async def health_check():
    """Primary health check endpoint"""
    detector_status = "stopped"
    if detector_thread and detector_thread.is_alive():
        det = app_state.get_detector()
        if det and hasattr(det, 'authenticator') and det.authenticator.is_authenticated:
            detector_status = "running_authenticated"
        else:
            detector_status = "running"

    return JSONResponse(
        status_code=200,
        content={
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "uptime": time.time() - start_time,
            "detector_status": detector_status,
            "service": "penny-detector"
        }
    )


@app.get("/status")
async def get_status():
    """Get detailed system status"""
    det = app_state.get_detector()
    detector_info = {}
    if det:
        detector_info = {
            "authenticated": det.authenticator.is_authenticated,
            "total_ticks": det.total_ticks,
            "trades_detected": det.individual_trades_detected,
            "uptime_seconds": time.time() - det.start_time
        }

    return JSONResponse(
        status_code=200,
        content={
            "health_service": {
                "status": "running",
                "uptime_seconds": time.time() - start_time,
                "uptime_minutes": (time.time() - start_time) / 60
            },
            "detector": detector_info,
            "supervisor_alive": detector_thread.is_alive() if detector_thread else False,
            "timestamp": datetime.now().isoformat()
        }
    )

# =============================================================================
# DETECTOR THREAD MANAGEMENT
# =============================================================================

def start_detector_thread():
    """Run the fyers supervisor loop in a background thread."""
    global detector_thread
    if detector_thread and detector_thread.is_alive():
        print("Detector thread already running")
        return

    detector_thread = threading.Thread(
        target=fyers.supervisor_loop,
        args=(app_state,),
        daemon=True,
        name="detector-supervisor"
    )
    detector_thread.start()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Detector supervisor thread started")

# =============================================================================
# SELF HEALTH CHECKER
# =============================================================================

def self_health_checker():
    """Send health check requests to own endpoint every 7 minutes"""
    global last_self_health_check

    time.sleep(30)

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Self health checker started")
    print(f"[INFO] Will ping {health_check_url} every 7 minutes")

    while True:
        try:
            time.sleep(420)

            print(f"[{datetime.now().strftime('%H:%M:%S')}] Sending self health check...")

            response = requests.get(health_check_url, timeout=30)

            if response.status_code == 200:
                last_self_health_check = time.time()
                data = response.json()
                detector_status = data.get('detector_status', 'unknown')
                print(f"[HEALTH] Health check OK - Detector: {detector_status}")
            else:
                print(f"[HEALTH] Health check failed - Status: {response.status_code}")

        except requests.exceptions.Timeout:
            print(f"[HEALTH] Health check timeout")
        except requests.exceptions.RequestException as e:
            print(f"[HEALTH] Health check error: {e}")
        except Exception as e:
            print(f"[ERROR] Unexpected error in health checker: {e}")

# =============================================================================
# STARTUP
# =============================================================================

@app.on_event("startup")
async def on_startup():
    """Called when FastAPI starts."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Setting up Telegram webhook...")
    threading.Thread(target=setup_telegram_webhook, daemon=True).start()

    def delayed_start():
        time.sleep(5)
        send_telegram("<b>Service Started</b>\n\nPenny Detector is online.")
        start_detector_thread()

    threading.Thread(target=delayed_start, daemon=True).start()
    threading.Thread(target=self_health_checker, daemon=True).start()


if __name__ == "__main__":
    print("=" * 70)
    print("Penny Detector Service - Unified Architecture")
    print("=" * 70)
    print(f"Webhook URL: {WEBHOOK_URL}")
    print(f"Health Check URL: {health_check_url}")
    print(f"Self-ping interval: 7 minutes")
    print("=" * 70)

    port = int(os.getenv("PORT", 8000))
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting server on port {port}")
    print("=" * 70)

    uvicorn.run(app, host="0.0.0.0", port=port)
