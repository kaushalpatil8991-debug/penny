#!/usr/bin/env python3
"""
Penny Health Server - Keeps Render alive and runs fyers.py
"""

import os
import time
import threading
import subprocess
import sys
import requests
from datetime import datetime
from fastapi import FastAPI
import uvicorn

app = FastAPI()

health_check_url = "https://penny-00h7.onrender.com/health"

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

def start_detector():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    fyers_path = os.path.join(script_dir, "fyers.py")

    process = subprocess.Popen(
        [sys.executable, fyers_path],
        cwd=script_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        bufsize=1
    )
    print(f"Detector started with PID: {process.pid}")

    for line in iter(process.stdout.readline, ''):
        if line:
            print(f"[DETECTOR] {line.strip()}")

def self_ping():
    time.sleep(30)
    while True:
        try:
            time.sleep(420)
            requests.get(health_check_url, timeout=30)
            print(f"[PING] OK - {datetime.now().strftime('%H:%M:%S')}")
        except Exception as e:
            print(f"[PING] Failed - {e}")

if __name__ == "__main__":
    threading.Thread(target=start_detector, daemon=True).start()
    threading.Thread(target=self_ping, daemon=True).start()

    port = int(os.getenv("PORT", 8000))
    print(f"Penny server starting on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
