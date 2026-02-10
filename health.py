#!/usr/bin/env python3
"""
Enhanced FastAPI Health Check Server
Manages Render hosting, detector lifecycle, and self-health monitoring
"""

import os
import time
import threading
import subprocess
import sys
import requests
from datetime import datetime
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

app = FastAPI(title="Health Check Service", version="2.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global state
start_time = time.time()
detector_process = None
detector_starting = False
restart_count = 0
max_restarts = 5
last_restart_time = 0
manual_restart_count = 0
health_check_url = "https://fyers-volume-spike-detector.onrender.com/health"
last_self_health_check = time.time()

@app.get("/")
async def root():
    return {
        "message": "Health service running",
        "status": "online",
        "detector_status": "running" if (detector_process and detector_process.poll() is None) else "stopped"
    }

@app.get("/health")
async def health_check():
    """Primary health check endpoint"""
    global detector_process, restart_count, manual_restart_count
    detector_status = "unknown"
    
    if detector_process:
        if detector_process.poll() is None:
            detector_status = "running"
        else:
            detector_status = "stopped"
    else:
        detector_status = "not_started"
    
    return JSONResponse(
        status_code=200,
        content={
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "uptime": time.time() - start_time,
            "detector_status": detector_status,
            "restart_count": restart_count,
            "manual_restart_count": manual_restart_count,
            "service": "fyers-volume-spike-detector"
        }
    )

@app.post("/restart-detector")
async def restart_detector():
    """Manually restart the detector"""
    global detector_process, restart_count, manual_restart_count
    
    try:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Manual restart requested")
        
        # Stop current process if running
        if detector_process and detector_process.poll() is None:
            print("Stopping current detector process...")
            detector_process.terminate()
            time.sleep(2)
            if detector_process.poll() is None:
                detector_process.kill()
        
        # Reset restart counter for manual restart
        restart_count = 0
        manual_restart_count += 1
        
        # Start new process
        start_detector()
        
        return JSONResponse(
            status_code=200,
            content={
                "message": "Detector restart initiated",
                "restart_count": restart_count,
                "manual_restart_count": manual_restart_count,
                "timestamp": datetime.now().isoformat()
            }
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"Failed to restart detector: {str(e)}"}
        )

@app.post("/stop-detector")
async def stop_detector():
    """Stop the detector"""
    global detector_process
    
    try:
        if detector_process and detector_process.poll() is None:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Stopping detector...")
            detector_process.terminate()
            time.sleep(2)
            if detector_process.poll() is None:
                detector_process.kill()
            detector_process = None
            return JSONResponse(
                status_code=200,
                content={
                    "message": "Detector stopped",
                    "timestamp": datetime.now().isoformat()
                }
            )
        else:
            return JSONResponse(
                status_code=200,
                content={
                    "message": "Detector was not running",
                    "timestamp": datetime.now().isoformat()
                }
            )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"Failed to stop detector: {str(e)}"}
        )

@app.get("/status")
async def get_status():
    """Get detailed system status"""
    global detector_process, restart_count, manual_restart_count, last_self_health_check
    
    detector_status = "unknown"
    detector_pid = None
    
    if detector_process:
        if detector_process.poll() is None:
            detector_status = "running"
            detector_pid = detector_process.pid
        else:
            detector_status = "stopped"
            
    return JSONResponse(
        status_code=200,
        content={
            "health_service": {
                "status": "running",
                "uptime_seconds": time.time() - start_time,
                "uptime_minutes": (time.time() - start_time) / 60
            },
            "detector": {
                "status": detector_status,
                "pid": detector_pid,
                "restart_count": restart_count,
                "manual_restart_count": manual_restart_count,
                "max_restarts": max_restarts
            },
            "self_monitoring": {
                "last_health_check": datetime.fromtimestamp(last_self_health_check).isoformat(),
                "health_check_url": health_check_url
            },
            "timestamp": datetime.now().isoformat()
        }
    )

def start_detector():
    """Start the Fyers detector process"""
    global detector_process, detector_starting
    
    if detector_starting:
        print("Detector start already in progress")
        return
        
    detector_starting = True
    
    try:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting Fyers detector...")
        
        # Use fyers.py instead of fyers_detector.py
        detector_process = subprocess.Popen(
            [sys.executable, "fyers.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1
        )
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Detector started with PID: {detector_process.pid}")
        
        # Start a thread to read output
        def read_output():
            try:
                for line in iter(detector_process.stdout.readline, ''):
                    if line:
                        print(f"[DETECTOR] {line.strip()}")
            except Exception as e:
                print(f"[ERROR] Output reader error: {e}")
        
        threading.Thread(target=read_output, daemon=True).start()
        
    except Exception as e:
        print(f"[ERROR] Failed to start detector: {e}")
    finally:
        detector_starting = False

def monitor_detector():
    """Keep detector running with restart limits"""
    global detector_process, restart_count, last_restart_time
    
    # Wait before starting monitoring
    time.sleep(10)
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Detector monitor started")
    
    while True:
        try:
            if detector_process is None or detector_process.poll() is not None:
                current_time = time.time()
                
                # Check if we've exceeded max restarts within a time window
                if restart_count >= max_restarts:
                    if current_time - last_restart_time < 300:  # 5 minutes
                        print(f"[WARNING] Max restarts ({max_restarts}) reached within 5 minutes")
                        print("[WARNING] Stopping auto-restart. Manual intervention required.")
                        time.sleep(300)  # Wait 5 minutes before resetting counter
                        restart_count = 0
                        continue
                    else:
                        # Reset counter if enough time has passed
                        restart_count = 0
                
                if detector_process and detector_process.poll() is not None:
                    exit_code = detector_process.returncode
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Detector exited with code: {exit_code}")
                
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Restarting detector (attempt {restart_count + 1}/{max_restarts})...")
                restart_count += 1
                last_restart_time = current_time
                
                # Wait longer between restarts to avoid rapid cycling
                time.sleep(10)
                start_detector()
            
            time.sleep(30)  # Check every 30 seconds
            
        except Exception as e:
            print(f"[ERROR] Monitor error: {e}")
            time.sleep(10)

def self_health_checker():
    """Send health check requests to own endpoint every 7 minutes"""
    global last_self_health_check
    
    # Wait for service to fully start
    time.sleep(30)
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Self health checker started")
    print(f"[INFO] Will ping {health_check_url} every 7 minutes")
    
    while True:
        try:
            time.sleep(420)  # 7 minutes = 420 seconds
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Sending self health check...")
            
            response = requests.get(health_check_url, timeout=30)
            
            if response.status_code == 200:
                last_self_health_check = time.time()
                data = response.json()
                detector_status = data.get('detector_status', 'unknown')
                print(f"[HEALTH] ✓ Health check successful - Detector: {detector_status}")
            else:
                print(f"[HEALTH] ✗ Health check failed - Status code: {response.status_code}")
                
        except requests.exceptions.Timeout:
            print(f"[HEALTH] ✗ Health check timeout")
        except requests.exceptions.RequestException as e:
            print(f"[HEALTH] ✗ Health check error: {e}")
        except Exception as e:
            print(f"[ERROR] Unexpected error in health checker: {e}")

if __name__ == "__main__":
    print("="*70)
    print("Enhanced Health Server Starting")
    print("="*70)
    print(f"Service: Fyers Volume Spike Detector")
    print(f"Health Check URL: {health_check_url}")
    print(f"Self-ping interval: 7 minutes")
    print("="*70)
    
    # Start detector after a short delay
    def delayed_start():
        time.sleep(2)
        start_detector()
    
    threading.Thread(target=delayed_start, daemon=True).start()
    
    # Start detector monitor
    threading.Thread(target=monitor_detector, daemon=True).start()
    
    # Start self health checker
    threading.Thread(target=self_health_checker, daemon=True).start()
    
    # Start health server
    port = int(os.getenv("PORT", 8000))
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Health server starting on port {port}")
    print("="*70)
    
    uvicorn.run(app, host="0.0.0.0", port=port)
