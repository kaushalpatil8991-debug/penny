#!/usr/bin/env python3
"""
Simple FastAPI Health Check Server
Runs fyers.py and keeps Render hosting alive with periodic health pings
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

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("Environment variables loaded")
except ImportError:
    print("python-dotenv not installed")
except Exception as e:
    print(f"Could not load .env file: {e}")

app = FastAPI(title="Health Check Service", version="1.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

start_time = time.time()
fyers_process = None

# Health ping configuration
HEALTH_PING_URL = "https://penny-00h7.onrender.com/health"
HEALTH_PING_INTERVAL = 420  # 7 minutes in seconds
last_health_ping_time = 0
health_ping_success_count = 0
health_ping_failure_count = 0

@app.get("/")
async def root():
    return {
        "message": "Health service running", 
        "status": "online"
    }

@app.get("/health")
async def health_check():
    """Primary health check endpoint"""
    global fyers_process, health_ping_success_count, health_ping_failure_count
    
    fyers_status = "unknown"
    if fyers_process:
        if fyers_process.poll() is None:
            fyers_status = "running"
        else:
            fyers_status = "stopped"
    else:
        fyers_status = "not_started"
    
    return JSONResponse(
        status_code=200,
        content={
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "uptime": time.time() - start_time,
            "fyers_status": fyers_status,
            "health_ping_success": health_ping_success_count,
            "health_ping_failure": health_ping_failure_count
        }
    )

@app.get("/ping-stats")
async def ping_stats():
    """Get health ping statistics"""
    global health_ping_success_count, health_ping_failure_count, last_health_ping_time
    
    return JSONResponse(
        status_code=200,
        content={
            "health_ping_url": HEALTH_PING_URL,
            "ping_interval_seconds": HEALTH_PING_INTERVAL,
            "ping_interval_minutes": HEALTH_PING_INTERVAL / 60,
            "total_success": health_ping_success_count,
            "total_failure": health_ping_failure_count,
            "last_ping_time": datetime.fromtimestamp(last_health_ping_time).isoformat() if last_health_ping_time > 0 else "never",
            "next_ping_in_seconds": max(0, HEALTH_PING_INTERVAL - (time.time() - last_health_ping_time)) if last_health_ping_time > 0 else 0
        }
    )

@app.get("/fyers-status")
async def fyers_status():
    """Get fyers.py process status"""
    global fyers_process
    
    status = "unknown"
    pid = None
    
    if fyers_process:
        if fyers_process.poll() is None:
            status = "running"
            pid = fyers_process.pid
        else:
            status = "stopped"
            exit_code = fyers_process.returncode
            pid = f"exited with code {exit_code}"
    else:
        status = "not_started"
    
    return JSONResponse(
        status_code=200,
        content={
            "fyers_status": status,
            "fyers_pid": pid,
            "working_directory": os.getcwd(),
            "fyers_py_exists": os.path.exists("fyers.py")
        }
    )

def send_health_ping():
    """Send health ping to keep service alive"""
    global last_health_ping_time, health_ping_success_count, health_ping_failure_count
    
    try:
        print(f"Sending health ping to {HEALTH_PING_URL}")
        response = requests.get(HEALTH_PING_URL, timeout=10)
        
        if response.status_code == 200:
            health_ping_success_count += 1
            print(f"Health ping successful (#{health_ping_success_count})")
        else:
            health_ping_failure_count += 1
            print(f"Health ping failed with status {response.status_code} (#{health_ping_failure_count})")
        
        last_health_ping_time = time.time()
        
    except Exception as e:
        health_ping_failure_count += 1
        print(f"Health ping error (#{health_ping_failure_count}): {e}")
        last_health_ping_time = time.time()

def health_ping_worker():
    """Background worker to send periodic health pings"""
    global last_health_ping_time
    
    print(f"Health ping worker started - will ping every {HEALTH_PING_INTERVAL} seconds (7 minutes)")
    
    # Wait 30 seconds before starting
    time.sleep(30)
    
    while True:
        try:
            current_time = time.time()
            
            # Check if it's time to send a ping
            if current_time - last_health_ping_time >= HEALTH_PING_INTERVAL:
                send_health_ping()
            
            # Sleep for 1 minute before checking again
            time.sleep(60)
            
        except Exception as e:
            print(f"Health ping worker error: {e}")
            time.sleep(60)

def start_fyers():
    """Start the fyers.py process"""
    global fyers_process
    
    try:
        print("\n" + "="*60)
        print("STARTING FYERS.PY")
        print("="*60)
        
        # Find fyers.py
        possible_locations = [
            os.path.dirname(os.path.abspath(__file__)),
            os.getcwd(),
            "/opt/render/project/src",
            ".",
        ]
        
        fyers_path = None
        working_dir = None
        
        for location in possible_locations:
            test_path = os.path.join(location, "fyers.py")
            if os.path.exists(test_path):
                fyers_path = test_path
                working_dir = location
                break
        
        if not fyers_path:
            print("ERROR: fyers.py not found in any location")
            return False
        
        print(f"Found fyers.py at: {fyers_path}")
        print(f"Working directory: {working_dir}")
        
        # Start fyers.py as subprocess
        fyers_process = subprocess.Popen(
            [sys.executable, fyers_path],
            cwd=working_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1
        )
        
        print(f"fyers.py started with PID: {fyers_process.pid}")
        print("="*60 + "\n")
        
        # Start thread to read and print output
        def read_output():
            try:
                for line in iter(fyers_process.stdout.readline, ''):
                    if line:
                        print(f"[FYERS] {line.strip()}", flush=True)
            except Exception as e:
                print(f"Output reader error: {e}")
        
        threading.Thread(target=read_output, daemon=True).start()
        
        return True
        
    except Exception as e:
        print(f"Failed to start fyers.py: {e}")
        import traceback
        traceback.print_exc()
        return False

def monitor_fyers():
    """Monitor fyers.py process and restart if it stops"""
    global fyers_process
    
    print("Starting fyers.py monitor...")
    time.sleep(10)  # Wait before starting monitoring
    
    while True:
        try:
            if fyers_process is None or fyers_process.poll() is not None:
                if fyers_process and fyers_process.poll() is not None:
                    print(f"fyers.py exited with code: {fyers_process.returncode}")
                
                print("Restarting fyers.py...")
                time.sleep(5)
                start_fyers()
            
            time.sleep(30)  # Check every 30 seconds
            
        except Exception as e:
            print(f"Monitor error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    print("\n" + "="*60)
    print("HEALTH SERVER STARTING")
    print("="*60)
    print(f"Python version: {sys.version}")
    print(f"Current directory: {os.getcwd()}")
    print(f"Script path: {os.path.abspath(__file__)}")
    
    # List files in current directory
    try:
        print("\nFiles in current directory:")
        for f in sorted(os.listdir('.')):
            print(f"  - {f}")
    except Exception as e:
        print(f"Error listing directory: {e}")
    
    # Find and change to correct directory if needed
    possible_locations = [
        os.path.dirname(os.path.abspath(__file__)),
        os.getcwd(),
        "/opt/render/project/src",
        ".",
    ]
    
    working_dir = None
    for location in possible_locations:
        if os.path.exists(os.path.join(location, "fyers.py")):
            working_dir = location
            break
    
    if working_dir and os.getcwd() != working_dir:
        print(f"Changing working directory to: {working_dir}")
        os.chdir(working_dir)
    
    print(f"\nWorking directory: {os.getcwd()}")
    print(f"fyers.py exists: {os.path.exists('fyers.py')}")
    print("="*60 + "\n")
    
    # Start fyers.py after a short delay
    def delayed_start():
        time.sleep(2)
        start_fyers()
    
    threading.Thread(target=delayed_start, daemon=True).start()
    
    # Start fyers.py monitor
    threading.Thread(target=monitor_fyers, daemon=True).start()
    
    # Start health ping worker
    threading.Thread(target=health_ping_worker, daemon=True).start()
    print("Health ping worker scheduled to start in 30 seconds\n")
    
    # Start health server
    port = int(os.getenv("PORT", 8000))
    print(f"Starting health server on port {port}\n")
    uvicorn.run(app, host="0.0.0.0", port=port)
