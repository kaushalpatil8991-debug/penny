#!/usr/bin/env python3
"""
Fyers Volume Spike Detector - Google Sheets Integration with Sector Classification
Detects large individual trades and updates Google Sheets in real-time with sector information
"""

import json
import os
import sys
import time
import threading
import requests
import re
from datetime import datetime
from zoneinfo import ZoneInfo
import pyotp
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws
import gspread
from google.oauth2.service_account import Credentials

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("Environment variables loaded from .env file")
except ImportError:
    print("python-dotenv not installed. Install with: pip install python-dotenv")
except Exception as e:
    print(f"Could not load .env file: {e}")

# =============================================================================
# CONFIGURATION - Update these with your actual credentials
# =============================================================================

# Fyers API Credentials
FYERS_CLIENT_ID = "EH8TE9J6PZ-100"
FYERS_SECRET_KEY = "V8EC76L8UP"
FYERS_REDIRECT_URI = "https://fyersauth.vercel.app/"
FYERS_TOTP_SECRET = "7JKB7FFBMZNQRYYV7PQ46L7XRUQLR6FV"
FYERS_PIN = "8905"

# Trading Configuration
INDIVIDUAL_TRADE_THRESHOLD = 30000000  # Rs 3 crore for individual trades
MIN_VOLUME_SPIKE = 1000  # Minimum volume spike to consider

# Google Sheets Configuration
GOOGLE_SHEETS_ID = "1kgrKVjUm0lB0fz-74Q_C-sXls7IyyqFDGhf8NZmGG4A"

# =============================================================================
# RUN CONTROLLER GLOBALS
# =============================================================================

_running_flag = False
_stop_event = threading.Event()

# =============================================================================
# RUN CONTROLLER FUNCTIONS
# =============================================================================

def _start_stream_once():
    """Start your Fyers WebSocket loop exactly once."""
    global _running_flag, _stop_event
    if _running_flag:
        return False
    _stop_event.clear()
    threading.Thread(target=_stream_worker, args=(_stop_event,), daemon=True).start()
    _running_flag = True
    print("Stream STARTED")
    return True

def _stop_stream_once():
    """Signal your stream loop to stop and wait briefly."""
    global _running_flag, _stop_event
    if not _running_flag:
        return False
    _stop_event.set()
    time.sleep(2)
    _running_flag = False
    print("Stream STOPPED")
    return False

def _stream_worker(stop_event: threading.Event):
    """Simplified worker that runs the detector with proper error handling"""
    try:
        print("Starting detector stream worker (single attempt)", flush=True)
        detector = VolumeSpikeDetector()
        detector.stop_event = stop_event
        
        print("Initializing detector...", flush=True)
        if detector.initialize():
            print("Detector initialized successfully", flush=True)
            print("Starting monitoring...", flush=True)
            detector.start_monitoring()
            print("Monitoring ended normally", flush=True)
        else:
            print("Detector initialization failed - exiting", flush=True)
            return
            
    except Exception as e:
        print(f"Stream worker error: {e}", flush=True)
        import traceback
        traceback.print_exc()
        print("Stream worker exiting due to error", flush=True)
        return
    
    print("Stream worker stopped", flush=True)

def _inside_window_ist() -> bool:
    """Check if current IST time is within market hours."""
    # Check if force start flag is set
    import builtins
    if hasattr(builtins, 'FORCE_START') and builtins.FORCE_START:
        return True  # Always return True when forced
    
    now = datetime.now(ZoneInfo("Asia/Kolkata"))
    hhmm = now.strftime("%H:%M")
    return MARKET_START_TIME <= hhmm < MARKET_END_TIME

def supervisor_loop():
    """Simplified supervisor that manages the detector lifecycle"""
    print("Supervisor loop started", flush=True)
    detector = None
    last_auth_check = time.time()
    last_command_check = time.time()
    AUTH_CHECK_INTERVAL = 3600
    COMMAND_CHECK_INTERVAL = 3  # Check for commands every 10 seconds
    
    # Create telegram handler for checking commands
    telegram = TelegramHandler()
    
    while True:
        try:
            current_time = time.time()
            
            # Check for force/restart commands periodically
            if current_time - last_command_check > COMMAND_CHECK_INTERVAL:
                print("[SUPERVISOR] Checking for commands...", flush=True)
                
                # Check for force command
                if telegram.check_for_force_command():
                    print("[SUPERVISOR] Force command detected! Setting FORCE_START flag", flush=True)
                    telegram.send_message("🚀 <b>Force Start Initiated</b>\n\n⏳ Bypassing market hours...\n📊 Starting detector immediately...")
                    import builtins
                    builtins.FORCE_START = True
                
                # Check for restart command
                if telegram.check_for_restart_command():
                    print("[SUPERVISOR] Restart command detected! Restarting detector only...", flush=True)
                    
                    # Send status message
                    telegram.send_message("🔄 <b>Restarting Detector...</b>\n\n⏳ Stopping current detector process...")
                    
                    # Stop only the detector, not the summary scheduler
                    _stop_stream_once()
                    detector = None
                    
                    telegram.send_message("✅ Detector stopped\n⏳ Starting fresh detector instance...")
                    
                    time.sleep(2)
                    
                    # Create new detector instance
                    try:
                        detector = VolumeSpikeDetector()
                        telegram.send_message("🔧 Detector instance created\n⏳ Initializing...")
                        
                        _start_stream_once()
                        
                        telegram.send_message("✅ <b>Detector Restarted Successfully!</b>\n\n📊 Status: Monitoring active\n⏰ Time: " + datetime.now().strftime('%H:%M:%S'))
                        print("[SUPERVISOR] Detector restarted successfully", flush=True)
                        
                    except Exception as restart_error:
                        error_msg = f"❌ <b>Restart Failed</b>\n\nError: {str(restart_error)}\n\nPlease try again or check logs."
                        telegram.send_message(error_msg)
                        print(f"[SUPERVISOR] Restart error: {restart_error}", flush=True)
                    
                    # Continue to next iteration
                    last_command_check = current_time
                    continue
                
                last_command_check = current_time
            
            # Check if we're in market hours
            in_window = _inside_window_ist()
            
            if SCHEDULING_ENABLED and not in_window:
                if detector:
                    print("Outside market hours, stopping detector...", flush=True)
                    _stop_stream_once()
                    detector = None
                # Don't spam logs, only print occasionally
                if int(current_time) % 300 == 0:  # Every 5 minutes
                    print("Waiting for market hours (or send 'force' command)...", flush=True)
                time.sleep(60)
                continue
            
            # We should be running - start detector if not running
            if not detector or not _running_flag:
                print("Starting detector...", flush=True)
                _stop_stream_once()
                time.sleep(2)
                
                try:
                    detector = VolumeSpikeDetector()
                    print("Detector instance created, starting stream...", flush=True)
                    _start_stream_once()
                    print("Stream started successfully", flush=True)
                    
                    # Send connection status to user after successful start
                    try:
                        # Wait a moment for initialization to complete
                        time.sleep(3)
                        
                        # Check if detector is actually running and authenticated
                        if detector and hasattr(detector, 'authenticator') and detector.authenticator.is_authenticated:
                            # Get user info if available
                            user_name = "Unknown"
                            try:
                                if detector.authenticator.fyers_model:
                                    profile = detector.authenticator.fyers_model.get_profile()
                                    if profile.get('s') == 'ok':
                                        user_name = profile['data']['name']
                            except:
                                pass
                            
                            connection_msg = f"""✅ <b>Detector Connected Successfully!</b>

👤 <b>User:</b> {user_name}
📊 <b>Status:</b> Monitoring Active
🎯 <b>Symbols:</b> {len(STOCK_SYMBOLS)} stocks
💰 <b>Threshold:</b> Rs {INDIVIDUAL_TRADE_THRESHOLD/10000000:.1f} Cr
⏰ <b>Started:</b> {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}

🔔 You will receive alerts for large volume spikes!"""
                            
                            telegram.send_message(connection_msg)
                            print("Connection status sent to user", flush=True)
                            
                    except Exception as msg_error:
                        print(f"Could not send connection message: {msg_error}", flush=True)
                        
                except Exception as start_error:
                    print(f"Error starting detector: {start_error}", flush=True)
                    telegram.send_message(f"❌ <b>Detector Start Failed</b>\n\nError: {str(start_error)}\n\nWill retry in 30 seconds...")
                    time.sleep(30)
                
            # Periodic auth check (every hour)
            if current_time - last_auth_check > AUTH_CHECK_INTERVAL:
                print("Performing periodic auth check...", flush=True)
                if detector and hasattr(detector, 'authenticator'):
                    if not detector.authenticator.is_authenticated:
                        print("Auth expired, will re-authenticate on next cycle", flush=True)
                        _stop_stream_once()
                        detector = None
                last_auth_check = current_time
            
            # Sleep before next check
            time.sleep(5)  # Check more frequently
            
        except Exception as e:
            print(f"Supervisor error: {e}", flush=True)
            import traceback
            traceback.print_exc()
            time.sleep(10)

# Load Google Credentials from Environment Variables
try:
    google_creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
    if google_creds_json:
        GOOGLE_CREDENTIALS = json.loads(google_creds_json)
        if 'private_key' in GOOGLE_CREDENTIALS:
            GOOGLE_CREDENTIALS['private_key'] = GOOGLE_CREDENTIALS['private_key'].replace('\\n', '\n')
        print("Google Sheets credentials loaded from environment variable")
    else:
        private_key = os.getenv('GOOGLE_PRIVATE_KEY')
        if private_key:
            private_key = private_key.replace('\\n', '\n')
        
        GOOGLE_CREDENTIALS = {
            "type": "service_account",
            "project_id": os.getenv('GOOGLE_PROJECT_ID'),
            "private_key_id": os.getenv('GOOGLE_PRIVATE_KEY_ID'),
            "private_key": private_key,
            "client_email": os.getenv('GOOGLE_CLIENT_EMAIL'),
            "client_id": os.getenv('GOOGLE_CLIENT_ID'),
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": os.getenv('GOOGLE_CLIENT_X509_CERT_URL'),
            "universe_domain": "googleapis.com"
        }
        
        required_fields = ['project_id', 'private_key_id', 'private_key', 'client_email', 'client_id']
        missing_fields = [field for field in required_fields if not GOOGLE_CREDENTIALS.get(field)]
        
        if missing_fields:
            print(f"Missing required Google credentials environment variables: {', '.join(missing_fields)}")
            GOOGLE_CREDENTIALS = None
        else:
            print("Google Sheets credentials loaded from individual environment variables")
            
except json.JSONDecodeError as e:
    print(f"Error parsing Google credentials JSON from environment: {e}")
    GOOGLE_CREDENTIALS = None
except Exception as e:
    print(f"Error loading Google credentials from environment: {e}")
    GOOGLE_CREDENTIALS = None

# Load Fyers Access Token from JSON file or environment variables
try:
    with open('fyers_access_token.json', 'r') as f:
        token_data = json.load(f)
        FYERS_ACCESS_TOKEN = token_data.get('access_token', '')
        FYERS_TOKEN_TIMESTAMP = float(token_data.get('timestamp', 0))
        FYERS_TOKEN_CREATED_AT = token_data.get('created_at', '')
        print("Fyers access token loaded from JSON file")
except FileNotFoundError:
    FYERS_ACCESS_TOKEN = os.getenv('FYERS_ACCESS_TOKEN', '')
    FYERS_TOKEN_TIMESTAMP = float(os.getenv('FYERS_TOKEN_TIMESTAMP', '0'))
    FYERS_TOKEN_CREATED_AT = os.getenv('FYERS_TOKEN_CREATED_AT', '')
    print("Fyers access token JSON file not found, using environment variables")
except Exception as e:
    FYERS_ACCESS_TOKEN = os.getenv('FYERS_ACCESS_TOKEN', '')
    FYERS_TOKEN_TIMESTAMP = float(os.getenv('FYERS_TOKEN_TIMESTAMP', '0'))
    FYERS_TOKEN_CREATED_AT = os.getenv('FYERS_TOKEN_CREATED_AT', '')
    print(f"Error loading Fyers token from JSON: {e}, using environment variables")

def validate_fyers_token_from_json():
    """Validate if the Fyers token from JSON file is still valid"""
    try:
        if not FYERS_ACCESS_TOKEN or FYERS_ACCESS_TOKEN.strip() == "":
            return False, "No token available"
        
        current_time = time.time()
        token_time = FYERS_TOKEN_TIMESTAMP
        
        if current_time - token_time < 28800:
            print("Fyers token from JSON file is valid")
            return True, "Token is valid"
        else:
            print("Fyers token from JSON file expired, need fresh authentication")
            return False, "Token expired"
            
    except Exception as e:
        print(f"Error validating Fyers token: {e}")
        return False, f"Validation error: {str(e)}"

def save_fyers_token_to_json(access_token, timestamp=None, created_at=None):
    """Save Fyers access token to JSON file"""
    try:
        if timestamp is None:
            timestamp = time.time()
        if created_at is None:
            created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        token_data = {
            "access_token": access_token,
            "timestamp": timestamp,
            "created_at": created_at
        }
        
        with open('fyers_access_token.json', 'w') as f:
            json.dump(token_data, f, indent=2)
        
        print("Fyers access token saved to JSON file")
        return True
        
    except Exception as e:
        print(f"Error saving Fyers token to JSON: {e}")
        return False

# Telegram Configuration - For Detector
TELEGRAM_BOT_TOKEN = "8564259065:AAGM5clL_pOagSAEiNURB0ltu1E0yvdC-4Q"
TELEGRAM_CHAT_ID = "8388919023"
TELEGRAM_POLLING_INTERVAL = 5
TELEGRAM_AUTH_TIMEOUT = 300

# Login URL Retry Configuration
LOGIN_URL_RETRY_INTERVAL = 300  # 5 minutes in seconds
LOGIN_URL_SENT_FLAG = False  # Track if URL was already sent in current auth attempt

# Scheduling Configuration
MARKET_START_TIME = "09:13"
MARKET_END_TIME = "16:00"
SCHEDULING_ENABLED = True

# =============================================================================
# COMPREHENSIVE SECTOR MAPPING FOR NSE STOCKS
# =============================================================================

SECTOR_MAPPING = {
    # Textiles & Apparel
    "NSE:ROLLT-EQ": "Textiles",
    "NSE:TRIDENT-EQ": "Textiles",
    "NSE:VIPCLOTHNG-EQ": "Textiles",
    "NSE:INDORAMA-EQ": "Textiles",
    "NSE:FILATEX-EQ": "Textiles",
    "NSE:RSWM-EQ": "Textiles",
    "NSE:ABFRL-EQ": "Textiles",
    
    # Pharmaceuticals & Healthcare
    "NSE:ESSENTIA-EQ": "Pharmaceuticals",
    "NSE:NIVABUPA-EQ": "Pharmaceuticals",
    "NSE:KOPRAN-EQ": "Pharmaceuticals",
    "NSE:GPTHEALTH-EQ": "Pharmaceuticals",
    
    # Auto Components
    "NSE:SUNDARAM-EQ": "Automobiles",
    "NSE:OMAXAUTO-EQ": "Automobiles",
    "NSE:ROLEXRINGS-EQ": "Automobiles",
    "NSE:TVSSCS-EQ": "Automobiles",
    "NSE:MOTHERSON-EQ": "Automobiles",
    "NSE:UCAL-EQ": "Automobiles",
    "NSE:REMSONSIND-EQ": "Automobiles",
    "NSE:GANDHAR-EQ": "Automobiles",
    "NSE:JTEKTINDIA-EQ": "Automobiles",
    "NSE:ASHOKLEY-EQ": "Automobiles",
    "NSE:AUTOIND-EQ": "Automobiles",
    
    # Oil & Gas
    "NSE:PARASPETRO-EQ": "Oil & Gas",
    "NSE:GULFPETRO-EQ": "Oil & Gas",
    "NSE:CONFIPET-EQ": "Oil & Gas",
    "NSE:MRPL-EQ": "Oil & Gas",
    
    # Financial Services - Housing Finance
    "NSE:RHFL-EQ": "Financial Services",
    "NSE:BAJAJHFL-EQ": "Financial Services",
    "NSE:PNBGILTS-EQ": "Financial Services",
    "NSE:FEDFINA-EQ": "Financial Services",
    
    # Sugar & Agriculture
    "NSE:DAVANGERE-EQ": "Agriculture",
    "NSE:DWARKESH-EQ": "Agriculture",
    "NSE:HMAAGRO-EQ": "Agriculture",
    "NSE:PARADEEP-EQ": "Agriculture",
    
    # Metals & Mining
    "NSE:RAJMET-EQ": "Metals & Mining",
    "NSE:RAMASTEEL-EQ": "Metals & Mining",
    "NSE:MANAKSTEEL-EQ": "Metals & Mining",
    "NSE:VISAKAIND-EQ": "Metals & Mining",
    "NSE:NMDC-EQ": "Metals & Mining",
    "NSE:ELECTCAST-EQ": "Metals & Mining",
    "NSE:RAIN-EQ": "Metals & Mining",
    
    # Construction & Infrastructure
    "NSE:SARVESHWAR-EQ": "Construction",
    "NSE:RUCHINFRA-EQ": "Construction",
    "NSE:JYOTISTRUC-EQ": "Construction",
    "NSE:SADBHAV-EQ": "Construction",
    "NSE:IRB-EQ": "Construction",
    "NSE:ATLANTAA-EQ": "Construction",
    "NSE:OMAXE-EQ": "Construction",
    "NSE:GPTINFRA-EQ": "Construction",
    "NSE:NBCC-EQ": "Construction",
    "NSE:MANINFRA-EQ": "Construction",
    "NSE:URBANCO-EQ": "Construction",
    "NSE:GMRAIRPORT-EQ": "Construction",
    
    # Agri Products & Seeds
    "NSE:AGSTRA-EQ": "Agriculture",
    "NSE:KRITINUT-EQ": "Agriculture",
    
    # Cement
    "NSE:KESORAMIND-EQ": "Cement",
    "NSE:SANGHIIND-EQ": "Cement",
    "NSE:JSWCEMENT-EQ": "Cement",
    
    # Gems & Jewellery
    "NSE:LYPSAGEMS-EQ": "Consumer Goods",
    "NSE:RBZJEWEL-EQ": "Consumer Goods",
    
    # Real Estate
    "NSE:UNITECH-EQ": "Real Estate",
    "NSE:ATALREAL-EQ": "Real Estate",
    "NSE:KOHINOOR-EQ": "Real Estate",
    "NSE:IMAGICAA-EQ": "Real Estate",
    "NSE:VASWANI-EQ": "Real Estate",
    "NSE:UNIVASTU-EQ": "Real Estate",
    "NSE:TARC-EQ": "Real Estate",
    
    # Travel & Tourism
    "NSE:EASEMYTRIP-EQ": "Travel & Transport",
    "NSE:THOMASCOOK-EQ": "Travel & Transport",
    
    # Financial Services - NBFC
    "NSE:AFIL-EQ": "Financial Services",
    "NSE:BAIDFIN-EQ": "Financial Services",
    "NSE:CAPTRUST-EQ": "Financial Services",
    "NSE:CIFL-EQ": "Financial Services",
    "NSE:PAISALO-EQ": "Financial Services",
    "NSE:CENTRUM-EQ": "Financial Services",
    "NSE:ARFIN-EQ": "Financial Services",
    "NSE:GEOJITFSL-EQ": "Financial Services",
    "NSE:EDELWEISS-EQ": "Financial Services",
    "NSE:MUFIN-EQ": "Financial Services",
    "NSE:ARIHANTCAP-EQ": "Financial Services",
    "NSE:JMFINANCIL-EQ": "Financial Services",
    "NSE:SAMMAANCAP-EQ": "Financial Services",
    "NSE:BIRLAMONEY-EQ": "Financial Services",
    
    # Paper & Packaging
    "NSE:ASTRON-EQ": "Paper & Packaging",
    "NSE:PDMJEPAPER-EQ": "Paper & Packaging",
    "NSE:PAKKA-EQ": "Paper & Packaging",
    
    # IT Services & Software
    "NSE:VAKRANGEE-EQ": "Information Technology",
    "NSE:3IINFOLTD-EQ": "Information Technology",
    "NSE:TRACXN-EQ": "Information Technology",
    "NSE:VERTOZ-EQ": "Information Technology",
    "NSE:ISFT-EQ": "Information Technology",
    "NSE:XELPMOC-EQ": "Information Technology",
    "NSE:DIGITIDE-EQ": "Information Technology",
    
    # Telecommunications
    "NSE:GTL-EQ": "Telecommunications",
    "NSE:IDEA-EQ": "Telecommunications",
    "NSE:HATHWAY-EQ": "Telecommunications",
    "NSE:DEN-EQ": "Telecommunications",
    "NSE:TTML-EQ": "Telecommunications",
    "NSE:STLTECH-EQ": "Telecommunications",
    
    # Building Materials
    "NSE:SAMBHAAV-EQ": "Construction",
    "NSE:AXITA-EQ": "Construction",
    "NSE:SANGINITA-EQ": "Construction",
    
    # Power & Energy
    "NSE:DPSCLTD-EQ": "Power",
    "NSE:URJA-EQ": "Power",
    "NSE:INDOWIND-EQ": "Power",
    "NSE:JPPOWER-EQ": "Power",
    "NSE:RPOWER-EQ": "Power",
    "NSE:SUZLON-EQ": "Power",
    "NSE:INOXWIND-EQ": "Power",
    "NSE:WEBELSOLAR-EQ": "Power",
    "NSE:ADANIPOWER-EQ": "Power",
    "NSE:SJVN-EQ": "Power",
    "NSE:IREDA-EQ": "Power",
    "NSE:IRFC-EQ": "Power",
    "NSE:PTC-EQ": "Power",
    "NSE:BEPL-EQ": "Power",
    
    # Steel Products
    "NSE:SGL-EQ": "Metals & Mining",
    "NSE:BALAJEE-EQ": "Metals & Mining",
    "NSE:BIRLACABLE-EQ": "Metals & Mining",
    
    # IT Hardware
    "NSE:HCL-INSYS-EQ": "Information Technology",
    
    # Logistics & Supply Chain
    "NSE:PATINTLOG-EQ": "Travel & Transport",
    "NSE:SNOWMAN-EQ": "Travel & Transport",
    "NSE:SOMICONVEY-EQ": "Travel & Transport",
    "NSE:GAEL-EQ": "Travel & Transport",
    "NSE:BLUSPRING-EQ": "Travel & Transport",
    
    # Insurance & Asset Management
    "NSE:NECLIFE-EQ": "Financial Services",
    
    # Plastic & Polymer
    "NSE:SYNCOMF-EQ": "Chemicals",
    "NSE:DCMNVL-EQ": "Chemicals",
    
    # Electronics & Electrical
    "NSE:HARDWYN-EQ": "Consumer Goods",
    "NSE:CUBEXTUB-EQ": "Consumer Goods",
    "NSE:EXICOM-EQ": "Consumer Goods",
    
    # Media & Entertainment
    "NSE:CINEVISTA-EQ": "Media",
    "NSE:ARCHIES-EQ": "Media",
    "NSE:NETWORK18-EQ": "Media",
    "NSE:NDTV-EQ": "Media",
    "NSE:ENIL-EQ": "Media",
    
    # Chemicals & Petrochemicals
    "NSE:ASHIMASYN-EQ": "Chemicals",
    "NSE:ESTER-EQ": "Chemicals",
    "NSE:KHAICHEM-EQ": "Chemicals",
    "NSE:DCWLTD-EQ": "Chemicals",
    "NSE:ANIKINDS-EQ": "Chemicals",
    "NSE:INDOAMIN-EQ": "Chemicals",
    "NSE:APCL-EQ": "Chemicals",
    "NSE:ADVANCE-EQ": "Chemicals",
    
    # Banking
    "NSE:YESBANK-EQ": "Banking",
    "NSE:CENTRALBK-EQ": "Banking",
    "NSE:MAHABANK-EQ": "Banking",
    "NSE:IDFCFIRSTB-EQ": "Banking",
    "NSE:J&KBANK-EQ": "Banking",
    "NSE:PNB-EQ": "Banking",
    "NSE:BANKINDIA-EQ": "Banking",
    "NSE:BANDHANBNK-EQ": "Banking",
    "NSE:CANBK-EQ": "Banking",
    
    # Diversified
    "NSE:RUSHIL-EQ": "Diversified",
    "NSE:USK-EQ": "Diversified",
    "NSE:LANCORHOL-EQ": "Diversified",
    
    # Engineering & Capital Goods
    "NSE:TREL-EQ": "Construction",
    "NSE:NSLNISP-EQ": "Construction",
    "NSE:RAJOOENG-EQ": "Construction",
    "NSE:APTECHT-EQ": "Construction",
    "NSE:BEDMUTHA-EQ": "Construction",
    "NSE:VIKRAN-EQ": "Construction",
    "NSE:LOKESHMACH-EQ": "Construction",
    
    # Food Processing & FMCG
    "NSE:TAKE-EQ": "FMCG",
    "NSE:SUMEETINDS-EQ": "FMCG",
    "NSE:DEVYANI-EQ": "FMCG",
    
    # Fashion & Lifestyle
    "NSE:IRISDOREME-EQ": "Consumer Goods",
    
    # Pharmaceuticals - API/Intermediates
    "NSE:SIGACHI-EQ": "Pharmaceuticals",
    "NSE:LAXMIINDIA-EQ": "Pharmaceuticals",
    
    # Textiles - Technical Textiles
    "NSE:FIBERWEB-EQ": "Textiles",
    
    # Steel Trading & Distribution
    "NSE:MIRZAINT-EQ": "Metals & Mining",
    
    # Cables & Wires
    "NSE:PARACABLES-EQ": "Consumer Goods",
    "NSE:DIACABS-EQ": "Consumer Goods",
    
    # Microfinance
    "NSE:MICEL-EQ": "Financial Services",
    
    # Railways & Defense
    "NSE:RTNINDIA-EQ": "Construction",
    
    # Entertainment & Gaming
    "NSE:CTE-EQ": "Media",
    "NSE:DELTACORP-EQ": "Media",
    
    # Consulting & Analytics
    "NSE:JISLJALEQS-EQ": "Information Technology",
    "NSE:SAGILITY-EQ": "Information Technology",
    
    # Industrial Manufacturing
    "NSE:BLKASHYAP-EQ": "Construction",
    "NSE:SCILAL-EQ": "Construction",
    "NSE:AARTECH-EQ": "Construction",
    
    # Real Estate - Commercial
    "NSE:VASCONEQ-EQ": "Real Estate",
    
    # Hotels & Hospitality
    "NSE:ADVANIHOTR-EQ": "Hotels & Tourism",
    "NSE:LEMONTREE-EQ": "Hotels & Tourism",
    
    # Diversified - Trading
    "NSE:ORICONENT-EQ": "Diversified",
    
    # Plastic Products
    "NSE:ROTO-EQ": "Chemicals",
    
    # Textiles - Home Furnishing
    "NSE:VMSTMT-EQ": "Textiles",
    
    # Chemicals - Specialty
    "NSE:DCW-EQ": "Chemicals",
    
    # Petrochemicals
    "NSE:MANALIPETC-EQ": "Chemicals",
    
    # Fertilizers
    "NSE:NFL-EQ": "Agriculture",
    
    # Gaming & Entertainment
    "NSE:MOL-EQ": "Media",
    
    # Financial Services - Broking
    "NSE:DJML-EQ": "Financial Services",
    
    # Coating & Paints
    "NSE:TFCILTD-EQ": "Chemicals",
    
    # Paper - Writing & Printing
    "NSE:SSDL-EQ": "Paper & Packaging",
    
    # Specialty Products
    "NSE:REGAAL-EQ": "Consumer Goods",
    "NSE:VPRPL-EQ": "Consumer Goods",
    
    # Oilfield Services
    "NSE:IOLCP-EQ": "Oil & Gas",
    
    # Engineering Services
    "NSE:KUANTUM-EQ": "Construction",
    
    # Paper - Packaging
    "NSE:NAVKARCORP-EQ": "Paper & Packaging",
    
    # Shipping & Ports
    "NSE:RGL-EQ": "Travel & Transport",
    
    # Paper - Industrial
    "NSE:RUCHIRA-EQ": "Paper & Packaging",
    
    # Building Products
    "NSE:SHANKARA-EQ": "Construction",
    
    # Travel Services
    "NSE:DREAMFOLKS-EQ": "Travel & Transport",
    "NSE:MASTERTR-EQ": "Travel & Transport",
    
    # Consulting Services
    "NSE:SECMARK-EQ": "Information Technology",
    
    # Infrastructure Services
    "NSE:WCIL-EQ": "Construction",
    
    # Financial Services - Asset Reconstruction
    "NSE:SDBL-EQ": "Financial Services",
    
    # Metals - Aluminum
    "NSE:ABLBL-EQ": "Metals & Mining",
    
    # Aerospace Components
    "NSE:SPARC-EQ": "Automobiles",
    
    # Diversified Manufacturing
    "NSE:VMM-EQ": "Diversified",
    
    # Industrial Supplies
    "NSE:AARVI-EQ": "Construction",
    
    # Auto Ancillaries
    "NSE:PWL-EQ": "Automobiles",
    "NSE:MANBA-EQ": "Automobiles",
    "NSE:PRSMJOHNSN-EQ": "Automobiles",
    
    # Petrochemicals - Specialty
    "NSE:KOTHARIPET-EQ": "Chemicals",
    
    # Textiles - Denim
    "NSE:BOMDYEING-EQ": "Textiles",
    
    # Electricals
    "NSE:TOLINS-EQ": "Consumer Goods",
    
    # Telecom Equipment
    "NSE:BSHSL-EQ": "Telecommunications",
    
    # Chemicals - Industrial
    "NSE:RELTD-EQ": "Chemicals",
    
    # Metals - Diversified
    "NSE:INDOUS-EQ": "Metals & Mining",
    
    # Fintech & Digital Services
    "NSE:GROWW-EQ": "Financial Services",
}

def get_sector_for_symbol(symbol):
    """Get sector for a given symbol"""
    return SECTOR_MAPPING.get(symbol, "Others")

# =============================================================================
# STOCK SYMBOLS (keeping existing list)
# =============================================================================

STOCK_SYMBOLS = [
    'NSE:ROLLT-EQ',
    'NSE:ESSENTIA-EQ',
    'NSE:SUNDARAM-EQ',
    'NSE:PARASPETRO-EQ',
    'NSE:RHFL-EQ',
    'NSE:DAVANGERE-EQ',
    'NSE:RAJMET-EQ',
    'NSE:SARVESHWAR-EQ',
    'NSE:AGSTRA-EQ',
    'NSE:KESORAMIND-EQ',
    'NSE:LYPSAGEMS-EQ',
    'NSE:UNITECH-EQ',
    'NSE:RUCHINFRA-EQ',
    'NSE:EASEMYTRIP-EQ',
    'NSE:AFIL-EQ',
    'NSE:ASTRON-EQ',
    'NSE:VAKRANGEE-EQ',
    'NSE:GTL-EQ',
    'NSE:SAMBHAAV-EQ',
    'NSE:AXITA-EQ',
    'NSE:RAMASTEEL-EQ',
    'NSE:IDEA-EQ',
    'NSE:BAIDFIN-EQ',
    'NSE:SANGINITA-EQ',
    'NSE:DPSCLTD-EQ',
    'NSE:JYOTISTRUC-EQ',
    'NSE:SADBHAV-EQ',
    'NSE:URJA-EQ',
    'NSE:HATHWAY-EQ',
    'NSE:CAPTRUST-EQ',
    'NSE:SGL-EQ',
    'NSE:HCL-INSYS-EQ',
    'NSE:PATINTLOG-EQ',
    'NSE:NECLIFE-EQ',
    'NSE:SYNCOMF-EQ',
    'NSE:INDOWIND-EQ',
    'NSE:3IINFOLTD-EQ',
    'NSE:HARDWYN-EQ',
    'NSE:CINEVISTA-EQ',
    'NSE:JPPOWER-EQ',
    'NSE:ARCHIES-EQ',
    'NSE:ASHIMASYN-EQ',
    'NSE:YESBANK-EQ',
    'NSE:ATALREAL-EQ',
    'NSE:RUSHIL-EQ',
    'NSE:USK-EQ',
    'NSE:LANCORHOL-EQ',
    'NSE:TREL-EQ',
    'NSE:TRIDENT-EQ',
    'NSE:KOHINOOR-EQ',
    'NSE:CENTRUM-EQ',
    'NSE:HMAAGRO-EQ',
    'NSE:TAKE-EQ',
    'NSE:SUMEETINDS-EQ',
    'NSE:DEN-EQ',
    'NSE:VIPCLOTHNG-EQ',
    'NSE:IRISDOREME-EQ',
    'NSE:CIFL-EQ',
    'NSE:SIGACHI-EQ',
    'NSE:PAISALO-EQ',
    'NSE:GULFPETRO-EQ',
    'NSE:FIBERWEB-EQ',
    'NSE:CONFIPET-EQ',
    'NSE:CENTRALBK-EQ',
    'NSE:MIRZAINT-EQ',
    'NSE:RPOWER-EQ',
    'NSE:PARACABLES-EQ',
    'NSE:DWARKESH-EQ',
    'NSE:BALAJEE-EQ',
    'NSE:NSLNISP-EQ',
    'NSE:MICEL-EQ',
    'NSE:SNOWMAN-EQ',
    'NSE:IRB-EQ',
    'NSE:RTNINDIA-EQ',
    'NSE:ATLANTAA-EQ',
    'NSE:NETWORK18-EQ',
    'NSE:CTE-EQ',
    'NSE:TRACXN-EQ',
    'NSE:JISLJALEQS-EQ',
    'NSE:BLKASHYAP-EQ',
    'NSE:SCILAL-EQ',
    'NSE:AARTECH-EQ',
    'NSE:SAGILITY-EQ',
    'NSE:IMAGICAA-EQ',
    'NSE:VASCONEQ-EQ',
    'NSE:INDORAMA-EQ',
    'NSE:TTML-EQ',
    'NSE:FILATEX-EQ',
    'NSE:ADVANIHOTR-EQ',
    'NSE:VASWANI-EQ',
    'NSE:SUZLON-EQ',
    'NSE:ARFIN-EQ',
    'NSE:ORICONENT-EQ',
    'NSE:MAHABANK-EQ',
    'NSE:MANAKSTEEL-EQ',
    'NSE:ROTO-EQ',
    'NSE:VMSTMT-EQ',
    'NSE:DCW-EQ',
    'NSE:SANGHIIND-EQ',
    'NSE:MANALIPETC-EQ',
    'NSE:AUTOIND-EQ',
    'NSE:UNIVASTU-EQ',
    'NSE:ANIKINDS-EQ',
    'NSE:VISAKAIND-EQ',
    'NSE:MOL-EQ',
    'NSE:DELTACORP-EQ',
    'NSE:OMAXE-EQ',
    'NSE:GEOJITFSL-EQ',
    'NSE:BLUSPRING-EQ',
    'NSE:DJML-EQ',
    'NSE:TFCILTD-EQ',
    'NSE:VERTOZ-EQ',
    'NSE:NMDC-EQ',
    'NSE:ELECTCAST-EQ',
    'NSE:NIVABUPA-EQ',
    'NSE:RAJOOENG-EQ',
    'NSE:ABFRL-EQ',
    'NSE:SJVN-EQ',
    'NSE:IDFCFIRSTB-EQ',
    'NSE:SSDL-EQ',
    'NSE:KRITINUT-EQ',
    'NSE:REGAAL-EQ',
    'NSE:VPRPL-EQ',
    'NSE:PNBGILTS-EQ',
    'NSE:NDTV-EQ',
    'NSE:KHAICHEM-EQ',
    'NSE:NFL-EQ',
    'NSE:OMAXAUTO-EQ',
    'NSE:ISFT-EQ',
    'NSE:IOLCP-EQ',
    'NSE:BEPL-EQ',
    'NSE:CUBEXTUB-EQ',
    'NSE:KUANTUM-EQ',
    'NSE:ARIHANTCAP-EQ',
    'NSE:NAVKARCORP-EQ',
    'NSE:PDMJEPAPER-EQ',
    'NSE:APTECHT-EQ',
    'NSE:ROLEXRINGS-EQ',
    'NSE:BAJAJHFL-EQ',
    'NSE:BEDMUTHA-EQ',
    'NSE:STLTECH-EQ',
    'NSE:WEBELSOLAR-EQ',
    'NSE:VIKRAN-EQ',
    'NSE:GMRAIRPORT-EQ',
    'NSE:GAEL-EQ',
    'NSE:J&KBANK-EQ',
    'NSE:BSHSL-EQ',
    'NSE:TVSSCS-EQ',
    'NSE:ESTER-EQ',
    'NSE:RAIN-EQ',
    'NSE:GPTINFRA-EQ',
    'NSE:EDELWEISS-EQ',
    'NSE:MUFIN-EQ',
    'NSE:EXICOM-EQ',
    'NSE:MOTHERSON-EQ',
    'NSE:SECMARK-EQ',
    'NSE:PAKKA-EQ',
    'NSE:MASTERTR-EQ',
    'NSE:WCIL-EQ',
    'NSE:JSWCEMENT-EQ',
    'NSE:NBCC-EQ',
    'NSE:IRFC-EQ',
    'NSE:XELPMOC-EQ',
    'NSE:SDBL-EQ',
    'NSE:DREAMFOLKS-EQ',
    'NSE:RGL-EQ',
    'NSE:RUCHIRA-EQ',
    'NSE:SHANKARA-EQ',
    'NSE:UCAL-EQ',
    'NSE:REMSONSIND-EQ',
    'NSE:INDOAMIN-EQ',
    'NSE:PNB-EQ',
    'NSE:ENIL-EQ',
    'NSE:APCL-EQ',
    'NSE:SOMICONVEY-EQ',
    'NSE:ADVANCE-EQ',
    'NSE:MANINFRA-EQ',
    'NSE:PWL-EQ',
    'NSE:AARVI-EQ',
    'NSE:GANDHAR-EQ',
    'NSE:ABLBL-EQ',
    'NSE:SPARC-EQ',
    'NSE:VMM-EQ',
    'NSE:INOXWIND-EQ',
    'NSE:TARC-EQ',
    'NSE:URBANCO-EQ',
    'NSE:LAXMIINDIA-EQ',
    'NSE:KOPRAN-EQ',
    'NSE:DEVYANI-EQ',
    'NSE:MANBA-EQ',
    'NSE:PRSMJOHNSN-EQ',
    'NSE:DIGITIDE-EQ',
    'NSE:DIACABS-EQ',
    'NSE:KOTHARIPET-EQ',
    'NSE:RBZJEWEL-EQ',
    'NSE:DCMNVL-EQ',
    'NSE:GPTHEALTH-EQ',
    'NSE:BOMDYEING-EQ',
    'NSE:JMFINANCIL-EQ',
    'NSE:BANKINDIA-EQ',
    'NSE:FEDFINA-EQ',
    'NSE:THOMASCOOK-EQ',
    'NSE:TOLINS-EQ',
    'NSE:ADANIPOWER-EQ',
    'NSE:BANDHANBNK-EQ',
    'NSE:BIRLACABLE-EQ',
    'NSE:RELTD-EQ',
    'NSE:CANBK-EQ',
    'NSE:JTEKTINDIA-EQ',
    'NSE:LEMONTREE-EQ',
    'NSE:SAMMAANCAP-EQ',
    'NSE:BIRLAMONEY-EQ',
    'NSE:PARADEEP-EQ',
    'NSE:LOKESHMACH-EQ',
    'NSE:INDOUS-EQ',
    'NSE:ASHOKLEY-EQ',
    'NSE:RSWM-EQ',
    'NSE:GROWW-EQ',
    'NSE:PTC-EQ',
    'NSE:MRPL-EQ'
]

MAX_SYMBOLS = len(STOCK_SYMBOLS)

# =============================================================================
# TELEGRAM HANDLER FOR DETECTOR AUTHENTICATION
# =============================================================================

class TelegramHandler:
    def __init__(self):
        self.bot_token = TELEGRAM_BOT_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
        self.last_update_id = 0
        self.last_url_sent_time = 0  # Track when URL was last sent
        self.url_send_count = 0  # Track how many times URL was sent in current session
        self.current_auth_session_id = None  # Unique ID for current auth session
        
    def send_message(self, message):
        """Send a message to Telegram"""
        try:
            url = f"{self.base_url}/sendMessage"
            data = {
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "HTML"
            }
            response = requests.post(url, data=data, timeout=10)
            return response.status_code == 200
        except Exception as e:
            print(f"Error sending Telegram message: {e}")
            return False
    
    def get_updates(self):
        """Get latest messages from Telegram"""
        try:
            url = f"{self.base_url}/getUpdates"
            params = {
                "offset": self.last_update_id + 1,
                "timeout": 30
            }
            response = requests.get(url, params=params, timeout=35)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("ok") and data.get("result"):
                    updates = data["result"]
                    if updates:
                        self.last_update_id = updates[-1]["update_id"]
                        print(f"[TELEGRAM] Received {len(updates)} updates, last ID: {self.last_update_id}", flush=True)
                    return updates
                else:
                    print(f"[TELEGRAM] Response OK but no results: {data}", flush=True)
            else:
                print(f"[TELEGRAM] Bad response: {response.status_code}", flush=True)
            
            return []
        except Exception as e:
            print(f"[TELEGRAM] Error getting updates: {e}", flush=True)
            return []
    
    def extract_auth_code(self, message_text):
        """Extract auth_code from Fyers redirect URL using regex"""
        try:
            pattern1 = r'auth_code=([^&]+)&state=None'
            match1 = re.search(pattern1, message_text)
            if match1:
                auth_code = match1.group(1)
                print(f"Auth code extracted (with state=None): {auth_code[:20]}...")
                return auth_code
            
            pattern2 = r'auth_code=([^&\s]+)'
            match2 = re.search(pattern2, message_text)
            if match2:
                auth_code = match2.group(1)
                print(f"Auth code extracted (general pattern): {auth_code[:20]}...")
                return auth_code
            
            print("No auth_code found in message")
            return None
        except Exception as e:
            print(f"Error extracting auth code: {e}")
            return None
    
    def wait_for_auth_code(self, timeout_seconds=TELEGRAM_AUTH_TIMEOUT, auth_url=None, resend_callback=None):
        """Wait for auth code message from Telegram with 5-minute URL retry"""
        print(f"Waiting for auth code from Telegram (will resend URL every 5 minutes)...")
        start_time = time.time()
        last_url_resend = time.time()

        # Continue indefinitely until auth is successful (no timeout for retries)
        while True:
            try:
                current_time = time.time()

                # Resend URL every 5 minutes if not authenticated
                if current_time - last_url_resend >= LOGIN_URL_RETRY_INTERVAL:
                    print(f"5 minutes elapsed, resending login URL...")
                    if resend_callback:
                        resend_callback()
                    last_url_resend = current_time

                updates = self.get_updates()

                for update in updates:
                    if "message" in update and "text" in update["message"]:
                        message_text = update["message"]["text"]

                        if "auth_code=" in message_text:
                            auth_code = self.extract_auth_code(message_text)
                            if auth_code:
                                print("Auth code received successfully!")
                                self.url_send_count = 0  # Reset counter on success
                                return auth_code

                time.sleep(TELEGRAM_POLLING_INTERVAL)

            except Exception as e:
                print(f"Telegram connection error: {e}")
                time.sleep(10)

        return None

    def reset_auth_session(self):
        """Reset auth session tracking for new authentication attempt"""
        self.url_send_count = 0
        self.current_auth_session_id = time.time()
        self.last_url_sent_time = 0
    
    def check_for_restart_command(self):
        """Check for restart command in Telegram messages"""
        try:
            print("Checking Telegram for restart command...", flush=True)
            updates = self.get_updates()
            
            for update in updates:
                if "message" in update and "text" in update["message"]:
                    message_text = update["message"]["text"].strip()
                    print(f"Received message: '{message_text}'", flush=True)
                    
                    # Check various forms of restart command (case insensitive)
                    if message_text.lower() in ["restart", "restart!", "restart.", "restart bot", "restart system", "reboot"]:
                        print(f"Restart command received: '{message_text}'", flush=True)
                        self.send_message("Restart command received! Initiating restart...")
                        return True
            
            return False
        except Exception as e:
            print(f"Error checking for restart command: {e}", flush=True)
            return False

    def check_for_force_command(self):
        """Check for Force command in Telegram messages"""
        try:
            print("Checking Telegram for force command...", flush=True)
            updates = self.get_updates()
            print(f"Got {len(updates)} Telegram updates", flush=True)
            
            for update in updates:
                if "message" in update and "text" in update["message"]:
                    message_text = update["message"]["text"].strip()
                    print(f"Received message: '{message_text}'", flush=True)
                    
                    # Check various forms of force command (case insensitive)
                    if message_text.lower() in ["force", "start", "start now"]:
                        print(f"Force command detected: '{message_text}'", flush=True)
                        self.send_message("Force command received! Starting detector immediately, bypassing market hours.")
                        return True
            
            print("No force command found in messages", flush=True)
            return False
        except Exception as e:
            print(f"Error checking for Force command: {e}", flush=True)
            import traceback
            traceback.print_exc()
            return False

# =============================================================================
# FYERS AUTHENTICATOR CLASS
# =============================================================================

class FyersAuthenticator:
    def __init__(self):
        self.client_id = FYERS_CLIENT_ID
        self.secret_key = FYERS_SECRET_KEY
        self.redirect_uri = FYERS_REDIRECT_URI
        self.totp_secret = FYERS_TOTP_SECRET
        self.pin = FYERS_PIN
        self.access_token = None
        self.fyers_model = None
        self.telegram = TelegramHandler()
        self.is_authenticated = False
        self.current_session = None
        self.current_auth_url = None

    def generate_totp(self):
        """Generate TOTP code"""
        totp = pyotp.TOTP(self.totp_secret)
        return totp.now()

    def check_token_expiry_from_fyers(self):
        """Check if current token is valid by making an API call to Fyers"""
        try:
            if not self.access_token:
                return False, "No token available"

            # Create temporary Fyers model to test token
            test_fyers = fyersModel.FyersModel(
                client_id=self.client_id,
                token=self.access_token,
                log_path=""
            )

            # Make a profile API call to check if token is valid
            profile = test_fyers.get_profile()

            if profile and profile.get('s') == 'ok':
                print("Token is valid (verified from Fyers API)")
                self.fyers_model = test_fyers
                return True, "Token is valid"
            else:
                error_msg = profile.get('message', 'Token validation failed')
                print(f"Token expired or invalid: {error_msg}")
                return False, f"Token expired: {error_msg}"

        except Exception as e:
            print(f"Error checking token with Fyers: {e}")
            return False, f"Token check failed: {str(e)}"

    def send_auth_url(self):
        """Send authentication URL to Telegram"""
        try:
            # Create new session for fresh URL
            self.current_session = fyersModel.SessionModel(
                client_id=self.client_id,
                secret_key=self.secret_key,
                redirect_uri=self.redirect_uri,
                response_type="code",
                grant_type="authorization_code"
            )

            self.current_auth_url = self.current_session.generate_authcode()
            totp_code = self.generate_totp()
            print(f"\nAuthorization URL: {self.current_auth_url}\n")

            telegram_message = f"""<b>🔐 Fyers Authentication Required</b>

Please click the link below to authorize:

{self.current_auth_url}

<b>TOTP Code:</b> <code>{totp_code}</code>

After authorizing, send the complete redirect URL here.

<i>This URL will be resent every 5 minutes until authentication is successful.</i>
            """

            self.telegram.send_message(telegram_message)
            self.telegram.url_send_count += 1
            self.telegram.last_url_sent_time = time.time()

            return True
        except Exception as e:
            print(f"Error sending auth URL: {e}")
            return False

    def save_token(self, token):
        """Save token to global variables and JSON file"""
        global FYERS_ACCESS_TOKEN, FYERS_TOKEN_TIMESTAMP, FYERS_TOKEN_CREATED_AT

        FYERS_ACCESS_TOKEN = token
        FYERS_TOKEN_TIMESTAMP = time.time()
        FYERS_TOKEN_CREATED_AT = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        save_fyers_token_to_json(token, FYERS_TOKEN_TIMESTAMP, FYERS_TOKEN_CREATED_AT)

        self.is_authenticated = True
        print("Token updated and saved to JSON file")

    def authenticate(self):
        """Perform authentication with 5-minute retry and token expiry check"""
        print("="*50, flush=True)
        print("Starting Fyers Authentication", flush=True)
        print("="*50, flush=True)

        # First check if token is valid from JSON file timestamp
        is_valid_timestamp, message = validate_fyers_token_from_json()

        if is_valid_timestamp and FYERS_ACCESS_TOKEN:
            self.access_token = FYERS_ACCESS_TOKEN

            # Additionally verify token is actually valid with Fyers API
            is_valid_fyers, fyers_message = self.check_token_expiry_from_fyers()

            if is_valid_fyers:
                print("Using existing valid token (verified with Fyers)", flush=True)
                self.is_authenticated = True
                return True
            else:
                print(f"Token invalid from Fyers: {fyers_message}", flush=True)
                # Token is expired according to Fyers, need fresh auth

        print("Performing fresh authentication...", flush=True)

        # Reset auth session tracking
        self.telegram.reset_auth_session()

        # Send initial auth URL
        self.send_auth_url()

        # Define callback to resend URL
        def resend_url_callback():
            print("Resending authentication URL...", flush=True)
            self.send_auth_url()

        # Wait for auth code with 5-minute resend
        auth_code = self.telegram.wait_for_auth_code(
            resend_callback=resend_url_callback
        )

        if not auth_code:
            print("Failed to get auth code from Telegram", flush=True)
            return False

        # Use the current session to generate token
        print("Auth code received, generating token...", flush=True)
        self.current_session.set_token(auth_code)
        response = self.current_session.generate_token()

        if response and response.get('s') == 'ok':
            self.access_token = response['access_token']
            self.save_token(self.access_token)

            self.fyers_model = fyersModel.FyersModel(
                client_id=self.client_id,
                token=self.access_token,
                log_path=""
            )

            print("Authentication successful!", flush=True)
            self.telegram.send_message("<b>✅ Fyers Authentication Successful!</b>\n\nYou can now start monitoring.")
            self.is_authenticated = True
            return True
        else:
            error_msg = response.get('message', 'Unknown error') if response else 'No response'
            print(f"Authentication failed: {response}", flush=True)
            self.telegram.send_message(f"<b>❌ Authentication Failed</b>\n\nError: {error_msg}\n\nPlease try again with a fresh URL.")
            return False

    def refresh_token_if_expired(self):
        """Check if token is expired and trigger re-authentication if needed"""
        try:
            is_valid, message = self.check_token_expiry_from_fyers()

            if not is_valid:
                print(f"Token expired: {message}", flush=True)
                print("Initiating re-authentication...", flush=True)

                self.is_authenticated = False
                self.access_token = None

                # Trigger fresh authentication
                return self.authenticate()

            return True

        except Exception as e:
            print(f"Error refreshing token: {e}", flush=True)
            return False

    def get_fyers_model(self):
        """Get Fyers model, refreshing token if needed"""
        if not self.fyers_model:
            if not self.authenticate():
                raise Exception("Authentication failed")
        else:
            # Periodically verify token is still valid
            is_valid, _ = self.check_token_expiry_from_fyers()
            if not is_valid:
                print("Token expired, re-authenticating...", flush=True)
                self.is_authenticated = False
                if not self.authenticate():
                    raise Exception("Re-authentication failed")

        return self.fyers_model

# =============================================================================
# GOOGLE SHEETS MANAGER
# =============================================================================

class GoogleSheetsManager:
    def __init__(self, detector=None):
        self.gc = None
        self.worksheet = None
        self.lock = threading.Lock()
        self.detector = detector
        self.sheets_initialized = self.initialize_sheets()
        
        if not self.sheets_initialized:
            print("Google Sheets initialization failed")
    
    def initialize_sheets(self):
        """Initialize Google Sheets connection"""
        try:
            if GOOGLE_CREDENTIALS is None:
                print("Google credentials not available")
                return False
                
            scope = [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"
            ]
            
            creds = Credentials.from_service_account_info(GOOGLE_CREDENTIALS, scopes=scope)
            self.gc = gspread.authorize(creds)
            
            try:
                sheet = self.gc.open_by_key(GOOGLE_SHEETS_ID)
                self.worksheet = sheet.sheet1
                print(f"Connected to Google Sheet!")
                
                try:
                    headers = self.worksheet.row_values(1)
                    if not headers or len(headers) < 8:
                        headers = [
                            'Date', 'Time', 'Symbol', 'LTP', 'Volume_Spike',
                            'Trd_Val_Cr', 'Spike_Type', 'Sector'
                        ]
                        self.worksheet.insert_row(headers, 1)
                        print("Added headers to sheet")
                except:
                    headers = [
                        'Date', 'Time', 'Symbol', 'LTP', 'Volume_Spike',
                        'Trd_Val_Cr', 'Spike_Type', 'Sector'
                    ]
                    self.worksheet.append_row(headers)
                
            except gspread.SpreadsheetNotFound:
                print(f"Could not access Google Sheet")
                return False
            
            return True
            
        except Exception as e:
            print(f"Error initializing Google Sheets: {e}")
            return False
    
    def add_trade_to_sheets(self, symbol, ltp, volume_spike, trade_value,
                           spike_type, previous_volume, current_volume,
                           previous_ltp=None, ltp_color_format=None):
        """Add a new trade record to Google Sheets"""
        try:
            if self.worksheet is None:
                return False
                
            with self.lock:
                current_time = datetime.now()
                sector = get_sector_for_symbol(symbol)
                
                row = [
                    current_time.strftime('%Y-%m-%d'),
                    current_time.strftime('%H:%M:%S'),
                    symbol,
                    round(ltp, 2),
                    int(volume_spike),
                    round(trade_value / 10000000, 2),
                    spike_type,
                    sector
                ]
                
                self.worksheet.append_row(row)
                print(f"Added to Google Sheets: {symbol} ({sector}) - Rs{trade_value/10000000:.2f} crore")
                
                return True
                
        except Exception as e:
            print(f"Error adding to Google Sheets: {e}")
            return False

# =============================================================================
# VOLUME SPIKE DETECTOR
# =============================================================================

class VolumeSpikeDetector:
    def __init__(self):
        self.authenticator = FyersAuthenticator()
        self.sheets_manager = GoogleSheetsManager(self)
        self.access_token = None
        self.fyers_ws = None
        self.total_ticks = 0
        self.individual_trades_detected = 0
        self.start_time = time.time()
        self.stop_event = None
        
        self.previous_volumes = {}
        self.last_alert_time = {}
        self.previous_ltp = {}
        self.sector_counts = {}
        
        self.websocket_retry_count = 0
        self.max_websocket_retries = 1
        
    def initialize(self):
        print("Initializing Volume Spike Detector...", flush=True)
        
        print("Attempting authentication...", flush=True)
        if not self.authenticator.authenticate():
            print("Authentication failed!", flush=True)
            return False
        
        print("Authentication successful!", flush=True)
        self.access_token = self.authenticator.access_token
    
        try:
            print("Getting Fyers model...", flush=True)
            fyers = self.authenticator.get_fyers_model()
            
            print("Getting profile...", flush=True)
            profile = fyers.get_profile()
            
            if profile['s'] == 'ok':
                print(f"Connected! User: {profile['data']['name']}", flush=True)
                return True
            else:
                print(f"Profile check failed: {profile}", flush=True)
                return False
        except Exception as e:
            print(f"Connection error: {e}", flush=True)
            import traceback
            traceback.print_exc()
            return False
    
    def on_tick_received(self, *args):
        try:
            message = args[-1] if args else None
            
            if isinstance(message, dict):
                if message.get('type') in ['cn', 'ful', 'sub']:
                    return
                
                if 'symbol' in message:
                    self.detect_individual_trade(message)
                        
        except Exception as e:
            print(f"Error in tick handler: {e}")
    
    def detect_individual_trade(self, tick_data):
        """Detect individual large trades"""
        try:
            self.total_ticks += 1
            
            symbol = tick_data.get('symbol', '')
            ltp = float(tick_data.get('ltp', 0))
            current_volume = float(tick_data.get('vol_traded_today', 0))
            
            if not symbol or ltp <= 0 or current_volume <= 0:
                return
            
            previous_volume = self.previous_volumes.get(symbol, current_volume)
            previous_ltp = self.previous_ltp.get(symbol, None)
            
            volume_spike = current_volume - previous_volume
            
            self.previous_volumes[symbol] = current_volume
            self.previous_ltp[symbol] = ltp
            
            if volume_spike <= MIN_VOLUME_SPIKE:
                return
            
            individual_trade_value = ltp * volume_spike
            
            if individual_trade_value >= INDIVIDUAL_TRADE_THRESHOLD:
                last_alert = self.last_alert_time.get(symbol, 0)
                time_since_last = time.time() - last_alert
                
                if time_since_last > 60:
                    self.individual_trades_detected += 1
                    self.last_alert_time[symbol] = time.time()
                    
                    sector = get_sector_for_symbol(symbol)
                    self.sector_counts[sector] = self.sector_counts.get(sector, 0) + 1
                    
                    spike_percentage = (volume_spike / previous_volume * 100) if previous_volume > 0 else 0
                    if spike_percentage > 50:
                        spike_type = "Large Spike"
                    elif spike_percentage > 20:
                        spike_type = "Medium Spike"
                    else:
                        spike_type = "Volume Increase"

                    ltp_change = ltp - previous_ltp if previous_ltp else 0
                    
                    print(f"\nLARGE TRADE: {symbol} ({sector}) - Rs{individual_trade_value/10000000:.2f} Cr")
                    
                    self.sheets_manager.add_trade_to_sheets(
                        symbol=symbol,
                        ltp=ltp,
                        volume_spike=volume_spike,
                        trade_value=individual_trade_value,
                        spike_type=spike_type,
                        previous_volume=previous_volume,
                        current_volume=current_volume,
                        previous_ltp=previous_ltp
                    )
                    
                    telegram_alert = f"""
<b>🚨 LARGE TRADE DETECTED</b>

<b>Symbol:</b> {symbol}
<b>Sector:</b> {sector}
<b>LTP:</b> Rs{ltp:,.2f}
<b>Volume:</b> {volume_spike:,.0f}
<b>Value:</b> Rs{individual_trade_value/10000000:.2f} Cr
<b>Time:</b> {datetime.now().strftime('%H:%M:%S')}
                    """
                    self.authenticator.telegram.send_message(telegram_alert)
                
        except Exception as e:
            print(f"Error detecting trade: {e}")
    
    def start_monitoring(self):
        """Start monitoring"""
        try:
            print("Creating WebSocket connection...")
            self.fyers_ws = data_ws.FyersDataSocket(
                access_token=f"{FYERS_CLIENT_ID}:{self.access_token}",
                log_path="",
                litemode=False,
                write_to_file=False,
                reconnect=True,
                on_message=self.on_tick_received
            )
            
            symbols_to_monitor = STOCK_SYMBOLS[:MAX_SYMBOLS]
            print(f"Subscribing to {len(symbols_to_monitor)} symbols...")
            
            self.fyers_ws.connect()
            time.sleep(3)
            self.fyers_ws.subscribe(symbols=symbols_to_monitor, data_type="SymbolUpdate")
            
            print("Monitoring started")
            
            while True:
                if self.stop_event and self.stop_event.is_set():
                    break
                time.sleep(5)
                
        except Exception as e:
            print(f"Monitoring error: {e}")
            raise
        finally:
            if self.fyers_ws:
                try:
                    self.fyers_ws.close_connection()
                except:
                    pass

# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    import sys

    # Flush output immediately for better logging
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)

    try:
        print("="*70, flush=True)
        print("Fyers Volume Spike Detector", flush=True)
        print("="*70, flush=True)

        print("\nCommands available:", flush=True)
        print("   Detector Bot: 'force' or 'restart'", flush=True)
        print("\nSupervisor will check for commands every 10 seconds", flush=True)
        print("="*70, flush=True)

        print("\nStarting supervisor loop...", flush=True)
        supervisor_loop()

    except KeyboardInterrupt:
        print("\nShutting down...", flush=True)
        _stop_stream_once()
        sys.exit(0)
    except Exception as e:
        print(f"Fatal error: {e}", flush=True)
        import traceback
        traceback.print_exc()
        _stop_stream_once()
        sys.exit(1)

