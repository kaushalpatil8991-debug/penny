#!/usr/bin/env python3
"""
Fyers Volume Spike Detector - Google Sheets Integration with Sector Classification
Detects large individual trades and updates Google Sheets in real-time with sector information

FIXED ISSUES:
1. Removed duplicate function definitions
2. Fixed Google Sheets header to use 'Trd_Val_Cr'
3. Ensured data format consistency
"""

import json
import os
import sys
import time
import threading
import requests
import re
from datetime import datetime, timedelta
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
INDIVIDUAL_TRADE_THRESHOLD = 3000000  # Rs 3 million for individual trades
MIN_VOLUME_SPIKE = 1000  # Minimum volume spike to consider

# Google Sheets Configuration
GOOGLE_SHEETS_ID = "1kgrKVjUm0lB0fz-74Q_C-sXls7IyyqFDGhf8NZmGG4A"  # Your Google Sheet ID

# =============================================================================
# RUN CONTROLLER GLOBALS
# =============================================================================

_running_flag = False
_stop_event = threading.Event()
_market_end_message_sent = False  # Flag to track if end message was sent today

# =============================================================================
# RUN CONTROLLER FUNCTIONS
# =============================================================================

def _start_stream_once():
    """Start your Fyers WebSocket loop exactly once."""
    global _running_flag, _stop_event
    if _running_flag:
        return False
    _stop_event.clear()
    # Start the detector in a separate thread
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
    time.sleep(2)  # give your WS loop time to close
    _running_flag = False
    print("Stream STOPPED")
    return False

def _stream_worker(stop_event: threading.Event):
    """
    Simplified worker that runs the detector with proper error handling
    No retries - single attempt only for clean restart behavior
    """
    try:
        print("Starting detector stream worker (single attempt)")
        detector = VolumeSpikeDetector()
        detector.stop_event = stop_event
        
        # Initialize and run
        if detector.initialize():
            print("Detector initialized successfully")
            detector.start_monitoring()
        else:
            print("Detector initialization failed - exiting")
            return
            
    except Exception as e:
        print(f"Stream worker error: {e}")
        import traceback
        traceback.print_exc()
        print("Stream worker exiting due to error")
        return
    
    print("Stream worker stopped")

def _inside_window_ist() -> bool:
    """Check if current IST time is within market hours."""
    now = datetime.now(ZoneInfo("Asia/Kolkata"))
    hhmm = now.strftime("%H:%M")
    return MARKET_START_TIME <= hhmm < MARKET_END_TIME

def supervisor_loop():
    """
    Simplified supervisor that manages the detector lifecycle
    """
    print("Supervisor loop started")
    detector = None
    last_auth_check = time.time()
    AUTH_CHECK_INTERVAL = 3600  # Check auth every hour
    
    while True:
        try:
            current_time = time.time()
            
            # Check if we're in market hours
            if SCHEDULING_ENABLED and not _inside_window_ist():
                if detector:
                    print("Outside market hours, stopping detector...")
                    _stop_stream_once()
                    detector = None
                time.sleep(60)
                continue
            
            # We should be running - start detector if not running
            if not detector or not _running_flag:
                print("Starting detector...")
                _stop_stream_once()  # Clean stop if anything is running
                time.sleep(2)
                
                # Create new detector instance
                detector = VolumeSpikeDetector()
                _start_stream_once()
                
            # Periodic auth check (every hour)
            if current_time - last_auth_check > AUTH_CHECK_INTERVAL:
                print("Performing periodic auth check...")
                if detector and hasattr(detector, 'authenticator'):
                    if not detector.authenticator.is_authenticated:
                        print("Auth expired, will re-authenticate on next cycle")
                        _stop_stream_once()
                        detector = None
                last_auth_check = current_time
            
            # Sleep before next check
            time.sleep(30)
            
        except Exception as e:
            print(f"Supervisor error: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(10)

# Load Google Credentials from Environment Variables
try:
    # Try to load credentials from environment variable
    google_creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
    if google_creds_json:
        # Process the JSON string to handle newlines properly
        GOOGLE_CREDENTIALS = json.loads(google_creds_json)
        # Ensure private key newlines are properly formatted
        if 'private_key' in GOOGLE_CREDENTIALS:
            GOOGLE_CREDENTIALS['private_key'] = GOOGLE_CREDENTIALS['private_key'].replace('\\n', '\n')
        print("Google Sheets credentials loaded from environment variable")
    else:
        # Fallback: try to load from individual environment variables
        private_key = os.getenv('GOOGLE_PRIVATE_KEY')
        if private_key:
            # Ensure newlines are properly handled in private key
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
        
        # Check if all required fields are present
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
    # Try to load from JSON file first
    with open('fyers_access_token.json', 'r') as f:
        token_data = json.load(f)
        FYERS_ACCESS_TOKEN = token_data.get('access_token', '')
        FYERS_TOKEN_TIMESTAMP = float(token_data.get('timestamp', 0))
        FYERS_TOKEN_CREATED_AT = token_data.get('created_at', '')
        print("Fyers access token loaded from JSON file")
except FileNotFoundError:
    # Fallback to environment variables
    FYERS_ACCESS_TOKEN = os.getenv('FYERS_ACCESS_TOKEN', '')
    FYERS_TOKEN_TIMESTAMP = float(os.getenv('FYERS_TOKEN_TIMESTAMP', '0'))
    FYERS_TOKEN_CREATED_AT = os.getenv('FYERS_TOKEN_CREATED_AT', '')
    print("Fyers access token JSON file not found, using environment variables")
except Exception as e:
    # Fallback to environment variables on any error
    FYERS_ACCESS_TOKEN = os.getenv('FYERS_ACCESS_TOKEN', '')
    FYERS_TOKEN_TIMESTAMP = float(os.getenv('FYERS_TOKEN_TIMESTAMP', '0'))
    FYERS_TOKEN_CREATED_AT = os.getenv('FYERS_TOKEN_CREATED_AT', '')
    print(f"Error loading Fyers token from JSON: {e}, using environment variables")

# Function to validate Fyers token from JSON file
def validate_fyers_token_from_json():
    """Validate if the Fyers token from JSON file is still valid"""
    try:
        if not FYERS_ACCESS_TOKEN or FYERS_ACCESS_TOKEN.strip() == "":
            return False, "No token available"
        
        # Check if token is expired (8 hours = 28800 seconds)
        current_time = time.time()
        token_time = FYERS_TOKEN_TIMESTAMP
        
        if current_time - token_time < 28800:  # 8 hours
            print("Fyers token from JSON file is valid")
            return True, "Token is valid"
        else:
            print("Fyers token from JSON file expired, need fresh authentication")
            return False, "Token expired"
            
    except Exception as e:
        print(f"Error validating Fyers token: {e}")
        return False, f"Validation error: {str(e)}"

# Function to save Fyers token to JSON file
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

# Telegram Configuration - Hardcoded
TELEGRAM_BOT_TOKEN = "8303548716:AAF2jXwncMuW8VvI3wl8l4RXObDgkDvCvfo"
TELEGRAM_CHAT_ID = "5715256800"
TELEGRAM_POLLING_INTERVAL = 5
TELEGRAM_AUTH_TIMEOUT = 300

# Summary Telegram Bot Configuration
SUMMARY_TELEGRAM_BOT_TOKEN = "8225228168:AAFVxVL_ygeTz8IDVIt7Qp1qlkra7qgoAKY"
SUMMARY_TELEGRAM_CHAT_ID = "8388919023"
SUMMARY_SEND_TIME = "16:30"  # 4:30 PM IST
SUMMARY_SEND_INTERVAL = 7200  # 2 hours in seconds

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
# SCHEDULING UTILITIES
# =============================================================================

def is_market_time():
    """Check if current time is within market hours"""
    if not SCHEDULING_ENABLED:
        return True
    
    current_time = datetime.now()
    current_time_str = current_time.strftime("%H:%M")
    
    return MARKET_START_TIME <= current_time_str <= MARKET_END_TIME

def get_time_until_market_start():
    """Get time until market starts (in seconds)"""
    current_time = datetime.now()
    start_hour, start_minute = map(int, MARKET_START_TIME.split(":"))
    market_start = current_time.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0)
    
    if current_time.time() >= market_start.time():
        return 0
    
    time_diff = market_start - current_time
    return time_diff.total_seconds()

def get_time_until_market_end():
    """Get time until market ends (in seconds)"""
    current_time = datetime.now()
    end_hour, end_minute = map(int, MARKET_END_TIME.split(":"))
    market_end = current_time.replace(hour=end_hour, minute=end_minute, second=0, microsecond=0)
    
    if current_time.time() >= market_end.time():
        return 0
    
    time_diff = market_end - current_time
    return time_diff.total_seconds()

def wait_for_market_start():
    """Wait until market start time"""
    if not SCHEDULING_ENABLED:
        return
    
    while not is_market_time():
        time_until_start = get_time_until_market_start()
        if time_until_start > 0:
            hours = int(time_until_start // 3600)
            minutes = int((time_until_start % 3600) // 60)
            seconds = int(time_until_start % 60)
            
            print(f"Waiting for market to start at {MARKET_START_TIME}...")
            print(f"   Time remaining: {hours:02d}:{minutes:02d}:{seconds:02d}")
            
            if int(time_until_start) % 1800 == 0:
                status_message = f"""
<b>Market Schedule Status</b>

<b>Current Time:</b> {datetime.now().strftime('%H:%M:%S')}
<b>Market Start:</b> {MARKET_START_TIME}
<b>Time Remaining:</b> {hours:02d}:{minutes:02d}:{seconds:02d}

<b>Status:</b> Waiting for market to open
                """
                telegram_handler = TelegramHandler()
                telegram_handler.send_message(status_message)
            
            time.sleep(60)
        else:
            break
    
    print(f"Market is now open! Starting monitoring at {datetime.now().strftime('%H:%M:%S')}")

def check_market_end():
    """Check if market has ended and stop monitoring"""
    global _market_end_message_sent
    
    if not SCHEDULING_ENABLED:
        return False
    
    if not is_market_time():
        # Only send message once per day
        if not _market_end_message_sent:
            print(f"Market has ended at {MARKET_END_TIME}. Stopping monitoring...")
            
            end_message = f"""<b>Market Session Ended</b>

<b>End Time:</b> {datetime.now().strftime('%H:%M:%S')}
<b>Session Duration:</b> {MARKET_START_TIME} - {MARKET_END_TIME}

<b>Monitoring Status:</b> Stopped
<b>Next Session:</b> Tomorrow at {MARKET_START_TIME}
            """
            telegram_handler = TelegramHandler()
            telegram_handler.send_message(end_message)
            _market_end_message_sent = True
        
        return True
    
    # Reset flag when market is open
    _market_end_message_sent = False
    return False

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

def get_sector_for_symbol(symbol):
    """Get sector for a given symbol"""
    return SECTOR_MAPPING.get(symbol, "Others")

MAX_SYMBOLS = len(STOCK_SYMBOLS)

# =============================================================================
# TELEGRAM HANDLER FOR AUTOMATED AUTHENTICATION
# =============================================================================

class TelegramHandler:
    def __init__(self):
        self.bot_token = TELEGRAM_BOT_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
        self.last_update_id = 0
        
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
            if response.status_code == 200:
                return True
            else:
                print(f"Failed to send Telegram message: {response.text}")
                return False
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
                    return updates
            return []
        except Exception as e:
            print(f"Error getting Telegram updates: {e}")
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
            print(f"Message content: {message_text[:100]}...")
            return None
        except Exception as e:
            print(f"Error extracting auth code: {e}")
            return None
    
    def wait_for_auth_code(self, timeout_seconds=TELEGRAM_AUTH_TIMEOUT):
        """Wait for auth code message from Telegram with network error handling"""
        print(f"Waiting for auth code from Telegram (timeout: {timeout_seconds}s)...")
        start_time = time.time()
        
        while time.time() - start_time < timeout_seconds:
            try:
                updates = self.get_updates()
                
                for update in updates:
                    if "message" in update and "text" in update["message"]:
                        message_text = update["message"]["text"]
                        
                        if "auth_code=" in message_text:
                            auth_code = self.extract_auth_code(message_text)
                            if auth_code:
                                print("Auth code received successfully!")
                                return auth_code
                
                time.sleep(TELEGRAM_POLLING_INTERVAL)
                
            except Exception as e:
                print(f"Error while waiting for auth code: {e}")
                time.sleep(TELEGRAM_POLLING_INTERVAL)
        
        print("Timeout waiting for auth code")
        return None

# =============================================================================
# SUMMARY TELEGRAM HANDLER
# =============================================================================

class SummaryTelegramHandler:
    def __init__(self):
        self.bot_token = SUMMARY_TELEGRAM_BOT_TOKEN
        self.chat_id = SUMMARY_TELEGRAM_CHAT_ID
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
        self.last_update_id = 0
        self.stop_sending_today = False

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
            if response.status_code == 200:
                print("Summary message sent successfully")
                return True
            else:
                print(f"Failed to send summary message: {response.text}")
                return False
        except Exception as e:
            print(f"Error sending summary message: {e}")
            return False

    def send_messages(self, messages):
        """Send multiple messages to Telegram"""
        try:
            success_count = 0
            for i, message in enumerate(messages, 1):
                print(f"\nSending message {i}/{len(messages)}...")
                if self.send_message(message):
                    success_count += 1
                    time.sleep(1)  # Small delay between messages
                else:
                    print(f"Failed to send message {i}")

            print(f"\nSent {success_count}/{len(messages)} messages successfully")
            return success_count == len(messages)

        except Exception as e:
            print(f"Error sending multiple messages: {e}")
            return False

    def get_updates(self):
        """Get latest messages from Telegram"""
        try:
            url = f"{self.base_url}/getUpdates"
            params = {
                "offset": self.last_update_id + 1,
                "timeout": 10
            }
            response = requests.get(url, params=params, timeout=15)
            if response.status_code == 200:
                data = response.json()
                if data.get("ok") and data.get("result"):
                    updates = data["result"]
                    if updates:
                        self.last_update_id = updates[-1]["update_id"]
                    return updates
            return []
        except Exception as e:
            print(f"Error getting Telegram updates: {e}")
            return []
    
    def check_for_done_message(self):
        """Check if 'done' message has been received"""
        try:
            updates = self.get_updates()
            for update in updates:
                if "message" in update and "text" in update["message"]:
                    text = update["message"]["text"].strip().lower()
                    if text == "done":
                        print("'Done' message received - stopping summaries for today")
                        self.stop_sending_today = True
                        return True
            return False
        except Exception as e:
            print(f"Error checking for done message: {e}")
            return False
    
    def check_for_send_message(self):
        """Check if 'send' message has been received"""
        try:
            updates = self.get_updates()
            for update in updates:
                if "message" in update and "text" in update["message"]:
                    text = update["message"]["text"].strip().lower()
                    if text == "send":
                        print("'Send' message received - will send immediate summary")
                        return True
            return False
        except Exception as e:
            print(f"Error checking for send message: {e}")
            return False

# =============================================================================
# GOOGLE SHEETS SUMMARY EXTRACTOR
# =============================================================================

class DailySummaryGenerator:
    def __init__(self):
        self.sheets_id = GOOGLE_SHEETS_ID
        self.credentials = GOOGLE_CREDENTIALS
        self.worksheet = None
        
    def initialize_sheets(self):
        """Initialize Google Sheets connection"""
        try:
            if not self.credentials:
                print("Google Sheets credentials not available")
                return False
            
            scopes = ['https://www.googleapis.com/auth/spreadsheets']
            creds = Credentials.from_service_account_info(self.credentials, scopes=scopes)
            client = gspread.authorize(creds)
            
            spreadsheet = client.open_by_key(self.sheets_id)
            self.worksheet = spreadsheet.sheet1
            
            print("Google Sheets initialized for summary")
            return True
        except Exception as e:
            print(f"Error initializing sheets for summary: {e}")
            return False
    
    def get_today_data(self):
        """Get all data for today's date from Google Sheets"""
        try:
            if not self.worksheet:
                if not self.initialize_sheets():
                    return []
            
            # Get current date in multiple formats
            now = datetime.now(ZoneInfo("Asia/Kolkata"))
            today_formats = [
                now.strftime("%d-%m-%Y"),    # 15-10-2025
                now.strftime("%Y-%m-%d"),    # 2025-10-15
                now.strftime("%d/%m/%Y"),    # 15/10/2025
                now.strftime("%m/%d/%Y"),    # 10/15/2025
                now.strftime("%d-%m-%y"),    # 15-10-25
            ]
            
            # Get all values including headers
            all_values = self.worksheet.get_all_values()
            
            if not all_values or len(all_values) < 2:
                print("No data in sheet")
                return []
            
            # First row is headers
            headers = all_values[0]
            print(f"Sheet headers: {headers}")
            
            # Find column indices
            date_col_idx = None
            symbol_col_idx = None
            value_col_idx = None
            
            for idx, header in enumerate(headers):
                header_lower = header.lower().strip()
                
                # Match Date column
                if header_lower == 'date':
                    date_col_idx = idx
                    print(f"OK Found Date column at index {idx}")
                
                # Match Symbol column
                elif header_lower == 'symbol':
                    symbol_col_idx = idx
                    print(f"OK Found Symbol column at index {idx}")
                
                # Match Value column - looking for Trd_Val_Cr or similar
                elif ('trd' in header_lower and 'val' in header_lower and 'cr' in header_lower) or \
                     ('value' in header_lower and ('cr' in header_lower or 'crore' in header_lower)):
                    value_col_idx = idx
                    print(f"OK Found Value column at index {idx}: '{header}'")
            
            if date_col_idx is None or symbol_col_idx is None or value_col_idx is None:
                print(f"ERROR Required columns not found!")
                print(f"   Date column index: {date_col_idx}")
                print(f"   Symbol column index: {symbol_col_idx}")
                print(f"   Value column index: {value_col_idx}")
                print(f"\nINFO Looking for columns named:")
                print(f"   - 'Date' (exact match)")
                print(f"   - 'Symbol' (exact match)")
                print(f"   - 'Trd_Val_Cr' or 'Value (Rs Crores)' or similar")
                return []
            
            print(f"\nOK All required columns found!")
            print(f"  Date: column {date_col_idx}")
            print(f"  Symbol: column {symbol_col_idx}")
            print(f"  Value: column {value_col_idx} ('{headers[value_col_idx]}')")
            
            # Process data rows
            today_records = []
            
            for row_idx, row in enumerate(all_values[1:], start=2):  # Skip header row
                if len(row) <= max(date_col_idx, symbol_col_idx, value_col_idx):
                    continue
                
                date_value = str(row[date_col_idx]).strip()
                
                # Check if date matches today
                is_today = False
                for date_format in today_formats:
                    if date_value == date_format or date_value.startswith(date_format):
                        is_today = True
                        break
                
                if is_today:
                    symbol = str(row[symbol_col_idx]).strip()
                    value_str = str(row[value_col_idx]).strip()
                    
                    record = {
                        'Date': date_value,
                        'Symbol': symbol,
                        'Trd_Val_Cr': value_str
                    }
                    today_records.append(record)
            
            print(f"\nData Summary:")
            print(f"   Total rows in sheet: {len(all_values) - 1}")
            print(f"   Records for today ({today_formats[0]}): {len(today_records)}")
            
            # Debug: Print first few records
            if today_records:
                print(f"\nSample records (first 3):")
                for i, record in enumerate(today_records[:3], 1):
                    print(f"   {i}. {record['Date']} | {record['Symbol']:20s} | Rs.{record['Trd_Val_Cr']} Cr")
            else:
                print(f"\nWARNING No records found for today's date: {today_formats[0]}")
                print(f"   Sample dates in sheet:")
                for row in all_values[1:6]:  # Show first 5 dates
                    if len(row) > date_col_idx:
                        print(f"   - {row[date_col_idx]}")
            
            return today_records
            
        except Exception as e:
            print(f"ERROR getting today's data: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_date_range_data(self, days_back=0):
        """Get data for a specific date range"""
        try:
            if not self.worksheet:
                if not self.initialize_sheets():
                    return []
            
            # Calculate target dates
            now = datetime.now(ZoneInfo("Asia/Kolkata"))
            target_dates = []
            
            for i in range(days_back + 1):
                target_date = now - timedelta(days=i)
                date_formats = [
                    target_date.strftime("%d-%m-%Y"),    # 15-10-2025
                    target_date.strftime("%Y-%m-%d"),    # 2025-10-15
                    target_date.strftime("%d/%m/%Y"),    # 15/10/2025
                    target_date.strftime("%m/%d/%Y"),    # 10/15/2025
                    target_date.strftime("%d-%m-%y"),    # 15-10-25
                ]
                target_dates.extend(date_formats)
            
            # Get all values including headers
            all_values = self.worksheet.get_all_values()
            
            if not all_values or len(all_values) < 2:
                print("No data in sheet")
                return []
            
            # First row is headers
            headers = all_values[0]
            
            # Find column indices (same logic as get_today_data)
            date_col_idx = None
            symbol_col_idx = None
            value_col_idx = None
            
            for idx, header in enumerate(headers):
                header_lower = header.lower().strip()
                
                if header_lower == 'date':
                    date_col_idx = idx
                elif header_lower == 'symbol':
                    symbol_col_idx = idx
                elif ('trd' in header_lower and 'val' in header_lower and 'cr' in header_lower) or \
                     ('value' in header_lower and ('cr' in header_lower or 'crore' in header_lower)):
                    value_col_idx = idx
            
            if date_col_idx is None or symbol_col_idx is None or value_col_idx is None:
                print(f"ERROR Required columns not found for date range!")
                return []
            
            # Process data rows
            date_range_records = []
            
            for row_idx, row in enumerate(all_values[1:], start=2):
                if len(row) > max(date_col_idx, symbol_col_idx, value_col_idx):
                    date_val = row[date_col_idx].strip()
                    symbol_val = row[symbol_col_idx].strip()
                    value_val = row[value_col_idx].strip()
                    
                    if date_val in target_dates and symbol_val and value_val:
                        try:
                            trd_val_cr = float(value_val.replace(',', '')) if value_val else 0.0
                            record = {
                                'Date': date_val,
                                'Symbol': symbol_val,
                                'Trd_Val_Cr': trd_val_cr
                            }
                            date_range_records.append(record)
                        except ValueError:
                            continue
            
            return date_range_records
            
        except Exception as e:
            print(f"ERROR getting date range data: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def generate_top_15_summary(self, days_back=0, summary_type="Daily"):
        """Generate top 15 stocks summary based on count and total value"""
        try:
            print("\n" + "="*70)
            print(f"GENERATING TOP 15 {summary_type.upper()} SUMMARY")
            print("="*70)
            
            # Step 1: Get records based on date range
            if days_back == 0:
                records = self.get_today_data()
            else:
                records = self.get_date_range_data(days_back)
            
            if not records:
                print(f"ERROR No records found for {summary_type.lower()} - cannot generate summary")
                return None
            
            print(f"\nOK Processing {len(records)} records for {summary_type.lower()} summary...")
            
            # Step 2: Process each record
            symbol_stats = {}
            parse_success = 0
            parse_failed = 0
            
            for record in records:
                # Get symbol
                symbol = record.get('Symbol', '').strip()
                
                # Get trade value
                value_str = str(record.get('Trd_Val_Cr', '0')).strip()
                
                # Skip if symbol is empty or invalid
                if not symbol or symbol == 'Unknown' or symbol == '':
                    continue
                
                # Parse the trade value
                try:
                    # Remove any non-numeric characters except decimal point and minus sign
                    value_clean = re.sub(r'[^\d.\-]', '', value_str)
                    trd_val = float(value_clean) if value_clean and value_clean != '-' else 0.0
                    
                    if trd_val > 0:
                        parse_success += 1
                        if parse_success <= 5:  # Show first 5 successful parses
                            print(f"   OK {symbol:20s}: '{value_str}' -> {trd_val:.2f} Cr")
                except (ValueError, AttributeError) as e:
                    trd_val = 0.0
                    parse_failed += 1
                    if parse_failed <= 3:  # Show first 3 failures
                        print(f"   ERROR {symbol:20s}: Failed to parse '{value_str}'")
                
                # Initialize or update symbol stats
                if symbol not in symbol_stats:
                    symbol_stats[symbol] = {
                        'count': 0,
                        'total_trd_val_cr': 0.0
                    }
                
                # Increment count
                symbol_stats[symbol]['count'] += 1
                
                # Add to total value
                symbol_stats[symbol]['total_trd_val_cr'] += trd_val
            
            print(f"\nParsing Results:")
            print(f"   OK Successfully parsed: {parse_success}")
            print(f"   ERROR Failed to parse: {parse_failed}")
            print(f"   INFO Unique symbols: {len(symbol_stats)}")
            
            # Step 3: Sort by count and get top 15
            sorted_symbols = sorted(
                symbol_stats.items(),
                key=lambda x: x[1]['count'],
                reverse=True
            )
            
            top_15 = sorted_symbols[:15]
            
            print(f"\n{'='*70}")
            print("TOP 15 SYMBOLS BY COUNT")
            print(f"{'='*70}")
            print(f"{'Rank':<6} {'Symbol':<20} {'Count':<8} {'Total Value (Cr)':<18} {'Avg/Trade (Cr)'}")
            print("-"*70)
            
            for i, (symbol, stats) in enumerate(top_15, 1):
                count = stats['count']
                total_val = stats['total_trd_val_cr']
                avg_val = total_val / count if count > 0 else 0
                
                print(f"{i:2d}.   {symbol:<20} {count:<8} Rs.{total_val:>15,.2f}  Rs.{avg_val:>10,.2f}")
            
            print("="*70 + "\n")
            
            return top_15, len(records), len(symbol_stats)
            
        except Exception as e:
            print(f"ERROR generating summary: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def format_single_summary_message(self, days_back=0, summary_type="Daily"):
        """Format a single summary message for Telegram"""
        try:
            result = self.generate_top_15_summary(days_back, summary_type)
            
            if not result:
                return f"No volume spike data available for {summary_type.lower()}'s summary"
            
            top_15, total_records, unique_symbols = result
            
            # Get date range info
            now = datetime.now(ZoneInfo("Asia/Kolkata"))
            if days_back == 0:
                date_info = now.strftime("%d-%m-%Y")
            else:
                end_date = now
                start_date = now - timedelta(days=days_back)
                date_info = f"{start_date.strftime('%d-%m-%Y')} to {end_date.strftime('%d-%m-%Y')}"
            
            # Calculate total value across top 15 stocks
            total_top15_value = sum(stats['total_trd_val_cr'] for _, stats in top_15)
            
            message = f"""<b>{summary_type} Volume Spike Summary</b>
Date: {date_info}
Total Records: {total_records}
Unique Symbols: {unique_symbols}
Top 15 Total Value: Rs.{total_top15_value:,.2f} Cr

<b>TOP 15 RANKINGS (by Count):</b>

"""
            
            for idx, (symbol, stats) in enumerate(top_15, 1):
                count = stats['count']
                total_trd_val_cr = stats['total_trd_val_cr']
                avg_per_trade = total_trd_val_cr / count if count > 0 else 0
                
                message += f"""{idx}. <b>{symbol}</b>
   Count: <b>{count}</b> trades
   Total Value: Rs.{total_trd_val_cr:,.2f} Cr
   Avg per Trade: Rs.{avg_per_trade:.2f} Cr
   
"""
            
            message += f"""====================
<i>Analysis Complete for {date_info}</i>
<i>Ranked by highest trade count</i>
<i>Values from Trd_Val_Cr column</i>

Reply 'send' for fresh summary or 'done' to stop
"""
            
            return message
            
        except Exception as e:
            print(f"ERROR formatting {summary_type.lower()} summary message: {e}")
            import traceback
            traceback.print_exc()
            return f"ERROR generating {summary_type.lower()} summary: {str(e)}"
    
    def format_summary_message(self):
        """Format the summary message for Telegram with day-specific logic"""
        try:
            now = datetime.now(ZoneInfo("Asia/Kolkata"))
            current_day = now.strftime("%A")  # Monday, Tuesday, etc.

            print(f"Today is {current_day} - determining summary types...")

            messages = []

            # Always send daily summary
            print("Generating daily summary...")
            daily_summary = self.format_single_summary_message(0, "Daily")
            messages.append(daily_summary)

            # Wednesday and Friday: Add 3-day summary
            if current_day in ["Wednesday", "Friday"]:
                print("Generating 3-day summary...")
                three_day_summary = self.format_single_summary_message(2, "3-Day")
                messages.append(three_day_summary)

            # Friday only: Add weekly summary
            if current_day == "Friday":
                print("Generating weekly summary...")
                weekly_summary = self.format_single_summary_message(4, "Weekly")
                messages.append(weekly_summary)

            print(f"Generated {len(messages)} summary types")
            return messages

        except Exception as e:
            print(f"ERROR formatting summary message: {e}")
            import traceback
            traceback.print_exc()
            return [f"ERROR generating summary: {str(e)}"]

# =============================================================================
# SUMMARY SCHEDULER
# =============================================================================

def summary_scheduler():
    """Background thread to handle summary sending"""
    print("Summary scheduler started")

    summary_handler = SummaryTelegramHandler()
    summary_generator = DailySummaryGenerator()

    last_sent_date = None
    last_sent_time = None

    # Send summary immediately on startup for testing
    print("\n" + "="*50)
    print("SENDING IMMEDIATE SUMMARY ON STARTUP")
    print("="*50)
    try:
        summary_messages = summary_generator.format_summary_message()
        if summary_handler.send_messages(summary_messages):
            print("Initial summary sent successfully")
        else:
            print("Failed to send initial summary")
    except Exception as e:
        print(f"Error sending initial summary: {e}")
    print("="*50 + "\n")

    while True:
        try:
            now = datetime.now(ZoneInfo("Asia/Kolkata"))
            current_date = now.strftime("%d-%m-%Y")
            current_time = now.strftime("%H:%M")

            if last_sent_date != current_date:
                summary_handler.stop_sending_today = False
                last_sent_date = current_date
                last_sent_time = None
                print(f"New day started: {current_date}")

            summary_handler.check_for_done_message()

            # Print status every 5 minutes
            if now.minute % 5 == 0 and now.second < 10:
                print(f"[{current_time}] Scheduler running... (waiting for {SUMMARY_SEND_TIME})")
                print(f"   Stop flag: {summary_handler.stop_sending_today}")
                if last_sent_time:
                    print(f"   Last sent: {last_sent_time}")

            if (current_time >= SUMMARY_SEND_TIME and
                not summary_handler.stop_sending_today):

                should_send = False

                if last_sent_time is None:
                    should_send = True
                else:
                    last_dt = datetime.strptime(f"{current_date} {last_sent_time}", "%d-%m-%Y %H:%M")
                    current_dt = datetime.strptime(f"{current_date} {current_time}", "%d-%m-%Y %H:%M")
                    time_diff = (current_dt - last_dt).total_seconds()

                    if time_diff >= SUMMARY_SEND_INTERVAL:
                        should_send = True

                if should_send:
                    print(f"Sending summary at {current_time}")

                    summary_messages = summary_generator.format_summary_message()

                    if summary_handler.send_messages(summary_messages):
                        last_sent_time = current_time
                        print(f"Summary sent successfully at {current_time}")
                    else:
                        print(f"Failed to send summary at {current_time}")

            time.sleep(60)

        except Exception as e:
            print(f"Error in summary scheduler: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(60)

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
        self.access_token = FYERS_ACCESS_TOKEN
        self.is_authenticated = False
        self.telegram_handler = TelegramHandler()
        
    def generate_totp(self):
        """Generate TOTP code"""
        try:
            totp = pyotp.TOTP(self.totp_secret)
            totp_code = totp.now()
            print(f"Generated TOTP: {totp_code}")
            return totp_code
        except Exception as e:
            print(f"Error generating TOTP: {e}")
            return None
    
    def authenticate(self):
        """Perform fresh authentication"""
        try:
            print("="*50)
            print("Starting Fyers Authentication")
            print("="*50)
            
            is_valid, message = validate_fyers_token_from_json()
            if is_valid and FYERS_ACCESS_TOKEN:
                print("Using existing valid token from JSON file")
                self.access_token = FYERS_ACCESS_TOKEN
                self.is_authenticated = True
                return True
            
            print("Performing fresh authentication...")
            
            session = fyersModel.SessionModel(
                client_id=self.client_id,
                secret_key=self.secret_key,
                redirect_uri=self.redirect_uri,
                response_type="code",
                grant_type="authorization_code"
            )
            
            auth_url = session.generate_authcode()
            print(f"\nAuthorization URL: {auth_url}\n")
            
            telegram_message = f"""<b>Fyers Authentication Required</b>

Please click the link below to authorize:

{auth_url}

After authorizing, send the complete redirect URL here.
            """
            
            self.telegram_handler.send_message(telegram_message)
            
            auth_code = self.telegram_handler.wait_for_auth_code()
            
            if not auth_code:
                print("Failed to get auth code from Telegram")
                return False
            
            session.set_token(auth_code)
            response = session.generate_token()
            
            if response and 'access_token' in response:
                self.access_token = response['access_token']
                self.is_authenticated = True
                
                save_fyers_token_to_json(self.access_token)
                
                print("Authentication successful!")
                print(f"Access Token: {self.access_token[:20]}...")
                
                success_message = "<b>Fyers Authentication Successful!</b>\n\nYou can now start monitoring."
                self.telegram_handler.send_message(success_message)
                
                return True
            else:
                print(f"Authentication failed: {response}")
                return False
                
        except Exception as e:
            print(f"Error during authentication: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def get_access_token(self):
        """Get the access token"""
        if not self.is_authenticated:
            if not self.authenticate():
                return None
        return self.access_token

# =============================================================================
# VOLUME SPIKE DETECTOR CLASS
# =============================================================================

class VolumeSpikeDetector:
    def __init__(self):
        self.authenticator = FyersAuthenticator()
        self.fyers = None
        self.worksheet = None
        self.stop_event = threading.Event()
        self.telegram_handler = TelegramHandler()
        self.message_count = 0  # Debug counter for messages received
        self.last_debug_time = time.time()  # For periodic debug output
        
    def initialize(self):
        """Initialize Fyers and Google Sheets connections"""
        try:
            access_token = self.authenticator.get_access_token()
            if not access_token:
                print("Failed to get access token")
                return False
            
            self.fyers = fyersModel.FyersModel(
                client_id=self.authenticator.client_id,
                token=access_token,
                log_path=""
            )
            
            print("Fyers client initialized")
            
            if not self.initialize_sheets():
                print("Failed to initialize Google Sheets")
                return False
            
            return True
            
        except Exception as e:
            print(f"Error during initialization: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def initialize_sheets(self):
        """Initialize Google Sheets connection"""
        try:
            if not GOOGLE_CREDENTIALS:
                print("Google Sheets credentials not available")
                return False
            
            scopes = ['https://www.googleapis.com/auth/spreadsheets']
            creds = Credentials.from_service_account_info(GOOGLE_CREDENTIALS, scopes=scopes)
            client = gspread.authorize(creds)
            
            spreadsheet = client.open_by_key(GOOGLE_SHEETS_ID)
            self.worksheet = spreadsheet.sheet1
            
            headers = self.worksheet.row_values(1)
            if not headers:
                headers = ['Date', 'Time', 'Symbol', 'LTP', 'Volume_Spike', 'Trd_Val_Cr', 'Spike_Type', 'Sector', 'Symbol_Count', 'Sector_Count']
                self.worksheet.append_row(headers)
                print("Created new header row in Google Sheets with correct column name")
            
            print("Google Sheets initialized successfully")
            return True
            
        except Exception as e:
            print(f"Error initializing Google Sheets: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def add_to_sheet(self, data):
        """Add data to Google Sheets"""
        try:
            print(f"\n[DEBUG] add_to_sheet called for {data['symbol']}")

            if not self.worksheet:
                print("[ERROR] Worksheet not initialized")
                return False

            print(f"[DEBUG] Worksheet is initialized: {self.worksheet.title}")

            row = [
                data['date'],
                data['time'],
                data['symbol'],
                data['price'],
                data['volume'],
                data['value_crores'],
                data['type'],
                data['sector'],
                '',  # Symbol_Count placeholder
                ''   # Sector_Count placeholder
            ]

            print(f"[DEBUG] Preparing to append row: {row}")

            self.worksheet.append_row(row)

            print(f"[SUCCESS] Added to Google Sheets: {data['symbol']} - Rs{data['value_crores']:.2f} Cr")
            return True

        except Exception as e:
            print(f"[ERROR] Failed adding to Google Sheets: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def send_telegram_alert(self, data):
        """Send alert to Telegram"""
        try:
            message = f"""<b>Volume Spike Alert</b>

<b>Symbol:</b> {data['symbol']}
<b>Sector:</b> {data['sector']}
<b>Volume:</b> {data['volume']:,}
<b>Price:</b> Rs{data['price']:.2f}
<b>Value:</b> Rs{data['value_crores']:.2f} Crores
<b>Type:</b> {data['type']}
<b>Time:</b> {data['time']}
            """
            
            return self.telegram_handler.send_message(message)
            
        except Exception as e:
            print(f"Error sending Telegram alert: {e}")
            return False
    
    def on_message(self, message):
        """Callback for WebSocket messages"""
        try:
            self.message_count += 1

            # Periodic debug output every 10 seconds
            current_time = time.time()
            if current_time - self.last_debug_time >= 10:
                print(f"\n[DEBUG] Messages received so far: {self.message_count}")
                self.last_debug_time = current_time

            if check_market_end():
                print("Market ended, stopping monitoring...")
                self.stop_event.set()
                return

            # FIXED: Handle the actual Fyers WebSocket data structure
            # Data comes at root level, not inside 'd' key
            if not isinstance(message, dict):
                return

            # Skip control messages (authentication, mode changes, etc.)
            msg_type = message.get('type', '')
            if msg_type in ['cn', 'ful', 'sub']:  # Control message types
                return

            # Process symbol data messages (type 'sf' = symbol feed)
            if msg_type == 'sf' and 'symbol' in message and 'ltp' in message and 'last_traded_qty' in message:
                symbol = message['symbol']
                price = message['ltp']
                volume = message['last_traded_qty']  # FIXED: Use last_traded_qty instead of vol_traded_today

                trade_value = volume * price

                # Debug: Show threshold comparison for first few trades
                if self.message_count <= 30:
                    print(f"[DEBUG] {symbol}: last_traded_qty={volume:,}, price={price:.2f}, trade_value={trade_value:.2f}, threshold={INDIVIDUAL_TRADE_THRESHOLD}")

                if trade_value >= INDIVIDUAL_TRADE_THRESHOLD:
                    print(f"[DEBUG] Threshold met! trade_value={trade_value:.2f} >= threshold={INDIVIDUAL_TRADE_THRESHOLD}")

                    value_crores = trade_value / 10000000
                    sector = get_sector_for_symbol(symbol)

                    now = datetime.now(ZoneInfo("Asia/Kolkata"))

                    data = {
                        'date': now.strftime("%d-%m-%Y"),
                        'time': now.strftime("%H:%M:%S"),
                        'symbol': symbol,
                        'sector': sector,
                        'volume': volume,
                        'price': price,
                        'value_crores': value_crores,
                        'type': 'Individual Trade'
                    }

                    print(f"\n{'='*50}")
                    print(f"VOLUME SPIKE DETECTED!")
                    print(f"Symbol: {symbol}")
                    print(f"Sector: {sector}")
                    print(f"Volume: {volume:,}")
                    print(f"Price: Rs{price:.2f}")
                    print(f"Value: Rs{value_crores:.2f} Crores")
                    print(f"{'='*50}\n")

                    self.add_to_sheet(data)
                    self.send_telegram_alert(data)

        except Exception as e:
            print(f"Error processing message: {e}")
            import traceback
            traceback.print_exc()
    
    def on_error(self, error):
        """Callback for WebSocket errors"""
        print(f"\n[ERROR] WebSocket Error: {error}")
        print(f"[ERROR] Error type: {type(error)}")
        import traceback
        traceback.print_exc()
    
    def on_close(self):
        """Callback for WebSocket close"""
        print("WebSocket connection closed")
    
    def on_open(self):
        """Callback for WebSocket open"""
        print("WebSocket connection opened")

        # FIXED: Changed from "symbolData" to "SymbolUpdate" to match Fyers API v3
        data_type = "SymbolUpdate"
        symbols = STOCK_SYMBOLS[:MAX_SYMBOLS]

        print(f"\n[DEBUG] Subscription Details:")
        print(f"[DEBUG] Data Type: {data_type}")
        print(f"[DEBUG] Number of symbols: {len(symbols)}")
        print(f"[DEBUG] First 5 symbols: {symbols[:5]}")
        print(f"[DEBUG] Last 5 symbols: {symbols[-5:]}")

        print(f"\nSubscribing to {len(symbols)} symbols...")

        try:
            self.fyers_ws.subscribe(symbols=symbols, data_type=data_type)
            print("Subscription successful!")
        except Exception as e:
            print(f"[ERROR] Subscription failed: {e}")
            import traceback
            traceback.print_exc()

        print(f"Monitoring {len(symbols)} stocks for volume spikes...")
        print(f"Threshold: Rs{INDIVIDUAL_TRADE_THRESHOLD/10000000:.2f} Crores")
        print(f"\n[DEBUG] Waiting for data messages with 'd' key...")
    
    def start_monitoring(self):
        """Start monitoring stocks"""
        try:
            wait_for_market_start()
            
            print("\nStarting Volume Spike Detector...")
            print(f"Market Hours: {MARKET_START_TIME} - {MARKET_END_TIME}")
            print(f"Individual Trade Threshold: Rs{INDIVIDUAL_TRADE_THRESHOLD/10000000:.2f} Crores")
            print(f"Monitoring {MAX_SYMBOLS} symbols")
            
            access_token = f"{self.authenticator.client_id}:{self.authenticator.access_token}"
            
            self.fyers_ws = data_ws.FyersDataSocket(
                access_token=access_token,
                log_path="",
                litemode=False,
                write_to_file=False,
                reconnect=True,
                on_connect=self.on_open,
                on_close=self.on_close,
                on_error=self.on_error,
                on_message=self.on_message
            )
            
            self.fyers_ws.connect()
            
            while not self.stop_event.is_set():
                if check_market_end():
                    print("Market hours ended, stopping monitoring...")
                    break
                time.sleep(1)
            
            print("Closing WebSocket connection...")
            if self.fyers_ws:
                try:
                    self.fyers_ws.close()
                    print("WebSocket connection closed successfully")
                except Exception as e:
                    print(f"Error closing WebSocket: {e}")
            
        except Exception as e:
            print(f"Error in monitoring: {e}")
            import traceback
            traceback.print_exc()

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def start_all_services():
    """Start both the volume detector and summary scheduler"""
    print("Starting all services...")
    
    summary_thread = threading.Thread(target=summary_scheduler, daemon=True)
    summary_thread.start()
    print("Summary scheduler started in background")
    
    supervisor_loop()

if __name__ == "__main__":
    try:
        print("="*50)
        print("Fyers Volume Spike Detector with Daily Summary")
        print("="*50)
        
        start_all_services()
        
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
        _stop_stream_once()
    except Exception as e:
        print(f"\nFatal error: {e}")
        import traceback
        traceback.print_exc()