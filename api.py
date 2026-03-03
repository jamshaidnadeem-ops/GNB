"""
GNB Headless API for Railway
Endpoints:
  POST /start  -> Trigger GNB Scraper (Postman)
  GET  /leads  -> Fetch all results from DB
  GET  /status -> Check running state and counts
  POST /stop   -> Stop the scraper
"""

import sys, os, subprocess, threading, time, logging
import pymysql
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("api.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

BASE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(BASE, "GNB.py")
TABLE_NAME = os.environ.get("TABLE_NAME", "car_detailers")

app = FastAPI(title="GNB Headless Scraper", version="3.0.0")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

# Database Configuration
DB_CONFIG = {
    "host":            os.environ.get("DB_HOST", ""),
    "port":            int(os.environ.get("DB_PORT", 18897)),
    "user":            os.environ.get("DB_USER", ""),
    "password":        os.environ.get("DB_PASSWORD", ""),
    "database":        os.environ.get("DB_NAME", "defaultdb"),
    "charset":         "utf8mb4",
}

def get_db_connection():
    try:
        return pymysql.connect(**DB_CONFIG)
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return None

# Scraper Process Management
_proc = None
_lock = threading.Lock()

def _is_alive():
    return _proc is not None and _proc.poll() is None

@app.get("/")
async def root():
    return {
        "message": "GNB Headless API is active.",
        "endpoints": {
            "POST /start": "Start the scraper",
            "POST /stop":  "Stop the scraper",
            "GET /leads":  "View all results",
            "GET /status": "Check scraper health"
        }
    }

@app.post("/start")
async def start_scraper():
    global _proc
    with _lock:
        if _is_alive():
            return {"status": "error", "message": "Scraper is already running"}
        
        if not os.path.exists(SCRIPT):
            raise HTTPException(status_code=404, detail="GNB.py script not found")
        
        try:
            # We use DEVNULL to avoid filling up pipes which can hang the process
            _proc = subprocess.Popen(
                [sys.executable, SCRIPT],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                cwd=BASE
            )
            logging.info(f"Scraper started with PID: {_proc.pid}")
            return {"status": "success", "message": "Scraper triggered", "pid": _proc.pid}
        except Exception as e:
            logging.error(f"Failed to start scraper: {e}")
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/stop")
async def stop_scraper():
    global _proc
    with _lock:
        if not _is_alive():
            return {"status": "info", "message": "Scraper is not running"}
        
        _proc.terminate()
        try:
            _proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _proc.kill()
        
        logging.info("Scraper stopped manually.")
        return {"status": "success", "message": "Scraper stopped"}

@app.get("/status")
async def get_status():
    stats = {}
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor(pymysql.cursors.DictCursor)
            cur.execute(f"SELECT COUNT(*) as total FROM {TABLE_NAME}")
            total = cur.fetchone()["total"]
            
            cur.execute(f"SELECT COUNT(DISTINCT City) as cities FROM {TABLE_NAME}")
            cities = cur.fetchone()["cities"]
            
            stats = {"total_leads": total, "total_cities": cities}
            cur.close()
        except:
            stats = {"error": "Could not fetch stats"}
        finally:
            conn.close()

    return {
        "running": _is_alive(),
        "stats": stats,
        "time": time.strftime("%Y-%m-%d %H:%M:%S")
    }

@app.get("/leads")
async def get_leads():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cur = conn.cursor(pymysql.cursors.DictCursor)
        cur.execute(f"SELECT * FROM {TABLE_NAME} ORDER BY `Scraped At` DESC")
        rows = cur.fetchall()
        cur.close()
        
        # Convert datetime objects to string for JSON serialization
        for r in rows:
            for k, v in r.items():
                if hasattr(v, "isoformat"):
                    r[k] = str(v)
                    
        return {"count": len(rows), "leads": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    logging.info(f"Starting API on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)