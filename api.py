"""
GNB FastAPI Server
Endpoints:
  GET  /              -> dashboard HTML
  POST /scraper/start -> start GNB.py (from dashboard OR Postman)
  POST /scraper/stop  -> stop scraper
  GET  /status        -> running state + progress + stats
  GET  /logs?since=N  -> tail log file (dashboard polls this)
  GET  /leads         -> all leads (Phase 1 + Phase 2 data)
  GET  /stats         -> aggregate counts

Railway deploy: start command = python api.py
Railway auto-sets PORT. Anyone can POST /scraper/start anytime.
"""

import sys, os, subprocess, threading, time, logging

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

# Load .env file (local dev). On Railway, vars come from the Variables dashboard.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import pymysql

BASE = os.path.dirname(os.path.abspath(__file__))

# ── Standalone DB config (no GNB import — avoids selenium dependency on Railway)
TABLE_NAME = os.environ.get("TABLE_NAME", "car_detailers")

DB_CONFIG = {
    "host":            os.environ.get("DB_HOST", ""),
    "port":            int(os.environ.get("DB_PORT", 18897)),
    "user":            os.environ.get("DB_USER", ""),
    "password":        os.environ.get("DB_PASSWORD", ""),
    "database":        os.environ.get("DB_NAME", "defaultdb"),
    "charset":         "utf8mb4",
    "connect_timeout": 30,
    "read_timeout":    30,
    "write_timeout":   30,
}

def get_db_connection(max_retries=3, retry_delay=5):
    for attempt in range(1, max_retries + 1):
        try:
            return pymysql.connect(**DB_CONFIG)
        except Exception as e:
            logging.warning(f"DB connect attempt {attempt}/{max_retries} failed: {e}")
            if attempt < max_retries:
                time.sleep(retry_delay)
    logging.error("All DB connection attempts failed.")
    return None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("api.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

LOG_FILE  = os.path.join(BASE, "gnb_scraper.log")
SCRIPT    = os.path.join(BASE, "GNB.py")
DASHBOARD = os.path.join(BASE, "gnb_dashboard.html")

app = FastAPI(title="GNB Scraper API", version="2.0.0")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


@app.get("/", include_in_schema=False)
async def root():
    if os.path.exists(DASHBOARD):
        return FileResponse(DASHBOARD, media_type="text/html")
    return {"message": "GNB API running. Place gnb_dashboard.html next to api.py"}


# ── Scraper process ───────────────────────────────────────────────────────────
_proc = None
_lock = threading.Lock()


def _alive():
    return _proc is not None and _proc.poll() is None


@app.post("/scraper/start")
async def start_scraper():
    """
    Start GNB.py as a background process.
    Call from dashboard button OR Postman:
      POST https://your-url.railway.app/scraper/start
    No request body needed.
    """
    global _proc
    with _lock:
        if _alive():
            raise HTTPException(status_code=409, detail="Scraper already running")
        if not os.path.exists(SCRIPT):
            raise HTTPException(status_code=404, detail=f"GNB.py not found at {SCRIPT}")
        try:
            _proc = subprocess.Popen(
                [sys.executable, SCRIPT],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                cwd=BASE,
            )
            logging.info(f"Scraper started PID={_proc.pid}")
            return {"status": "started", "pid": _proc.pid}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@app.post("/scraper/stop")
async def stop_scraper():
    global _proc
    with _lock:
        if not _alive():
            return {"status": "not_running"}
        _proc.terminate()
        try:
            _proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            _proc.kill()
        logging.info("Scraper stopped")
        return {"status": "stopped"}


# ── Status ────────────────────────────────────────────────────────────────────
@app.get("/status")
async def get_status():
    progress = []
    current_city = None
    stats = {}

    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor(pymysql.cursors.DictCursor)

            cur.execute("SELECT city, phase, status FROM scraper_progress ORDER BY id")
            progress = cur.fetchall()

            cur.execute(
                "SELECT city FROM scraper_progress WHERE status='in_progress' "
                "ORDER BY started_at DESC LIMIT 1"
            )
            row = cur.fetchone()
            if row:
                current_city = row["city"]

            cur.execute(f"SELECT COUNT(*) as n FROM {TABLE_NAME}")
            total = (cur.fetchone() or {}).get("n", 0)

            cur.execute(f"SELECT COUNT(DISTINCT City) as n FROM {TABLE_NAME}")
            cities = (cur.fetchone() or {}).get("n", 0)

            cur.execute(
                f"SELECT COUNT(*) as n FROM {TABLE_NAME} "
                f"WHERE Website!='N/A' AND Website LIKE 'http%%'"
            )
            websites = (cur.fetchone() or {}).get("n", 0)

            cur.execute(
                f"SELECT COUNT(*) as n FROM {TABLE_NAME} "
                f"WHERE Phone!='N/A' AND Phone!=''"
            )
            phones = (cur.fetchone() or {}).get("n", 0)

            stats = {
                "total": total, "cities": cities,
                "websites": websites, "phones": phones,
                "current_city": current_city,
            }
            cur.close()
        except Exception as e:
            logging.warning(f"Status DB error: {e}")
        finally:
            conn.close()

    return {
        "running": _alive(),
        "current_city": current_city,
        "progress": progress,
        "stats": stats,
    }


# ── Stats ─────────────────────────────────────────────────────────────────────
@app.get("/stats")
async def get_stats():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB connection failed")
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        total = cur.fetchone()[0]
        cur.execute(f"SELECT COUNT(DISTINCT City) FROM {TABLE_NAME}")
        cities = cur.fetchone()[0]
        cur.execute(
            f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE Website!='N/A' AND Website LIKE 'http%%'"
        )
        websites = cur.fetchone()[0]
        cur.execute(
            f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE Phone!='N/A' AND Phone!=''"
        )
        phones = cur.fetchone()[0]
        cur.close()
        return {"total": total, "cities": cities, "websites": websites, "phones": phones}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ── Live log tail ─────────────────────────────────────────────────────────────
@app.get("/logs")
async def get_logs(
    since: int = Query(0, ge=0, description="Return lines starting from this line number"),
    limit: int = Query(300, ge=1, le=1000),
):
    lines = []
    total = 0
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
                all_lines = f.readlines()
            total = len(all_lines)
            lines = [l.rstrip("\n") for l in all_lines[since: since + limit]]
        except Exception as e:
            logging.warning(f"Log read error: {e}")
    return {
        "lines": lines,
        "next_line": since + len(lines),
        "total": total,
        "running": _alive(),
    }


# ── Leads ─────────────────────────────────────────────────────────────────────
@app.get("/leads")
async def get_leads():
    """Returns all leads — includes Phase 1 data + Phase 2 (services, pricing, logo_url)."""
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB connection failed")
    try:
        cur = conn.cursor(pymysql.cursors.DictCursor)
        cur.execute(f"SELECT * FROM {TABLE_NAME} ORDER BY City, Name")
        rows = cur.fetchall()
        cur.close()
        for r in rows:
            for k, v in r.items():
                if hasattr(v, "isoformat"):
                    r[k] = str(v)
        logging.info(f"Served {len(rows)} leads")
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ── Ngrok (local dev only — skipped on Railway) ───────────────────────────────
NGROK_AUTHTOKEN = os.environ.get("NGROK_AUTHTOKEN", "")


def start_ngrok(port):
    try:
        from pyngrok import ngrok
        ngrok.set_auth_token(NGROK_AUTHTOKEN)
        url = ngrok.connect(port)
        logging.info("=" * 55)
        logging.info(f"DASHBOARD  : {url}")
        logging.info(f"START URL  : {url}/scraper/start  [POST]")
        logging.info(f"LEADS URL  : {url}/leads")
        logging.info(f"API DOCS   : {url}/docs")
        logging.info("=" * 55)
    except ImportError:
        logging.warning("pyngrok not installed — run: pip install pyngrok")
    except Exception as e:
        logging.error(f"ngrok error: {e}")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8000))  # Railway injects PORT automatically

    if not os.environ.get("RAILWAY_ENVIRONMENT"):
        threading.Thread(target=start_ngrok, args=(port,), daemon=True).start()
        time.sleep(2)

    logging.info(f"GNB Dashboard starting on port {port}")
    uvicorn.run("api:app", host="0.0.0.0", port=port)