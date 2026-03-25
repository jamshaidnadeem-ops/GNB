"""
GNB FastAPI Server (Digital Ocean)
  POST /start -> start scraper (GNB.py)
  POST /stop  -> stop scraper
  GET  /      -> simple info + dashboard if present
"""

import sys, os, subprocess, threading, time, logging

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

# Load .env file (local dev). On Digital Ocean, vars from environment.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import pymysql
from pymysql.cursors import DictCursor
from pydantic import BaseModel
from typing import List, Optional

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)
from GNB import TABLE_NAME, get_db_connection

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
    return {
        "message": "GNB Headless API is active.",
        "endpoints": {
            "POST /start": "Start the scraper",
            "POST /stop": "Stop the scraper",
            "GET /leads": "All leads",
            "GET /leads/full_details": "Leads with no N/A in any field",
        },
    }


# ── Scraper process ───────────────────────────────────────────────────────────
_proc = None
_lock = threading.Lock()


def _alive():
    return _proc is not None and _proc.poll() is None


class StartRequest(BaseModel):
    cities: List[str]


@app.post("/start")
async def start_scraper(request: StartRequest):
    """Start GNB.py as a background process. Cities list required in JSON."""
    global _proc
    with _lock:
        if _alive():
            raise HTTPException(status_code=409, detail="Scraper already running")
        if not os.path.exists(SCRIPT):
            raise HTTPException(status_code=404, detail=f"GNB.py not found at {SCRIPT}")
        try:
            # Convert list of cities to comma-separated string for GNB.py
            cities_str = ",".join(request.cities)
            
            # Inherit stdout/stderr
            _proc = subprocess.Popen(
                [sys.executable, SCRIPT, "--cities", cities_str],
                cwd=BASE,
            )
            logging.info(f"Scraper started PID={_proc.pid} for {len(request.cities)} cities")
            return {"status": "started", "pid": _proc.pid, "cities": request.cities}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@app.post("/stop")
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


# ── Status (optional) ─────────────────────────────────────────────────────────
@app.get("/status", include_in_schema=False)
async def get_status():
    progress = []
    current_city = None
    stats = {}

    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor(DictCursor)

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
                f"WHERE Website!='N/A' AND Website LIKE 'http%'"
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
            if conn:
                conn.close()

    return {
        "running": _alive(),
        "current_city": current_city,
        "progress": progress,
        "stats": stats,
    }


@app.get("/stats", include_in_schema=False)
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
            f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE Website!='N/A' AND Website LIKE 'http%'"
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
        if conn:
            conn.close()


@app.get("/logs", include_in_schema=False)
async def get_logs(
    since: int = Query(0, ge=0, description="Return lines starting from this line number"),
    limit: int = Query(300, ge=1, le=1000),
):
    lines = []
    total = 0
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
                total = 0
                for i, line in enumerate(f):
                    total = i + 1
                    if i < since:
                        continue
                    if len(lines) < limit:
                        lines.append(line.rstrip("\n"))
                    # If we already have enough lines, we must continue to count the rest
                    # or stop and store the final total elsewhere.
                    # To be truly efficient, we should just read the rest of the file
                    # for the count only.
                
        except Exception as e:
            logging.warning(f"Log read error: {e}")
    return {
        "lines": lines,
        "next_line": since + len(lines),
        "total": total,
        "running": _alive(),
    }


@app.get("/leads", include_in_schema=False)
async def get_leads():
    """Returns all leads — includes Phase 1 data + Phase 2 (services, pricing, logo_url)."""
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB connection failed")
    try:
        cur = conn.cursor(DictCursor)
        cur.execute(f"SELECT * FROM {TABLE_NAME} ORDER BY City, Name")
        rows = cur.fetchall()
        cur.close()
        processed = []
        for r in rows:
            row_dict = dict(r)
            for k, v in row_dict.items():
                if hasattr(v, "isoformat"):
                    row_dict[k] = str(v)
            processed.append(row_dict)
        logging.info(f"Served {len(processed)} leads")
        return processed
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@app.get("/leads/full_details", include_in_schema=True)
async def get_leads_full_details():
    """Returns leads that have no N/A in any key field (City, Name, Rating, Address, Phone, Website, Timings, reviews, logo_url, about_us, services, pricing)."""
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB connection failed")
    try:
        cur = conn.cursor(DictCursor)
        cur.execute(f"""
            SELECT * FROM {TABLE_NAME}
            WHERE City     != 'N/A' AND COALESCE(City, '') != ''
              AND Name     != 'N/A' AND COALESCE(Name, '') != ''
              AND Rating   != 'N/A' AND COALESCE(Rating, '') != ''
              AND Address  != 'N/A' AND COALESCE(Address, '') != ''
              AND Phone    != 'N/A' AND COALESCE(Phone, '') != ''
              AND Website  != 'N/A' AND COALESCE(Website, '') != ''
              AND Timings  != 'N/A' AND COALESCE(Timings, '') != ''
              AND reviews  != 'N/A' AND COALESCE(reviews, '') != ''
              AND logo_url != 'N/A' AND COALESCE(logo_url, '') != ''
              AND about_us != 'N/A' AND COALESCE(about_us, '') != ''
              AND services != 'N/A' AND COALESCE(services, '') != ''
              AND pricing  != 'N/A' AND COALESCE(pricing, '') != ''
            ORDER BY City, Name
        """)
        rows = cur.fetchall()
        cur.close()
        processed = []
        for r in rows:
            row_dict = dict(r)
            for k, v in row_dict.items():
                if hasattr(v, "isoformat"):
                    row_dict[k] = str(v)
            processed.append(row_dict)
        logging.info(f"Served {len(processed)} full-detail leads")
        return processed
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
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
        logging.info(f"START URL  : {url}/start  [POST]")
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

    port = int(os.environ.get("PORT", 8000))

    if os.environ.get("NGROK_AUTHTOKEN"):
        threading.Thread(target=start_ngrok, args=(port,), daemon=True).start()
        time.sleep(2)

    logging.info(f"GNB Dashboard starting on port {port}")
    uvicorn.run("api:app", host="0.0.0.0", port=port)