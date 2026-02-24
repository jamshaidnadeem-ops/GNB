"""
FastAPI application for retrieving car detailer leads from the database.
Simple endpoint to get all data in one go.
"""

import sys

# Fix console encoding for Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

from fastapi import FastAPI, HTTPException
import pymysql
from pymysql import Error
import logging

# Import database config from GNB.py
from GNB import TABLE_NAME, get_db_connection

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("api.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# =========================
# FASTAPI APP
# =========================
app = FastAPI(
    title="Car Detailers API",
    description="API to retrieve all car detailer leads from the database",
    version="1.0.0"
)

# =========================
# API ENDPOINT
# =========================
@app.get("/leads")
async def get_all_leads():
    """
    Get all leads from the database in one go
    
    Returns all leads with all their information:
    - City, Name, Rating, Address, Phone, Website, Timings
    - logo_url, services, pricing (from website scraping)
    - Scraped At, created_at timestamps
    """
    connection = None
    try:
        connection = get_db_connection()
        if not connection:
            raise HTTPException(status_code=500, detail="Failed to connect to database")
        
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        query = f"SELECT * FROM {TABLE_NAME} ORDER BY created_at DESC"
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        
        logging.info(f"Retrieved {len(results)} leads from database")
        return results
        
    except HTTPException:
        raise
    except Error as e:
        logging.error(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
    finally:
        if connection:
            connection.close()

# =========================
# NGROK SETUP
# =========================
NGROK_AUTHTOKEN = "37htcbzMDolE5iAOvCmhkJ7OQAR_4Q5L264jz1JAVpyYSZifz"

def setup_ngrok(port=8000):
    """Setup ngrok tunnel for the FastAPI server"""
    try:
        from pyngrok import ngrok
        
        # Set ngrok authtoken
        ngrok.set_auth_token(NGROK_AUTHTOKEN)
        logging.info("Ngrok authtoken configured")
        
        # Start ngrok tunnel
        public_url = ngrok.connect(port)
        logging.info("="*60)
        logging.info("NGROK TUNNEL CREATED")
        logging.info("="*60)
        logging.info(f"Public URL: {public_url}")
        logging.info(f"API Endpoint: {public_url}/leads")
        logging.info(f"API Docs: {public_url}/docs")
        logging.info("="*60)
        return public_url
    except ImportError:
        logging.warning("pyngrok not installed. Install with: pip install pyngrok")
        logging.warning("Server will only be accessible locally.")
        return None
    except Exception as e:
        logging.error(f"Error setting up ngrok: {e}")
        logging.warning("Server will only be accessible locally.")
        return None

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    import uvicorn
    import threading
    
    port = 8000
    
    logging.info("Starting FastAPI server...")
    
    # Setup ngrok in a separate thread to avoid blocking
    ngrok_thread = threading.Thread(target=setup_ngrok, args=(port,), daemon=True)
    ngrok_thread.start()
    
    # Give ngrok a moment to start
    import time
    time.sleep(2)
    
    # Start the FastAPI server
    uvicorn.run(app, host="0.0.0.0", port=port)

