from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import logging
import random
import re
import os
import shutil
from datetime import datetime
import sys
from urllib.error import HTTPError
from bs4 import BeautifulSoup

try:
    import pymysql
    from pymysql import Error
except ImportError:
    logging.error("pymysql is not installed. Please install it with: pip install pymysql")
    raise

# Load .env file (local dev). On Railway, vars come from the Variables dashboard.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# =========================
# CONFIG
# =========================
BASE_URL = "https://www.google.com/maps/@40.6971415,-73.979506,8z?entry=ttu&g_ep=EgoyMDI2MDIwNC4wIKXMDSoASAFQAw%3D%3D"
SEARCH_QUERY = "car detailers"
# ── ALREADY SCRAPED (skipped — progress table handles these) ─────────────────
# Anaheim, Arlington, Aurora, Bakersfield, Chicago, Cleveland,
# Colorado Springs, Columbus, Houston, Los Angeles, New York,
# Philadelphia, Phoenix
# ─────────────────────────────────────────────────────────────────────────────
CITIES = [
  "Albuquerque", "Anchorage", "Atlanta", "Austin",
  "Baltimore", "Baton Rouge", "Birmingham", "Boise", "Boston", "Buffalo",
  "Chandler", "Charlotte", "Chesapeake", "Chula Vista", "Cincinnati",
  "Clarksville", "Corpus Christi",
  "Dallas", "Denver", "Des Moines", "Detroit", "Durham",
  "El Paso",
  "Fayetteville", "Fort Wayne", "Fort Worth", "Fremont", "Fresno",
  "Garland", "Gilbert", "Glendale", "Grand Rapids", "Greensboro",
  "Henderson", "Hialeah", "Honolulu", "Huntington Beach", "Huntsville",
  "Indianapolis", "Irvine", "Irving",
  "Jacksonville", "Jersey City",
  "Kansas City", "Knoxville",
  "Laredo", "Las Vegas", "Lexington", "Lincoln", "Long Beach", "Louisville", "Lubbock",
  "Madison", "Memphis", "Mesa", "Miami", "Milwaukee", "Minneapolis",
  "Modesto", "Montgomery", "Moreno Valley",
  "Nashville", "New Orleans", "Norfolk", "North Las Vegas",
  "Oakland", "Oklahoma City", "Omaha", "Orlando", "Oxnard",
  "Pittsburgh", "Plano", "Portland",
  "Raleigh", "Reno", "Richmond", "Riverside", "Rochester",
  "Sacramento", "Saint Paul", "Salt Lake City", "San Antonio",
  "San Bernardino", "San Diego", "San Francisco", "San Jose",
  "Santa Ana", "Santa Clarita", "Scottsdale", "Seattle",
  "Shreveport", "Spokane", "Stockton", "St. Louis", "St. Petersburg", "Syracuse",
  "Tacoma", "Tallahassee", "Tampa", "Toledo", "Tucson", "Tulsa",
  "Virginia Beach",
  "Washington DC", "Wichita", "Winston-Salem", "Worcester",
  "Yonkers",
]
MAX_LEADS_PER_CITY = 200
CITY_BATCH_SIZE = 1  # Restart browser between every city (prevents renderer memory leaks)
# Restart browser every N leads during Phase 2 to prevent tab crashes from memory buildup
PHASE2_RESTART_EVERY_N_LEADS = 5

# Delays (seconds)
SEARCH_DELAY = 2
DETAIL_PAGE_DELAY = 2
SCROLL_DELAY = 1.5
PAGE_LOAD_DELAY = 4

# ─── BACKGROUND MODE ───────────────────────────────────────────────────────────
# Instead of minimizing (which breaks Chrome rendering), we move the window
# OFF-SCREEN to a large negative coordinate. Chrome still thinks it is "visible"
# so lazy-load, Intersection Observers, and JS timers all keep firing normally.
# Set HEADLESS = True only for server/deployment (no display at all).
HEADLESS = True
WINDOW_WIDTH  = 1920
WINDOW_HEIGHT = 1080
# Off-screen position — browser is "open" but behind / off your monitor
OFFSCREEN_X = -3000
OFFSCREEN_Y = 0
# ───────────────────────────────────────────────────────────────────────────────

# Fix console encoding for Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("gnb_scraper.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# =========================
# DRIVER
# =========================

def _start_selenium_chrome_fallback(browser_path=None):
    """Fallback when undetected_chromedriver fails (e.g. 404 from ChromeDriver repo). Uses Selenium's Chrome + Selenium Manager."""
    from selenium.webdriver import Chrome
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    opts = ChromeOptions()
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-infobars")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--no-default-browser-check")
    opts.add_argument("--no-first-run")
    opts.add_argument("--ignore-certificate-errors")
    opts.add_argument("--password-store=basic")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-backgrounding-occluded-windows")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--disable-ipc-flooding-protection")
    opts.add_argument("--disable-hang-monitor")
    opts.add_argument("--disable-prompt-on-repost")
    opts.add_argument("--disable-client-side-phishing-detection")
    opts.add_argument("--disable-features=TranslateUI,OptimizationHints,MediaRouter,DialMediaRouteProvider")
    opts.add_argument("--metrics-recording-only")
    opts.add_argument("--mute-audio")
    opts.add_argument(f"--window-size={WINDOW_WIDTH},{WINDOW_HEIGHT}")
    opts.add_argument(f"--window-position={OFFSCREEN_X},{OFFSCREEN_Y}")
    opts.add_argument("--force-device-scale-factor=1")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    if HEADLESS:
        opts.add_argument("--headless=new")
        opts.add_argument("--disable-gpu")
    else:
        opts.add_argument("--use-gl=angle")
        opts.add_argument("--use-angle=swiftshader")
    opts.page_load_strategy = "eager"
    if browser_path:
        opts.binary_location = browser_path
    for attempt in range(1, 4):
        try:
            return Chrome(options=opts)
        except Exception as e:
            # Resource temporarily unavailable (errno 11) or driver obtain failure — retry with delay
            if attempt < 3 and (
                (isinstance(e, (BlockingIOError, OSError)) and getattr(e, "errno", None) == 11)
                or "resource temporarily unavailable" in str(e).lower()
                or "unable to obtain driver" in str(e).lower()
            ):
                logging.warning(f"Selenium Chrome start failed (attempt {attempt}/3), retrying in 10s: {e}")
                time.sleep(10)
                continue
            logging.error(f"Selenium Chrome fallback failed: {e}")
            return None
    return None


def start_driver():
    """Initialize undetected Chrome driver optimised for background use with enhanced stealth."""
    import undetected_chromedriver as uc
    # Try to detect version to avoid mismatch (Windows & Linux)
    version_main = None
    try:
        if sys.platform == 'win32':
            import winreg
            for reg_path in [
                (winreg.HKEY_CURRENT_USER,  r"Software\Google\Chrome\BLBeacon"),
                (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Google\Chrome\BLBeacon"),
            ]:
                try:
                    key = winreg.OpenKey(reg_path[0], reg_path[1])
                    v, _ = winreg.QueryValueEx(key, "version")
                    version_main = int(v.split('.')[0])
                    break
                except: continue
        else:
            # Linux detection (Railway/Docker)
            import subprocess
            for cmd in ["google-chrome", "google-chrome-stable", "chromium-browser"]:
                try:
                    out = subprocess.check_output([cmd, "--version"], stderr=subprocess.STDOUT).decode()
                    v = re.search(r'(\d+)\.', out)
                    if v:
                        version_main = int(v.group(1))
                        break
                except: continue
    except:
        pass
    
    if version_main:
        logging.info(f"Detected Chrome version: {version_main}")

    # On Linux (e.g. Digital Ocean), find Chrome so uc doesn't set binary_location to non-string.
    browser_path = None
    if sys.platform != "win32":
        for name in ["google-chrome", "google-chrome-stable", "chromium", "chromium-browser"]:
            p = shutil.which(name)
            if p and isinstance(p, str):
                browser_path = p
                break

    def create_options():
        opts = uc.ChromeOptions()
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-infobars")
        opts.add_argument("--disable-extensions")
        opts.add_argument("--disable-notifications")
        opts.add_argument("--no-default-browser-check")
        opts.add_argument("--no-first-run")
        opts.add_argument("--ignore-certificate-errors")
        opts.add_argument("--password-store=basic")
        opts.add_argument("--disable-background-timer-throttling")
        opts.add_argument("--disable-backgrounding-occluded-windows")
        opts.add_argument("--disable-renderer-backgrounding")
        opts.add_argument("--disable-ipc-flooding-protection")
        opts.add_argument("--disable-hang-monitor")
        opts.add_argument("--disable-prompt-on-repost")
        opts.add_argument("--disable-client-side-phishing-detection")
        opts.add_argument("--disable-features=TranslateUI,OptimizationHints,MediaRouter,DialMediaRouteProvider")
        opts.add_argument("--metrics-recording-only")
        opts.add_argument("--mute-audio")
        opts.add_argument(f"--window-size={WINDOW_WIDTH},{WINDOW_HEIGHT}")
        opts.add_argument(f"--window-position={OFFSCREEN_X},{OFFSCREEN_Y}")
        opts.add_argument("--force-device-scale-factor=1")
        opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
        if HEADLESS:
            opts.add_argument("--headless=new")
            opts.add_argument("--disable-gpu")
        else:
            opts.add_argument("--use-gl=angle")
            opts.add_argument("--use-angle=swiftshader")
        opts.page_load_strategy = 'eager'
        return opts

    try:
        kwargs = {"options": create_options(), "use_subprocess": True, "version_main": version_main}
        if browser_path:
            kwargs["browser_executable_path"] = browser_path
        driver = uc.Chrome(**kwargs)
    except (HTTPError, Exception) as e:
        if isinstance(e, HTTPError) and e.code == 404:
            logging.warning("undetected_chromedriver patcher got 404 (ChromeDriver repo). Falling back to standard Selenium Chrome.")
            driver = _start_selenium_chrome_fallback(browser_path)
            if driver is None:
                raise e
        elif "version" in str(e).lower() or "session not created" in str(e).lower():
            logging.warning(f"Version mismatch retry... Attempting fallback version. Error: {e}")
            try:
                kwargs = {"options": create_options(), "use_subprocess": True, "version_main": 145}
                if browser_path:
                    kwargs["browser_executable_path"] = browser_path
                driver = uc.Chrome(**kwargs)
            except Exception as e2:
                logging.warning(f"UC fallback failed: {e2}. Trying standard Selenium Chrome.")
                driver = _start_selenium_chrome_fallback(browser_path)
            if driver is None:
                raise e
        else:
            driver = None
            # On resource exhaustion (errno 11) after long runs, retry uc.Chrome with delay before fallback
            if (isinstance(e, (BlockingIOError, OSError)) and getattr(e, "errno", None) == 11) or "resource temporarily unavailable" in str(e).lower():
                for retry in range(2):
                    logging.warning(f"Resource temporarily unavailable, retrying in 10s (attempt {retry+2}/3)...")
                    time.sleep(10)
                    try:
                        kwargs = {"options": create_options(), "use_subprocess": True, "version_main": version_main}
                        if browser_path:
                            kwargs["browser_executable_path"] = browser_path
                        driver = uc.Chrome(**kwargs)
                        break
                    except Exception as re:
                        e = re
            if driver is None:
                logging.warning(f"undetected_chromedriver failed: {e}. Trying standard Selenium Chrome.")
                driver = _start_selenium_chrome_fallback(browser_path)
                if driver is None:
                    logging.error(f"Failed to initialize driver: {e}")
                    raise e

    driver.set_page_load_timeout(60)
    driver.set_script_timeout(20)   # Fail fast on frozen renderers (was 60 s)

    # Patch __del__ to prevent errors during cleanup
    if hasattr(driver, "__del__"):
        driver.__del__ = lambda *args, **kwargs: None

    # BLOCK 1: CDP-based Stealth Overrides
    # This is the most effective way to mask headless mode.
    # We patch navigator properties that anti-bot scripts (like Akamai, DataDome) check.
    stealth_script = """
    (() => {
        // 1. Remove webdriver flag
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

        // 2. Set languages and platform to common Windows values
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
        Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });

        // 3. Mock window.chrome (headless usually lacks this)
        window.chrome = {
            runtime: {},
            loadTimes: function() {},
            csi: function() {},
            app: {}
        };

        // 4. Mock Permissions
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
        );

        // 5. WebGL Vendor/Renderer (avoid SwiftShader/Mesa)
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(parameter) {
            if (parameter === 37445) return 'Intel Inc.';
            if (parameter === 37446) return 'Intel(R) Iris(R) Xe Graphics Direct3D11 vs_5_0 ps_5_0';
            return getParameter.apply(this, arguments);
        };
    })();
    """
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": stealth_script})
    except Exception as e:
        logging.warning(f"Failed to apply expanded stealth CDP: {e}")

    # Patch quit to avoid WinError 6 on Windows
    original_quit = driver.quit
    def patched_quit():
        try:
            original_quit()
        except OSError as e:
            if "WinError 6" not in str(e):
                raise
    driver.quit = patched_quit

    if not HEADLESS:
        try:
            driver.set_window_rect(x=OFFSCREEN_X, y=OFFSCREEN_Y,
                                   width=WINDOW_WIDTH, height=WINDOW_HEIGHT)
        except Exception:
            pass

    return driver



def js(driver, script, *args):
    """Shorthand for execute_script."""
    return driver.execute_script(script, *args)


def is_driver_alive(driver):
    try:
        driver.title
        return True
    except Exception:
        return False


def _is_tab_or_session_crash(e):
    """True if the exception indicates a dead tab or session (need browser restart)."""
    msg = (str(e) or "").lower()
    return (
        "tab crashed" in msg
        or "session not created" in msg
        or "invalid session" in msg
        or "target window already closed" in msg
        or "no such window" in msg
        or "connection refused" in msg
        or "max retries exceeded" in msg
    )


def get_cities_with_both_phases_completed():
    """Return set of city names that have both phase1 and phase2 completed in scraper_progress."""
    connection = get_db_connection()
    if not connection:
        return set()
    try:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT city FROM scraper_progress
            WHERE status = 'completed'
            GROUP BY city
            HAVING COUNT(DISTINCT phase) = 2
        """)
        rows = cursor.fetchall()
        cursor.close()
        return {row[0] for row in rows} if rows else set()
    except Exception as e:
        logging.warning(f"Could not get completed cities: {e}")
        return set()
    finally:
        connection.close()


def is_google_signin_page(driver):
    try:
        current_url = driver.current_url.lower()
        if 'accounts.google.com' in current_url or 'signin' in current_url:
            return True
        if driver.find_elements(By.XPATH, "//input[@type='email' and contains(@aria-label,'Email')]"):
            if 'google' in driver.title.lower():
                return True
        return False
    except:
        return False


def handle_google_signin(driver, wait, city):
    logging.warning("Detected Google sign-in page. Re-searching location...")
    try:
        return search_location(driver, wait, city)
    except Exception as e:
        logging.error(f"Error handling sign-in: {e}")
        return False

# =========================
# DATABASE CONFIGURATION
# =========================
DB_CONFIG = {
    'host':     os.environ.get('DB_HOST', ''),
    'port':     int(os.environ.get('DB_PORT', 18897)),
    'user':     os.environ.get('DB_USER', ''),
    'password': os.environ.get('DB_PASSWORD', ''),
    'database': os.environ.get('DB_NAME', 'defaultdb'),
    'charset':  'utf8mb4',
    'connect_timeout': 60,
    'read_timeout':    60,
    'write_timeout':   60,
    'autocommit':      True
}
TABLE_NAME = 'car_detailers'

# =========================
# DATABASE HELPERS
# =========================
def get_db_connection(max_retries=3, retry_delay=5):
    for attempt in range(1, max_retries + 1):
        try:
            connection = pymysql.connect(**DB_CONFIG)
            return connection
        except Exception as e:
            if attempt < max_retries:
                wait_time = retry_delay * attempt
                logging.warning(f"DB connect failed ({attempt}/{max_retries}): {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                logging.error(f"DB connect failed after {max_retries} attempts: {e}")
                return None
    return None


def init_database():
    connection = None
    try:
        connection = get_db_connection()
        if not connection:
            return False
        cursor = connection.cursor()

        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            City VARCHAR(255),
            Name VARCHAR(255),
            Rating VARCHAR(50),
            Address TEXT,
            Phone VARCHAR(50),
            Website TEXT,
            Timings TEXT,
            reviews TEXT,
            logo_url TEXT,
            about_us TEXT,
            services TEXT,
            pricing TEXT,
            `Scraped At` DATETIME,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_business (City, Name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)
        # Check if columns exist (for existing DBs)
        cursor.execute(f"SHOW COLUMNS FROM {TABLE_NAME} LIKE 'reviews'")
        if not cursor.fetchone():
            cursor.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN reviews TEXT AFTER Timings")
        cursor.execute(f"SHOW COLUMNS FROM {TABLE_NAME} LIKE 'about_us'")
        if not cursor.fetchone():
            cursor.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN about_us TEXT AFTER logo_url")
        cursor.execute(f"SHOW COLUMNS FROM {TABLE_NAME} LIKE 'phase2_retry_attempted'")
        if not cursor.fetchone():
            cursor.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN phase2_retry_attempted TINYINT(1) DEFAULT 0")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS scraper_progress (
            id INT AUTO_INCREMENT PRIMARY KEY,
            city VARCHAR(255) NOT NULL,
            phase ENUM('phase1','phase2') NOT NULL,
            status ENUM('in_progress','completed') DEFAULT 'in_progress',
            started_at DATETIME,
            completed_at DATETIME,
            UNIQUE KEY unique_city_phase (city, phase)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)

        connection.commit()
        logging.info("Database tables initialized.")
        return True
    except Error as e:
        logging.error(f"Error initializing database: {e}")
        return False
    finally:
        if connection:
            cursor.close()
            connection.close()


def mark_phase_started(city, phase):
    connection = get_db_connection()
    if not connection:
        return
    try:
        cursor = connection.cursor()
        cursor.execute("""
            INSERT INTO scraper_progress (city, phase, status, started_at)
            VALUES (%s, %s, 'in_progress', %s)
            ON DUPLICATE KEY UPDATE status='in_progress', started_at=%s
        """, (city, phase, datetime.now(), datetime.now()))
        connection.commit()
        cursor.close()
    except Exception as e:
        logging.warning(f"Could not mark phase started: {e}")
    finally:
        connection.close()


def mark_phase_completed(city, phase):
    connection = get_db_connection()
    if not connection:
        return
    try:
        cursor = connection.cursor()
        cursor.execute("""
            INSERT INTO scraper_progress (city, phase, status, completed_at)
            VALUES (%s, %s, 'completed', %s)
            ON DUPLICATE KEY UPDATE status='completed', completed_at=%s
        """, (city, phase, datetime.now(), datetime.now()))
        connection.commit()
        cursor.close()
        logging.info(f"Marked {city} {phase} as completed")
    except Exception as e:
        logging.warning(f"Could not mark phase completed: {e}")
    finally:
        connection.close()


def is_phase_completed(city, phase):
    connection = get_db_connection()
    if not connection:
        return False
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT status FROM scraper_progress WHERE city=%s AND phase=%s", (city, phase))
        row = cursor.fetchone()
        cursor.close()
        return row is not None and row[0] == 'completed'
    except Exception as e:
        logging.warning(f"Could not check phase status: {e}")
        return False
    finally:
        connection.close()


def report_progress():
    """
    Query scraper_progress and print how many cities have completed phase1 and phase2.
    Run with: python GNB.py --report
    """
    connection = get_db_connection()
    if not connection:
        print("Could not connect to database. Check .env / DB config.")
        return
    try:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT phase, COUNT(*) AS n
            FROM scraper_progress
            WHERE status = 'completed'
            GROUP BY phase
        """)
        rows = cursor.fetchall()
        cursor.close()
        phase1_count = phase2_count = 0
        for phase, n in rows:
            if phase == 'phase1':
                phase1_count = n
            elif phase == 'phase2':
                phase2_count = n
        print(f"Phase 1 completed: {phase1_count} cities")
        print(f"Phase 2 completed: {phase2_count} cities")
        print(f"(Phase 2 = logo, services, pricing scraped for that city)")
    except Exception as e:
        print(f"Error reporting progress: {e}")
    finally:
        connection.close()


def check_duplicate(connection, city, name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE City=%s AND Name=%s", (city, name))
        result = cursor.fetchone()
        cursor.close()
        return result[0] > 0
    except Error as e:
        logging.error(f"Error checking duplicate: {e}")
        return False


def save_google_maps_data(connection, city, name, rating, address, phone, website, timings, data, scraped_at):
    """
    Saves data using an EXISTING connection to reduce handshake overhead and timeout errors.
    """
    if not connection:
        return False
    try:
        if check_duplicate(connection, city, name):
            return False
        cursor = connection.cursor()
        cursor.execute(f"""
        INSERT INTO {TABLE_NAME}
        (City, Name, Rating, Address, Phone, Website, Timings, reviews, logo_url, about_us, services, pricing, `Scraped At`)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (city, name, rating, address, phone, website, timings, data.get('reviews', 'N/A'), "N/A", "N/A", "N/A", "N/A", scraped_at))
        # Autocommit is enabled in config, so no need for manual connection.commit()
        cursor.close()
        return True
    except Error as e:
        logging.error(f"Error saving data: {e}")
        return False


def update_website_data(city, name, logo_url, about_us, services, pricing):
    connection = None
    try:
        if logo_url == "N/A" and services == "N/A" and pricing == "N/A":
            return False
        connection = get_db_connection()
        if not connection:
            return False
        cursor = connection.cursor()
        cursor.execute(f"""
        UPDATE {TABLE_NAME} SET logo_url=%s, about_us=%s, services=%s, pricing=%s
        WHERE City=%s AND Name=%s
        """, (logo_url, about_us, services, pricing, city, name))
        if cursor.rowcount > 0:
            connection.commit()
            cursor.close()
            return True
        cursor.close()
        return False
    except Error as e:
        logging.error(f"Error updating website data: {e}")
        if connection:
            connection.rollback()
        return False
    finally:
        if connection:
            connection.close()


def get_leads_with_websites(connection, city):
    try:
        cursor = connection.cursor()
        # IMPORTANT: Use %% to escape literal % inside an f-string passed to pymysql.
        # pymysql uses % for parameter substitution, so a bare % in LIKE 'http%'
        # gets misread as a missing format argument → "not enough arguments" error.
        cursor.execute(f"""
        SELECT Name, Website FROM {TABLE_NAME}
        WHERE City=%s AND Website!='N/A' AND Website LIKE 'http%%'
        AND logo_url='N/A' AND services='N/A' AND pricing='N/A'
        """, (city,))
        results = cursor.fetchall()
        cursor.close()
        return results
    except Error as e:
        logging.error(f"Error getting leads with websites: {e}")
        return []


def get_existing_count(connection, city):
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE City=%s", (city,))
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result else 0
    except Error as e:
        logging.error(f"Error getting count: {e}")
        return 0


def get_existing_names(connection):
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT DISTINCT City, Name FROM {TABLE_NAME}")
        results = cursor.fetchall()
        cursor.close()
        return {(r[0].lower() if r[0] else '', r[1].lower() if r[1] else '') for r in results}
    except Error as e:
        logging.error(f"Error getting names: {e}")
        return set()


def mark_lead_phase2_retry_attempted(city, name):
    """Mark a lead as having been attempted in a phase-2 retry sweep so we don't retry it again."""
    connection = get_db_connection()
    if not connection:
        return
    try:
        cursor = connection.cursor()
        cursor.execute(f"""
            UPDATE {TABLE_NAME} SET phase2_retry_attempted = 1
            WHERE City = %s AND Name = %s
        """, (city, name))
        connection.commit()
        cursor.close()
    except Exception as e:
        logging.warning(f"Could not mark phase2_retry_attempted: {e}")
    finally:
        connection.close()


def get_na_leads_globally():
    """
    Return leads (across every city) that still have at least one of
    logo_url / services / pricing equal to 'N/A', have a valid website URL,
    and have NOT yet been attempted in a phase-2 retry sweep (phase2_retry_attempted = 0).
    Returns list of (city, name, website) tuples.
    """
    connection = get_db_connection()
    if not connection:
        logging.error("get_na_leads_globally: DB connection failed")
        return []
    try:
        cursor = connection.cursor()
        cursor.execute(f"""
            SELECT City, Name, Website
            FROM {TABLE_NAME}
            WHERE Website != 'N/A'
              AND Website LIKE 'http%%'
              AND (logo_url = 'N/A' OR services = 'N/A' OR pricing = 'N/A')
              AND (phase2_retry_attempted = 0 OR phase2_retry_attempted IS NULL)
            ORDER BY City, Name
        """)
        rows = cursor.fetchall()
        cursor.close()
        logging.info(f"[RETRY SWEEP] {len(rows)} leads still have N/A data globally.")
        return rows
    except Exception as e:
        logging.error(f"get_na_leads_globally error: {e}")
        return []
    finally:
        connection.close()


# =========================
# URL CLEANING
# =========================
def clean_and_validate_url(url):
    if not url or url == "N/A":
        return "N/A"
    url = url.split(',')[0]
    if 'google.com/url' in url or 'google.com/maps' in url:
        return "N/A"
    if url.startswith('http://'):
        url = url.replace('http://', 'https://', 1)
    if not url.startswith('https://') and not url.startswith('http://'):
        url = 'https://' + url
    return url.rstrip('/')

# =========================
# GOOGLE MAPS EXTRACTION
# (All reads use JS textContent so they work off-screen)
# =========================
def extract_name(driver):
    for by, sel in [
        (By.CSS_SELECTOR, "h1.DUwDvf"),
        (By.CSS_SELECTOR, "h1.LFB9uc"),
        (By.CSS_SELECTOR, "div.fontHeadlineLarge"),
        (By.CSS_SELECTOR, "h1.DUwDvf.lfPIob")
    ]:
        try:
            el = driver.find_element(by, sel)
            text = js(driver, "return arguments[0].textContent", el).strip()
            if text: return text
        except: pass
    return "N/A"


def extract_address(driver):
    """
    Extract the physical address of the business.
    Filters out phone numbers.
    """
    for by, sel in [
        # Priority 1: Explicit address button (most reliable)
        (By.XPATH, "//button[contains(@data-item-id,'address')]//div[contains(@class,'Io6YTe')]"),
        # Priority 2: Container with aria-label
        (By.XPATH, "//div[contains(@aria-label, 'Address')]"),
        # Priority 3: Generic info div
        (By.CSS_SELECTOR, "div.Io6YTe.fontBodyMedium.kR99db.fdkmkc"),
    ]:
        try:
            elements = driver.find_elements(by, sel)
            for el in elements:
                text = js(driver, "return arguments[0].textContent", el).strip()
                if not text or len(text) < 5:
                    continue
                
                # Skip if it looks like a phone number 
                # (e.g., "+1 907-854-4204" or "907-854-4204")
                digits_only = re.sub(r'\D', '', text)
                if len(digits_only) >= 7 and re.match(r'^[\+\s\d\-\(\).]{7,25}$', text):
                    continue

                return text
        except:
            pass
    return "N/A"


def extract_phone(driver):
    try:
        btns = driver.find_elements(By.XPATH,
            "//button[contains(@aria-label,'Phone:') or contains(@data-item-id,'phone')]")
        for btn in btns:
            divs = btn.find_elements(By.CSS_SELECTOR, "div.Io6YTe")
            for d in divs:
                text = js(driver, "return arguments[0].textContent", d).strip()
                cleaned = re.sub(r'\D', '', text)
                if len(cleaned) >= 7:
                    return cleaned
    except:
        pass
    return "N/A"


def extract_website(driver):
    for by, sel in [
        (By.XPATH, "//a[@data-item-id='authority']"),
        (By.XPATH, "//a[contains(@aria-label,'Website')]"),
    ]:
        try:
            link = driver.find_element(by, sel)
            href = link.get_attribute("href")
            if href and 'google.com/maps' not in href:
                return clean_and_validate_url(href)
        except:
            pass
    return "N/A"


def extract_rating(driver):
    """
    Extract the main business rating from the detail panel header.
    Uses the Name H1 as an anchor to ensure we are in the right panel.
    """
    try:
        # 1. Target the detail panel header where the name and rating coexist
        # h1.DUwDvf is the business name. The rating is usually a sibling or nearby.
        try:
            name_h1 = driver.find_element(By.CSS_SELECTOR, "h1.DUwDvf")
            # Go up to the header section (usually 2-3 levels up)
            header = js(driver, "return arguments[0].closest('.TIH4s') || arguments[0].parentElement.parentElement", name_h1)
            
            if header:
                # Search for F7nice specifically in this header
                containers = header.find_elements(By.CSS_SELECTOR, "div.F7nice")
                for c in containers:
                    text = js(driver, "return arguments[0].textContent", c).strip()
                    m = re.search(r'(\d\.\d)', text)
                    if m:
                        rating = m.group(1)
                        logging.debug(f"Rating found via H1 anchor: {rating}")
                        return rating
        except:
            pass

        # 2. Global search but strictly EXCLUDING the sidebar list
        # Sidebar cards have class 'Nv2PK' or 'jNb09'
        all_ratings = driver.find_elements(By.CSS_SELECTOR, "div.F7nice")
        for container in all_ratings:
            try:
                # Use JS to check if this element is inside the sidebar list
                is_in_sidebar = js(driver, """
                    var el = arguments[0];
                    while (el && el !== document.body) {
                        if (el.classList.contains('Nv2PK') || 
                            el.classList.contains('jNb09') || 
                            el.getAttribute('role') === 'article') return true;
                        el = el.parentElement;
                    }
                    return false;
                """, container)
                
                if not is_in_sidebar:
                    text = js(driver, "return arguments[0].textContent", container).strip()
                    m = re.search(r'(\d\.\d)', text)
                    if m: return m.group(1)
            except: continue

        # 3. Last resort: specific aria-label star search (excluding sidebar)
        stars = driver.find_elements(By.CSS_SELECTOR, "span[role='img'][aria-label*='stars']")
        for s in stars:
            try:
                is_in_sidebar = js(driver, "return !!arguments[0].closest('.Nv2PK, [role=\"article\"]')", s)
                if not is_in_sidebar:
                    label = s.get_attribute("aria-label") or ""
                    m = re.search(r'(\d\.\d)', label)
                    if m: return m.group(1)
            except: continue

    except Exception as e:
        logging.debug(f"Rating extraction error: {e}")
        
    return "N/A"


def extract_reviews(driver):
    """
    Extract up to 5 reviews from the Maps detail panel.
    Tries to click the Reviews tab via JS first (works in headless).
    Falls back to scrolling the panel if tab click fails.
    Extracted with multiple fallback detection for 'Lite' mode maps.
    """
    try:
        # Step 1: Try clicking the Reviews tab via JS
        clicked = False
        try:
            # Method A: Try by aria-label (Best for standard UI)
            all_btns = driver.find_elements(By.XPATH, "//button[@aria-label]")
            for b in all_btns:
                label = (b.get_attribute("aria-label") or "").lower()
                if "review" in label and "write" not in label and "disclosure" not in label and "learn more" not in label:
                    js(driver, "arguments[0].click()", b)
                    clicked = True
                    logging.info(f"Reviews tab clicked via label: {label[:60]}")
                    time.sleep(2)
                    break
            
            if not clicked:
                # Method B: Try direct text search in tabs (Lite mode fallback)
                tabs = driver.find_elements(By.CSS_SELECTOR, "div[role='tablist'] button")
                for t in tabs:
                    text = js(driver, "return arguments[0].textContent", t).strip()
                    if "Reviews" in text:
                        js(driver, "arguments[0].click()", t)
                        clicked = True
                        logging.info(f"Reviews tab clicked via text: {text}")
                        time.sleep(2)
                        break

            if not clicked:
                # Method C: Click the rating value itself (Last resort)
                rating_links = driver.find_elements(By.CSS_SELECTOR, "span[aria-label*='reviews']")
                for rl in rating_links:
                    js(driver, "arguments[0].click()", rl)
                    clicked = True
                    logging.info("Clicked rating link for reviews fallback.")
                    time.sleep(2)
                    break
        except Exception as e:
            logging.warning(f"Error clicking reviews tab: {e}")
            
        if clicked:
            # Settle delay
            time.sleep(4)
        else:
            logging.info("Reviews tab not found or already active. Attempting extraction anyway.")

        # Step 2: Scroll the panel to load cards
        panel = None
        for sel in [
            "div.m6QErb.DxyBCb.kA9KIf.dS8AEf",
            "div.m6QErb.DxyBCb",
            "div.m6QErb",
            "div[role='main']",
        ]:
            try:
                el = driver.find_element(By.CSS_SELECTOR, sel)
                if el:
                    panel = el
                    break
            except:
                pass

        if panel:
            for i in range(1, 8):
                js(driver, "arguments[0].scrollTop += 500", panel)
                js(driver, "arguments[0].dispatchEvent(new Event('scroll', {bubbles:true}))", panel)
                time.sleep(0.5)
            time.sleep(2)

        # Step 3: Parse review cards
        reviews = []
        cards = driver.find_elements(By.CSS_SELECTOR, "div.jftiEf")
        
        logging.info(f"Review cards found: {len(cards)}")

        for card in cards[:5]:
            try:
                # 1. Name
                name = js(driver, """
                    var n = arguments[0].querySelector('.d4r55') || 
                            arguments[0].querySelector('.fontTitleMedium') || 
                            arguments[0].querySelector('.TSZ61b');
                    return n ? n.textContent : "User";
                """, card)

                # 2. Comment
                text = js(driver, """
                    var t = arguments[0].querySelector('.wiI7pd') || 
                            arguments[0].querySelector('.MyEned') || 
                            arguments[0].querySelector('.K70oJc');
                    return t ? t.textContent : "";
                """, card)

                # 3. Rating
                rating = js(driver, "return (arguments[0].querySelector('.kv7ab1') || {}).ariaLabel", card) or \
                         js(driver, "return (arguments[0].querySelector('.kvMYJc') || {}).ariaLabel", card) or ""
                
                if text:
                    reviews.append(f"{name.strip()} ({rating.strip()}): {text.strip()}")
            except:
                pass

        # Step 4: Back to Overview
        try:
            overview_btn = driver.find_element(By.CSS_SELECTOR, "button[aria-label*='Overview'], [data-tab-index='0']")
            js(driver, "arguments[0].click()", overview_btn)
            time.sleep(1)
        except:
            pass

        if reviews:
            return " | ".join(reviews)

    except Exception as e:
        logging.warning(f"Error extracting reviews: {e}")
    return "N/A"





def _extract_timings_from_website(driver, website_url):
    """
    Fallback: navigate to the business website, scroll to footer,
    and extract opening hours using keyword + day-pattern regex matching.
    Navigates back to the Maps page after extraction so panel state is restored.
    """
    if not website_url or website_url == "N/A":
        return []
    try:
        current_url = driver.current_url
        
        # Set a shorter timeout specifically for external websites to prevent renderer hangs
        driver.set_page_load_timeout(35) 
        
        try:
            driver.get(website_url)
        except Exception as e:
            if "timeout" in str(e).lower():
                logging.warning(f"Website load timed out for {website_url}, attempting to extract partial data...")
            else:
                raise e

        time.sleep(2)
        # Scroll to wake up lazy elements
        js(driver, "window.scrollTo(0, document.body.scrollHeight / 2);")
        time.sleep(1)
        js(driver, "window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)

        soup = BeautifulSoup(driver.page_source, "html.parser")

        hour_keywords = re.compile(
            r"(business hours|opening hours|open hours|hours of operation|"
            r"our hours|timings|schedule|open daily|mon\s*[-\u2013]?\s*sun|"
            r"monday\s*[-\u2013]?\s*sunday|monday through)",
            re.IGNORECASE
        )
        day_pattern = re.compile(
            r"(mon(?:day)?|tue(?:sday)?|wed(?:nesday)?|thu(?:rsday)?|"
            r"fri(?:day)?|sat(?:urday)?|sun(?:day)?)"
            r"[\s\-\u2013:,]+(?:through|to|[-\u2013])?\s*"
            r"(mon(?:day)?|tue(?:sday)?|wed(?:nesday)?|thu(?:rsday)?|"
            r"fri(?:day)?|sat(?:urday)?|sun(?:day)?)?"
            r"[\s\-\u2013:,]*"
            r"(\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm)?)\s*[-\u2013to]+\s*"
            r"(\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm)?)",
            re.IGNORECASE
        )

        found_timings = []

        for tag in soup.find_all(["div", "section", "footer", "p", "li", "span", "td"]):
            text = tag.get_text(separator=" ", strip=True)
            if len(text) > 300 or len(text) < 5:
                continue
            if hour_keywords.search(text):
                matches = day_pattern.findall(text)
                if matches:
                    for m in matches:
                        day1, day2, t1, t2 = m
                        if day2:
                            entry = f"{day1.capitalize()}-{day2.capitalize()}: {t1} - {t2}"
                        else:
                            entry = f"{day1.capitalize()}: {t1} - {t2}"
                        if entry not in found_timings:
                            found_timings.append(entry)
                else:
                    clean = text.strip()
                    if re.search(r"\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm)", clean):
                        found_timings.append(clean)

        if not found_timings:
            time_line = re.compile(
                r"(mon(?:day)?|tue(?:sday)?|wed(?:nesday)?|thu(?:rsday)?|"
                r"fri(?:day)?|sat(?:urday)?|sun(?:day)?)"
                r"[\s\S]{0,40}?"
                r"(\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm))\s*[-\u2013to]+\s*"
                r"(\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm))",
                re.IGNORECASE
            )
            for m in time_line.finditer(soup.get_text(separator=" ")):
                entry = m.group(0).strip()
                if entry not in found_timings:
                    found_timings.append(entry)

        # Always navigate back to the Maps page so the detail panel is restored
        # Restore the standard timeout and go back
        try:
            driver.set_page_load_timeout(60)
            driver.get(current_url)
            time.sleep(2)
        except:
            pass

        return found_timings[:7]

    except Exception as e:
        logging.warning(f"Website timings fallback error: {e}")
        return []


def extract_timings(driver, website_url="N/A"):
    """
    Extract opening hours with 3-level fallback:
      1. Google Maps dropdown table (original GNB logic).
         — seen set is declared OUTSIDE the selector loop so the same day
           is never added twice even if both CSS and XPath find the same table.
         — break after first successful table so XPath doesn't re-process.
         — If 2+ days found, return immediately.
      2. If Maps gave 0-1 days: scrape remaining days from business website
         (no duplicates — only adds days not already found from Maps).
      3. If still only 1 day total after both: fill the remaining 6 days
         with the same hours.
    All DOM reads use JS textContent so they work off-screen.
    """
    ALL_DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    maps_timings = []

    # ── Level 1: Google Maps dropdown table ──────────────────────────────────
    try:
        # Only click toggle if the hours table is not already open
        if not driver.find_elements(By.CSS_SELECTOR, "table.eK4R0e"):
            for by, sel in [
                (By.CSS_SELECTOR, "div.OqCZI.fontBodyMedium.VrynGf.WVXvdc"),
                (By.XPATH, "//button[contains(@aria-label,'Hours') or contains(@aria-label,'hours')]"),
            ]:
                try:
                    el = driver.find_element(by, sel)
                    js(driver, "arguments[0].click()", el)
                    time.sleep(2)
                    break
                except:
                    pass

        time.sleep(1.5)

        # seen declared OUTSIDE the selector loop — prevents duplicate days
        # when both CSS and XPath selectors find the same underlying table.
        seen = set()
        for by, sel in [
            (By.CSS_SELECTOR, "table.eK4R0e"),
            (By.XPATH, "//div[@role='region']//table"),
        ]:
            try:
                table = driver.find_element(by, sel)
                rows = table.find_elements(By.CSS_SELECTOR, "tr.y0skZc") or \
                       table.find_elements(By.TAG_NAME, "tr")
                for row in rows:
                    try:
                        day_el = row.find_element(By.CSS_SELECTOR, "td.ylH6lf div")
                        day = js(driver, "return arguments[0].textContent", day_el).strip()
                        hrs_els = row.find_elements(By.CSS_SELECTOR, "td.mxowUb li.G8aQO")
                        if hrs_els:
                            hrs = js(driver, "return arguments[0].textContent", hrs_els[0]).strip()
                        else:
                            hrs_cell = row.find_element(By.CSS_SELECTOR, "td.mxowUb")
                            hrs = (hrs_cell.get_attribute("aria-label") or "").strip()
                        if day and hrs and day not in seen:
                            seen.add(day)
                            maps_timings.append(f"{day}: {hrs}")
                    except:
                        pass
                if maps_timings:
                    break  # found the table — don't try XPath fallback on same data
            except:
                pass

        if len(maps_timings) >= 2:
            logging.info(f"Timings from Maps table: {len(maps_timings)} days")
            return " | ".join(maps_timings)

    except Exception as e:
        logging.warning(f"Error extracting timings: {e}")

    # ── Level 2: Website fallback for missing days only ───────────────────────
    if len(maps_timings) < 2:
        logging.info("Maps timings incomplete — trying website fallback...")
        days_found = set(e.split(":")[0].strip().lower() for e in maps_timings)
        web_timings = _extract_timings_from_website(driver, website_url)
        if web_timings:
            for wt in web_timings:
                wt_day = wt.split(":")[0].strip().lower()
                if wt_day not in days_found:
                    maps_timings.append(wt)
                    days_found.add(wt_day)
        if len(maps_timings) >= 2:
            logging.info(f"Timings after website merge: {len(maps_timings)} days")
            return " | ".join(maps_timings)

    # ── Level 3: Still only 1 day — fill remaining days with same hours ───────
    if len(maps_timings) == 1:
        logging.info("Only 1 day total — filling remaining days with same hours.")
        single = maps_timings[0]
        hours = single.split(":", 1)[1].strip() if ":" in single else ""
        found_day = single.split(":")[0].strip().lower()
        result = list(maps_timings)
        for d in ALL_DAYS:
            if d.lower() != found_day:
                result.append(f"{d}: {hours}")
        return " | ".join(result)

    return "N/A"

# =========================
# BACKGROUND-SAFE SCROLLING
# =========================
def scroll_results_container(driver, target_count):
    """
    Scroll the Google Maps results panel using pure JS on the feed container.
    Does NOT use scrollIntoView or window.scroll (both fail when off-screen).
    Fires synthetic scroll events to wake Intersection Observers.
    """
    container = None
    for by, sel in [
        (By.CSS_SELECTOR, "div[role='feed']"),
        (By.CSS_SELECTOR, "div.m6QErb.DxyBCb.kA9KIf.dS8AEf"),
        (By.CSS_SELECTOR, "div.m6QErb"),
    ]:
        try:
            el = driver.find_element(by, sel)
            if el:
                container = el
                break
        except:
            pass

    if not container:
        logging.warning("Could not find results container for scrolling")
        return

    last_count = len(get_all_result_cards(driver))
    no_change_streak = 0
    max_no_change = 6

    for attempt in range(300):
        current_count = len(get_all_result_cards(driver))
        if current_count >= target_count:
            logging.info(f"Target card count reached: {current_count}")
            break

        # Scroll the container via JS — always works regardless of window state
        js(driver, "arguments[0].scrollTop = arguments[0].scrollHeight", container)
        time.sleep(SCROLL_DELAY)

        # Fire a synthetic scroll event to wake up Intersection Observers
        js(driver, """
            var evt = new Event('scroll', {bubbles: true});
            arguments[0].dispatchEvent(evt);
        """, container)
        time.sleep(0.3)

        new_count = len(get_all_result_cards(driver))

        if attempt % 10 == 0:
            logging.info(f"Scroll attempt {attempt}: {new_count} cards loaded")

        # End-of-results check
        try:
            page_lower = driver.page_source.lower()
            if any(t in page_lower for t in ["reached the end", "no more results"]):
                logging.info("End of results detected.")
                break
        except:
            pass

        if new_count > last_count:
            no_change_streak = 0
            last_count = new_count
        else:
            no_change_streak += 1
            if no_change_streak >= max_no_change:
                # Nudge: scroll back up slightly then back down to re-trigger IO
                js(driver, "arguments[0].scrollTop -= 800", container)
                time.sleep(1)
                js(driver, "arguments[0].scrollTop = arguments[0].scrollHeight", container)
                time.sleep(SCROLL_DELAY * 2)
                final_count = len(get_all_result_cards(driver))
                if final_count <= last_count:
                    logging.info(f"No new cards after nudge. Stopping at {final_count} cards.")
                    break
                no_change_streak = 0
                last_count = final_count

    logging.info(f"Scrolling complete. Total cards: {len(get_all_result_cards(driver))}")


def smart_click_card(driver, card):
    """
    Click a result card using CDP Input.dispatchMouseEvent on the element's
    bounding rect centre — this bypasses all visibility/focus requirements.
    Falls back to plain JS click.
    """
    # Method 1: CDP mouse event (works regardless of window focus/position)
    try:
        rect = js(driver, """
            var r = arguments[0].getBoundingClientRect();
            return {x: r.left + r.width/2, y: r.top + r.height/2, w: r.width, h: r.height};
        """, card)

        if rect and rect.get('w', 0) > 0:
            x, y = rect['x'], rect['y']
            driver.execute_cdp_cmd("Input.dispatchMouseEvent", {
                "type": "mousePressed", "x": x, "y": y,
                "button": "left", "clickCount": 1
            })
            time.sleep(0.05)
            driver.execute_cdp_cmd("Input.dispatchMouseEvent", {
                "type": "mouseReleased", "x": x, "y": y,
                "button": "left", "clickCount": 1
            })
            return True
    except Exception as e:
        logging.debug(f"CDP click failed: {e}")

    # Method 2: Plain JS click
    try:
        js(driver, "arguments[0].click()", card)
        return True
    except Exception as e:
        logging.debug(f"JS click failed: {e}")
        return False


def get_all_result_cards(driver):
    """Get result cards. Never checks is_displayed() so works off-screen."""
    for selector in ["a.hfpxzc", "div.Nv2PK", "a[href*='/maps/place/']", "div[role='article']"]:
        try:
            cards = driver.find_elements(By.CSS_SELECTOR, selector)
            valid = []
            for card in cards:
                try:
                    label = card.get_attribute("aria-label")
                    if label and len(label) > 3:
                        valid.append(card)
                except:
                    pass
            if valid:
                return valid
        except:
            pass
    return []

# =========================
# GOOGLE MAPS SEARCHING
# =========================
def search_location(driver, wait, city):
    try:
        logging.info(f"Navigating to Google Maps for: {city}")
        driver.get(BASE_URL)
        time.sleep(SEARCH_DELAY)

        search_box = None
        for by, sel in [
            (By.ID, "ucc-1"),
            (By.CSS_SELECTOR, "input.UGojuc.fontBodyMedium.EmSKud.lpggsf"),
            (By.CSS_SELECTOR, "input#searchboxinput"),
            (By.XPATH, "//input[@id='searchboxinput']"),
        ]:
            try:
                search_box = wait.until(EC.presence_of_element_located((by, sel)))
                break
            except:
                pass

        if not search_box:
            logging.error("Could not find search box!")
            return False

        query = f"{SEARCH_QUERY} in {city}"
        logging.info(f"Searching: {query}")

        js(driver, "arguments[0].value = ''", search_box)
        js(driver, "arguments[0].focus()", search_box)
        search_box.send_keys(query)
        time.sleep(1.5)

        # Submit form via JS then also send Enter as backup
        js(driver, "if(arguments[0].form) arguments[0].form.submit()", search_box)
        time.sleep(1)
        try:
            search_box = driver.find_element(By.CSS_SELECTOR, "input#searchboxinput")
            search_box.send_keys(Keys.ENTER)
        except:
            pass

        deadline = time.time() + 20
        while time.time() < deadline:
            if driver.find_elements(By.CSS_SELECTOR, "div[role='feed'], a.hfpxzc, div.Nv2PK"):
                logging.info("Search results loaded.")
                return True
            time.sleep(1)

        logging.warning("Timed out waiting for results — continuing anyway.")
        return True

    except Exception as e:
        logging.error(f"Error in search_location: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return False

# =========================
# DETAIL EXTRACTION
# =========================
def scrape_dealership_details(driver, wait):
    """
    Collect all Maps panel fields for one business.
    IMPORTANT: reviews runs BEFORE timings.
    Reason: _extract_timings_from_website navigates away from Maps to the
    business website and then back — this reloads the Maps panel and destroys
    the div.jftiEf review cards before extract_reviews can read them.
    Running reviews first avoids this race condition entirely.
    """
    try:
        time.sleep(DETAIL_PAGE_DELAY)
        website = extract_website(driver)
        name    = extract_name(driver)
        rating  = extract_rating(driver)
        address = extract_address(driver)
        phone   = extract_phone(driver)
        reviews = extract_reviews(driver)
        timings = extract_timings(driver, website_url=website)
        return {
            'name':    name,
            'rating':  rating,
            'address': address,
            'phone':   phone,
            'website': website,
            'timings': timings,
            'reviews': reviews,
        }
    except Exception as e:
        logging.error(f"Error scraping details: {e}")
        return None

# =========================
# WEBSITE SCRAPING
# =========================
# Max scroll steps: 150 × 300 px = 45 000 px cap.
# Prevents infinite loops on huge SPAs that grow their DOM while scrolling.
_MAX_SCROLL_STEPS = 150

def scroll_page_fully(driver):
    """Smoothly scroll down to trigger lazy loading and then return to top.
    Capped at _MAX_SCROLL_STEPS iterations so a runaway page can never block forever.
    """
    try:
        logging.info("Scrolling page smoothly...")
        total_height = js(driver, "return document.body.scrollHeight") or 0
        step = 300
        steps_taken = 0
        for i in range(step, total_height + step, step):
            if steps_taken >= _MAX_SCROLL_STEPS:
                logging.warning(f"Scroll capped at {_MAX_SCROLL_STEPS} steps ({steps_taken*step}px).")
                break
            js(driver, f"window.scrollTo(0, {i});")
            time.sleep(0.1)
            steps_taken += 1
        # One last jump to ensure we hit the bottom
        js(driver, "window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        js(driver, "window.scrollTo(0, 0);")
        time.sleep(0.5)
    except Exception as e:
        logging.warning(f"Scroll failed: {e}")


def extract_logo_url(driver):
    for by, sel in [
        (By.XPATH, "//header//img[1]"),
        (By.CSS_SELECTOR, "img[class*='logo' i]"),
        (By.CSS_SELECTOR, "img[alt*='logo' i]"),
        (By.CSS_SELECTOR, "img[id*='logo' i]"),
        (By.CSS_SELECTOR, "a.navbar-brand img"),
        (By.CSS_SELECTOR, ".logo img"),
        (By.CSS_SELECTOR, "header img:first-of-type"),
        (By.CSS_SELECTOR, "nav img:first-of-type"),
        (By.CSS_SELECTOR, "img[fetchpriority='high']"),
    ]:
        try:
            el = driver.find_element(by, sel)
            src = el.get_attribute("src")
            if src:
                return src
        except:
            pass
    return "N/A"


def extract_services(driver):
    service_keywords = {
        'hand wash': 'Hand Wash', 'hand car wash': 'Hand Car Wash',
        'pressure wash': 'Pressure Wash', 'touchless wash': 'Touchless Wash',
        'express wash': 'Express Wash', 'full service wash': 'Full Service Wash',
        'wax': 'Waxing', 'hand wax': 'Hand Waxing',
        'ceramic coating': 'Ceramic Coating', 'nano coating': 'Nano Coating',
        'graphene coating': 'Graphene Coating', 'paint sealant': 'Paint Sealant',
        'auto detailing': 'Auto Detailing', 'car detailing': 'Car Detailing',
        'mobile detailing': 'Mobile Detailing', 'interior detailing': 'Interior Detailing',
        'exterior detailing': 'Exterior Detailing', 'full detail': 'Full Detailing',
        'express detail': 'Express Detailing', 'premium detail': 'Premium Detailing',
        'paint correction': 'Paint Correction', 'paint restoration': 'Paint Restoration',
        'scratch removal': 'Scratch Removal', 'swirl removal': 'Swirl Removal',
        'buffing': 'Buffing & Polishing', 'polishing': 'Polishing',
        'paint protection film': 'Paint Protection Film (PPF)', 'ppf': 'PPF',
        'clear bra': 'Clear Bra', 'vinyl wrap': 'Vinyl Wrapping',
        'headlight restoration': 'Headlight Restoration',
        'interior cleaning': 'Interior Cleaning', 'carpet cleaning': 'Carpet Cleaning',
        'upholstery cleaning': 'Upholstery Cleaning', 'leather cleaning': 'Leather Cleaning',
        'steam cleaning': 'Steam Cleaning', 'odor removal': 'Odor Removal',
        'pet hair removal': 'Pet Hair Removal', 'stain removal': 'Stain Removal',
        'wheel cleaning': 'Wheel Cleaning', 'tire shine': 'Tire Shine',
        'engine bay cleaning': 'Engine Bay Cleaning', 'window tinting': 'Window Tinting',
        'dent repair': 'Dent Repair', 'paintless dent repair': 'Paintless Dent Repair',
        'powder coating': 'Powder Coating', 'trim restoration': 'Trim Restoration',
    }
    try:
        page_text = js(driver, "return document.body.innerText").lower()
        services = []
        for kw, name in service_keywords.items():
            if kw in page_text and name not in services:
                services.append(name)

        # Keyword-based mapping logic requested by user
        if services:
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            # Look for specific high-value phrases in headings/divs
            for tag in soup.find_all(['h1', 'h2', 'h3', 'div', 'li']):
                txt = tag.get_text().lower()
                if any(k in txt for k in ['discover', 'experts', 'services', 'packages']):
                    # Check if any service keywords are in this specific element's text
                    for kw, name in service_keywords.items():
                        if kw in txt and name not in services:
                            services.append(name)

        return list(set(services)) if services else ["N/A"]
    except Exception as e:
        logging.error(f"Error extracting services: {e}")
        return ["N/A"]


def extract_about_us(driver):
    # Phrases to skip (Boilerplate/Cookies)
    exclude_patterns = [
        'cookies', 'browser activity', 'privacy policy', 'terms of use',
        'personalize content', 'analyze how our sites', 'review our terms',
        'improve your experience', 'personalize content and ads',
        'more information on how we collect and use', 'please review ourprivacy policy',
        'we use cookies', 'to improve your experience', 'personalize content'
    ]
    # Minimum chars required — avoids returning nav labels like "About Us" (8 chars)
    MIN_LENGTH = 80

    try:
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        patterns = ['Discover a better way', 'About Us', 'Our story', 'Who we are']
        for p in patterns:
            target = soup.find(string=re.compile(p, re.IGNORECASE))
            if target:
                parent = target.parent
                # Walk up the DOM up to 5 levels until we find real content
                for _ in range(5):
                    text = parent.get_text(separator=' ', strip=True)[:1500]
                    if (len(text) >= MIN_LENGTH
                            and not any(ep in text.lower() for ep in exclude_patterns)):
                        return text
                    if parent.parent:
                        parent = parent.parent
                    else:
                        break

        # Last resort: longest descriptive paragraph
        paragraphs = [p.get_text(strip=True) for p in soup.find_all('p') if len(p.get_text()) > 150]
        for p_text in paragraphs:
            if not any(ep in p_text.lower() for ep in exclude_patterns):
                return p_text

    except Exception as e:
        logging.warning(f"About us extraction error: {e}")
    return "N/A"


def find_pricing_cards(driver):
    """Scan page for pricing cards like in the user image."""
    found_pricing = []
    try:
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        price_pattern = re.compile(r'\$\d+(?:,\d{3})*(?:\.\d{2})?')

        # Method 1: Container-level check (High Confidence)
        containers = soup.find_all(['div', 'section', 'article', 'li'])
        for container in containers:
            text = container.get_text(separator=' ', strip=True)
            pm = price_pattern.search(text)
            if pm:
                title = "Unknown"
                # Look for bold/header text first
                headers = container.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'strong', 'b', 'title'])
                for h in headers:
                    h_txt = h.get_text(strip=True)
                    if h_txt and 3 < len(h_txt) < 80 and '$' not in h_txt:
                        title = h_txt
                        break

                # If no header, take first few words of the container if they look like a title
                if title == "Unknown":
                    lines = [l.strip() for l in text.split(' ') if l.strip()]
                    if lines:
                        title = " ".join(lines[:3])

                if title != "Unknown":
                    found_pricing.append(f"{title}: {pm.group(0)}")

        # Method 2: Global text node check (Fallback)
        if not found_pricing:
            for text_node in soup.find_all(string=re.compile(r'\$\d+')):
                parent = text_node.parent
                price_match = price_pattern.search(text_node)
                if not price_match:
                    continue
                price = price_match.group(0)

                # Look for a title nearby
                temp = parent
                title = "N/A"
                for _ in range(4):
                    if not temp:
                        break
                    potential = temp.find_parent(['div', 'section'])
                    if potential:
                        h = potential.find(['h1', 'h2', 'h3', 'h4', 'strong', 'b'])
                        if h:
                            t = h.get_text(strip=True)
                            if t and '$' not in t:
                                title = t
                                break
                    temp = temp.parent

                if title != "N/A":
                    found_pricing.append(f"{title}: {price}")

    except Exception as e:
        logging.warning(f"Error in find_pricing_cards: {e}")

    # Deduplicate and filter
    clean_pricing = []
    seen_names = set()
    for s in found_pricing:
        if ':' in s:
            name, p = s.split(':', 1)
            name = name.strip()
            if name.lower() not in seen_names and 2 < len(name) < 100:
                clean_pricing.append(s)
                seen_names.add(name.lower())

    return clean_pricing


def navigate_to_pricing_page(driver):
    """Look for Pricing/Services in header, handle dropdowns/links."""
    logging.info("Checking header for Pricing/Services links...")

    # Try the specific XPath provided by user first
    hint_xpath = "/html/body/div[1]/div/nav/div/div/div[2]/a[3]"
    try:
        link = driver.find_element(By.XPATH, hint_xpath)
        if link.is_displayed():
            logging.info(f"Found hint link: {link.text}")
            js(driver, "arguments[0].click()", link)
            time.sleep(3)
            return True
    except:
        pass

    # Strategy: Find nav links and hover/click them
    try:
        from selenium.webdriver.common.action_chains import ActionChains
        links = driver.find_elements(By.TAG_NAME, "a")

        # Priority 1: Direct link search
        for link in links:
            try:
                t = link.text.strip().lower()
                if t in ['pricing', 'services', 'packages', 'our services', 'plans']:
                    logging.info(f"Clicking Direct Nav Link: {t}")
                    js(driver, "arguments[0].click()", link)
                    time.sleep(3)
                    return True
            except:
                continue

        # Priority 2: Dropdown menus
        for link in links:
            try:
                t = link.text.strip().lower()
                if any(k in t for k in ['services', 'detailing', 'menu']):
                    # Hover to trigger dropdown
                    ActionChains(driver).move_to_element(link).perform()
                    time.sleep(1)
                    # Look for newly visible links
                    sub_links = driver.find_elements(By.TAG_NAME, "a")
                    for sl in sub_links:
                        if sl.is_displayed():
                            st = sl.text.strip().lower()
                            if any(sk in st for sk in ['detailing', 'wash', 'ceramic', 'pricing']):
                                logging.info(f"Found dropdown option: {st}")
                                js(driver, "arguments[0].click()", sl)
                                time.sleep(3)
                                return True
            except:
                continue
    except:
        pass
    return False


def extract_pricing(driver, services):
    """Enhanced pricing extraction using card-based detection."""
    results = find_pricing_cards(driver)
    if not results:
        return ["Contact for pricing"]
    return results


# Hard per-website budget: 3 minutes total regardless of how many calls hang.
_WEBSITE_TIMEOUT_SECONDS = 180

def scrape_website_details(driver, url, business_name):
    """Scrape a single business website with a hard 3-minute wall-clock timeout.
    Uses a daemon thread so one frozen Chrome renderer can never block Phase 2 forever.
    """
    import threading
    logging.info(f"Scraping website: {url}")

    # Shared mutable container so the inner thread can return a value
    result_box = [{'logo_url': 'N/A', 'about_us': 'N/A', 'services': [], 'pricing': []}]

    def _scrape():
        details = {'logo_url': 'N/A', 'about_us': 'N/A', 'services': [], 'pricing': []}
        try:
            for retry in range(2):
                try:
                    driver.get(url)
                    time.sleep(3)
                    if len(driver.page_source) > 500:
                        break
                except:
                    time.sleep(2)

            time.sleep(PAGE_LOAD_DELAY)
            scroll_page_fully(driver)

            # Homepage collection
            details['logo_url'] = extract_logo_url(driver)
            details['about_us'] = extract_about_us(driver)
            details['services'] = extract_services(driver)
            details['pricing']  = find_pricing_cards(driver)

            # Try navigating to pricing/services page if needed
            if navigate_to_pricing_page(driver):
                scroll_page_fully(driver)
                sub_pricing  = find_pricing_cards(driver)
                sub_services = extract_services(driver)
                details['pricing']  = list(set(details['pricing']  + sub_pricing))
                details['services'] = list(set(details['services'] + sub_services))

            if not details['pricing']:
                details['pricing'] = ["Contact for pricing"]

        except Exception as e:
            logging.error(f"Error scraping {url}: {e}")
        result_box[0] = details

    t = threading.Thread(target=_scrape, daemon=True)
    t.start()
    t.join(timeout=_WEBSITE_TIMEOUT_SECONDS)

    if t.is_alive():
        logging.warning(
            f"⏰ Website timeout ({_WEBSITE_TIMEOUT_SECONDS}s) for {business_name} ({url}). "
            f"Returning partial data and moving on."
        )
        # Thread is still stuck in Chrome — we return whatever was collected so far
        # (may be all N/A). The next lead will reuse the same driver; if the renderer
        # is truly dead the driver-alive check in run_scraper will trigger a restart.

    return result_box[0]

# =========================
# PHASE 1 — Google Maps scraping for one city
# =========================
def run_phase1_for_city(driver, wait, city):
    """
    Scrape all Google Maps card data for a city.
    Skips cities already marked completed (crash recovery).
    Pre-scrolls ALL cards before clicking, so lazy-load happens before interaction.
    """
    if is_phase_completed(city, 'phase1'):
        logging.info(f"[SKIP] Phase 1 already done for {city}")
        return 0
    logging.info(f"\n{'='*60}\nPHASE 1 START: {city}\n{'='*60}")
    mark_phase_started(city, 'phase1')

    connection = get_db_connection()
    if not connection:
        logging.error("DB connection failed. Skipping city.")
        return 0
        
    try:
        existing_count = get_existing_count(connection, city)
        existing_names = get_existing_names(connection) # set of (city.lower(), name.lower())
        
        # Pre-seed seen names for this session to skip existing leads entirely
        names_seen_this_session = set()
        for c_low, n_low in existing_names:
            if c_low == city.lower():
                names_seen_this_session.add(n_low)

        if not search_location(driver, wait, city):
            logging.error(f"Search failed for {city}")
            return 0

        # Pre-scroll
        logging.info("Pre-scrolling to load all cards...")
        scroll_results_container(driver, target_count=MAX_LEADS_PER_CITY + 20)

        scraped_count = existing_count
        error_count   = 0
        last_name     = "N/A"

        while scraped_count < MAX_LEADS_PER_CITY:
            try:
                # Basic check: ensure DB is still alive
                try:
                    connection.ping(reconnect=True)
                except:
                    logging.warning("DB ping failed. Attempting reconnect...")
                    connection = get_db_connection()
                    if not connection: break

                if is_google_signin_page(driver):
                    logging.warning("Sign-in page detected...")
                    if not handle_google_signin(driver, wait, city): break
                    scroll_results_container(driver, target_count=MAX_LEADS_PER_CITY + 20)
                    continue

                cards = get_all_result_cards(driver)
                target_card = None
                
                for c in cards:
                    try:
                        lbl = (c.get_attribute("aria-label") or "").strip().lower()
                        if lbl and lbl not in names_seen_this_session:
                            # Strict check: card label might be different from stored name
                            # but we attempt to skip obvious duplicates from DB
                            if (city.lower(), lbl) in existing_names:
                                names_seen_this_session.add(lbl)
                                continue
                            target_card = c
                            break
                    except: continue

                if not target_card:
                    logging.info("Scrolling for more leads...")
                    before = len(cards)
                    scroll_results_container(driver, target_count=len(cards) + 20)
                    if len(get_all_result_cards(driver)) <= before: break
                    continue

                logging.info(f"\n--- Processing Card {len(names_seen_this_session) + 1} | Leads in DB: {scraped_count}/{MAX_LEADS_PER_CITY} | city: {city} ---")
                
                js(driver, "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", target_card)
                time.sleep(0.5)

                if smart_click_card(driver, target_card):
                    # Deduced name for tracking even if detail scrape fails
                    deduced_name = (target_card.get_attribute("aria-label") or "").strip()
                    
                    # Panel update wait: wait for the detail panel name to match the card name or change from last
                    updated = False
                    for _ in range(15):
                        current = extract_name(driver)
                        if current != "N/A" and (current != last_name or (deduced_name and deduced_name in current)):
                            updated = True
                            break
                        time.sleep(0.5)

                    data = scrape_dealership_details(driver, wait)
                    if data and data['name'] != "N/A":
                        name = data['name'].strip()
                        name_low = name.lower()
                        
                        # Mark BOTH the panel name and the card label as seen
                        names_seen_this_session.add(name_low)
                        if deduced_name: 
                            names_seen_this_session.add(deduced_name.lower())

                        # Detailed Logging
                        logging.info(f"--- [SCRAPED DATA: {name}] ---")
                        logging.info(f"  Rating:  {data['rating']}")
                        logging.info(f"  Address: {data['address']}")
                        logging.info(f"  Phone:   {data['phone']}")
                        logging.info(f"  Website: {data['website']}")
                        logging.info(f"{'-'*40}")
                        
                        at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        
                        # Use a temporary check to see if it's a duplicate before saving
                        if check_duplicate(connection, city, name):
                            logging.info(f"Lead '{name}' already exists in DB. Moving on.")
                            # We increment count anyway because we "found" it and it's in DB
                            # Or we can just skip incrementing and move to next card
                            last_name = name
                        else:
                            if save_google_maps_data(connection, city, name, data['rating'], data['address'],
                                                     data['phone'], data['website'], data['timings'], data, at):
                                scraped_count += 1
                                last_name = name
                                existing_names.add((city.lower(), name.lower()))
                            else:
                                logging.error(f"Failed to save {name} to database.")
                                error_count += 1
                    else:
                        if deduced_name: names_seen_this_session.add(deduced_name)
                        logging.warning(f"Failed to extract details for card: {deduced_name}")
                
                time.sleep(random.uniform(1, 2))

            except Exception as e:
                logging.warning(f"Error in Phase 1 lead loop: {e}")
                time.sleep(2)

    finally:
        if connection:
            connection.close()

    logging.info(f"\nPHASE 1 DONE: {city} — {scraped_count} leads, {error_count} errors")
    mark_phase_completed(city, 'phase1')
    return scraped_count

# =========================
# PHASE 2 — Website scraping for one city
# =========================
def run_phase2_for_city(get_driver, city, restart_fn=None):
    """
    Visit websites for all leads in a city that still have N/A for logo/services/pricing.
    get_driver: callable that returns the current driver (so after restart we get the new one).
    Restarts browser every PHASE2_RESTART_EVERY_N_LEADS to prevent tab crashes from memory buildup.
    On tab/session crash, restarts and retries the same lead once.
    """
    connection = get_db_connection()
    if not connection:
        logging.error("DB connection failed for Phase 2")
        return 0

    try:
        leads = get_leads_with_websites(connection, city)
    finally:
        connection.close()

    if not leads:
        logging.info(f"[SKIP] No leads needing Phase 2 for {city} (all have website data or no website).")
        return 0

    logging.info(f"\n{'='*60}\nPHASE 2 START: {city} — {len(leads)} leads with websites to scrape\n{'='*60}")
    mark_phase_started(city, 'phase2')

    ok, err = 0, 0
    for idx, (name, website) in enumerate(leads, 1):
        driver = get_driver()
        details = None
        try:
            logging.info(f"\n--- Website: {name} | {website} ---")
            details = scrape_website_details(driver, website, name)
        except Exception as e:
            if _is_tab_or_session_crash(e) and restart_fn:
                logging.warning(f"Tab/session crashed for {name} — restarting browser and retrying once...")
                try:
                    restart_fn()
                    time.sleep(2)
                    driver = get_driver()
                    details = scrape_website_details(driver, website, name)
                except Exception as e2:
                    err += 1
                    logging.error(f"Error for {name} (after retry): {e2}")
                    continue
            else:
                err += 1
                logging.error(f"Error for {name}: {e}")
                import traceback
                logging.error(traceback.format_exc())
                continue

        if details is not None:
            try:
                logging.info(f"--- [WEBSITE DATA: {name}] ---")
                logging.info(f"  Logo URL: {details['logo_url']}")
                logging.info(f"  Services: {', '.join(details['services']) if isinstance(details['services'], list) else details['services']}")
                logging.info(f"  Pricing:  {', '.join(details['pricing']) if isinstance(details['pricing'], list) else details['pricing']}")
                about_log = details['about_us'][:300] + "..." if len(details.get('about_us', '')) > 300 else details.get('about_us', '')
                logging.info(f"  About Us: {about_log}")
                logging.info(f"{'-'*40}")

                logo_url = details.get('logo_url', 'N/A')
                about_us = details.get('about_us', 'N/A')
                services = '; '.join(details.get('services', [])) or 'N/A'
                pricing  = '; '.join(details.get('pricing', []))  or 'N/A'

                if update_website_data(city, name, logo_url, about_us, services, pricing):
                    ok += 1
                    logging.info(f"✓ Updated ({ok}/{len(leads)}): {name}")
                else:
                    logging.info(f"Skipped (all N/A): {name}")
            except Exception as e:
                err += 1
                logging.error(f"Error saving/logging for {name}: {e}")
            time.sleep(random.uniform(2, 4))

        # Restart browser every N leads to prevent memory buildup and tab crashes
        if idx % PHASE2_RESTART_EVERY_N_LEADS == 0 and restart_fn and idx < len(leads):
            logging.info(f"♻️  Restarting browser after {idx} leads to free memory...")
            restart_fn()
            time.sleep(2)

    logging.info(f"\nPHASE 2 DONE: {city} — {ok}/{len(leads)} updated, {err} errors")
    mark_phase_completed(city, 'phase2')

    if restart_fn:
        logging.info(f"♻️  Restarting browser after Phase 2 for {city} to free memory...")
        restart_fn()

    return ok

# =========================
# RETRY SWEEP — re-scrape all remaining N/A leads across every city
# =========================

def run_retry_sweep(get_driver, restart_fn=None):
    """
    Re-scrape leads still missing logo/services/pricing. get_driver: callable
    returning current driver. On tab crash, restarts and retries once.
    """
    logging.info("\n" + "*"*60)
    logging.info("RETRY SWEEP: Checking for leads with N/A data across all cities...")
    logging.info("*"*60)

    leads = get_na_leads_globally()   # [(city, name, website), ...]
    if not leads:
        logging.info("[RETRY SWEEP] Nothing to retry — all leads are fully populated. ✓")
        return

    logging.info(f"[RETRY SWEEP] Will attempt to re-scrape {len(leads)} leads.")
    ok = err = skipped = 0

    for idx, (city, name, website) in enumerate(leads, 1):
        driver = get_driver()
        if not is_driver_alive(driver) and restart_fn:
            logging.warning("[RETRY SWEEP] Driver dead — restarting before next lead.")
            restart_fn()
            time.sleep(2)
            driver = get_driver()

        try:
            logging.info(f"[RETRY {idx}/{len(leads)}] {name} ({city}) → {website}")
            details = scrape_website_details(driver, website, name)
        except Exception as e:
            if _is_tab_or_session_crash(e) and restart_fn:
                logging.warning(f"[RETRY SWEEP] Tab crashed for {name} — restarting and retrying once...")
                try:
                    restart_fn()
                    time.sleep(2)
                    driver = get_driver()
                    details = scrape_website_details(driver, website, name)
                except Exception as e2:
                    err += 1
                    logging.error(f"[RETRY SWEEP] Error for {name} (after retry): {e2}")
                    mark_lead_phase2_retry_attempted(city, name)
                    continue
            else:
                err += 1
                logging.error(f"[RETRY SWEEP] Error for {name}: {e}")
                mark_lead_phase2_retry_attempted(city, name)
                import traceback
                logging.error(traceback.format_exc())
                continue

        try:
            logo_url = details.get('logo_url', 'N/A')
            about_us = details.get('about_us', 'N/A')
            services = '; '.join(details.get('services', [])) or 'N/A'
            pricing  = '; '.join(details.get('pricing',  [])) or 'N/A'

            logging.info(f"  Logo: {logo_url[:60]}  |  Services: {services[:60]}  |  Pricing: {pricing[:60]}")

            if update_website_data(city, name, logo_url, about_us, services, pricing):
                ok += 1
                logging.info(f"  ✓ Updated ({ok} so far): {name}")
            else:
                skipped += 1
                logging.info(f"  — Skipped (still all N/A or no change): {name}")
        except Exception as e:
            err += 1
            logging.error(f"[RETRY SWEEP] Error updating for {name}: {e}")
        mark_lead_phase2_retry_attempted(city, name)
        time.sleep(random.uniform(2, 4))

    logging.info(
        f"[RETRY SWEEP] Done — {ok} updated, {skipped} skipped, {err} errors "
        f"out of {len(leads)} leads."
    )

    # Restart browser after the sweep to free memory from all the sites visited.
    if restart_fn:
        logging.info("♻️  Restarting browser after retry sweep to free memory...")
        restart_fn()


# =========================
# MAIN — Batched execution
# =========================
def run_scraper():
    """
    Batch strategy:
      - Only run cities that have not completed both phase1 and phase2 (skips completed).
      - Split remaining cities into batches of CITY_BATCH_SIZE
      - For each batch: Phase 1 all cities → Phase 2 all cities → next batch
      - Both phases use scraper_progress table for crash recovery
      - Browser placed off-screen (not minimized) so you can use your PC freely

    Common errors and causes:
      - "Connection refused" / "Max retries exceeded": Chrome process died but script
        kept using the old session; we treat these as dead session and restart+retry.
      - "Resource temporarily unavailable" (errno 11): System out of processes/memory
        when starting Chrome; we retry with delay and wait longer after quit() before
        starting a new driver so the OS can reclaim resources.
    """
    logging.info("Starting Google Maps Car Detailers Scraper")
    logging.info(f"Cities: {CITIES}")
    logging.info(f"Batch size: {CITY_BATCH_SIZE} | Headless: {HEADLESS}")
    logging.info("Browser placed off-screen — Chrome stays active, you can use your PC normally.")

    if not init_database():
        logging.error("Failed to initialize database. Exiting.")
        return

    # Only run cities that have not completed both phase1 and phase2 (reduces load, avoids rework).
    completed_cities = get_cities_with_both_phases_completed()
    cities_to_run = [c for c in CITIES if c not in completed_cities]
    if not cities_to_run:
        logging.info("All cities already have phase1 and phase2 completed. Nothing to do.")
        return
    if len(completed_cities) > 0:
        logging.info(f"Skipping {len(completed_cities)} completed cities. Running {len(cities_to_run)} cities.")

    # Use a mutable dict so restart_driver() can update the driver reference
    # in-place and both the outer loop and phase functions always use the latest.
    ctx = {"driver": start_driver()}
    ctx["wait"] = WebDriverWait(ctx["driver"], 20)

    def restart_driver():
        """Kill current driver and start a fresh one, updating ctx in-place."""
        logging.warning("Restarting Chrome driver...")
        try:
            ctx["driver"].quit()
        except:
            pass
        # Longer pause so OS can reclaim memory/FDs before starting new Chrome (avoids errno 11).
        time.sleep(10)
        try:
            ctx["driver"] = start_driver()
            ctx["wait"]   = WebDriverWait(ctx["driver"], 20)
            logging.info("Driver restarted successfully.")
        except Exception as e:
            logging.error(f"Failed to restart driver: {e}")

    try:
        batches = [cities_to_run[i:i+CITY_BATCH_SIZE] for i in range(0, len(cities_to_run), CITY_BATCH_SIZE)]

        for batch_num, batch in enumerate(batches, 1):
            logging.info(f"\n{'#'*60}")
            logging.info(f"BATCH {batch_num}/{len(batches)}: {batch}")
            logging.info(f"{'#'*60}")

            # ── Phase 1 for every city in this batch ─────────────────────────
            for city in batch:
                # Retry up to 3 times per city in case of driver crash mid-scroll
                for attempt in range(1, 4):
                    if not is_driver_alive(ctx["driver"]):
                        restart_driver()
                    try:
                        run_phase1_for_city(ctx["driver"], ctx["wait"], city)
                        time.sleep(random.uniform(4, 7))
                        break  # success — move to next city
                    except Exception as e:
                        import traceback as tb
                        logging.error(f"Phase 1 {city} attempt {attempt}/3 failed: {e}")
                        logging.error(tb.format_exc())
                        restart_driver()
                        if attempt == 3:
                            logging.error(f"Giving up on Phase 1 for {city} after 3 attempts.")

            # ── Phase 2 for every city in this batch ─────────────────────────
            for city in batch:
                # Retry up to 3 times per city in case of driver crash
                for attempt in range(1, 4):
                    if not is_driver_alive(ctx["driver"]):
                        restart_driver()
                    try:
                        # get_driver so Phase 2 can use fresh driver after restarts (avoids tab crashes).
                        run_phase2_for_city(lambda: ctx["driver"], city, restart_fn=restart_driver)
                        time.sleep(random.uniform(3, 6))
                        break  # success — move to next city
                    except Exception as e:
                        import traceback as tb
                        logging.error(f"Phase 2 {city} attempt {attempt}/3 failed: {e}")
                        logging.error(tb.format_exc())
                        restart_driver()
                        if attempt == 3:
                            logging.error(f"Giving up on Phase 2 for {city} after 3 attempts.")

            logging.info(f"\nBatch {batch_num} complete: {batch}\n")

    finally:
        logging.info("\n" + "="*60 + "\nSCRAPING COMPLETED\n" + "="*60)

        # ── Final global retry sweep ──────────────────────────────────────────
        # Runs once after ALL batches finish to catch any leads that were
        # skipped/timed-out during the run and haven't been retried yet.
        try:
            logging.info("[FINAL SWEEP] Running end-of-run N/A retry sweep...")
            run_retry_sweep(lambda: ctx["driver"], restart_fn=restart_driver)
        except Exception as e:
            logging.error(f"[FINAL SWEEP] Failed: {e}")
        # ─────────────────────────────────────────────────────────────────────

        try:
            ctx["driver"].quit()
        except:
            try:
                ctx["driver"].close()
            except:
                pass

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--report":
        report_progress()
        sys.exit(0)
    run_scraper()