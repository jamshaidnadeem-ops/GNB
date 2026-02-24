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
from datetime import datetime
import sys
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
CITY_BATCH_SIZE = 2  # Phase 2 runs after every N cities finish Phase 1

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
HEADLESS = os.environ.get('HEADLESS', 'True').lower() == 'true'
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
def start_driver():
    """Initialize undetected Chrome driver optimised for background use."""
    import undetected_chromedriver as uc
    options = uc.ChromeOptions()

    # Core anti-detection
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # Keep Chrome fully active even when off-screen / not focused.
    # These flags prevent Chrome from throttling timers, pausing rendering,
    # or sleeping background tabs — which is what causes the "minimized" bug.
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--disable-ipc-flooding-protection")
    options.add_argument("--disable-hang-monitor")
    options.add_argument("--disable-prompt-on-repost")
    options.add_argument("--disable-client-side-phishing-detection")
    options.add_argument("--disable-features=TranslateUI,OptimizationHints,MediaRouter,DialMediaRouteProvider")
    options.add_argument("--metrics-recording-only")
    options.add_argument("--mute-audio")

    # Force a fixed virtual viewport so Intersection Observers always fire.
    # KEY FIX: Chrome lazy-load uses IntersectionObserver which requires elements
    # to be "in viewport". Moving the window off-screen (not minimizing) keeps
    # the viewport real so all IO callbacks still fire.
    options.add_argument(f"--window-size={WINDOW_WIDTH},{WINDOW_HEIGHT}")
    options.add_argument(f"--window-position={OFFSCREEN_X},{OFFSCREEN_Y}")
    options.add_argument("--force-device-scale-factor=1")

    if HEADLESS:
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
    else:
        # Off-screen but NOT headless: GPU compositing stays on so
        # Intersection Observer sees a real painted viewport.
        options.add_argument("--use-gl=angle")
        options.add_argument("--use-angle=swiftshader")

    options.page_load_strategy = 'eager'

    try:
        # Use auto-detection by default for better compatibility on Railway/Linux
        driver = uc.Chrome(options=options, use_subprocess=True)
    except Exception as e:
        logging.error(f"Failed to initialize driver: {e}")
        raise e

    driver.set_page_load_timeout(60)
    driver.set_script_timeout(60)

    if hasattr(driver, "__del__"):
        driver.__del__ = lambda *args, **kwargs: None

    # Remove webdriver flag via CDP
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"}
    )

    # Ensure window is positioned off-screen (belt-and-suspenders)
    if not HEADLESS:
        try:
            driver.set_window_rect(x=OFFSCREEN_X, y=OFFSCREEN_Y,
                                   width=WINDOW_WIDTH, height=WINDOW_HEIGHT)
        except Exception:
            pass  # Some OSes ignore negative coords — the flags handle it

    return driver


def js(driver, script, *args):
    """Shorthand for execute_script."""
    return driver.execute_script(script, *args)


def is_driver_alive(driver):
    try:
        driver.title
        return True
    except:
        return False


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
    'connect_timeout': 30,
    'read_timeout':    30,
    'write_timeout':   30
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
            logo_url TEXT,
            services TEXT,
            pricing TEXT,
            `Scraped At` DATETIME,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_business (City, Name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)

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


def save_google_maps_data(city, name, rating, address, phone, website, timings, scraped_at):
    connection = None
    try:
        connection = get_db_connection()
        if not connection:
            return False
        if check_duplicate(connection, city, name):
            return False
        cursor = connection.cursor()
        cursor.execute(f"""
        INSERT INTO {TABLE_NAME}
        (City, Name, Rating, Address, Phone, Website, Timings, logo_url, services, pricing, `Scraped At`)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (city, name, rating, address, phone, website, timings, "N/A", "N/A", "N/A", scraped_at))
        connection.commit()
        cursor.close()
        return True
    except Error as e:
        logging.error(f"Error saving data: {e}")
        if connection:
            connection.rollback()
        return False
    finally:
        if connection:
            connection.close()


def update_website_data(city, name, logo_url, services, pricing):
    connection = None
    try:
        if logo_url == "N/A" and services == "N/A" and pricing == "N/A":
            return False
        connection = get_db_connection()
        if not connection:
            return False
        cursor = connection.cursor()
        cursor.execute(f"""
        UPDATE {TABLE_NAME} SET logo_url=%s, services=%s, pricing=%s
        WHERE City=%s AND Name=%s
        """, (logo_url, services, pricing, city, name))
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
        (By.CSS_SELECTOR, "h1.DUwDvf.lfPIob"),
        (By.CSS_SELECTOR, "div.fontHeadlineLarge"),
        (By.CSS_SELECTOR, "div.lP3y9d"),
        (By.XPATH, "//h1[contains(@class,'DUwDvf')]"),
    ]:
        try:
            el = driver.find_element(by, sel)
            text = js(driver, "return arguments[0].textContent", el).strip()
            if text and text != "N/A":
                return text
        except:
            pass
    return "N/A"


def extract_address(driver):
    for by, sel in [
        (By.CSS_SELECTOR, "div.Io6YTe.fontBodyMedium.kR99db.fdkmkc"),
        (By.XPATH, "//button[contains(@data-item-id,'address')]//div[contains(@class,'Io6YTe')]"),
    ]:
        try:
            el = driver.find_element(by, sel)
            text = js(driver, "return arguments[0].textContent", el).strip()
            if text:
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
    try:
        for container in driver.find_elements(By.CSS_SELECTOR, "div.F7nice"):
            text = js(driver, "return arguments[0].textContent", container).strip().split('\n')[0].strip()
            if re.match(r'^\d+\.\d+$', text):
                return text
    except:
        pass
    try:
        el = driver.find_element(By.CSS_SELECTOR, "span[role='img'][aria-label*='stars']")
        m = re.search(r'(\d+\.\d+)', el.get_attribute("aria-label") or "")
        if m:
            return m.group(1)
    except:
        pass
    return "N/A"


def extract_timings(driver):
    """
    Extract opening hours. All DOM reads use JS so they work off-screen.
    """
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

        for by, sel in [
            (By.CSS_SELECTOR, "table.eK4R0e"),
            (By.XPATH, "//div[@role='region']//table"),
        ]:
            try:
                table = driver.find_element(by, sel)
                rows = table.find_elements(By.CSS_SELECTOR, "tr.y0skZc") or \
                       table.find_elements(By.TAG_NAME, "tr")
                timings, seen = [], set()
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
                            timings.append(f"{day}: {hrs}")
                    except:
                        pass
                if timings:
                    return " | ".join(timings)
            except:
                pass
    except Exception as e:
        logging.warning(f"Error extracting timings: {e}")
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
    try:
        time.sleep(DETAIL_PAGE_DELAY)
        return {
            'name':    extract_name(driver),
            'rating':  extract_rating(driver),
            'address': extract_address(driver),
            'phone':   extract_phone(driver),
            'website': extract_website(driver),
            'timings': extract_timings(driver),
        }
    except Exception as e:
        logging.error(f"Error scraping details: {e}")
        return None

# =========================
# WEBSITE SCRAPING
# =========================
def scroll_page_fully(driver):
    try:
        last_h = js(driver, "return document.body.scrollHeight")
        js(driver, "window.scrollTo(0,0)")
        time.sleep(0.5)
        for _ in range(12):
            js(driver, "window.scrollBy(0, window.innerHeight * 0.8)")
            time.sleep(SCROLL_DELAY)
            new_h = js(driver, "return document.body.scrollHeight")
            if new_h == last_h:
                break
            last_h = new_h
        js(driver, "window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(1)
    except Exception as e:
        logging.warning(f"Error scrolling page: {e}")


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
        return list(set(services)) if services else ["N/A"]
    except Exception as e:
        logging.error(f"Error extracting services: {e}")
        return ["N/A"]


def extract_pricing(driver):
    try:
        page_text = js(driver, "return document.body.innerText")
        prices = re.findall(r'\$\d+(?:,\d{3})*(?:\.\d{2})?', page_text)
        if prices:
            unique = list(set(prices[:10]))
            result = []
            for price in unique:
                els = driver.find_elements(By.XPATH, f"//*[contains(text(),'{price}')]")
                if els:
                    try:
                        parent = els[0].find_element(By.XPATH, "./parent::*")
                        ctx = js(driver, "return arguments[0].textContent", parent)[:200].strip()
                        result.append(ctx)
                    except:
                        result.append(price)
                else:
                    result.append(price)
            return result if result else ["Prices found - see website"]
        ptl = page_text.lower()
        if any(k in ptl for k in ['pricing', 'packages', 'rates', 'cost']):
            return ["Pricing page available - visit website"]
        if 'contact' in ptl and 'quote' in ptl:
            return ["Contact for quote"]
        return ["Contact business for estimate"]
    except Exception as e:
        logging.error(f"Error extracting pricing: {e}")
        return ["N/A"]


def scrape_website_details(driver, url, business_name):
    logging.info(f"Scraping website: {url}")
    details = {'logo_url': 'N/A', 'services': [], 'pricing': []}
    try:
        for retry in range(3):
            try:
                driver.get(url)
                time.sleep(2)
                if driver.current_url and len(driver.page_source) > 1000:
                    break
            except Exception as e:
                logging.warning(f"Load retry {retry+1}: {e}")
                time.sleep(2)

        time.sleep(PAGE_LOAD_DELAY + random.uniform(0.5, 1.5))
        scroll_page_fully(driver)
        details['logo_url'] = extract_logo_url(driver)
        details['services']  = extract_services(driver)
        details['pricing']   = extract_pricing(driver)
        return details
    except Exception as e:
        logging.error(f"Error scraping {url}: {e}")
        return details

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
        existing_names = get_existing_names(connection)
    finally:
        connection.close()

    if not search_location(driver, wait, city):
        logging.error(f"Search failed for {city}")
        return 0

    # Pre-scroll to load as many cards as possible BEFORE we start clicking.
    # This prevents the "loading more" loop mid-scrape.
    logging.info("Pre-scrolling to load all cards...")
    scroll_results_container(driver, target_count=MAX_LEADS_PER_CITY + 20)

    cards = get_all_result_cards(driver)
    logging.info(f"Cards loaded after pre-scroll: {len(cards)}")
    if not cards:
        logging.warning(f"No cards found for {city}")
        return 0

    scraped_count = existing_count
    error_count   = 0
    idx           = 0
    last_name     = "N/A"

    while scraped_count < MAX_LEADS_PER_CITY:
        try:
            if is_google_signin_page(driver):
                logging.warning("Sign-in page detected. Re-searching...")
                if not handle_google_signin(driver, wait, city):
                    break
                scroll_results_container(driver, target_count=MAX_LEADS_PER_CITY + 20)
                cards = get_all_result_cards(driver)
                idx = 0
                continue

            cards = get_all_result_cards(driver)

            if idx >= len(cards):
                logging.info(f"Card index {idx} >= total {len(cards)}. Scrolling for more...")
                before = len(cards)
                scroll_results_container(driver, target_count=idx + 15)
                cards = get_all_result_cards(driver)
                if len(cards) <= before:
                    logging.info(f"No more cards. Done at {scraped_count} leads.")
                    break

            logging.info(f"\n--- Card {idx+1}/{len(cards)} ---")

            try:
                cards = get_all_result_cards(driver)
                if idx >= len(cards):
                    idx += 1
                    continue

                card = cards[idx]

                # Scroll card within the feed container via JS (not scrollIntoView)
                try:
                    js(driver, """
                        var container = document.querySelector("div[role='feed']") ||
                                        document.querySelector("div.m6QErb");
                        if (container) {
                            var r = arguments[0].getBoundingClientRect();
                            var cr = container.getBoundingClientRect();
                            if (r.top < cr.top || r.bottom > cr.bottom) {
                                container.scrollTop += (r.top - cr.top - 100);
                            }
                        }
                    """, card)
                    time.sleep(0.4)
                except:
                    pass

                # Click using CDP (bypasses visibility/focus requirements)
                clicked = smart_click_card(driver, card)
                if not clicked:
                    logging.warning(f"Could not click card {idx+1}. Skipping.")
                    idx += 1
                    continue

                # Wait for detail panel to update with a new name
                deadline = time.time() + 10
                updated  = False
                while time.time() < deadline:
                    current = extract_name(driver)
                    if current != "N/A" and current != last_name:
                        updated = True
                        break
                    time.sleep(0.5)

                if not updated:
                    logging.warning(f"Detail panel didn't update for card {idx+1}. Retry click...")
                    cards = get_all_result_cards(driver)
                    if idx < len(cards):
                        smart_click_card(driver, cards[idx])
                        time.sleep(3)
                        current = extract_name(driver)
                        if current == "N/A" or current == last_name:
                            logging.warning(f"Still no update. Skipping card {idx+1}.")
                            idx += 1
                            continue

                data = scrape_dealership_details(driver, wait)

                if data and data['name'] != "N/A":
                    name = data['name']
                    key  = (city.lower(), name.lower())
                    if key in existing_names:
                        logging.info(f"Duplicate: {name}")
                        idx += 1
                        continue

                    at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    if save_google_maps_data(city, name, data['rating'], data['address'],
                                             data['phone'], data['website'], data['timings'], at):
                        existing_names.add(key)
                        last_name     = name
                        scraped_count += 1
                        logging.info(f"✓ Saved ({scraped_count}/{MAX_LEADS_PER_CITY}): {name}")
                    else:
                        logging.info(f"Duplicate skipped: {name}")
                else:
                    logging.warning(f"No data extracted for card {idx+1}")

                idx += 1
                time.sleep(random.uniform(0.8, 2.0))

            except Exception as inner_e:
                if "stale" in str(inner_e).lower():
                    logging.warning("Stale element — re-fetching cards...")
                    cards = get_all_result_cards(driver)
                    time.sleep(0.5)
                    continue
                raise inner_e

        except Exception as e:
            error_count += 1
            logging.error(f"Error on card {idx+1}: {e}")
            import traceback
            logging.error(traceback.format_exc())
            try:
                if is_google_signin_page(driver):
                    if handle_google_signin(driver, wait, city):
                        scroll_results_container(driver, target_count=MAX_LEADS_PER_CITY + 20)
                        cards = get_all_result_cards(driver)
                        idx = 0
            except:
                pass
            idx += 1
            time.sleep(2)

    logging.info(f"\nPHASE 1 DONE: {city} — {scraped_count} leads, {error_count} errors")
    mark_phase_completed(city, 'phase1')
    return scraped_count

# =========================
# PHASE 2 — Website scraping for one city
# =========================
def run_phase2_for_city(driver, city):
    """
    Visit websites for all leads in a city and update DB.
    Skips cities already marked completed (crash recovery).
    Navigates directly to URLs — no tab switching needed.
    """
    if is_phase_completed(city, 'phase2'):
        logging.info(f"[SKIP] Phase 2 already done for {city}")
        return 0

    logging.info(f"\n{'='*60}\nPHASE 2 START: {city}\n{'='*60}")
    mark_phase_started(city, 'phase2')

    connection = get_db_connection()
    if not connection:
        logging.error("DB connection failed for Phase 2")
        return 0

    try:
        leads = get_leads_with_websites(connection, city)
        logging.info(f"{len(leads)} leads with websites for {city}")
    finally:
        connection.close()

    if not leads:
        logging.info(f"No website leads for {city}. Phase 2 done.")
        mark_phase_completed(city, 'phase2')
        return 0

    ok, err = 0, 0
    for name, website in leads:
        try:
            logging.info(f"\n--- Website: {name} | {website} ---")
            details  = scrape_website_details(driver, website, name)
            logo_url = details.get('logo_url', 'N/A')
            services = '; '.join(details.get('services', [])) or 'N/A'
            pricing  = '; '.join(details.get('pricing', []))  or 'N/A'

            if update_website_data(city, name, logo_url, services, pricing):
                ok += 1
                logging.info(f"✓ Updated ({ok}/{len(leads)}): {name}")
            else:
                logging.info(f"Skipped (all N/A): {name}")

            time.sleep(random.uniform(2, 4))

        except Exception as e:
            err += 1
            logging.error(f"Error for {name}: {e}")
            import traceback
            logging.error(traceback.format_exc())
            continue

    logging.info(f"\nPHASE 2 DONE: {city} — {ok}/{len(leads)} updated, {err} errors")
    mark_phase_completed(city, 'phase2')
    return ok

# =========================
# MAIN — Batched execution
# =========================
def run_scraper():
    """
    Batch strategy:
      - Split cities into batches of CITY_BATCH_SIZE
      - For each batch: Phase 1 all cities → Phase 2 all cities → next batch
      - Both phases use scraper_progress table for crash recovery
      - Browser placed off-screen (not minimized) so you can use your PC freely
    """
    logging.info("Starting Google Maps Car Detailers Scraper")
    logging.info(f"Cities: {CITIES}")
    logging.info(f"Batch size: {CITY_BATCH_SIZE} | Headless: {HEADLESS}")
    logging.info("Browser placed off-screen — Chrome stays active, you can use your PC normally.")

    if not init_database():
        logging.error("Failed to initialize database. Exiting.")
        return

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
        time.sleep(3)
        try:
            ctx["driver"] = start_driver()
            ctx["wait"]   = WebDriverWait(ctx["driver"], 20)
            logging.info("Driver restarted successfully.")
        except Exception as e:
            logging.error(f"Failed to restart driver: {e}")

    try:
        batches = [CITIES[i:i+CITY_BATCH_SIZE] for i in range(0, len(CITIES), CITY_BATCH_SIZE)]

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
                        run_phase2_for_city(ctx["driver"], city)
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
        try:
            ctx["driver"].quit()
        except:
            try:
                ctx["driver"].close()
            except:
                pass

if __name__ == "__main__":
    run_scraper()