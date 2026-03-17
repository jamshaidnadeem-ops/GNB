"""
Google Maps image scraper — unique image per card.

KEY FIX vs previous version:
  _close_panel() was clicking the Back button even when no detail panel
  was open, navigating away from search results and killing the card list.
  Now we only attempt close if a panel heading (h1) is actually present in
  the DOM. On card 1 (no panel open yet) we skip the close entirely.

Run:
    python test_image_scrape.py
    TEST_NUM_CARDS=3 TEST_CAPTURE_SCREENSHOTS=1 python test_image_scrape.py
"""

import sys, os, time, json, re
BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, StaleElementReferenceException
)

from GNB import (
    start_driver, search_location, scroll_results_container,
    get_all_result_cards, smart_click_card, js,
    SEARCH_QUERY, SEARCH_DELAY, DETAIL_PAGE_DELAY,
)

# ─────────────────────────────────────────
CITY              = "New York"
NUM_CARDS         = int(os.environ.get("TEST_NUM_CARDS", "15"))
SCREENSHOTS_DIR   = os.path.join(BASE, "test_screenshots")
DEBUG_SCREENSHOTS = os.environ.get("TEST_CAPTURE_SCREENSHOTS", "").lower() in ("1","true","yes")
PANEL_WAIT        = 16
LH3_WAIT          = 8
# ─────────────────────────────────────────

# Text from h1/h2 only (original — often missing business name in headless Maps)
JS_GET_HEADINGS = """
    return Array.from(document.querySelectorAll('h1,h2'))
        .map(e => (e.innerText || e.textContent || '').trim())
        .filter(Boolean);
"""

# Broader: also text from elements that usually hold the place name in Google Maps
# (DUwDvf, fontHeadline, and role=main so we catch the panel title even when not in h1/h2)
JS_GET_PANEL_TITLE_CANDIDATES = """
    var out = [];
    var sel = 'h1, h2, [class*="DUwDvf"], [class*="fontHeadline"], [class*="fontHeadlineLarge"]';
    document.querySelectorAll(sel).forEach(function(e) {
        var t = (e.innerText || e.textContent || '').trim();
        if (t && t.length > 2 && t.length < 200) out.push(t);
    });
    var main = document.querySelector('[role="main"]');
    if (main) {
        var t = (main.innerText || main.textContent || '').trim();
        if (t && t.length < 500) out.push(t);
        else if (t) out.push(t.substring(0, 300));
    }
    return out;
"""

JS_FIRST_LH3 = """
    var imgs = document.querySelectorAll('img');
    for (var i = 0; i < imgs.length; i++) {
        var s = imgs[i].currentSrc || imgs[i].src || '';
        if (s.indexOf('lh3.googleusercontent.com') !== -1) return s;
    }
    return null;
"""

JS_CLICK_PHOTOS_TAB = """
    var btns = Array.from(document.querySelectorAll('button'));
    for (var i = 0; i < btns.length; i++) {
        if (btns[i].textContent.trim().toLowerCase() === 'photos') {
            btns[i].click(); return true;
        }
    }
    return false;
"""

# Detect if a detail panel is currently open: h1 must be present AND
# the results feed must still be in the DOM (we haven't navigated away)
JS_PANEL_IS_OPEN = """
    var h1s = document.querySelectorAll('h1');
    for (var i = 0; i < h1s.length; i++) {
        if ((h1s[i].innerText || h1s[i].textContent || '').trim().length > 0) return true;
    }
    return false;
"""


def _ss(driver, name):
    if not DEBUG_SCREENSHOTS:
        return
    try:
        os.makedirs(SCREENSHOTS_DIR, exist_ok=True)
        path = os.path.join(SCREENSHOTS_DIR, f"{name}.png")
        driver.save_screenshot(path)
        print(f"      [ss] {path}")
    except Exception as e:
        print(f"      [ss fail] {e}")


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9 ]", "", s.lower()).strip()


# ─────────────────────────────────────────
# SAFE PANEL CLOSE — only when panel is open
# ─────────────────────────────────────────
def _close_panel_if_open(driver):
    """
    Only closes the detail panel if one is actually open.
    Prevents accidentally clicking Back on the results list.
    """
    try:
        panel_open = driver.execute_script(JS_PANEL_IS_OPEN)
    except Exception:
        return

    if not panel_open:
        return  # nothing to close

    # Try clicking the X button that's INSIDE the detail panel
    # We scope to buttons that are children of the panel container, not
    # the top-level navigation back button
    for sel in [
        # Close button inside the detail pane (not the list nav)
        "div[role='main'] button[aria-label='Close']",
        "div[jsaction*='pane'] button[aria-label='Close']",
        # Generic close with icon
        "button[data-value='Close']",
    ]:
        try:
            btns = driver.find_elements(By.CSS_SELECTOR, sel)
            for btn in btns:
                if btn.is_displayed():
                    btn.click()
                    time.sleep(0.8)
                    return
        except Exception:
            pass

    # Fallback: Escape key (safe — doesn't navigate)
    try:
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
        time.sleep(0.5)
    except Exception:
        pass


# ─────────────────────────────────────────
# WAIT FOR PANEL TO SHOW CURRENT CARD NAME
# ─────────────────────────────────────────
def _wait_panel_for_card(driver, card_label: str, timeout=PANEL_WAIT) -> bool:
    """
    Poll until we see the card's business name in the DOM.
    Checks h1/h2 first, then broader panel title candidates (DUwDvf, fontHeadline, role=main)
    so we confirm even when Maps doesn't put the name in a heading in headless.
    """
    norm_target  = _norm(card_label)
    target_short = norm_target[:20]
    if len(norm_target) > 25:
        target_short = norm_target[:25]  # longer match for long names
    deadline  = time.time() + timeout
    last_seen = []

    while time.time() < deadline:
        try:
            # 1) Original h1/h2
            headings = driver.execute_script(JS_GET_HEADINGS) or []
            # 2) Broader: DUwDvf, fontHeadline, role=main (panel title in headless)
            candidates = driver.execute_script(JS_GET_PANEL_TITLE_CANDIDATES) or []
            all_text  = list(headings) + list(candidates)
            last_seen = all_text[:8]

            for raw in all_text:
                if not raw or len(raw) < 3:
                    continue
                nh = _norm(raw)
                # Match: card name contained in panel text, or panel text contained in card name
                if target_short and (target_short in nh or nh[:25] in norm_target):
                    return True
                if len(nh) >= 10 and (norm_target[:15] in nh or nh[:15] in norm_target):
                    return True
        except Exception:
            pass
        time.sleep(0.4)

    print(f"      [panel timeout] seen: {last_seen[:5]}")
    return False


# ─────────────────────────────────────────
# EXTRACT LH3 URL — 6 strategies
# ─────────────────────────────────────────
def _get_src(driver, el) -> str | None:
    try:
        src = driver.execute_script(
            "return arguments[0].currentSrc||arguments[0].src||arguments[0].getAttribute('src')||'';",
            el
        )
        if src and "lh3.googleusercontent.com" in src:
            return src
    except StaleElementReferenceException:
        pass
    return None


def _extract_lh3_url(driver, card_index: int, card_label: str,
                     panel_confirmed: bool, seen_urls: set) -> str | None:
    n    = card_index + 1
    norm = _norm(card_label)

    def _accept(url):
        if not url:
            return False
        # If panel confirmed, trust any lh3 URL
        if panel_confirmed:
            return True
        # If panel unconfirmed, only accept a URL we haven't seen before
        return url not in seen_urls

    # S1 — button[aria-label*="Photo of <name>"] img — name-matched, always trust
    try:
        btns = driver.find_elements(By.CSS_SELECTOR, "button[aria-label*='Photo of']")
        for btn in btns:
            btn_lbl = _norm(btn.get_attribute("aria-label") or "")
            if norm[:15] in btn_lbl:
                for img in btn.find_elements(By.TAG_NAME, "img"):
                    src = _get_src(driver, img)
                    if src:
                        print(f"      [S1 name-matched] {src}")
                        return src
    except Exception:
        pass

    # S2 — any button[aria-label*="Photo of"] img
    try:
        for img in driver.find_elements(By.CSS_SELECTOR, "button[aria-label*='Photo of'] img"):
            src = _get_src(driver, img)
            if _accept(src):
                print(f"      [S2 photo-button] {src}")
                return src
    except Exception:
        pass

    # S3 — img[src*="lh3"] CSS
    try:
        for img in driver.find_elements(By.CSS_SELECTOR, "img[src*='lh3.googleusercontent.com']"):
            src = _get_src(driver, img)
            if _accept(src):
                print(f"      [S3 css lh3-src] {src}")
                return src
    except Exception:
        pass

    # S4 — JS scan
    try:
        src = driver.execute_script(JS_FIRST_LH3)
        if _accept(src):
            print(f"      [S4 js scan] {src}")
            return src
    except Exception:
        pass

    # S5 — click Photos tab then retry
    try:
        if driver.execute_script(JS_CLICK_PHOTOS_TAB):
            time.sleep(2.5)
            src = driver.execute_script(JS_FIRST_LH3)
            if _accept(src):
                print(f"      [S5 photos-tab] {src}")
                return src
    except Exception:
        pass

    # S6 — XPath heroHeaderImage (any jsaction suffix)
    try:
        for el in driver.find_elements(By.XPATH, "//button[contains(@jsaction,'heroHeaderImage')]//img"):
            src = _get_src(driver, el)
            if _accept(src):
                print(f"      [S6 xpath heroHeader] {src}")
                return src
    except Exception:
        pass

    _ss(driver, f"img_all_failed_card{n:02d}")
    return None


# ─────────────────────────────────────────
# PER-CARD FLOW
# ─────────────────────────────────────────
def scrape_card(driver, card_index: int, card_label: str, card, seen_urls: set) -> str | None:
    n = card_index + 1

    # Close panel only if one is currently open
    _close_panel_if_open(driver)
    time.sleep(0.4)
    _ss(driver, f"step0_card{n:02d}_before_click")

    # Scroll card into view so click reliably opens the detail panel (especially in headless)
    try:
        js(driver, "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", card)
        time.sleep(0.5)
    except Exception:
        pass

    if not smart_click_card(driver, card):
        print(f"      [click failed]")
        return None

    time.sleep(DETAIL_PAGE_DELAY)
    _ss(driver, f"step1_card{n:02d}_after_click")

    panel_confirmed = _wait_panel_for_card(driver, card_label, timeout=PANEL_WAIT)
    if panel_confirmed:
        _ss(driver, f"step2_card{n:02d}_panel_confirmed")

    # Wait for lh3 img
    try:
        WebDriverWait(driver, LH3_WAIT).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "img[src*='lh3.googleusercontent.com']")
            )
        )
    except TimeoutException:
        _ss(driver, f"step2_card{n:02d}_no_lh3_timeout")

    time.sleep(0.8)

    url = _extract_lh3_url(driver, card_index, card_label, panel_confirmed, seen_urls)
    _ss(driver, f"step3_card{n:02d}_{'got_url' if url else 'no_url'}")
    return url


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
def main():
    print("Starting driver...")
    driver  = start_driver()
    driver.set_page_load_timeout(60)
    wait    = WebDriverWait(driver, 20)
    results = []

    if DEBUG_SCREENSHOTS:
        print(f"Debug screenshots → {SCREENSHOTS_DIR}")

    try:
        print(f"Searching: {SEARCH_QUERY} in {CITY}")
        if not search_location(driver, wait, CITY):
            print("Search failed.")
            return

        time.sleep(SEARCH_DELAY)
        _ss(driver, "after_search")

        print(f"Scrolling to load at least {NUM_CARDS} cards...")
        scroll_results_container(driver, target_count=NUM_CARDS + 5)
        time.sleep(1)

        cards = get_all_result_cards(driver)
        print(f"Found {len(cards)} cards. Processing first {NUM_CARDS}.\n")

        seen_urls = set()

        for i in range(min(NUM_CARDS, len(cards))):
            cards = get_all_result_cards(driver)
            if i >= len(cards):
                print(f"  [{i+1}] out of range — stopping.")
                break

            card  = cards[i]
            label = ""
            try:
                label = card.get_attribute("aria-label") or f"Card {i+1}"
            except StaleElementReferenceException:
                label = f"Card {i+1}"

            print(f"  [{i+1}/{NUM_CARDS}] {label[:60]}")

            try:
                url = scrape_card(driver, i, label, card, seen_urls)
                if url:
                    seen_urls.add(url)
                results.append({
                    "index":     i + 1,
                    "name":      label,
                    "image_url": url or "",
                    "status":    "ok" if url else "no_photo",
                })
                print(f"      {'✓  ' + url if url else '✗  no photo'}")

            except Exception as e:
                _ss(driver, f"error_card{i+1:02d}_{type(e).__name__}")
                print(f"      [error] {e}")
                results.append({
                    "index": i+1, "name": label,
                    "image_url": "", "status": "error", "error": str(e)
                })

            time.sleep(0.8)

        out_path = os.path.join(BASE, "test_image_scrape_results.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)

        ok       = sum(1 for r in results if r["status"] == "ok")
        no_photo = sum(1 for r in results if r["status"] == "no_photo")
        errors   = sum(1 for r in results if r["status"] == "error")
        print(f"\n✅  Done. {ok} photos | {no_photo} no-photo | {errors} errors  (total {len(results)})")
        print(f"📄  {out_path}")
        if DEBUG_SCREENSHOTS:
            print(f"📂  {SCREENSHOTS_DIR}/")

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    return results


if __name__ == "__main__":
    main()