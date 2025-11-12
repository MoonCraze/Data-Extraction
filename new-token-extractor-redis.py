# trending_extractor_redis.py
import os, json, time, random, threading, datetime as dt, math
import pytz, re
import redis  # pip install redis
from typing import List, Dict, Tuple
from seleniumbase import SB
from bs4 import BeautifulSoup
import mysql.connector
import urllib.parse

# ----------------------------
# Settings
# ----------------------------
REDIS_URL  = os.getenv("REDIS_URL", "redis://localhost:6379/0")
TRENDING_URL = os.getenv(
    "TRENDING_URL",
    "https://dexscreener.com/solana?rankBy=trendingScoreH24&order=desc"
)
CHAIN = "sol"
WINDOW_SIZE = int(os.getenv("TRENDING_WINDOW_SIZE", "100"))
INTERVAL_SEC = int(os.getenv("TRENDING_INTERVAL_SECONDS", "60"))
RANK_MOVE_THRESHOLD = int(os.getenv("RANK_MOVE_THRESHOLD", "999999"))  # start with only add/remove
TZ = pytz.timezone(os.getenv("TZ", "Asia/Colombo"))
HEADLESS = True
DEBUG = True

DB_WRITE = True  # Set to False to disable DB writes (for testing)

db_url = os.getenv("DB_URL")  # Set this in your .env file

if not db_url:
    raise RuntimeError("DB_URL environment variable is required")

url = urllib.parse.urlparse(db_url)

sqldb = mysql.connector.connect(
    host=url.hostname,
    port=url.port,
    user=url.username,
    password=url.password,
    database=url.path.lstrip("/"),
    ssl_disabled=False
)

sql_cursor = sqldb.cursor()

def dprint(msg: str):
    if DEBUG:
        t = threading.current_thread()
        print(f"{t.ident}::{msg}")

r = redis.from_url(REDIS_URL, decode_responses=True)

# redis keys
K_LATEST_VER = "trending:latest_version"          # string int
K_WINDOW_VER = "trending:window:{ver}"            # json array
K_WINDOW_META= "trending:window:{ver}:meta"       # hash (as_of)

# ----------------------------
# Helpers
# ----------------------------
def parse_num(s: str) -> float:
    if not s: return math.nan
    s = s.replace(",", "").strip()
    mult = 1.0
    if s.endswith("%"): s = s[:-1]
    if s.startswith("$"): s = s[1:]
    if s and s[-1] in "KkMmBb":
        m = {"K":1e3,"M":1e6,"B":1e9}[s[-1].upper()]
        mult = m; s = s[:-1]
    try:
        return float(s) * mult
    except:
        return math.nan

SOL_ADDR_RE = re.compile(r'/solana/([1-9A-HJ-NP-Za-km-z]{32,44})', re.IGNORECASE)

def scrape_trending_topN(n: int) -> List[Dict]:
    with SB(uc=True, test=True, locale_code="en", headless=HEADLESS) as sb:
        dprint(f"Navigate: {TRENDING_URL}")
        sb.activate_cdp_mode(TRENDING_URL)
        sb.sleep(4)
        try: sb.uc_gui_click_captcha()
        except Exception as e: dprint(f"Captcha not present/ignored: {e}")
        sb.wait_for_element_visible('img.ds-dex-table-row-token-icon-img', timeout=50)
        html = sb.get_page_source()

    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("a.ds-dex-table-row")
    out = []
    rank = 1
    for row in rows:

        # check the range has been exceeded, if yes -> break
        if rank > n: 
            break
        
        href = row.get("href","")

        # extract the SOL address from the href or img src
        m = SOL_ADDR_RE.search(href)
        if not m:
            img = row.select_one("img.ds-dex-table-row-token-icon-img")
            if img and img.get("src"):
                m = SOL_ADDR_RE.search(img["src"])
        if not m: continue

        # extract the token information
        contract = m.group(1)
        contract = contract.lower()
        name = (row.select_one(".ds-dex-table-row-base-token-name-text") or {}).get_text(strip=True) if row.select_one(".ds-dex-table-row-base-token-name-text") else None
        symbol = (row.select_one(".ds-dex-table-row-base-token-symbol") or {}).get_text(strip=True) if row.select_one(".ds-dex-table-row-base-token-symbol") else None
        mc_node = row.select_one(".ds-dex-table-row-col-market-cap")
        liq_node = row.select_one(".ds-dex-table-row-col-liquidity")
        vol_node = row.select_one(".ds-dex-table-row-col-volume") 
        market_cap = mc_node.text.strip() if mc_node else ""
        liquidity  = liq_node.text.strip() if liq_node else ""
        volume     = vol_node.text.strip() if vol_node else ""
        thumbnail  = (row.select_one("img.ds-dex-table-row-token-icon-img") or {}).get("src","") if row.select_one("img.ds-dex-table-row-token-icon-img") else ""

        # store in DB
        if DB_WRITE:
            try:
                sql_cursor.execute("""
                    INSERT INTO tokens (contract, chain, name, symbol, market_cap, liquidity, volume, thumbnail)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        name=VALUES(name),
                        symbol=VALUES(symbol),
                        market_cap=VALUES(market_cap),
                        liquidity=VALUES(liquidity),
                        volume=VALUES(volume),
                        thumbnail=VALUES(thumbnail)
                """, (
                    contract,
                    CHAIN,
                    name,
                    symbol,
                    parse_num(market_cap),
                    parse_num(liquidity),
                    parse_num(volume),
                    thumbnail
                ))
                sqldb.commit()
            except mysql.connector.Error as err:
                dprint(f"Error inserting/updating token {contract}: {err}")

        # append to output
        out.append({
            "chain": CHAIN,
            "contract": contract,
            "name": name,
            "symbol": symbol,
            "rank": rank,
            "market_cap_raw": market_cap,
            "liquidity_raw": liquidity,
            "volume_raw": volume,
            "market_cap": parse_num(market_cap),
            "liquidity": parse_num(liquidity),
            "volume": parse_num(volume),
            "link": f"https://dexscreener.com/solana/{contract}",
        })
        rank += 1

    # dprint(f"Scraped {len(out)} tokens")
    return out

def compute_diff(prev: List[Dict], curr: List[Dict], rank_move_threshold: int):
    prev_idx = {(t["chain"], t["contract"]): t["rank"] for t in prev}
    curr_idx = {(t["chain"], t["contract"]): t["rank"] for t in curr}
    prev_keys, curr_keys = set(prev_idx.keys()), set(curr_idx.keys())

    added = [(k, curr_idx[k]) for k in (curr_keys - prev_keys)]
    removed = [(k, prev_idx[k]) for k in (prev_keys - curr_keys)]
    moved = []
    for k in (prev_keys & curr_keys):
        dr = prev_idx[k] - curr_idx[k]
        if abs(dr) >= rank_move_threshold:
            moved.append((k, prev_idx[k], curr_idx[k]))
    return added, removed, moved

def publish_token_change(change_type, chain, contract, old_rank, new_rank, window_version, as_of):
    payload = {
        "event_id": f"{chain}:{contract}:{window_version}:{change_type}:{old_rank}:{new_rank}",
        "as_of": as_of.isoformat(),
        "change_type": change_type,
        "chain": chain,
        "contract": contract,
        "old_rank": old_rank,
        "new_rank": new_rank,
        "window_version": window_version
    }
    r.publish("token_changed", json.dumps(payload))
    dprint(f"Published token_changed: {payload}")

def get_latest_version() -> int:
    v = r.get(K_LATEST_VER)
    return int(v) if v else 0

def save_window(curr: List[Dict], as_of: dt.datetime) -> int:
    """
    Atomically bump the version and store the latest snapshot in Redis.
    """
    pipe = r.pipeline()
    # next version
    pipe.incr(K_LATEST_VER)
    new_ver = pipe.execute()[0]

    pipe = r.pipeline()
    pipe.set(K_WINDOW_VER.format(ver=new_ver), json.dumps(curr))
    pipe.hset(K_WINDOW_META.format(ver=new_ver), mapping={"as_of": as_of.isoformat()})
    pipe.execute()
    return new_ver

def load_window(version: int) -> List[Dict]:
    if version <= 0: return []
    raw = r.get(K_WINDOW_VER.format(ver=version))
    return json.loads(raw) if raw else []

def run_once():
    as_of = dt.datetime.now(TZ).replace(microsecond=0)
    dprint("Scraping trending window...")
    curr = scrape_trending_topN(WINDOW_SIZE)

    dprint("Saving window to Redis...")
    new_ver = save_window(curr, as_of)
    prev_ver = new_ver - 1
    prev = load_window(prev_ver)

    added, removed, moved = compute_diff(prev, curr, RANK_MOVE_THRESHOLD)
    dprint(f"Diff v{prev_ver}→v{new_ver}: +{len(added)} / -{len(removed)} / moved:{len(moved)}")

    for (k, new_rank) in added:
        chain, contract = k
        publish_token_change("ADDED", chain, contract, None, new_rank, new_ver, as_of)
    for (k, old_rank) in removed:
        chain, contract = k
        publish_token_change("REMOVED", chain, contract, old_rank, None, new_ver, as_of)
    for (k, old_rank, new_rank) in moved:
        chain, contract = k
        publish_token_change("MOVED", chain, contract, old_rank, new_rank, new_ver, as_of)

if __name__ == "__main__":
    dprint("Trending extractor (Redis only) started.")
    # Ensure version key exists
    if not r.exists(K_LATEST_VER):
        r.set(K_LATEST_VER, 0)

    # initialize the SQL connection
    try:
        sql_cursor.execute("CREATE DATABASE IF NOT EXISTS solana_tokens")
        sql_cursor.execute("USE solana_tokens")
        sql_cursor.execute("""
            CREATE TABLE IF NOT EXISTS tokens (
                contract VARCHAR(64) PRIMARY KEY,
                chain VARCHAR(10),
                name VARCHAR(255),
                symbol VARCHAR(50),
                market_cap DOUBLE,
                liquidity DOUBLE,
                volume DOUBLE,
                thumbnail VARCHAR(255)
            )
                    """)
        sqldb.commit()
    except mysql.connector.Error as err:
        dprint(f"Error initializing MySQL: {err}")
        exit(1)

    while True:
        try:
            run_once()
        except Exception as e:
            dprint(f"ERROR: {e}")
        # # small jitter
        # sleep_s = INTERVAL_SEC + random.randint(-5, 5)
        # time.sleep(60*5)
        time.sleep(5)
