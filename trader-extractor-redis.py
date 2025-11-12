#  redis-server --daemonize yes
import os
import json
import threading
import mysql.connector
import redis
import re
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from seleniumbase import SB
import urllib.parse

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

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
# DB_CFG = {
#     "host": os.getenv("MYSQL_HOST", "127.0.0.1"),  # use IP, not 'localhost'
#     "port": int(os.getenv("MYSQL_PORT", "3306")),
#     "user": os.getenv("MYSQL_USER", "root"),
#     "password": os.getenv("MYSQL_PASS", "1234"),
#     "database": os.getenv("MYSQL_DB", "solana_tokens"),
# }
EVENT_CHANNEL = "token_changed"
DEDUPE_SET = "processed_event_ids"  # Redis set for idempotency
DEDUPE_TTL = 600  # seconds

K_CUR = "trending:window:current"            # JSON array (latest-only snapshot)
K_LATEST_VER = "trending:latest_version"     # string int
K_WINDOW_VER = "trending:window:{ver}"       # JSON array by version

r = redis.from_url(REDIS_URL, decode_responses=True)

# sqldb = mysql.connector.connect(
#     host=DB_CFG["host"],
#     port=DB_CFG["port"],
#     user=DB_CFG["user"],
#     password=DB_CFG["password"],
#     database=DB_CFG["database"],
#     ssl_disabled=False
# )

sql_cursor = sqldb.cursor()

DEBUG_PRINT = True  # Set to True for debugging output
def dprint(message):
    if DEBUG_PRINT:
        thread = threading.current_thread()
        thread_info = f"tid-{thread.ident}"
        print(f"{thread_info}:: {message}")

def load_current_snapshot() -> List[Dict]:
    """Load the latest snapshot (supports both single 'current' and versioned)."""
    while(1):
        v = r.get(K_LATEST_VER)
        if v:
            raw = r.get(K_WINDOW_VER.format(ver=int(v)))
            if raw:
                try:
                    # display the info of the snapshot -> version, no. of tokens
                    snapshot = json.loads(raw)
                    dprint(f"Loaded snapshot for version {v} and no. of the tokens: {len(snapshot)}")
                    return snapshot
                
                except Exception:
                    dprint(f"Error loading snapshot for version {v}: {raw}")
                    continue
        else:
            dprint(f"No version found")
            continue

def parse_number(value_str):
    if not value_str:
        return None

    value_str = value_str.replace(",", "").strip().replace("$", "").replace("%", "")
    multiplier = 1.0

    if value_str.endswith("K"):
        multiplier = 1_000.0
        value_str = value_str[:-1]
    elif value_str.endswith("M"):
        multiplier = 1_000_000.0
        value_str = value_str[:-1]
    elif value_str.endswith("B"):
        multiplier = 1_000_000_000.0
        value_str = value_str[:-1]

    try:
        return float(value_str) * multiplier
    except ValueError:
        return None

def _process_one_token(token_address: str):
    url = f"https://dexscreener.com/solana/{token_address}"
    try:
        # Use SB context manager for automatic driver management and UC mode
        # uc=True enables undetected-chromedriver features
        # test=True can sometimes help with stability/configuration
        # locale_code sets browser language, potentially aiding bypass
        with SB(uc=True, test=True, locale_code="en", headless=True) as sb: # headless=True runs without visible browser

            # activate_cdp_mode often used with uc=True for better interaction & navigation
            sb.activate_cdp_mode(url)

            # Short wait for the potential verification page to appear
            sb.sleep(5)

            # Attempt to click the Cloudflare checkbox IF it appears visually
            # SeleniumBase tries to handle this automatically, but this adds robustness
            try:
                sb.uc_gui_click_captcha()
            except Exception as captcha_click_error:
                dprint(f"Captcha click failed or wasn't necessary: {captcha_click_error}")
            
            # wait until the class custom-1oq7u8k loads
            try:
                sb.wait_for_element_visible('div.custom-1oq7u8k', timeout=100)
            except Exception as e:
                dprint(f"Error waiting for class custom-1oq7u8k elements: {e}")
                # exit if the elements are not found
                return token_address

            # find the button with class 'custom-165cjlo' and click it
            try:
                sb.click('button:contains("Top Traders")')
                
                # wait until class_='custom-1hhf88o' loads
                try:
                    sb.wait_for_element_visible('a.custom-1hhf88o', timeout=100)
                except Exception as e:
                    dprint(f"Error waiting for trader address elements: {e}")
                    # Consider adding sb.save_screenshot_to_logs() here too on error
                    return token_address

                # extract the html of the page after clicking the button
                page_source = sb.get_page_source()
                soup = BeautifulSoup(page_source, 'html.parser')
                
                # find all the a tag with class 'ds-dex-table-row-trader-address'
                trader_address_tags = soup.find_all('a', class_='custom-1hhf88o')
                
                # loop through the tags and extract the href attribute
                if len(trader_address_tags) >= 10:
                    for tag in trader_address_tags[:10]:  # Limit to first 10 traders
                        if tag and 'href' in tag.attrs:
                            href = tag['href']
                            # debug_print(f"Found href: {href}")
                            # https://solscan.io/account/8Hw9X9UwBso7Sp2CFnEEeUGW8pGDj9wghc78ccWFZWpU get the last part of the href
                            wallet_address = href.split('/')[-1]

                            # # check wallet address already exists in the database, if yes -> skip
                            # sql_cursor.execute("SELECT COUNT(*) FROM traders WHERE wallet_address = %s", (wallet_address,))
                            # exists = sql_cursor.fetchone()[0] > 0

                            # if exists:
                            #     dprint(f"Wallet address {wallet_address} already exists in the database, skipping.")
                            #     continue

                            # get the trader's gross profit, win rate, wins, losses, etc.
                            target_url = f"https://dexcheck.ai/app/wallet-analyzer/{wallet_address}"

                            sb.open(target_url)

                            # Short wait for the potential verification page to appear
                            sb.sleep(2)

                            # Attempt to click the Cloudflare checkbox IF it appears visually
                            # SeleniumBase tries to handle this automatically, but this adds robustness
                            try:
                                sb.uc_gui_click_captcha()
                            except Exception as captcha_click_error:
                                dprint(f"Captcha click failed or wasn't necessary: {captcha_click_error}")

                            # wait until the element <h3 class="text-sm text-white/70"> is visible
                            sb.wait_for_element_visible('img.bg-brand-background-highlight', timeout=50)

                            page_source = sb.get_page_source()

                            soup = BeautifulSoup(page_source, 'html.parser')

                            # --- Check if the wallet is bot? if bot the name is like this "Bot (gasTzr94Pmp4Gf8vknQnqxeYxdgwFjbgdJa4msYRpnB)"
                            # begin with Bot 
                            bot_tag = soup.find('span', string=re.compile(r"^Bot\s*\(", flags=re.I))

                            # --- Gross Profit ---
                            gross_profit = soup.find('h3', string=re.compile(r"Gross Profit", flags=re.I))
                            if gross_profit:
                                gross_profit_value = parse_number(gross_profit.find_next('p').text.strip())

                            # -- Realized Profit ---
                            realized_profit = soup.find('p', string=re.compile(r"Realized", flags=re.I))
                            if realized_profit:
                                realized_profit_str = realized_profit.find_next('p').text.strip()
                                realized_profit_value = realized_profit_str.split(" ")[0].replace(",", "").replace("$", "")
                                realized_profit_percent = realized_profit_str.split(" ")[1].replace("(", "").replace(")", "").replace("%", "")

                            # --- Unrealized Profit ---
                            unrealized_profit = soup.find('p', string=re.compile(r"Unrealized", flags=re.I))
                            if unrealized_profit:
                                unrealized_profit_str = unrealized_profit.find_next('p').text.strip()
                                unrealized_profit_value = unrealized_profit_str.split(" ")[0].replace(",", "").replace("$", "")
                                unrealized_profit_percent = unrealized_profit_str.split(" ")[1].replace("(", "").replace(")", "").replace("%", "")

                            # --- Win Rate ---
                            win_rate = soup.find('h3', string=re.compile(r"Win Rate", flags=re.I))
                            if win_rate:
                                win_rate_value = parse_number(win_rate.find_next('p').text.strip())

                            # --- Wins and Losses ---
                            win_count = soup.find('p', string=re.compile(r"Win", flags=re.I))
                            if win_count:
                                win_value = parse_number(win_count.find_next('p').text.strip())
                            loss_count = soup.find('p', string=re.compile(r"Lose", flags=re.I))
                            if loss_count:
                                loss_value = parse_number(loss_count.find_next('p').text.strip())

                            # --- Trade Volume, Trades, Avg Trade Size ---
                            # Based on known layout order and heading structure
                            trade_volume = soup.find('p', string=re.compile(r"Trading Volume", flags=re.I))
                            if trade_volume:
                                trade_volume_value = parse_number(trade_volume.find_next('p').text.strip())

                            trades = soup.find('p', string=re.compile(r"Trades", flags=re.I))
                            if trades:
                                trades_value = parse_number(trades.find_next('p').text.strip())

                            avg_trade_size = soup.find('p', string=re.compile(r"Avg. Trade Size", flags=re.I))
                            if avg_trade_size:
                                avg_trade_size_value = parse_number(avg_trade_size.find_next('p').text.strip())
                            dprint(f"Is bot: {bot_tag is not None} "
                                f"Wallet {wallet_address} token {token_address}: "
                                f"Gross Profit: {gross_profit_value}, Win Rate: {win_rate_value}, "
                                f"Realized Profit: {realized_profit_value}, Unrealized Profit: {unrealized_profit_value}, "
                                f"Realized Profit (%): {realized_profit_percent}, Unrealized Profit (%): {unrealized_profit_percent}, "
                                f"Wins: {win_value}, Losses: {loss_value}, Trading Volume: {trade_volume_value}, "
                                f"Trades: {trades_value}, Avg. Trade Size: {avg_trade_size_value} \n")

                            if DB_WRITE:
                                # save to MySQL if already exists update the parameters
                                sql_cursor.execute("""
                                    INSERT INTO traders (wallet_address, token_address, gross_profit, realized_profit, 
                                    realized_profit_percent, unrealized_profit, unrealized_profit_percent, win_rate, wins, losses, 
                                    trade_volume, trades, avg_trade_size, is_bot)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    """, (wallet_address, token_address, gross_profit_value, realized_profit_value,
                                          realized_profit_percent, unrealized_profit_value, unrealized_profit_percent, win_rate_value,
                                          win_value, loss_value, trade_volume_value, trades_value, avg_trade_size_value,
                                          bot_tag is not None))
                                sqldb.commit()

                                

                    dprint(f"Extracted wallet data from {token_address}")
                    return token_address
                else:
                    dprint(f"Not enough traders found for token {token_address}")
                    return token_address
                             
            except Exception as e:
                dprint(f"Error clicking 'Top Traders' button: {e}")

    except Exception as e:
        dprint(f"An error occurred during SeleniumBase scraping: {e}")
        # Consider adding sb.save_screenshot_to_logs() here too on error


if __name__ == "__main__":
    dprint("Starting trader-extractor-redis.py...")

    # Check Redis connection
    try:
        r.ping()
    except redis.ConnectionError:
        dprint("Error connecting to Redis")
        exit(1)

    # initialize the SQL connection
    try:
        sql_cursor.execute("CREATE DATABASE IF NOT EXISTS solana_tokens")
        sql_cursor.execute("USE solana_tokens")
        sql_cursor.execute("""
            CREATE TABLE IF NOT EXISTS traders (
                wallet_address VARCHAR(44) PRIMARY KEY,
                token_address VARCHAR(44),
                gross_profit DECIMAL(20, 2),
                realized_profit DECIMAL(20, 2),
                realized_profit_percent DECIMAL(5, 2),
                unrealized_profit DECIMAL(20, 2),
                unrealized_profit_percent DECIMAL(5, 2),
                win_rate DECIMAL(5, 2),
                wins INT,
                losses INT,
                trade_volume DECIMAL(20, 2),
                trades INT,
                avg_trade_size DECIMAL(20, 2),
                is_bot BOOLEAN      
            )
                    """)
        sqldb.commit()
    except mysql.connector.Error as err:
        dprint(f"Error initializing MySQL: {err}")
        exit(1)

    # get initial sync
    snapshot = load_current_snapshot()
    if snapshot:
        for tok in snapshot:
            _process_one_token(tok['contract'])
    dprint("Initial sync complete")

    # Subscribe to Redis channel for token changes
    p = r.pubsub(ignore_subscribe_messages=True)
    p.subscribe("token_changed")

    for message in p.listen():
            # payload = {
            #     "event_id": f"{chain}:{contract}:{window_version}:{change_type}:{old_rank}:{new_rank}",
            #     "as_of": as_of.isoformat(),
            #     "change_type": change_type,
            #     "chain": chain,
            #     "contract": contract,
            #     "old_rank": old_rank,
            #     "new_rank": new_rank,
            #     "window_version": window_version
            # }

            dprint(f"Received message: {message}")