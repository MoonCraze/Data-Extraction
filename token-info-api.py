import os
import json
import mysql.connector
import re
from flask import Flask, jsonify
from seleniumbase import SB
import urllib.parse
from dotenv import load_dotenv
from bs4 import BeautifulSoup

load_dotenv()

app = Flask(__name__)

# Environment variables
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
    ssl_disabled=False,
    autocommit=True
)

HEADLESS = os.getenv("HEADLESS", "1") == "1"

def dprint(message):
    print(f"API:: {message}")

def scrape_token_info(addr: str) -> dict:
    with SB(uc=True, test=True, locale_code="en", headless=HEADLESS) as sb:
        dprint(f"Navigate: {addr}")
        sb.activate_cdp_mode(addr)
        sb.sleep(1)
        try: sb.uc_gui_click_captcha()
        except Exception as e: dprint(f"Captcha not present/ignored: {e}")
        sb.sleep(1)
        html = sb.get_page_source()

    soup = BeautifulSoup(html, "html.parser")
    
    # Find the logo image URL using BeautifulSoup
    logo_img = soup.find('img', src=re.compile(r'cdn\.dexscreener\.com/cms/images/'))
    logo_url = logo_img['src'] if logo_img else None
    
    # Extract token name (prefer header structure)
    # 1) Look inside <header> that has (itself or descendants) class 'chakra-stack'
    #    and contains h2.chakra-heading.
    header_candidates = []
    for hdr in soup.find_all('header'):
        has_stack = ('chakra-stack' in (hdr.get('class') or [])) or bool(hdr.find(class_='chakra-stack'))
        if not has_stack:
            continue
        h2 = hdr.find('h2', class_='chakra-heading')
        if not h2:
            continue
        span = h2.find('span')
        text = (span.get_text(strip=True) if span else h2.get_text(strip=True))
        if text and 1 <= len(text) <= 120:
            header_candidates.append((hdr, h2, text))

    if not header_candidates:
        dprint("Name pattern debug (header): No <header> with chakra-stack and h2.chakra-heading found")
    else:
        dprint(f"Name pattern debug (header): Found {len(header_candidates)} candidates")
        for idx, (_, _, txt) in enumerate(header_candidates):
            dprint(f"  [H{idx}] {txt}")

    # Extract token symbol (prefer element structure)
    # 1) Look inside <div> that has (itself or descendants) class 'chakra-stack'
    #    and contains h2.chakra-heading. 
    #  h2 contains span with symbol text.
    symbol_candidates = []
    for div in soup.find_all('div'):
        has_stack = ('chakra-stack' in (div.get('class') or [])) or bool(div.find(class_='chakra-stack'))
        if not has_stack:
            continue
        h2 = div.find('h2', class_='chakra-heading')
        if not h2:
            continue
        span = h2.find('span')
        text = (span.get_text(strip=True) if span else h2.get_text(strip=True))
        if text and 1 <= len(text) <= 20:
            symbol_candidates.append((div, h2, text))

    if not symbol_candidates:
        dprint("Symbol pattern debug (div): No <div> with chakra-stack and h2.chakra-heading found")
    else:
        dprint(f"Symbol pattern debug (div): Found {len(symbol_candidates)} candidates")
        for idx, (_, _, txt) in enumerate(symbol_candidates):
            dprint(f"  [S{idx}] {txt}")
    
    token_data = {
        # store the contract address in simple letter form
        'contract': addr.split('/')[-1].lower(),
        'name': header_candidates[0][2] if header_candidates else None,
        'symbol': symbol_candidates[0][2] if symbol_candidates else None,
        'logo_url': logo_url
    }

    return token_data

@app.route('/token/<token_address>', methods=['GET'])
def get_token_info(token_address: str):
    # Check if token exists in DB
    cursor = sqldb.cursor(dictionary=True, buffered=True)
    cursor.execute("USE solana_tokens")
    cursor.execute("SELECT * FROM tokens WHERE contract = %s", (token_address,))
    result = cursor.fetchone()
    cursor.close()

    if result:
        token_data = {
            'contract': result['contract'],
            'logo_url': result['thumbnail'],
            'name': result['name'],
            'symbol': result['symbol']
        }
        return jsonify(token_data)
    else:
        # Scrape from dexscreener
        addr = f"https://dexscreener.com/solana/{token_address}"
        token_data = scrape_token_info(addr)
        
        if token_data:
            # Insert into DB
            cursor = sqldb.cursor(buffered=True)
            cursor.execute("USE solana_tokens")
            cursor.execute("""
                INSERT INTO tokens (contract, name, symbol, thumbnail)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    symbol = VALUES(symbol),
                    thumbnail = VALUES(thumbnail)
            """, (token_data['contract'], token_data['name'], token_data['symbol'], token_data['logo_url']))
            sqldb.commit()
            cursor.close()
            return jsonify(token_data)
        else:
            return jsonify({"error": "Failed to scrape token data"}), 500

if __name__ == "__main__":
    # Use tokens table in solana_tokens DB
    try:
        sql_cursor = sqldb.cursor(buffered=True)
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
        sql_cursor.close()
    except mysql.connector.Error as err:
        print(f"Error initializing MySQL: {err}")
        exit(1)

    app.run(host='0.0.0.0', port=5000, debug=True)