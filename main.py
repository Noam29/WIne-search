#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import aiohttp
import async_timeout
import random
import json
import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import urllib.parse as up
from html import unescape

try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:
    BeautifulSoup = None

# =========================
# Config
# =========================
BASE = "https://www.the-importer.co.il"
CATEGORY_URL = f"{BASE}/יין/יין-אדום"
MAX_CATEGORY_PAGES = 10

# concurrency
MAX_CONCURRENT_PAGES = 8
MAX_CONCURRENT_PDP = 6
REQUEST_TIMEOUT_SEC = 40

# outputs
OUT_MATCHED = str(Path.cwd() / "the_importer_matched.json")
OUT_UNMATCHED = str(Path.cwd() / "the_importer_unmatched.json")

# search engines for Vivino
BING_SEARCH_URL = "https://www.bing.com/search"
VIVINO_FALLBACK_SEARCH = "https://www.vivino.com/search/wines"

# headers
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
}
PDP_HEADERS = {
    **DEFAULT_HEADERS,
    "Referer": BASE + "/",
}
VIVINO_HEADERS = {
    **DEFAULT_HEADERS,
    "Accept-Language": "en-US,en;q=0.9",
}

def log(msg: str) -> None:
    print(msg, flush=True)

# =========================
# Utils
# =========================
PRICE_RE = re.compile(r"([0-9]+(?:[.,][0-9]+)?)")
ML_RE = re.compile(r"(\d{2,5})\s*(?:ml|מ״ל|מל|ML|Ml)\b", re.IGNORECASE)
ML_NUMBER_RE = re.compile(r"(\d{2,5})")

def parse_price(text: str) -> Optional[float]:
    if not text:
        return None
    m = PRICE_RE.search(text.replace(",", ""))
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None

def extract_ml(text: str) -> Optional[int]:
    """Extract bottle size in milliliters from text.

    The original implementation failed to handle numbers containing comma
    thousand separators (e.g. ``"1,500 ml"``), returning only the digits after
    the comma.  Normalise the input by removing commas before running the
    regular expressions so such cases are parsed correctly.
    """
    if not text:
        return None

    # Remove common thousands separators to ensure accurate digit matching
    cleaned = text.replace(",", "")

    m = ML_RE.search(cleaned)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            pass

    m2 = ML_NUMBER_RE.search(cleaned)
    if m2:
        try:
            ml = int(m2.group(1))
            if 50 <= ml <= 3000:
                return ml
        except Exception:
            pass
    return None

def normalize_name(name: str) -> str:
    name = unescape(name or "")
    name = re.sub(r"\s+", " ", name).strip()
    return name

# =========================
# HTTP
# =========================
async def fetch_text(
    session: aiohttp.ClientSession,
    url: str,
    *,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    min_delay: float = 0.15,
    max_delay: float = 0.5,
) -> Optional[str]:
    await asyncio.sleep(random.uniform(min_delay, max_delay))
    try:
        async with async_timeout.timeout(REQUEST_TIMEOUT_SEC):
            async with session.get(url, params=params, headers=headers) as r:
                if r.status != 200:
                    log(f"[HTTP-ERR] {url} -> {r.status}")
                    return None
                return await r.text()
    except Exception as e:
        log(f"[HTTP-ERR] {url} -> {e}")
        return None

# =========================
# Category parsing
# =========================
def extract_product_urls_from_category(html: str) -> List[str]:
    urls: List[str] = []
    if BeautifulSoup:
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.select("a.product-item-link, h2 a, h3 a, a[href*='-']"):
            href = a.get("href")
            if not href:
                continue
            if href.startswith("/"):
                href = up.urljoin(BASE, href)
            if BASE not in href:
                continue
            # filter obvious non-product routes
            if any(x in href for x in ["/customer/", "/cart", "/checkout", "/search", "/account", "/login"]):
                continue
            urls.append(href)
    else:
        for m in re.finditer(r'<a[^>]+href="([^"]+)"', html):
            href = m.group(1)
            if href.startswith("/"):
                href = up.urljoin(BASE, href)
            if BASE in href and not any(x in href for x in ["/customer/", "/cart", "/checkout", "/search", "/account", "/login"]):
                urls.append(href)
    # dedupe
    return list(dict.fromkeys(urls))

# =========================
# PDP parsing (hardened)
# =========================
def _from_json_ld(soup) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            payload = json.loads(tag.get_text(strip=True))
        except Exception:
            continue
        # Sometimes an array of things
        candidates = payload if isinstance(payload, list) else [payload]
        for obj in candidates:
            if not isinstance(obj, dict):
                continue
            if obj.get("@type") in ("Product", ["Product"]):
                if "name" in obj and not data.get("name"):
                    data["name"] = normalize_name(obj["name"])
                offers = obj.get("offers")
                if isinstance(offers, dict):
                    price = offers.get("price") or offers.get("priceSpecification", {}).get("price")
                    if price and not data.get("price_value"):
                        data["price_value"] = parse_price(str(price))
                # size might be in name/description
                desc_text = " ".join(str(obj.get(k, "")) for k in ("description", "name"))
                ml = extract_ml(desc_text)
                if ml and not data.get("bottle_size_ml"):
                    data["bottle_size_ml"] = ml
    return data

def _from_meta_tags(soup) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    # Open Graph
    mt = soup.find("meta", property="og:title")
    if mt and not data.get("name"):
        data["name"] = normalize_name(mt.get("content", ""))
    # Price metas
    for sel in [
        ('meta', {"itemprop": "price"}),
        ('meta', {"property": "product:price:amount"}),
        ('meta', {"name": "twitter:data1"}),  # sometimes contains price string
    ]:
        tag = soup.find(sel[0], sel[1])
        if tag and not data.get("price_value"):
            data["price_value"] = parse_price(tag.get("content", "") or tag.get("value", ""))
    # look for og:description for ml
    md = soup.find("meta", property="og:description")
    if md and not data.get("bottle_size_ml"):
        data["bottle_size_ml"] = extract_ml(md.get("content", ""))
    return data

def _from_dom_selectors(soup) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    # name
    title = soup.select_one("h1.page-title .base") or soup.select_one("h1 .base") \
            or soup.select_one("h1.product-name") or soup.find("h1")
    if title and not data.get("name"):
        data["name"] = normalize_name(title.get_text(" ", strip=True))
    # price wrappers
    price_el = soup.select_one("span.price[data-price-amount]") \
            or soup.select_one(".price-wrapper .price") \
            or soup.find("span", {"class": "price"})
    if price_el and not data.get("price_value"):
        # prefer data attribute
        amt = price_el.get("data-price-amount")
        data["price_value"] = parse_price(amt or price_el.get_text(" ", strip=True))
    # try to find ml anywhere in the core info
    blobs = []
    for sel in ["#description", ".product.attribute.description", ".product-short-description", ".product-info-main"]:
        el = soup.select_one(sel)
        if el:
            blobs.append(el.get_text(" ", strip=True))
    if title:
        blobs.append(title.get_text(" ", strip=True))
    for row in soup.select("table, .additional-attributes-wrapper"):
        blobs.append(row.get_text(" ", strip=True))
    blob = " | ".join(blobs)
    if blob and not data.get("bottle_size_ml"):
        data["bottle_size_ml"] = extract_ml(blob)
    return data

def page_looks_like_product(soup) -> bool:
    ogtype = soup.find("meta", property="og:type")
    if ogtype and "product" in (ogtype.get("content") or "").lower():
        return True
    # JSON-LD Product present?
    j = _from_json_ld(soup)
    return bool(j.get("name"))

def parse_pdp(html: str, url: str) -> Optional[Dict[str, Any]]:
    if not html:
        return None
    if not BeautifulSoup:
        return None
    soup = BeautifulSoup(html, "html.parser")

    if not page_looks_like_product(soup):
        # Many of your recent failures are likely non-product pages or JS shells
        return None

    data: Dict[str, Any] = {}
    # priority: JSON-LD -> meta tags -> DOM
    for extractor in (_from_json_ld, _from_meta_tags, _from_dom_selectors):
        chunk = extractor(soup)
        for k, v in chunk.items():
            if v and not data.get(k):
                data[k] = v

    name = normalize_name(data.get("name") or "")
    price = data.get("price_value")
    ml = data.get("bottle_size_ml")

    if not name:
        return None
    if price is None:
        # final hail-mary: search any price-looking number with ₪ nearby
        m = re.search(r"₪\s*([0-9]+(?:[.,][0-9]+)?)", html)
        if m:
            try:
                price = float(m.group(1).replace(",", ""))
            except Exception:
                price = None
    if price is None:
        return None

    if not ml:
        ml = 750  # sane default

    return {
        "name": name,
        "price_value": float(price),
        "bottle_size_ml": int(ml),
        "url": url,
    }

# =========================
# Vivino helpers (unchanged from previous hardened version)
# =========================
VIVINO_CACHE_PATH = Path.cwd() / "vivino_cache.json"
vivino_cache: Dict[str, Any] = {}
vivino_fallback_sem = asyncio.Semaphore(1)

HEB_TOKEN_MAP = {
    "ירדן": "Yarden", "גמלא": "Gamla", "יקב רמת הגולן": "Golan Heights Winery",
    "שאטו גולן": "Chateau Golan", "רקנאטי": "Recanati", "יתיר": "Yatir",
    "הרי גליל": "Galil Mountain", "פסגות": "Psagot", "דלתון": "Dalton",
    "ברקן": "Barkan", "כרמל": "Carmel", "טוליפ": "Tulip", "ויתקין": "Vitkin",
    "1848": "1848 Winery", "אבני החושן": "Even Hahoshen", "מוני": "Moni",
    "צרעה": "Tzora", "צובה": "Tzuba", "אדיר": "Adir",
    "קברנה סוביניון": "Cabernet Sauvignon", "קברנה פרנק": "Cabernet Franc",
    "מרלו": "Merlot", "שיראז": "Shiraz", "סירה": "Syrah", "פטיט סירה": "Petite Sirah",
    "פטי ורדו": "Petit Verdot", "מלבק": "Malbec", "טמפרניו": "Tempranillo",
    "סנג'ובזה": "Sangiovese", "גראנש": "Grenache", "פינו נואר": "Pinot Noir",
    "ריוחה": "Rioja", "רזרבה": "Reserva", "גראן רזרבה": "Gran Reserva",
    "קריאנזה": "Crianza", "בלנד": "Blend",
}

def _he_to_en_query(name: str) -> str:
    q = name
    for he, en in HEB_TOKEN_MAP.items():
        q = q.replace(he, en)
    q = re.sub(r"[\"'’]", " ", q)
    q = re.sub(r"\s+", " ", q).strip()
    return q

def _extract_all_vivino_links_from_bing(html: str) -> List[str]:
    urls: List[str] = []
    for m in re.finditer(r'<a href="(https?://[^"]+vivino[^"]+)"', html):
        urls.append(m.group(1))
    urls = [u for u in urls if "/wines/" in u]
    clean: List[str] = []
    for u in urls:
        try:
            parts = up.urlsplit(u)
            clean.append(up.urlunsplit((parts.scheme, parts.netloc, parts.path, "", "")))
        except Exception:
            clean.append(u)
    return list(dict.fromkeys(clean))

def _extract_vivino_rating_from_html(html: str) -> Optional[float]:
    m = re.search(r'"rating"\s*:\s*{[^}]*"average"\s*:\s*([0-9.]+)', html)
    if m:
        try:
            val = float(m.group(1))
            if 2.5 <= val <= 5.0:
                return val
        except Exception:
            pass
    m2 = re.search(r'([3-5]\.[0-9])\s*(?:/|out of)?\s*5', html, re.I)
    if m2:
        try:
            val = float(m2.group(1))
            if 2.5 <= val <= 5.0:
                return val
        except Exception:
            pass
    return None

async def _fetch_vivino_search_with_backoff(session: aiohttp.ClientSession, params: dict) -> Optional[str]:
    delay = 2.0
    max_tries = 6
    async with vivino_fallback_sem:
        for attempt in range(1, max_tries + 1):
            await asyncio.sleep(delay if attempt > 1 else 0.0)
            try:
                async with session.get(VIVINO_FALLBACK_SEARCH, params=params, headers=VIVINO_HEADERS) as r:
                    if r.status == 429:
                        log(f"[VIVINO-429] backoff attempt {attempt}, sleeping {delay:.1f}s")
                        delay *= 1.7 + random.random() * 0.3
                        continue
                    r.raise_for_status()
                    return await r.text()
            except Exception as e:
                log(f"[HTTP-ERR] {VIVINO_FALLBACK_SEARCH} -> {e}")
                delay *= 1.4
        return None

async def vivino_lookup(session: aiohttp.ClientSession, wine_name: str) -> Optional[Dict[str, Any]]:
    key = wine_name.strip()
    if key in vivino_cache:
        return vivino_cache[key]

    q_en = _he_to_en_query(wine_name)
    params = {"q": f'site:vivino.com "{q_en}"', "setlang": "en"}
    html = await fetch_text(session, BING_SEARCH_URL, params=params, headers=VIVINO_HEADERS, min_delay=0.4, max_delay=1.0)
    viv_urls: List[str] = []
    if html:
        viv_urls = _extract_all_vivino_links_from_bing(html)
        if viv_urls:
            log(f"[VIVINO] Bing hits for '{q_en[:60]}…': {len(viv_urls)}")

    if not viv_urls:
        vhtml = await _fetch_vivino_search_with_backoff(session, {"q": q_en})
        if vhtml:
            for m in re.finditer(r'href="(/wines/[^"#?]+)"', vhtml):
                viv_urls.append(up.urljoin("https://www.vivino.com", m.group(1)))
            viv_urls = list(dict.fromkeys(viv_urls))
            if viv_urls:
                log(f"[VIVINO] Vivino fallback hits for '{q_en[:60]}…': {len(viv_urls)}")

    for vu in viv_urls[:3]:
        page = await fetch_text(session, vu, headers=VIVINO_HEADERS, min_delay=0.6, max_delay=1.2)
        if not page:
            continue
        rating = _extract_vivino_rating_from_html(page)
        if rating is not None:
            res = {"vivino_url": vu, "vivino_rating": round(float(rating), 2)}
            vivino_cache[key] = res
            return res

    vivino_cache[key] = None
    return None

# =========================
# Crawl logic
# =========================
async def fetch_category_page(session: aiohttp.ClientSession, page: int) -> Tuple[int, List[str]]:
    url = CATEGORY_URL if page == 1 else f"{CATEGORY_URL}?p={page}"
    html = await fetch_text(session, url, headers=DEFAULT_HEADERS, min_delay=0.2, max_delay=0.5)
    urls: List[str] = []
    if html:
        urls = extract_product_urls_from_category(html)
    log(f"[PARSE] page {page}: collected {len(urls)} product URLs")
    return page, urls

async def fetch_pdp(session: aiohttp.ClientSession, url: str) -> Optional[Dict[str, Any]]:
    # try twice with different pacing/headers if the first parse yields nothing
    html = await fetch_text(session, url, headers=PDP_HEADERS, min_delay=0.3, max_delay=0.8)
    data = parse_pdp(html or "", url)
    if not data:
        # second attempt with slower delay (some anti-bot pages swap on timing)
        html = await fetch_text(session, url, headers=PDP_HEADERS, min_delay=1.0, max_delay=1.8)
        data = parse_pdp(html or "", url)

    if data:
        log(f"[PDP] ok: {data['name']}  ₪{data['price_value']}  {data['bottle_size_ml']}ml")
        return data
    else:
        log(f"[PDP-ERR] {url} -> (parse failed)")
        return None

# =========================
# Main
# =========================
async def main():
    print(f"[OUT] Matched -> {OUT_MATCHED}")
    print(f"[OUT] Unmatched -> {OUT_UNMATCHED}")

    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SEC)
    connector = aiohttp.TCPConnector(limit_per_host=MAX_CONCURRENT_PAGES)
    async with aiohttp.ClientSession(headers=DEFAULT_HEADERS, timeout=timeout, connector=connector) as session:
        # Load Vivino cache
        global vivino_cache
        if VIVINO_CACHE_PATH.exists():
            try:
                vivino_cache = json.loads(VIVINO_CACHE_PATH.read_text(encoding="utf-8"))
                log(f"[CACHE] loaded {len([k for k,v in vivino_cache.items() if v])} hits "
                    f"(+{len([k for k,v in vivino_cache.items() if v is None])} misses).")
            except Exception as e:
                log(f"[CACHE] load failed: {e}")
                vivino_cache = {}

        # 1) category pages
        page_tasks = [fetch_category_page(session, p) for p in range(1, MAX_CATEGORY_PAGES + 1)]
        results = await asyncio.gather(*page_tasks)
        all_urls: List[str] = []
        for _, urls in results:
            all_urls.extend(urls)
        all_urls = list(dict.fromkeys(all_urls))
        log(f"[PARSE] total unique product URLs: {len(all_urls)}")

        # 2) fetch PDPs
        sem = asyncio.Semaphore(MAX_CONCURRENT_PDP)
        async def _bounded_fetch(u: str):
            async with sem:
                return await fetch_pdp(session, u)

        pdp_results: List[Optional[Dict[str, Any]]] = []
        done = 0
        for chunk_start in range(0, len(all_urls), MAX_CONCURRENT_PDP):
            chunk = all_urls[chunk_start:chunk_start + MAX_CONCURRENT_PDP]
            chunk_res = await asyncio.gather(*[_bounded_fetch(u) for u in chunk])
            pdp_results.extend(chunk_res)
            done += len(chunk)
            kept = len([x for x in pdp_results if x])
            log(f"[PDP] parsed {done}/{len(all_urls)} … kept {kept}")

        products: List[Dict[str, Any]] = [x for x in pdp_results if x]
        log(f"[PDP] done: kept {len(products)} / {len(all_urls)}")
        log(f"[PARSE] collected {len(products)} products; kept {len(products)} after filtering")

        # 3) Vivino matching
        matched: List[Dict[str, Any]] = []
        unmatched: List[Dict[str, Any]] = []
        viv_sem = asyncio.Semaphore(6)

        async def _match(prod: Dict[str, Any]) -> None:
            async with viv_sem:
                res = await vivino_lookup(session, prod["name"])
                if res:
                    matched.append({**prod, **res})
                else:
                    unmatched.append({**prod, "reason": "No Vivino candidates / rating extract failed"})

        await asyncio.gather(*[_match(p) for p in products])

        # 4) Write outputs
        Path(OUT_MATCHED).write_text(json.dumps(matched, ensure_ascii=False, indent=2), encoding="utf-8")
        Path(OUT_UNMATCHED).write_text(json.dumps(unmatched, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"[WRITE] {OUT_MATCHED} ({Path(OUT_MATCHED).stat().st_size} bytes) — items: {len(matched)}")
        log(f"[WRITE] {OUT_UNMATCHED} ({Path(OUT_UNMATCHED).stat().st_size} bytes) — items: {len(unmatched)}")

        # Save cache
        try:
            VIVINO_CACHE_PATH.write_text(json.dumps(vivino_cache, ensure_ascii=False, indent=2), encoding="utf-8")
            log(f"[CACHE] saved {len(vivino_cache)} entries -> {VIVINO_CACHE_PATH}")
        except Exception as e:
            log(f"[CACHE] save failed: {e}")

        print(f"Done. Matched: {len(matched)} | Unmatched: {len(unmatched)}")
        print(f"Files written: {OUT_MATCHED}, {OUT_UNMATCHED}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
