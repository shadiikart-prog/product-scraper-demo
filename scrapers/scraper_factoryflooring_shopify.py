"""
scraper_complete_system.py
════════════════════════════════════════════════════════════════
COMPLETE scraper — visits every product page individually
Gets: ALL images, prices, descriptions, SKUs, brands, stock

Strategy:
  Step 1 → Collect all product URLs from category pages (fast)
  Step 2 → Visit each product page in parallel (10 threads)
  Step 3 → Extract everything from JSON-LD + HTML
  Step 4 → Save 4 output files (Shopify ready)
"""

import requests
import json
import csv
import time
import re
import os
from datetime import datetime
from html import unescape
from concurrent.futures import ThreadPoolExecutor, as_completed

OUTPUT_DIR  = "output"
BASE_URL    = "https://www.factory-direct-flooring.co.uk"
TIMESTAMP   = datetime.now().strftime("%Y%m%d_%H%M%S")
REAL_CDN    = "imagely.factory-direct-flooring.co.uk/media/catalog/product"
PLACEHOLDER = "placeholder"
THREADS     = 10
DELAY       = 0.2

HEADERS = {
    "User-Agent"     : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept"         : "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}

SHOPIFY_TAXONOMY = {
    "wood"       : "Home & Garden > Decor > Flooring",
    "laminate"   : "Home & Garden > Decor > Flooring",
    "lvt"        : "Home & Garden > Decor > Flooring",
    "vinyl"      : "Home & Garden > Decor > Flooring",
    "herringbone": "Home & Garden > Decor > Flooring",
    "carpet"     : "Home & Garden > Decor > Rugs",
    "underlay"   : "Home & Garden > Decor > Flooring",
    "flooring"   : "Home & Garden > Decor > Flooring",
}

CATEGORY_URLS = [
    ("Solid Wood Flooring",      f"{BASE_URL}/solid-wood-flooring"),
    ("Engineered Wood Flooring", f"{BASE_URL}/engineered-wood-flooring"),
    ("Laminate Flooring",        f"{BASE_URL}/laminate-flooring"),
    ("LVT Flooring",             f"{BASE_URL}/lvt-flooring"),
    ("Herringbone Flooring",     f"{BASE_URL}/herringbone-flooring"),
    ("Vinyl Flooring",           f"{BASE_URL}/vinyl-flooring"),
    ("Carpet",                   f"{BASE_URL}/carpet"),
    ("Underlay",                 f"{BASE_URL}/underlay"),
]

SHOPIFY_PRODUCT_COLS = [
    "Handle","Title","Body (HTML)","Vendor","Product Category","Type","Tags",
    "Published","Option1 Name","Option1 Value","Variant SKU","Variant Grams",
    "Variant Inventory Tracker","Variant Inventory Qty","Variant Inventory Policy",
    "Variant Fulfillment Service","Variant Price","Variant Compare At Price",
    "Variant Requires Shipping","Variant Taxable","Image Src","Image Position",
    "Image Alt Text","SEO Title","SEO Description","Status",
]

SHOPIFY_COLLECTION_COLS = [
    "Handle","Title","Body (HTML)","Published",
    "Image Src","Image Alt Text","Sort Order","Updated At",
]


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def fetch(url):
    for attempt in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            if r.status_code == 200:
                return r.text
            if r.status_code == 404:
                return ""
        except Exception:
            time.sleep(1.5)
    return ""

def clean(text):
    if not text:
        return ""
    text = unescape(str(text))
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()

def make_handle(s):
    h = re.sub(r"[^a-z0-9]+", "-", s.lower().strip()).strip("-")
    return h[:200]

def norm_price(p):
    try:
        v = str(p).replace(",","").replace("£","").strip()
        return f"{float(v):.2f}" if v else ""
    except Exception:
        return ""

def is_real_img(url):
    return bool(url) and REAL_CDN in url and PLACEHOLDER not in url.lower()

def clean_img(url):
    return url.strip().split("?")[0] if url else ""

def get_shopify_cat(cat_name):
    cat_lower = cat_name.lower()
    for key, val in SHOPIFY_TAXONOMY.items():
        if key in cat_lower:
            return val
    return "Home & Garden > Decor > Flooring"


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1 — COLLECT ALL PRODUCT URLs FROM CATEGORY PAGES
# ═══════════════════════════════════════════════════════════════════════════════

def collect_product_urls():
    """
    Fast pass — just collect all product URLs from category listing pages.
    No individual page visits yet.
    """
    url_to_cat = {}   # url → category name
    seen       = set()

    print("\n" + "═"*62)
    print("  STEP 1: Collecting all product URLs...")
    print("═"*62)

    for cat_name, base_url in CATEGORY_URLS:
        page    = 1
        cur_url = base_url
        cat_new = 0

        while True:
            html = fetch(cur_url)
            if not html:
                break

            # Method 1: card-image links
            card_urls = re.findall(
                r'<div[^>]+class="[^"]*card-image[^"]*"[^>]*>.*?'
                r'href=["\'](' + re.escape(BASE_URL) + r'/[a-z0-9][a-z0-9-]+)["\']',
                html, re.DOTALL
            )
            for u in card_urls:
                if u not in seen and is_product_url(u):
                    seen.add(u)
                    url_to_cat[u] = cat_name
                    cat_new += 1

            # Method 2: any product link on page
            all_links = re.findall(
                r'href=["\'](' + re.escape(BASE_URL) + r'/[a-z0-9][a-z0-9-]{4,})["\']',
                html
            )
            for u in all_links:
                if u not in seen and is_product_url(u):
                    seen.add(u)
                    url_to_cat[u] = cat_name
                    cat_new += 1

            # Next page
            next_url = get_next_page(html, cur_url, page)
            if not next_url:
                break
            cur_url = next_url
            page   += 1
            time.sleep(DELAY)

        print(f"  {cat_name}: {cat_new} URLs")

    print(f"\n  ✓ Total: {len(url_to_cat)} unique product URLs")
    return url_to_cat


def is_product_url(url):
    skip = [
        "solid-wood-flooring","engineered-wood","laminate-flooring",
        "lvt-flooring","herringbone-flooring","vinyl-flooring",
        "/carpet","/underlay","/blog","/about","/contact","/advice",
        "/faq","/account","/login","/register","/wishlist","/cart",
        "/checkout","/search","/brand","/brands","?","#",".xml",
    ]
    path = url.replace(BASE_URL, "")
    return not any(s in url for s in skip) and path.count("/") == 1


def get_next_page(html, cur_url, page):
    rel = re.search(r'<link[^>]+rel=["\']next["\'][^>]+href=["\']([^"\']+)["\']', html)
    if rel:
        nxt = rel.group(1)
        return nxt if nxt.startswith("http") else BASE_URL + nxt

    pg = re.search(r'href=["\']([^"\']*[?&]p=' + str(page+1) + r'[^"\']*)["\']', html)
    if pg:
        nxt = pg.group(1)
        return nxt if nxt.startswith("http") else BASE_URL + nxt
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2 — SCRAPE EACH PRODUCT PAGE (PARALLEL)
# ═══════════════════════════════════════════════════════════════════════════════

def scrape_product_page(url, category):
    """
    Visit one product page and extract ALL data.
    Returns complete product dict.
    """
    html = fetch(url)
    if not html:
        return None

    product = {
        "url"        : url,
        "handle"     : make_handle(url.replace(BASE_URL+"/", "")),
        "category"   : category,
        "name"       : "",
        "sku"        : "",
        "price"      : "",
        "compare"    : "",
        "description": "",
        "brand"      : "Factory Direct Flooring",
        "images"     : [],
        "stock"      : "active",
        "tags"       : [],
    }

    # ── 1. JSON-LD (most reliable — has everything) ───────────────────────────
    json_blocks = re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL
    )
    for block in json_blocks:
        try:
            data  = json.loads(block.strip())
            items = data if isinstance(data, list) else [data]
            for item in items:
                if not isinstance(item, dict):
                    continue
                if item.get("@type") != "Product":
                    continue

                # Name
                if not product["name"]:
                    product["name"] = clean(item.get("name", ""))

                # SKU
                if not product["sku"]:
                    product["sku"] = str(item.get("sku", ""))

                # Description
                if not product["description"]:
                    product["description"] = clean(item.get("description", ""))[:800]

                # Brand
                b = item.get("brand", {})
                if isinstance(b, dict) and b.get("name"):
                    product["brand"] = clean(b["name"])
                elif isinstance(b, str) and b:
                    product["brand"] = clean(b)

                # Price + stock
                offers = item.get("offers", {})
                if isinstance(offers, list): offers = offers[0] if offers else {}
                if isinstance(offers, dict):
                    if not product["price"]:
                        product["price"] = str(offers.get("price", ""))
                    high = str(offers.get("highPrice", ""))
                    if high and high != product["price"]:
                        product["compare"] = high
                    avail = str(offers.get("availability", ""))
                    if avail:
                        product["stock"] = "active" if "InStock" in avail else "draft"

                # Images from JSON-LD
                imgs = item.get("image", [])
                if isinstance(imgs, str): imgs = [imgs]
                if isinstance(imgs, dict): imgs = [imgs.get("url","")]
                for img in imgs:
                    img_clean = clean_img(str(img))
                    if is_real_img(img_clean) and img_clean not in product["images"]:
                        product["images"].append(img_clean)

        except Exception:
            pass

    # ── 2. Extract ALL images from HTML (CDN only) ────────────────────────────
    # This catches images not in JSON-LD
    all_img_srcs = re.findall(
        r'(?:src|data-src|data-lazy-src|data-original)=["\']([^"\']+)["\']',
        html, re.IGNORECASE
    )
    for src in all_img_srcs:
        src_clean = clean_img(src)
        if is_real_img(src_clean) and src_clean not in product["images"]:
            product["images"].append(src_clean)

    # ── 3. HTML fallbacks if JSON-LD missing ──────────────────────────────────
    if not product["name"]:
        m = re.search(r'<h1[^>]*>.*?<span[^>]*>(.*?)</span>', html, re.DOTALL)
        if m:
            product["name"] = clean(m.group(1))
        else:
            slug = url.rstrip("/").split("/")[-1]
            product["name"] = " ".join(w.capitalize() for w in slug.split("-"))

    if not product["price"]:
        m = re.search(r'£\s*([\d,]+\.?\d*)', html)
        if m:
            product["price"] = m.group(1).replace(",","")

    if not product["description"]:
        # Try meta description
        m = re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)', html)
        if m:
            product["description"] = clean(m.group(1))[:800]

    if not product["sku"]:
        # Try from HTML pattern
        m = re.search(r'(?:SKU|Product Code|Ref)[^:]*:\s*([A-Z0-9-]{3,30})', html, re.IGNORECASE)
        if m:
            product["sku"] = m.group(1).strip()

    # ── 4. Tags from meta keywords ────────────────────────────────────────────
    mk = re.search(r'<meta[^>]+name=["\']keywords["\'][^>]+content=["\']([^"\']+)', html)
    if mk:
        product["tags"] = [t.strip() for t in mk.group(1).split(",") if t.strip()][:10]
    if not product["tags"]:
        product["tags"] = [category.lower().replace(" ","-")]

    return product if product["name"] else None


def scrape_all_products(url_to_cat):
    """Scrape all product pages in parallel batches."""
    all_products = []
    urls         = list(url_to_cat.items())
    total        = len(urls)
    done         = 0
    failed       = 0

    print("\n" + "═"*62)
    print(f"  STEP 2: Scraping {total} product pages ({THREADS} parallel)...")
    print("═"*62)

    # Process in batches to show progress
    batch_size = 50
    for batch_start in range(0, total, batch_size):
        batch = urls[batch_start:batch_start + batch_size]

        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            futures = {
                executor.submit(scrape_product_page, url, cat): url
                for url, cat in batch
            }
            for future in as_completed(futures):
                done += 1
                try:
                    result = future.result()
                    if result:
                        all_products.append(result)
                    else:
                        failed += 1
                except Exception:
                    failed += 1

        imgs   = sum(1 for p in all_products if p["images"])
        priced = sum(1 for p in all_products if p["price"])
        print(f"  Progress: {done}/{total} | Found: {len(all_products)} | "
              f"With images: {imgs} | Priced: {priced} | Failed: {failed}")

        time.sleep(0.5)

    return all_products


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3 — NAVIGATION STRUCTURE
# ═══════════════════════════════════════════════════════════════════════════════

def scrape_navigation():
    """Get full nav structure from homepage."""
    print("\n" + "═"*62)
    print("  STEP 3: Scraping navigation structure...")
    print("═"*62)

    html = fetch(BASE_URL)
    nav  = {"site": BASE_URL, "scraped_at": datetime.now().isoformat(), "categories": []}

    if not html:
        return nav

    # Find all nav links
    seen_urls = set()
    links = re.findall(
        r'href=["\'](' + re.escape(BASE_URL) + r'/[a-z0-9][a-z0-9-]+/?)["\'][^>]*>\s*([^<]{2,50})\s*</a>',
        html, re.IGNORECASE
    )

    skip = ["blog","about","contact","advice","faq","account",
            "login","register","wishlist","cart","checkout","search"]

    for url, name in links:
        url  = url.rstrip("/")
        name = clean(name)
        if not name or url in seen_urls:
            continue
        if any(s in url for s in skip):
            continue
        if url.replace(BASE_URL,"").count("/") != 1:
            continue

        seen_urls.add(url)
        nav["categories"].append({
            "name"  : name,
            "url"   : url,
            "handle": make_handle(name),
            "slug"  : url.replace(BASE_URL+"/",""),
        })

    print(f"  ✓ Found {len(nav['categories'])} navigation categories")
    return nav


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 4 — SAVE ALL OUTPUT FILES
# ═══════════════════════════════════════════════════════════════════════════════

def save_products_csv(products):
    rows = []
    for p in products:
        name   = p.get("name","")
        if not name: continue

        handle  = p.get("handle","") or make_handle(name)
        cat     = p.get("category","Flooring")
        body    = p.get("description","") or ""
        if body and not body.strip().startswith("<"):
            body = f"<p>{body}</p>"

        vendor  = p.get("brand","") or "Factory Direct Flooring"
        sh_cat  = get_shopify_cat(cat)
        tags    = ", ".join(p.get("tags",[]))
        images  = [i for i in p.get("images",[]) if is_real_img(i)]
        status  = p.get("stock","active")
        price   = norm_price(p.get("price","")) or "0.00"
        compare = norm_price(p.get("compare",""))
        sku     = p.get("sku","")
        first   = images[0] if images else ""

        rows.append({
            "Handle"                    : handle,
            "Title"                     : name,
            "Body (HTML)"               : body,
            "Vendor"                    : vendor,
            "Product Category"          : sh_cat,
            "Type"                      : cat,
            "Tags"                      : tags,
            "Published"                 : "TRUE",
            "Option1 Name"              : "Title",
            "Option1 Value"             : "Default Title",
            "Variant SKU"               : sku,
            "Variant Grams"             : "0",
            "Variant Inventory Tracker" : "shopify",
            "Variant Inventory Qty"     : "100",
            "Variant Inventory Policy"  : "deny",
            "Variant Fulfillment Service": "manual",
            "Variant Price"             : price,
            "Variant Compare At Price"  : compare,
            "Variant Requires Shipping" : "TRUE",
            "Variant Taxable"           : "TRUE",
            "Image Src"                 : first,
            "Image Position"            : "1" if first else "",
            "Image Alt Text"            : name,
            "SEO Title"                 : name[:255],
            "SEO Description"           : clean(body)[:320],
            "Status"                    : status,
        })

        # Extra image rows
        for i, img in enumerate(images[1:], 2):
            blank = {k:"" for k in SHOPIFY_PRODUCT_COLS}
            blank.update({
                "Handle": handle, "Image Src": img,
                "Image Position": str(i), "Image Alt Text": name,
            })
            rows.append(blank)

    fname = f"{OUTPUT_DIR}/factory_shopify_products_{TIMESTAMP}.csv"
    with open(fname, "w", encoding="utf-8-sig", newline="") as f:
        csv.DictWriter(f, fieldnames=SHOPIFY_PRODUCT_COLS, extrasaction="ignore").writeheader()
        csv.DictWriter(f, fieldnames=SHOPIFY_PRODUCT_COLS, extrasaction="ignore").writerows(rows)

    print(f"  ✓ Products CSV      → {fname}  ({len(products)} products / {len(rows)} rows)")
    return fname


def save_collections_csv(nav, products):
    rows     = []
    cat_seen = set()

    # From navigation
    for cat in nav.get("categories",[]):
        h = cat.get("handle","")
        if h and h not in cat_seen:
            cat_seen.add(h)
            rows.append({
                "Handle"      : h,
                "Title"       : cat["name"],
                "Body (HTML)" : f"<p>Browse our full range of {cat['name']}.</p>",
                "Published"   : "TRUE",
                "Image Src"   : "",
                "Image Alt Text": cat["name"],
                "Sort Order"  : "best-selling",
                "Updated At"  : datetime.now().strftime("%Y-%m-%d"),
            })

    # From product categories (catch any not in nav)
    for p in products:
        cat  = p.get("category","")
        h    = make_handle(cat)
        if cat and h and h not in cat_seen:
            cat_seen.add(h)
            rows.append({
                "Handle"      : h,
                "Title"       : cat,
                "Body (HTML)" : f"<p>Browse our full range of {cat}.</p>",
                "Published"   : "TRUE",
                "Image Src"   : "",
                "Image Alt Text": cat,
                "Sort Order"  : "best-selling",
                "Updated At"  : datetime.now().strftime("%Y-%m-%d"),
            })

    fname = f"{OUTPUT_DIR}/factory_shopify_collections_{TIMESTAMP}.csv"
    with open(fname, "w", encoding="utf-8-sig", newline="") as f:
        csv.DictWriter(f, fieldnames=SHOPIFY_COLLECTION_COLS, extrasaction="ignore").writeheader()
        csv.DictWriter(f, fieldnames=SHOPIFY_COLLECTION_COLS, extrasaction="ignore").writerows(rows)

    print(f"  ✓ Collections CSV   → {fname}  ({len(rows)} collections)")
    return fname


def save_navigation_json(nav):
    fname = f"{OUTPUT_DIR}/factory_navigation_{TIMESTAMP}.json"
    with open(fname, "w", encoding="utf-8") as f:
        json.dump(nav, f, ensure_ascii=False, indent=2)
    print(f"  ✓ Navigation JSON   → {fname}")
    return fname


def save_products_json(products):
    fname = f"{OUTPUT_DIR}/factory_all_products_{TIMESTAMP}.json"
    data  = []
    for p in products:
        data.append({
            "name"        : p["name"],
            "handle"      : p["handle"],
            "sku"         : p["sku"],
            "price"       : p["price"],
            "compare_price": p["compare"],
            "brand"       : p["brand"],
            "category"    : p["category"],
            "stock"       : p["stock"],
            "description" : p["description"],
            "tags"        : p["tags"],
            "images"      : p["images"],
            "url"         : p["url"],
        })
    with open(fname, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  ✓ Products JSON     → {fname}")
    return fname


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    start = time.time()

    print("\n" + "═"*62)
    print("  COMPLETE WEBSITE SCRAPER")
    print(f"  {BASE_URL}")
    print("═"*62)

    # Step 1 — Collect URLs
    url_to_cat = collect_product_urls()

    if not url_to_cat:
        print("  No URLs found!")
        return

    # Step 2 — Scrape all product pages
    products = scrape_all_products(url_to_cat)

    if not products:
        print("  No products scraped!")
        return

    # Step 3 — Navigation
    nav = scrape_navigation()

    # Step 4 — Save outputs
    print("\n" + "═"*62)
    print("  SAVING OUTPUT FILES...")
    print("═"*62)

    save_products_csv(products)
    save_collections_csv(nav, products)
    save_navigation_json(nav)
    save_products_json(products)

    # Summary
    elapsed  = round(time.time() - start)
    with_img = sum(1 for p in products if any(is_real_img(i) for i in p.get("images",[])))
    priced   = sum(1 for p in products if norm_price(p.get("price","")))
    with_desc= sum(1 for p in products if p.get("description"))
    with_sku = sum(1 for p in products if p.get("sku"))

    print(f"\n{'═'*62}")
    print(f"  ✅ DONE in {elapsed//60}m {elapsed%60}s")
    print(f"{'─'*62}")
    print(f"  Total products   : {len(products)}")
    print(f"  With images      : {with_img}")
    print(f"  With prices      : {priced}")
    print(f"  With description : {with_desc}")
    print(f"  With SKU         : {with_sku}")
    print(f"\n  SHOPIFY IMPORT:")
    print(f"  1. Products   → factory_shopify_products_*.csv")
    print(f"  2. Collections→ factory_shopify_collections_*.csv")
    print("═"*62)


if __name__ == "__main__":
    main()
