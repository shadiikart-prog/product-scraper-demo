"""
╔══════════════════════════════════════════════════════════════════════╗
║      FACTORY DIRECT FLOORING — COMPLETE SHOPIFY SCRAPER v3          ║
║      Fixes: All products + No taxonomy errors + All images           ║
╚══════════════════════════════════════════════════════════════════════╝
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

BASE_URL        = "https://www.factory-direct-flooring.co.uk"
OUTPUT_DIR      = "output"
TIMESTAMP       = datetime.now().strftime("%Y%m%d_%H%M%S")
THREADS         = 10
DELAY           = 0.3

HEADERS = {
    "User-Agent"      : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept"          : "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language" : "en-GB,en;q=0.9",
    "Accept-Encoding" : "gzip, deflate, br",
    "Connection"      : "keep-alive",
}

# ── Category URL → readable name mapping ─────────────────────────────
CATEGORY_MAP = {
    "solid-wood-flooring"      : "Solid Wood Flooring",
    "engineered-wood-flooring" : "Engineered Wood Flooring",
    "laminate-flooring"        : "Laminate Flooring",
    "lvt-flooring"             : "LVT Flooring",
    "herringbone-flooring"     : "Herringbone Flooring",
    "vinyl-flooring"           : "Vinyl Flooring",
    "carpet"                   : "Carpet",
    "underlay"                 : "Underlay",
    "accessories"              : "Accessories",
}

# ── Shopify CSV columns ───────────────────────────────────────────────
SHOPIFY_COLS = [
    "Handle", "Title", "Body (HTML)", "Vendor",
    "Product Category", "Type", "Tags", "Published",
    "Option1 Name", "Option1 Value",
    "Variant SKU", "Variant Grams",
    "Variant Inventory Tracker", "Variant Inventory Qty",
    "Variant Inventory Policy", "Variant Fulfillment Service",
    "Variant Price", "Variant Compare At Price",
    "Variant Requires Shipping", "Variant Taxable",
    "Image Src", "Image Position", "Image Alt Text",
    "SEO Title", "SEO Description", "Status",
]

# ═════════════════════════════════════════════════════════════════════
#  UTILITIES
# ═════════════════════════════════════════════════════════════════════

def fetch(url, timeout=20):
    for i in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            if r.status_code == 200: return r.text
            if r.status_code == 404: return ""
            time.sleep(1)
        except: time.sleep(2)
    return ""

def clean(text):
    if not text: return ""
    text = unescape(str(text))
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()

def handle(t):
    h = re.sub(r"[^a-z0-9]+", "-", t.lower().strip()).strip("-")
    return h[:200]

def price(p):
    try:
        v = float(str(p).replace(",","").replace("£","").strip())
        return f"{v:.2f}" if v > 0 else ""
    except: return ""

# ═════════════════════════════════════════════════════════════════════
#  STEP 1 — GET ALL PRODUCT URLs
#  Method A: Sitemap (fast, gets everything)
#  Method B: Category pages (fallback)
# ═════════════════════════════════════════════════════════════════════

def get_urls_from_sitemap():
    """Get ALL product URLs from XML sitemap — fastest method."""
    product_urls = {}
    print("  Trying sitemap...")

    sitemap_index = fetch(f"{BASE_URL}/sitemap.xml", timeout=30)
    if not sitemap_index:
        sitemap_index = fetch(f"{BASE_URL}/pub/sitemap.xml", timeout=30)

    if not sitemap_index:
        print("  Sitemap not found")
        return {}

    # Find all sub-sitemaps
    sub_sitemaps = re.findall(r'<loc>(https?://[^<]+\.xml[^<]*)</loc>', sitemap_index)

    if not sub_sitemaps:
        # Single sitemap — parse directly
        sub_sitemaps = [f"{BASE_URL}/sitemap.xml"]

    print(f"  Found {len(sub_sitemaps)} sub-sitemaps")

    for sm_url in sub_sitemaps:
        sm_html = fetch(sm_url, timeout=30)
        if not sm_html: continue

        # Extract product URLs
        all_locs = re.findall(r'<loc>(https?://[^<]+)</loc>', sm_html)
        for url in all_locs:
            url = url.strip()
            if url.startswith(BASE_URL):
                path  = url.replace(BASE_URL, "").strip("/")
                parts = path.split("/")

                # Product URLs are single-slug: domain.com/product-name
                if len(parts) == 1 and parts[0] and "." not in parts[0]:
                    slug = parts[0]
                    # Detect category from slug keywords
                    cat = detect_category(slug)
                    if slug not in ["", "home"] and not is_nav_page(slug):
                        product_urls[url] = cat

    print(f"  Sitemap: {len(product_urls)} product URLs")
    return product_urls


def get_urls_from_categories():
    """Fallback: scrape category listing pages."""
    product_urls = {}

    cat_pages = [
        ("Solid Wood Flooring",       f"{BASE_URL}/solid-wood-flooring"),
        ("Engineered Wood Flooring",  f"{BASE_URL}/engineered-wood-flooring"),
        ("Laminate Flooring",         f"{BASE_URL}/laminate-flooring"),
        ("LVT Flooring",              f"{BASE_URL}/lvt-flooring"),
        ("Herringbone Flooring",      f"{BASE_URL}/herringbone-flooring"),
        ("Vinyl Flooring",            f"{BASE_URL}/vinyl-flooring"),
        ("Carpet",                    f"{BASE_URL}/carpet"),
        ("Underlay",                  f"{BASE_URL}/underlay"),
        ("Accessories",               f"{BASE_URL}/accessories"),
    ]

    for cat_name, base_cat in cat_pages:
        page = 1
        url  = f"{base_cat}?product_list_limit=100"
        print(f"\n  Scraping category: {cat_name}")

        while True:
            html = fetch(url, timeout=25)
            if not html: break

            found = 0
            # Multiple URL patterns for Magento
            patterns = [
                r'href=["\'](' + re.escape(BASE_URL) + r'/[a-z0-9][a-z0-9\-]+\.html)["\']',
                r'href=["\'](' + re.escape(BASE_URL) + r'/[a-z0-9][a-z0-9\-]{3,})["\']',
                r'<a[^>]+href=["\'](' + re.escape(BASE_URL) + r'/[^"\'?#]+)["\'][^>]*class="[^"]*product[^"]*"',
            ]
            for pat in patterns:
                for m in re.finditer(pat, html):
                    pu = m.group(1).rstrip("/")
                    if not is_nav_page(pu) and pu not in product_urls:
                        product_urls[pu] = cat_name
                        found += 1

            print(f"    Page {page}: +{found} | Total: {len(product_urls)}")

            # Next page
            nxt = None
            rel = re.search(r'<link[^>]+rel=["\']next["\'][^>]+href=["\']([^"\']+)["\']', html)
            if rel:
                n   = rel.group(1)
                nxt = n if n.startswith("http") else BASE_URL + n
            else:
                pg = re.search(r'href=["\']([^"\']*[?&]p=' + str(page+1) + r'[^"\']*)["\']', html)
                if pg:
                    n   = pg.group(1)
                    nxt = n if n.startswith("http") else BASE_URL + n

            if not nxt or found == 0: break
            url = nxt; page += 1
            time.sleep(DELAY)

    return product_urls


def detect_category(slug):
    """Guess category from product slug keywords."""
    slug = slug.lower()
    if any(k in slug for k in ["solid","oak","walnut","ash","pine","hardwood"]): return "Solid Wood Flooring"
    if any(k in slug for k in ["engineered","eng-"]): return "Engineered Wood Flooring"
    if any(k in slug for k in ["laminate","lam-"]): return "Laminate Flooring"
    if any(k in slug for k in ["lvt","luxury-vinyl-tile"]): return "LVT Flooring"
    if any(k in slug for k in ["herringbone"]): return "Herringbone Flooring"
    if any(k in slug for k in ["vinyl","click-vinyl","cushion-vinyl"]): return "Vinyl Flooring"
    if any(k in slug for k in ["carpet","rug"]): return "Carpet"
    if any(k in slug for k in ["underlay","under-lay"]): return "Underlay"
    if any(k in slug for k in ["adhesive","trim","gripper","clip","tool"]): return "Accessories"
    return "Flooring"


def is_nav_page(url):
    """Filter out non-product pages."""
    nav = [
        "solid-wood-flooring","engineered-wood-flooring","laminate-flooring",
        "lvt-flooring","herringbone-flooring","vinyl-flooring","carpet",
        "underlay","accessories","blog","about","contact","brand",
        "advice","customer","faq","search","checkout","cart","account",
        "wishlist","compare","sitemap","privacy","terms","delivery",
        "returns","finance","trade","samples","index","home",
    ]
    path = url.replace(BASE_URL,"").strip("/")
    return path in nav or any(n == path for n in nav)


# ═════════════════════════════════════════════════════════════════════
#  STEP 2 — EXTRACT IMAGES (8 METHODS)
# ═════════════════════════════════════════════════════════════════════

def get_images(html):
    imgs, seen = [], set()

    def add(u):
        u = str(u).strip().split("?")[0]
        if (u.startswith("http") and
            "catalog/product" in u and
            u not in seen and len(u) > 50):
            seen.add(u); imgs.append(u)

    # 1. src/data-src imagely CDN
    for m in re.finditer(
        r'(?:src|data-src|data-original|data-lazy|content)=["\']'
        r'(https?://(?:imagely\.factory-direct-flooring\.co\.uk|'
        r'www\.factory-direct-flooring\.co\.uk)/media/catalog/product/[^"\'>\s]+)',
        html, re.I): add(m.group(1))

    # 2. Any catalog/product URL
    for m in re.finditer(
        r'["\']?(https?://[^"\'>\s,]+/media/catalog/product/[^"\'>\s,?]+)', html):
        add(m.group(1))

    # 3. srcset
    for m in re.finditer(r'srcset=["\']([^"\']+)', html):
        for p in m.group(1).split(","):
            u = p.strip().split(" ")[0]
            if "catalog/product" in u: add(u)

    # 4. og:image
    for m in re.finditer(
        r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)', html):
        add(m.group(1))

    # 5. data-zoom-image
    for m in re.finditer(r'data-zoom-image=["\']([^"\']+)', html): add(m.group(1))

    # 6. Gallery JSON full
    for m in re.finditer(r'"full"\s*:\s*"([^"]+catalog/product[^"]+)"', html):
        add(m.group(1).replace("\\/","/"))

    # 7. JSON-LD image
    for m in re.finditer(r'"image"\s*:\s*"(https?://[^"]+catalog/product[^"]+)"', html):
        add(m.group(1))

    # 8. General image field in JSON
    for m in re.finditer(r'"(?:img|src|photo|picture)"\s*:\s*"(https?://[^"]+/media/[^"]+)"', html):
        add(m.group(1))

    return imgs


# ═════════════════════════════════════════════════════════════════════
#  STEP 3 — SCRAPE ONE PRODUCT PAGE
# ═════════════════════════════════════════════════════════════════════

def scrape(url, category):
    html = fetch(url, timeout=20)
    if not html: return None

    p = {
        "url":"","handle":"","name":"","sku":"","price":"",
        "compare":"","desc":"","brand":"","images":[],"tags":[],
        "stock":"active","category":category,"seo_title":"",
        "seo_desc":"","weight":"",
    }
    p["url"]    = url
    p["handle"] = handle(url.split("/")[-1].replace(".html",""))

    # JSON-LD
    for block in re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL):
        try:
            data  = json.loads(block.strip())
            items = data if isinstance(data, list) else [data]
            for item in items:
                if not isinstance(item, dict): continue

                if item.get("@type") == "Product":
                    p["name"] = clean(item.get("name",""))
                    p["sku"]  = str(item.get("sku",""))
                    p["desc"] = item.get("description","")
                    b = item.get("brand",{})
                    p["brand"] = clean(b.get("name","") if isinstance(b,dict) else b)
                    w = item.get("weight","")
                    if w: p["weight"] = str(w)

                    offers = item.get("offers",{})
                    if isinstance(offers,list): offers = offers[0] if offers else {}
                    if isinstance(offers,dict):
                        pv = offers.get("price") or offers.get("lowPrice","")
                        p["price"]   = str(pv)
                        hp = offers.get("highPrice","")
                        if hp and hp != pv: p["compare"] = str(hp)
                        avail = offers.get("availability","")
                        p["stock"] = "active" if "InStock" in avail else "draft"

                    imgs = item.get("image",[])
                    if isinstance(imgs,str): imgs=[imgs]
                    if isinstance(imgs,dict): imgs=[imgs.get("url","")]
                    for img in imgs:
                        img = str(img).split("?")[0]
                        if img.startswith("http") and img not in p["images"]:
                            p["images"].append(img)

        except: pass

    # HTML fallbacks
    if not p["name"]:
        for pat in [
            r'<h1[^>]*class="[^"]*page-title[^"]*"[^>]*>\s*<span[^>]*>(.*?)</span>',
            r'<h1[^>]*>(.*?)</h1>',
        ]:
            m = re.search(pat, html, re.DOTALL|re.I)
            if m:
                n = clean(m.group(1))
                if n: p["name"] = n; break

    if not p["price"]:
        m = re.search(r'£\s*([\d,]+\.?\d*)', html)
        if m: p["price"] = m.group(1).replace(",","")

    # SEO
    m = re.search(r'<title[^>]*>(.*?)</title>', html, re.DOTALL)
    if m: p["seo_title"] = clean(m.group(1)).split("|")[0].strip()

    m = re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)', html)
    if m: p["seo_desc"] = clean(m.group(1))

    m = re.search(r'<meta[^>]+name=["\']keywords["\'][^>]+content=["\']([^"\']+)', html)
    if m: p["tags"] = [k.strip() for k in clean(m.group(1)).split(",") if k.strip()][:15]

    if not p["seo_desc"]:
        m = re.search(r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']+)', html)
        if m: p["seo_desc"] = clean(m.group(1))

    # All images
    for img in get_images(html):
        if img not in p["images"]: p["images"].append(img)

    return p if p["name"] else None


# ═════════════════════════════════════════════════════════════════════
#  STEP 4 — BUILD SHOPIFY CSV ROWS
# ═════════════════════════════════════════════════════════════════════

def to_rows(p):
    name = p.get("name","").strip()
    if not name: return []

    h         = p.get("handle") or handle(name)
    body      = p.get("desc","")
    if body and not body.strip().startswith("<"):
        body  = f"<p>{body}</p>"

    vendor    = p.get("brand","").strip() or "Factory Direct Flooring"
    cat_raw   = p.get("category","Flooring")
    tags_list = list(set(p.get("tags",[]) + [cat_raw.lower().replace(" ","-")]))
    tags      = ", ".join(tags_list[:20])
    images    = [i for i in p.get("images",[]) if i and i.startswith("http")]
    status    = p.get("stock","active")
    pv        = price(p.get("price","")) or "0.00"
    cmp       = price(p.get("compare",""))
    sku       = p.get("sku","")

    wg = "0"
    if p.get("weight"):
        try: wg = str(int(float(str(p["weight"]))*1000))
        except: wg = "0"

    seo_t = (p.get("seo_title") or name)[:255]
    seo_d = (p.get("seo_desc") or clean(body))[:320]
    fi    = images[0] if images else ""

    rows = []

    # Row 1 — full product
    rows.append({
        "Handle"                     : h,
        "Title"                      : name,
        "Body (HTML)"                : body,
        "Vendor"                     : vendor,
        "Product Category"           : "",          # ← BLANK = zero errors
        "Type"                       : cat_raw,     # ← Category shown here
        "Tags"                       : tags,
        "Published"                  : "TRUE",
        "Option1 Name"               : "Title",
        "Option1 Value"              : "Default Title",
        "Variant SKU"                : sku,
        "Variant Grams"              : wg,
        "Variant Inventory Tracker"  : "shopify",
        "Variant Inventory Qty"      : "100",
        "Variant Inventory Policy"   : "deny",
        "Variant Fulfillment Service": "manual",
        "Variant Price"              : pv,
        "Variant Compare At Price"   : cmp,
        "Variant Requires Shipping"  : "TRUE",
        "Variant Taxable"            : "TRUE",
        "Image Src"                  : fi,
        "Image Position"             : "1" if fi else "",
        "Image Alt Text"             : name,
        "SEO Title"                  : seo_t,
        "SEO Description"            : seo_d,
        "Status"                     : status,
    })

    # Extra image rows
    for i, img in enumerate(images[1:], 2):
        er = {k:"" for k in SHOPIFY_COLS}
        er.update({"Handle":h,"Image Src":img,
                   "Image Position":str(i),"Image Alt Text":name})
        rows.append(er)

    return rows


# ═════════════════════════════════════════════════════════════════════
#  MAIN
# ═════════════════════════════════════════════════════════════════════

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    t0 = time.time()

    print("\n" + "="*65)
    print("  FACTORY DIRECT FLOORING — SHOPIFY SCRAPER v3")
    print(f"  {BASE_URL}")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*65)

    # ── PHASE 1: Get all product URLs ─────────────────────────────────
    print("\n[1/3] Getting product URLs...")

    all_urls = get_urls_from_sitemap()

    if len(all_urls) < 10:
        print("  Sitemap gave few results — trying category pages...")
        cat_urls = get_urls_from_categories()
        for u, c in cat_urls.items():
            if u not in all_urls:
                all_urls[u] = c

    total = len(all_urls)
    print(f"\n  Total product URLs: {total}")

    if total == 0:
        print("  No URLs found — saving empty CSV")
        with open(f"{OUTPUT_DIR}/factory_flooring_shopify_{TIMESTAMP}.csv",
                  "w", encoding="utf-8-sig", newline="") as f:
            csv.DictWriter(f, fieldnames=SHOPIFY_COLS).writeheader()
        return

    # ── PHASE 2: Scrape product pages ─────────────────────────────────
    print(f"\n[2/3] Scraping {total} products ({THREADS} threads)...")

    products, failed, done = [], [], 0
    url_list = list(all_urls.items())

    with ThreadPoolExecutor(max_workers=THREADS) as ex:
        futures = {ex.submit(scrape, u, c): u for u, c in url_list}
        for future in as_completed(futures):
            done += 1
            try:
                r = future.result()
                if r: products.append(r)
                else: failed.append(futures[future])
            except: failed.append(futures[future])

            if done % 50 == 0 or done == total:
                el = round(time.time()-t0)
                print(f"  [{done:>4}/{total}] OK:{len(products)} Fail:{len(failed)} "
                      f"Time:{el//60}m{el%60:02d}s")

    print(f"\n  Scraped: {len(products)} | Failed: {len(failed)}")
    if not products:
        print("  No products scraped.")
        return

    # ── PHASE 3: Save CSV + JSON ──────────────────────────────────────
    print(f"\n[3/3] Saving files...")

    rows = []
    for p in products: rows.extend(to_rows(p))

    csv_file  = f"{OUTPUT_DIR}/factory_flooring_shopify_{TIMESTAMP}.csv"
    json_file = f"{OUTPUT_DIR}/factory_flooring_{TIMESTAMP}.json"

    with open(csv_file, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SHOPIFY_COLS, extrasaction="ignore")
        w.writeheader(); w.writerows(rows)

    with open(json_file, "w", encoding="utf-8") as f:
        json.dump([{
            "name"    : p["name"],     "sku"     : p["sku"],
            "price"   : p["price"],    "category": p["category"],
            "brand"   : p["brand"],    "stock"   : p["stock"],
            "images"  : p["images"],   "img_count": len(p["images"]),
            "seo_title": p["seo_title"],"seo_desc": p["seo_desc"],
            "tags"    : p["tags"],     "url"     : p["url"],
        } for p in products], f, ensure_ascii=False, indent=2)

    el = round(time.time()-t0)
    wi = len([p for p in products if p["images"]])
    wp = len([p for p in products if p["price"]])
    ai = round(sum(len(p["images"]) for p in products)/len(products),1)

    # Category breakdown
    cats = {}
    for p in products:
        cats[p["category"]] = cats.get(p["category"],0) + 1

    print(f"\n{'='*65}")
    print(f"  COMPLETE in {el//60}m {el%60:02d}s")
    print(f"  Products      : {len(products)}")
    print(f"  CSV rows      : {len(rows)}")
    print(f"  With images   : {wi} ({round(wi/len(products)*100)}%)")
    print(f"  Avg images    : {ai}")
    print(f"  With price    : {wp}")
    print(f"\n  Categories:")
    for c,n in sorted(cats.items()): print(f"    {c:35} : {n}")
    print(f"\n  CSV  : {csv_file}")
    print(f"  JSON : {json_file}")
    print(f"\n  IMPORT: Shopify → Products → Import → Upload CSV")
    print(f"  No taxonomy errors (Product Category left blank)")
    print("="*65)


if __name__ == "__main__":
    main()
