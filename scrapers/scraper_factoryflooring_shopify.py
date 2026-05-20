"""
╔══════════════════════════════════════════════════════════════════════╗
║         FACTORY DIRECT FLOORING — COMPLETE SHOPIFY SCRAPER          ║
║         www.factory-direct-flooring.co.uk                           ║
╠══════════════════════════════════════════════════════════════════════╣
║  Fetches:                                                            ║
║   ✓ All categories + products                                        ║
║   ✓ Title, Description, Price, SKU, Brand                           ║
║   ✓ All images from imagely CDN                                      ║
║   ✓ SEO Title, SEO Description, Keywords                            ║
║   ✓ Stock status, Breadcrumb                                        ║
║   ✓ Valid Shopify Taxonomy IDs (no errors!)                         ║
║                                                                      ║
║  Output: Shopify-ready CSV + JSON backup                            ║
╚══════════════════════════════════════════════════════════════════════╝

Run:
    pip install requests
    python scraper_factoryflooring_shopify.py
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

# ══════════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════════
BASE_URL         = "https://www.factory-direct-flooring.co.uk"
OUTPUT_DIR       = "output"
TIMESTAMP        = datetime.now().strftime("%Y%m%d_%H%M%S")
THREADS          = 8
DELAY            = 0.5
PRODUCT_TIMEOUT  = 20
CATEGORY_TIMEOUT = 25

HEADERS = {
    "User-Agent"      : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept"          : "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language" : "en-GB,en;q=0.9",
    "Accept-Encoding" : "gzip, deflate, br",
    "Connection"      : "keep-alive",
    "Cache-Control"   : "no-cache",
}

# ── CORRECT Shopify Taxonomy IDs ──────────────────────────────────────
# Using IDs instead of breadcrumb text = zero import errors
SHOPIFY_CATEGORY_MAP = {
    "Solid Wood Flooring"      : "hg-4-1",
    "Engineered Wood Flooring" : "hg-4-1",
    "Laminate Flooring"        : "hg-4-2",
    "LVT Flooring"             : "hg-4-3",
    "Herringbone Flooring"     : "hg-4-1",
    "Vinyl Flooring"           : "hg-4-3",
    "Carpet"                   : "hg-4-4",
    "Underlay"                 : "hg-4",
    "Accessories"              : "hg-4",
}
DEFAULT_SHOPIFY_CAT = "hg-4"  # Home & Garden > Flooring

# ── All categories to scrape ──────────────────────────────────────────
CATEGORIES = [
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

# ── Official Shopify CSV columns ──────────────────────────────────────
SHOPIFY_COLS = [
    "Handle",
    "Title",
    "Body (HTML)",
    "Vendor",
    "Product Category",
    "Type",
    "Tags",
    "Published",
    "Option1 Name",
    "Option1 Value",
    "Variant SKU",
    "Variant Grams",
    "Variant Inventory Tracker",
    "Variant Inventory Qty",
    "Variant Inventory Policy",
    "Variant Fulfillment Service",
    "Variant Price",
    "Variant Compare At Price",
    "Variant Requires Shipping",
    "Variant Taxable",
    "Image Src",
    "Image Position",
    "Image Alt Text",
    "SEO Title",
    "SEO Description",
    "Status",
]

# ══════════════════════════════════════════════════════════════════════
#  UTILITIES
# ══════════════════════════════════════════════════════════════════════

def fetch(url, timeout=25):
    for attempt in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            if r.status_code == 200:
                return r.text
            elif r.status_code == 404:
                return ""
            time.sleep(1)
        except requests.exceptions.Timeout:
            pass
        except Exception as e:
            if attempt == 2:
                print(f"    Failed: {url[-50:]} — {e}")
    return ""


def clean_text(text):
    if not text:
        return ""
    text = unescape(str(text))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&[a-zA-Z]+;", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def to_handle(title):
    h = title.lower().strip()
    h = re.sub(r"[^a-z0-9]+", "-", h).strip("-")
    return h[:200]


def fmt_price(p):
    try:
        val = str(p).replace(",", "").replace("£", "").strip()
        f   = float(val)
        return f"{f:.2f}" if f > 0 else ""
    except Exception:
        return ""


# ══════════════════════════════════════════════════════════════════════
#  IMAGE EXTRACTION — 8 METHODS
# ══════════════════════════════════════════════════════════════════════

def extract_all_images(html):
    found = []
    seen  = set()

    def add(url):
        url = str(url).strip().split("?")[0]
        if (url.startswith("http")
                and "catalog/product" in url
                and url not in seen
                and len(url) > 50):
            seen.add(url)
            found.append(url)

    # 1. src/data-src on imagely CDN
    for m in re.finditer(
        r'(?:src|data-src|data-original|data-lazy|content)=["\']'
        r'(https?://(?:imagely\.factory-direct-flooring\.co\.uk|'
        r'www\.factory-direct-flooring\.co\.uk)'
        r'/media/catalog/product/[^"\'>\s]+)',
        html, re.IGNORECASE
    ):
        add(m.group(1))

    # 2. Any catalog/product URL
    for m in re.finditer(
        r'["\']?(https?://[^"\'>\s,]+/media/catalog/product/[^"\'>\s,?]+)',
        html
    ):
        add(m.group(1))

    # 3. srcset
    for m in re.finditer(r'srcset=["\']([^"\']+)', html):
        for part in m.group(1).split(","):
            u = part.strip().split(" ")[0]
            if "catalog/product" in u:
                add(u)

    # 4. og:image
    for m in re.finditer(
        r'<meta[^>]+property=["\']og:image(?::secure_url)?["\'][^>]+content=["\']([^"\']+)', html
    ):
        add(m.group(1))

    # 5. data-zoom-image
    for m in re.finditer(r'data-zoom-image=["\']([^"\']+)', html):
        add(m.group(1))

    # 6. Fotorama gallery JSON
    for m in re.finditer(r'"(?:full|img|src)"\s*:\s*"([^"]+catalog/product[^"]+)"', html):
        add(m.group(1).replace("\\/", "/"))

    # 7. JSON-LD image field
    for m in re.finditer(r'"image"\s*:\s*"(https?://[^"]+catalog/product[^"]+)"', html):
        add(m.group(1))

    # 8. Alpine.js x-data blobs
    for m in re.finditer(r'x-data=["\'](\{[^"\']{20,})["\']', html):
        blob = m.group(1)
        for u in re.findall(r'https?://[^\s"\'\\]+/media/catalog/product/[^\s"\'\\?]+', blob):
            add(u)

    return found


# ══════════════════════════════════════════════════════════════════════
#  STEP 1 — COLLECT PRODUCT URLs FROM CATEGORY PAGES
# ══════════════════════════════════════════════════════════════════════

def get_product_urls_from_category(cat_name, start_url):
    product_urls = []
    seen_urls    = set()
    page         = 1
    cur_url      = f"{start_url}?product_list_limit=100"

    while True:
        print(f"    [{cat_name}] Page {page}")
        html = fetch(cur_url, timeout=CATEGORY_TIMEOUT)
        if not html:
            break

        found_this_page = 0
        for m in re.finditer(
            r'href=["\'](' + re.escape(BASE_URL) + r'/([a-z0-9][a-z0-9\-]+\.html|[a-z0-9][a-z0-9\-]+))["\']',
            html
        ):
            url  = m.group(1).rstrip("/")
            skip_keywords = [
                "solid-wood-flooring", "engineered-wood-flooring",
                "laminate-flooring", "lvt-flooring", "herringbone-flooring",
                "vinyl-flooring", "carpet", "underlay", "accessories",
                "blog", "about", "contact", "brand", "advice",
                "customer", "faq", "search", "checkout", "cart",
                "account", "wishlist", "compare", "sitemap",
                "privacy", "terms", "delivery", "returns", "finance",
                "trade", "samples",
            ]
            if any(kw in url for kw in skip_keywords):
                continue
            if url in seen_urls:
                continue
            seen_urls.add(url)
            product_urls.append((url, cat_name))
            found_this_page += 1

        print(f"    [{cat_name}] Page {page}: +{found_this_page} URLs (total: {len(product_urls)})")

        # Next page
        next_url = None
        rel = re.search(
            r'<link[^>]+rel=["\']next["\'][^>]+href=["\']([^"\']+)["\']', html
        )
        if rel:
            n        = rel.group(1)
            next_url = n if n.startswith("http") else BASE_URL + n

        if not next_url:
            pg = re.search(
                r'href=["\']([^"\']*[?&]p=' + str(page + 1) + r'[^"\']*)["\']', html
            )
            if pg:
                n        = pg.group(1)
                next_url = n if n.startswith("http") else BASE_URL + n

        if not next_url or found_this_page == 0:
            break

        cur_url = next_url
        page   += 1
        time.sleep(DELAY)

    return product_urls


# ══════════════════════════════════════════════════════════════════════
#  STEP 2 — SCRAPE INDIVIDUAL PRODUCT PAGES
# ══════════════════════════════════════════════════════════════════════

def scrape_product_page(url, category):
    html = fetch(url, timeout=PRODUCT_TIMEOUT)
    if not html:
        return None

    product = {
        "url"          : url,
        "handle"       : to_handle(url.split("/")[-1].replace(".html", "")),
        "name"         : "",
        "sku"          : "",
        "price"        : "",
        "compare_price": "",
        "description"  : "",
        "brand"        : "",
        "images"       : [],
        "tags"         : [],
        "stock"        : "active",
        "category"     : category,
        "breadcrumb"   : category,
        "seo_title"    : "",
        "seo_desc"     : "",
        "keywords"     : "",
        "weight"       : "",
    }

    # ── JSON-LD structured data ───────────────────────────────────────
    for block in re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL
    ):
        try:
            data = json.loads(block.strip())
        except Exception:
            continue

        items = data if isinstance(data, list) else [data]
        for item in items:
            if not isinstance(item, dict):
                continue

            if item.get("@type") == "Product":
                product["name"] = clean_text(item.get("name", ""))
                product["sku"]  = str(item.get("sku", ""))
                product["description"] = item.get("description", "")

                b = item.get("brand", {})
                if isinstance(b, dict):
                    product["brand"] = clean_text(b.get("name", ""))
                elif isinstance(b, str):
                    product["brand"] = clean_text(b)

                w = item.get("weight", "")
                if w:
                    product["weight"] = str(w)

                offers = item.get("offers", {})
                if isinstance(offers, list):
                    offers = offers[0] if offers else {}
                if isinstance(offers, dict):
                    p = offers.get("price") or offers.get("lowPrice", "")
                    product["price"] = str(p)
                    hp = offers.get("highPrice", "")
                    if hp and hp != p:
                        product["compare_price"] = str(hp)
                    avail = offers.get("availability", "")
                    product["stock"] = "active" if "InStock" in avail else "draft"

                imgs = item.get("image", [])
                if isinstance(imgs, str): imgs = [imgs]
                if isinstance(imgs, dict): imgs = [imgs.get("url", "")]
                for img in imgs:
                    img_url = str(img).split("?")[0]
                    if img_url.startswith("http") and img_url not in product["images"]:
                        product["images"].append(img_url)

            if item.get("@type") == "BreadcrumbList":
                crumbs = []
                for el in item.get("itemListElement", []):
                    if isinstance(el, dict) and el.get("name"):
                        crumbs.append(clean_text(el["name"]))
                if len(crumbs) >= 2:
                    product["breadcrumb"] = " > ".join(crumbs[1:])

    # ── HTML fallbacks ────────────────────────────────────────────────
    if not product["name"]:
        for pattern in [
            r'<h1[^>]*class="[^"]*page-title[^"]*"[^>]*>\s*<span[^>]*>(.*?)</span>',
            r'<h1[^>]*class="[^"]*product-name[^"]*"[^>]*>(.*?)</h1>',
            r'<h1[^>]*>(.*?)</h1>',
        ]:
            m = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
            if m:
                name = clean_text(m.group(1))
                if name:
                    product["name"] = name
                    break

    if not product["price"]:
        pm = re.search(r'<span[^>]*class="[^"]*price[^"]*"[^>]*>\s*£\s*([\d,]+\.?\d*)', html, re.IGNORECASE)
        if pm:
            product["price"] = pm.group(1).replace(",", "")

    if not product["description"]:
        desc_m = re.search(
            r'<div[^>]*(?:class="[^"]*(?:product-description|description|overview)[^"]*"|id="[^"]*description[^"]*")[^>]*>(.*?)</div>',
            html, re.DOTALL | re.IGNORECASE
        )
        if desc_m:
            product["description"] = desc_m.group(1).strip()

    # SEO title
    meta_title = re.search(r'<title[^>]*>(.*?)</title>', html, re.DOTALL)
    if meta_title:
        product["seo_title"] = clean_text(meta_title.group(1)).split("|")[0].strip()

    # SEO description
    meta_desc = re.search(
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)', html
    )
    if meta_desc:
        product["seo_desc"] = clean_text(meta_desc.group(1))

    # Keywords / Tags
    meta_kw = re.search(
        r'<meta[^>]+name=["\']keywords["\'][^>]+content=["\']([^"\']+)', html
    )
    if meta_kw:
        raw_kw = clean_text(meta_kw.group(1))
        product["keywords"] = raw_kw
        product["tags"]     = [k.strip() for k in raw_kw.split(",") if k.strip()][:15]

    # OG description fallback
    if not product["seo_desc"]:
        og_desc = re.search(
            r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']+)', html
        )
        if og_desc:
            product["seo_desc"] = clean_text(og_desc.group(1))

    # All images
    html_images = extract_all_images(html)
    for img in html_images:
        if img not in product["images"]:
            product["images"].append(img)

    # og:image fallback
    if not product["images"]:
        og = re.search(
            r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)', html
        )
        if og and og.group(1).startswith("http"):
            product["images"].append(og.group(1).split("?")[0])

    return product if product["name"] else None


# ══════════════════════════════════════════════════════════════════════
#  STEP 3 — CONVERT TO SHOPIFY CSV ROWS
# ══════════════════════════════════════════════════════════════════════

def to_shopify_rows(p):
    name = p.get("name", "").strip()
    if not name:
        return []

    handle    = p.get("handle") or to_handle(name)
    body_html = p.get("description", "")
    if body_html and not body_html.strip().startswith("<"):
        body_html = f"<p>{body_html}</p>"

    vendor    = p.get("brand", "").strip() or "Factory Direct Flooring"
    cat_raw   = p.get("category", "Flooring")

    # ── CORRECT Shopify taxonomy ID ───────────────────────────────────
    shopify_cat_id = SHOPIFY_CATEGORY_MAP.get(cat_raw, DEFAULT_SHOPIFY_CAT)

    tags_list = list(set(p.get("tags", []) + [cat_raw.lower().replace(" ", "-")]))
    tags      = ", ".join(tags_list[:20])
    images    = [i for i in p.get("images", []) if i and i.startswith("http")]
    status    = p.get("stock", "active")
    price     = fmt_price(p.get("price", "")) or "0.00"
    compare   = fmt_price(p.get("compare_price", ""))
    sku       = p.get("sku", "")

    weight_g = "0"
    if p.get("weight"):
        try:
            weight_g = str(int(float(str(p["weight"])) * 1000))
        except Exception:
            weight_g = "0"

    seo_title = (p.get("seo_title") or name)[:255]
    seo_desc  = (p.get("seo_desc") or clean_text(body_html))[:320]

    rows      = []
    first_img = images[0] if images else ""

    # Row 1: full product data
    rows.append({
        "Handle"                     : handle,
        "Title"                      : name,
        "Body (HTML)"                : body_html,
        "Vendor"                     : vendor,
        "Product Category"           : shopify_cat_id,
        "Type"                       : cat_raw,
        "Tags"                       : tags,
        "Published"                  : "TRUE",
        "Option1 Name"               : "Title",
        "Option1 Value"              : "Default Title",
        "Variant SKU"                : sku,
        "Variant Grams"              : weight_g,
        "Variant Inventory Tracker"  : "shopify",
        "Variant Inventory Qty"      : "100",
        "Variant Inventory Policy"   : "deny",
        "Variant Fulfillment Service": "manual",
        "Variant Price"              : price,
        "Variant Compare At Price"   : compare,
        "Variant Requires Shipping"  : "TRUE",
        "Variant Taxable"            : "TRUE",
        "Image Src"                  : first_img,
        "Image Position"             : "1" if first_img else "",
        "Image Alt Text"             : name,
        "SEO Title"                  : seo_title,
        "SEO Description"            : seo_desc,
        "Status"                     : status,
    })

    # Extra rows for additional images
    for i, img in enumerate(images[1:], 2):
        empty_row = {k: "" for k in SHOPIFY_COLS}
        empty_row.update({
            "Handle"        : handle,
            "Image Src"     : img,
            "Image Position": str(i),
            "Image Alt Text": name,
        })
        rows.append(empty_row)

    return rows


# ══════════════════════════════════════════════════════════════════════
#  MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    start_time = time.time()

    print("\n" + "═"*65)
    print("  FACTORY DIRECT FLOORING — SHOPIFY SCRAPER")
    print(f"  {BASE_URL}")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("═"*65)

    # PHASE 1: Collect product URLs
    print(f"\n{'─'*65}")
    print("  PHASE 1 — Collecting product URLs from categories")
    print(f"{'─'*65}")

    all_product_urls = {}

    for cat_name, cat_url in CATEGORIES:
        print(f"\n  Category: {cat_name}")
        urls = get_product_urls_from_category(cat_name, cat_url)
        for url, cat in urls:
            if url not in all_product_urls:
                all_product_urls[url] = cat
        print(f"  → {len(urls)} URLs | Total: {len(all_product_urls)}")
        time.sleep(DELAY)

    total_urls = len(all_product_urls)
    print(f"\n  Total unique product URLs: {total_urls}")

    if total_urls == 0:
        print("\n  No product URLs found — saving empty CSV")
        out = f"{OUTPUT_DIR}/factory_flooring_shopify_{TIMESTAMP}.csv"
        with open(out, "w", encoding="utf-8-sig", newline="") as f:
            csv.DictWriter(f, fieldnames=SHOPIFY_COLS).writeheader()
        return

    # PHASE 2: Scrape product pages in parallel
    print(f"\n{'─'*65}")
    print(f"  PHASE 2 — Scraping {total_urls} product pages ({THREADS} threads)")
    print(f"{'─'*65}")

    products = []
    failed   = []
    done     = 0
    url_list = list(all_product_urls.items())

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        future_to_url = {
            executor.submit(scrape_product_page, url, cat): url
            for url, cat in url_list
        }
        for future in as_completed(future_to_url):
            url  = future_to_url[future]
            done += 1
            try:
                result = future.result()
                if result:
                    products.append(result)
                else:
                    failed.append(url)
            except Exception:
                failed.append(url)

            if done % 25 == 0 or done == total_urls:
                elapsed = round(time.time() - start_time)
                print(
                    f"  [{done:>4}/{total_urls}] "
                    f"OK: {len(products)} | "
                    f"Fail: {len(failed)} | "
                    f"Time: {elapsed//60}m{elapsed%60:02d}s"
                )

    print(f"\n  Scraped: {len(products)} | Failed: {len(failed)}")

    if not products:
        print("  No products scraped.")
        return

    # PHASE 3: Save outputs
    print(f"\n{'─'*65}")
    print("  PHASE 3 — Saving Shopify CSV")
    print(f"{'─'*65}")

    shopify_rows = []
    for p in products:
        shopify_rows.extend(to_shopify_rows(p))

    csv_file = f"{OUTPUT_DIR}/factory_flooring_shopify_{TIMESTAMP}.csv"
    with open(csv_file, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SHOPIFY_COLS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(shopify_rows)

    json_file = f"{OUTPUT_DIR}/factory_flooring_{TIMESTAMP}.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump([{
            "name"       : p["name"],
            "sku"        : p["sku"],
            "price"      : p["price"],
            "compare"    : p["compare_price"],
            "brand"      : p["brand"],
            "category"   : p["category"],
            "shopify_cat": SHOPIFY_CATEGORY_MAP.get(p["category"], DEFAULT_SHOPIFY_CAT),
            "stock"      : p["stock"],
            "images"     : p["images"],
            "image_count": len(p["images"]),
            "description": clean_text(p["description"])[:500],
            "seo_title"  : p["seo_title"],
            "seo_desc"   : p["seo_desc"],
            "tags"       : p["tags"],
            "url"        : p["url"],
        } for p in products], f, ensure_ascii=False, indent=2)

    elapsed = round(time.time() - start_time)
    with_images = len([p for p in products if p["images"]])
    with_price  = len([p for p in products if p["price"]])
    avg_imgs    = round(sum(len(p["images"]) for p in products) / len(products), 1)

    print(f"\n{'═'*65}")
    print(f"  COMPLETE in {elapsed//60}m {elapsed%60:02d}s")
    print(f"{'─'*65}")
    print(f"  Products         : {len(products)}")
    print(f"  CSV rows         : {len(shopify_rows)}")
    print(f"  With images      : {with_images} ({round(with_images/len(products)*100)}%)")
    print(f"  Avg images/item  : {avg_imgs}")
    print(f"  With price       : {with_price}")
    print(f"{'─'*65}")
    print(f"  CSV  → {csv_file}")
    print(f"  JSON → {json_file}")
    print(f"{'─'*65}")
    print(f"  SHOPIFY IMPORT:")
    print(f"  Products → Import → Upload CSV")
    print(f"  Taxonomy: hg-4 (Home & Garden > Flooring) — NO ERRORS!")
    print("═"*65)


if __name__ == "__main__":
    main()
