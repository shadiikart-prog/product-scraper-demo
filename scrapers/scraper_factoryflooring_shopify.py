"""
scraper_complete_system.py
════════════════════════════════════════════════════════════════
COMPLETE scraper for factory-direct-flooring.co.uk

SCRAPES:
  ✓ Full navigation structure (all categories + subcategories)
  ✓ All products from every category
  ✓ All product images (real CDN only, no placeholders)
  ✓ Prices + sale/compare prices
  ✓ Full descriptions
  ✓ SKU / product codes
  ✓ Brand / vendor info
  ✓ Stock status

OUTPUT FILES (in output/ folder):
  1. factory_shopify_products.csv     → Import to Shopify (Products)
  2. factory_shopify_collections.csv  → Import to Shopify (Collections)
  3. factory_navigation.json          → Full category tree
  4. factory_all_products.json        → Raw product data backup
"""

import requests
import json
import csv
import time
import re
import os
from datetime import datetime
from html import unescape

# ── Config ────────────────────────────────────────────────────────────────────
BASE_URL   = "https://www.factory-direct-flooring.co.uk"
OUTPUT_DIR = "output"
TIMESTAMP  = datetime.now().strftime("%Y%m%d_%H%M%S")
DELAY      = 0.8
REAL_CDN   = "imagely.factory-direct-flooring.co.uk/media/catalog/product/cache"
PLACEHOLDER= "placeholder"

HEADERS = {
    "User-Agent"     : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept"         : "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}

# Valid Shopify taxonomy
SHOPIFY_CAT_MAP = {
    "wood"       : "Home & Garden > Decor > Flooring",
    "laminate"   : "Home & Garden > Decor > Flooring",
    "lvt"        : "Home & Garden > Decor > Flooring",
    "vinyl"      : "Home & Garden > Decor > Flooring",
    "herringbone": "Home & Garden > Decor > Flooring",
    "carpet"     : "Home & Garden > Decor > Rugs",
    "underlay"   : "Home & Garden > Decor > Flooring",
    "flooring"   : "Home & Garden > Decor > Flooring",
}

SHOPIFY_PRODUCT_COLS = [
    "Handle","Title","Body (HTML)","Vendor","Product Category","Type","Tags",
    "Published","Option1 Name","Option1 Value","Variant SKU","Variant Grams",
    "Variant Inventory Tracker","Variant Inventory Qty","Variant Inventory Policy",
    "Variant Fulfillment Service","Variant Price","Variant Compare At Price",
    "Variant Requires Shipping","Variant Taxable","Image Src","Image Position",
    "Image Alt Text","SEO Title","SEO Description","Status",
]

SHOPIFY_COLLECTION_COLS = [
    "Handle","Title","Body (HTML)","Published","Image Src","Image Alt Text",
    "Sort Order","Template Suffix","Updated At",
]
# ─────────────────────────────────────────────────────────────────────────────


# ═══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def fetch(url):
    for attempt in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 200:
                return r.text
            if r.status_code == 404:
                return ""
        except Exception as e:
            if attempt == 2:
                print(f"    ✗ Failed: {url[:60]} — {e}")
            time.sleep(2)
    return ""


def clean(text):
    if not text:
        return ""
    text = unescape(str(text))
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def make_handle(title):
    h = title.lower().strip()
    h = re.sub(r"[^a-z0-9]+", "-", h).strip("-")
    return h[:200]


def norm_price(p):
    try:
        v = str(p).replace(",","").replace("£","").strip()
        return f"{float(v):.2f}" if v else ""
    except Exception:
        return ""


def is_real_image(url):
    if not url:
        return False
    return REAL_CDN in url and PLACEHOLDER not in url.lower()


def get_real_images(html_block):
    images = []
    for src in re.findall(r'(?:src|data-src)=["\']([^"\']+)["\']', html_block, re.IGNORECASE):
        src = src.strip().split("?")[0]
        if is_real_image(src) and src not in images:
            images.append(src)
    return images


def get_shopify_category(cat_name):
    cat_lower = cat_name.lower()
    for key, val in SHOPIFY_CAT_MAP.items():
        if key in cat_lower:
            return val
    return "Home & Garden > Decor > Flooring"


# ═══════════════════════════════════════════════════════════════════════════════
#  STEP 1 — SCRAPE FULL NAVIGATION
# ═══════════════════════════════════════════════════════════════════════════════

def scrape_navigation():
    """
    Scrape the full navigation menu from the homepage.
    Returns: dict with full category tree
    """
    print("\n" + "─"*60)
    print("  STEP 1: Scraping navigation structure...")
    print("─"*60)

    html = fetch(BASE_URL)
    if not html:
        print("  ✗ Could not fetch homepage")
        return {}

    nav = {
        "site"      : BASE_URL,
        "scraped_at": datetime.now().isoformat(),
        "categories": []
    }

    # ── Find main navigation menu ─────────────────────────────────────────────
    # Hyva/Magento nav: <nav> or <ul> with class containing "nav" or "menu"
    nav_block = ""
    for pattern in [
        r'<nav[^>]+(?:id|class)="[^"]*(?:main|primary|desktop)[^"]*"[^>]*>(.*?)</nav>',
        r'<ul[^>]+(?:id|class)="[^"]*(?:nav-items|navigation|menu)[^"]*"[^>]*>(.*?)</ul>',
        r'<nav[^>]*>(.*?)</nav>',
    ]:
        m = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
        if m:
            nav_block = m.group(1)
            break

    if not nav_block:
        nav_block = html  # fallback: search whole page

    # ── Extract category links ─────────────────────────────────────────────────
    # Find all links that look like categories
    cat_links = re.findall(
        r'<a[^>]+href="(' + re.escape(BASE_URL) + r'/[a-z0-9][a-z0-9-]+/?)"[^>]*>\s*([^<]{2,60})\s*</a>',
        nav_block, re.IGNORECASE
    )

    seen_urls = set()
    top_level = []

    for url, name in cat_links:
        url  = url.rstrip("/")
        name = clean(name)

        # Skip non-category pages
        skip_words = ["blog","about","contact","advice","faq","account",
                      "login","register","wishlist","cart","checkout","search"]
        if any(w in url.lower() for w in skip_words):
            continue
        if not name or len(name) < 3 or url in seen_urls:
            continue

        seen_urls.add(url)
        slug = url.replace(BASE_URL+"/","")

        top_level.append({
            "name"        : name,
            "url"         : url,
            "slug"        : slug,
            "handle"      : make_handle(name),
            "subcategories": [],
            "product_count": 0,
        })

    # ── Find subcategories for each top-level category ────────────────────────
    print(f"  Found {len(top_level)} top-level categories")

    for cat in top_level:
        cat_html = fetch(cat["url"])
        if not cat_html:
            continue

        # Find subcategory links within this category page
        sub_links = re.findall(
            r'<a[^>]+href="(' + re.escape(BASE_URL) + r'/[a-z0-9][a-z0-9-]+/?)"[^>]*>\s*([^<]{2,60})\s*</a>',
            cat_html, re.IGNORECASE
        )

        sub_seen = set()
        for url, name in sub_links:
            url  = url.rstrip("/")
            name = clean(name)

            # Must be child of parent
            if cat["url"] not in url and cat["slug"] not in url.lower():
                continue
            if url == cat["url"] or url in sub_seen or not name:
                continue

            sub_seen.add(url)
            cat["subcategories"].append({
                "name"  : name,
                "url"   : url,
                "handle": make_handle(name),
            })

        print(f"    ✓ {cat['name']}: {len(cat['subcategories'])} subcategories")
        time.sleep(DELAY)

    nav["categories"] = top_level

    # Save navigation JSON
    nav_file = f"{OUTPUT_DIR}/factory_navigation.json"
    with open(nav_file, "w", encoding="utf-8") as f:
        json.dump(nav, f, ensure_ascii=False, indent=2)
    print(f"\n  ✓ Navigation saved → {nav_file}")

    return nav


# ═══════════════════════════════════════════════════════════════════════════════
#  STEP 2 — EXTRACT PRODUCTS FROM ONE PAGE
# ═══════════════════════════════════════════════════════════════════════════════

def extract_products_from_page(html, category_name):
    """Extract all products from a category listing page."""
    products = {}  # handle → product dict

    # ── Method 1: card-image divs (exact structure from site inspect) ─────────
    card_sections = re.split(
        r'<div[^>]+class="[^"]*card-image[^"]*"[^>]*>',
        html
    )

    for i, section in enumerate(card_sections[1:], 1):
        # Get product URL
        url_m = re.search(
            r'href=["\'](' + re.escape(BASE_URL) + r'/[a-z0-9][a-z0-9-]+)["\']',
            section
        )
        if not url_m:
            continue
        prod_url = url_m.group(1)
        handle   = make_handle(prod_url.replace(BASE_URL+"/",""))

        if handle in products:
            continue

        # Real images only
        images = get_real_images(section)

        # Name from img alt
        name = ""
        alt_m = re.search(r'<img[^>]+alt=["\']([^"\']{5,})["\']', section)
        if alt_m:
            name = clean(alt_m.group(1))

        if not name:
            slug = prod_url.rstrip("/").split("/")[-1]
            name = " ".join(w.capitalize() for w in slug.split("-"))

        products[handle] = {
            "handle"     : handle,
            "name"       : name,
            "url"        : prod_url,
            "images"     : images,
            "category"   : category_name,
            "price"      : "",
            "compare"    : "",
            "sku"        : "",
            "description": "",
            "brand"      : "Factory Direct Flooring",
            "stock"      : "active",
            "tags"       : [category_name.lower().replace(" ","-")],
        }

    # ── Method 2: JSON-LD for prices, SKUs, descriptions ─────────────────────
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

                # Process ItemList
                if item.get("@type") == "ItemList":
                    for elem in item.get("itemListElement", []):
                        _enrich_from_jsonld(elem.get("item", elem), products)
                # Single product
                elif item.get("@type") == "Product":
                    _enrich_from_jsonld(item, products)

        except Exception:
            pass

    # ── Method 3: price from HTML if still missing ────────────────────────────
    # Find price blocks near product names
    price_blocks = re.findall(
        r'class="[^"]*price[^"]*"[^>]*>\s*£\s*([\d,]+\.?\d*)',
        html
    )
    if price_blocks:
        prices_list = [p.replace(",","") for p in price_blocks]
        p_idx = 0
        for h, prod in products.items():
            if not prod["price"] and p_idx < len(prices_list):
                prod["price"] = prices_list[p_idx]
                p_idx += 1

    return list(products.values())


def _enrich_from_jsonld(item, products):
    """Add price/sku/description from JSON-LD to existing product dict."""
    if not isinstance(item, dict) or item.get("@type") != "Product":
        return

    url    = str(item.get("url", ""))
    handle = make_handle(url.replace(BASE_URL+"/","")) if url else ""

    if not handle:
        return

    # Create if not exists
    if handle not in products:
        name = clean(item.get("name",""))
        if not name:
            return
        products[handle] = {
            "handle": handle, "name": name, "url": url,
            "images": [], "category": "", "price": "",
            "compare": "", "sku": "", "description": "",
            "brand": "Factory Direct Flooring",
            "stock": "active", "tags": [],
        }

    p = products[handle]

    # Name
    if not p["name"]:
        p["name"] = clean(item.get("name",""))

    # SKU
    if not p["sku"]:
        p["sku"] = str(item.get("sku",""))

    # Description
    if not p["description"]:
        p["description"] = clean(item.get("description",""))[:600]

    # Brand
    brand = item.get("brand",{})
    if isinstance(brand, dict) and brand.get("name"):
        p["brand"] = clean(brand["name"])
    elif isinstance(brand, str) and brand:
        p["brand"] = clean(brand)

    # Price + stock
    offers = item.get("offers",{})
    if isinstance(offers, list): offers = offers[0] if offers else {}
    if isinstance(offers, dict):
        if not p["price"]:
            p["price"] = str(offers.get("price", offers.get("lowPrice","")))
        if not p["compare"]:
            high = str(offers.get("highPrice",""))
            if high and high != p["price"]:
                p["compare"] = high
        avail = str(offers.get("availability",""))
        if avail:
            p["stock"] = "active" if "InStock" in avail else "draft"

    # Images — add CDN images from JSON-LD if we don't have any
    if not p["images"]:
        imgs = item.get("image",[])
        if isinstance(imgs, str): imgs = [imgs]
        if isinstance(imgs, dict): imgs = [imgs.get("url","")]
        for img in imgs:
            img = str(img).split("?")[0]
            if is_real_image(img) and img not in p["images"]:
                p["images"].append(img)


# ═══════════════════════════════════════════════════════════════════════════════
#  STEP 3 — SCRAPE ALL CATEGORIES
# ═══════════════════════════════════════════════════════════════════════════════

def scrape_all_categories(nav):
    """Scrape every category page and collect all products."""
    all_products = []
    seen_handles = set()

    # Build list of all category URLs to scrape
    categories_to_scrape = []
    for cat in nav.get("categories", []):
        categories_to_scrape.append((cat["name"], cat["url"]))
        for sub in cat.get("subcategories", []):
            categories_to_scrape.append((sub["name"], sub["url"]))

    # If no nav found, use hardcoded fallback
    if not categories_to_scrape:
        categories_to_scrape = [
            ("Solid Wood Flooring",      f"{BASE_URL}/solid-wood-flooring"),
            ("Engineered Wood Flooring", f"{BASE_URL}/engineered-wood-flooring"),
            ("Laminate Flooring",        f"{BASE_URL}/laminate-flooring"),
            ("LVT Flooring",             f"{BASE_URL}/lvt-flooring"),
            ("Herringbone Flooring",     f"{BASE_URL}/herringbone-flooring"),
            ("Vinyl Flooring",           f"{BASE_URL}/vinyl-flooring"),
            ("Carpet",                   f"{BASE_URL}/carpet"),
            ("Underlay",                 f"{BASE_URL}/underlay"),
        ]

    print(f"\n  Total categories to scrape: {len(categories_to_scrape)}")

    for cat_name, cat_url in categories_to_scrape:
        print(f"\n  [{cat_name}]")
        page    = 1
        cur_url = cat_url

        while True:
            print(f"    Page {page}...", end=" ")
            html = fetch(cur_url)
            if not html:
                print("✗ Failed")
                break

            products = extract_products_from_page(html, cat_name)

            # Deduplicate globally
            new = []
            for p in products:
                h = p["handle"]
                if h and h not in seen_handles:
                    seen_handles.add(h)
                    new.append(p)

            all_products.extend(new)
            imgs = sum(1 for p in new if p["images"])
            print(f"+{len(new)} products ({imgs} with images) | Total: {len(all_products)}")

            if not new and page > 1:
                break

            # Next page detection
            next_url = None
            rel = re.search(
                r'<link[^>]+rel=["\']next["\'][^>]+href=["\']([^"\']+)["\']', html
            )
            if rel:
                nxt = rel.group(1)
                next_url = nxt if nxt.startswith("http") else BASE_URL + nxt

            if not next_url:
                pg = re.search(
                    r'href=["\']([^"\']*[?&]p=' + str(page+1) + r'[^"\']*)["\']', html
                )
                if pg:
                    nxt = pg.group(1)
                    next_url = nxt if nxt.startswith("http") else BASE_URL + nxt

            if not next_url or next_url == cur_url:
                break

            cur_url = next_url
            page   += 1
            time.sleep(DELAY)

        time.sleep(DELAY)

    return all_products


# ═══════════════════════════════════════════════════════════════════════════════
#  STEP 4 — SAVE ALL OUTPUT FILES
# ═══════════════════════════════════════════════════════════════════════════════

def save_shopify_products_csv(products):
    """Save official Shopify product import CSV."""
    rows = []

    for p in products:
        name    = p.get("name","")
        if not name:
            continue

        handle  = p.get("handle","") or make_handle(name)
        body    = p.get("description","") or ""
        if body and not body.startswith("<"):
            body = f"<p>{body}</p>"

        cat     = p.get("category","Flooring")
        vendor  = p.get("brand","") or "Factory Direct Flooring"
        sh_cat  = get_shopify_category(cat)
        tags    = ", ".join(p.get("tags",[cat.lower().replace(" ","-")]))
        images  = [i for i in p.get("images",[]) if is_real_image(i)]
        status  = p.get("stock","active")
        price   = norm_price(p.get("price","")) or "0.00"
        compare = norm_price(p.get("compare",""))
        sku     = p.get("sku","")
        first   = images[0] if images else ""

        # Main row
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
                "Handle"        : handle,
                "Image Src"     : img,
                "Image Position": str(i),
                "Image Alt Text": name,
            })
            rows.append(blank)

    fname = f"{OUTPUT_DIR}/factory_shopify_products_{TIMESTAMP}.csv"
    with open(fname, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SHOPIFY_PRODUCT_COLS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    print(f"  ✓ Products CSV    → {fname}  ({len(products)} products, {len(rows)} rows)")
    return fname


def save_shopify_collections_csv(nav):
    """Save Shopify collections (categories) import CSV."""
    rows = []

    for cat in nav.get("categories", []):
        handle = cat.get("handle","") or make_handle(cat["name"])
        rows.append({
            "Handle"        : handle,
            "Title"         : cat["name"],
            "Body (HTML)"   : f"<p>Browse our range of {cat['name']}.</p>",
            "Published"     : "TRUE",
            "Image Src"     : "",
            "Image Alt Text": cat["name"],
            "Sort Order"    : "best-selling",
            "Template Suffix": "",
            "Updated At"    : datetime.now().strftime("%Y-%m-%d"),
        })

        # Subcategories as collections too
        for sub in cat.get("subcategories", []):
            sub_handle = sub.get("handle","") or make_handle(sub["name"])
            rows.append({
                "Handle"        : sub_handle,
                "Title"         : sub["name"],
                "Body (HTML)"   : f"<p>Browse our range of {sub['name']}.</p>",
                "Published"     : "TRUE",
                "Image Src"     : "",
                "Image Alt Text": sub["name"],
                "Sort Order"    : "best-selling",
                "Template Suffix": "",
                "Updated At"    : datetime.now().strftime("%Y-%m-%d"),
            })

    fname = f"{OUTPUT_DIR}/factory_shopify_collections_{TIMESTAMP}.csv"
    with open(fname, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SHOPIFY_COLLECTION_COLS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    print(f"  ✓ Collections CSV → {fname}  ({len(rows)} collections)")
    return fname


def save_json_backup(products, nav):
    """Save complete raw data as JSON."""
    backup = {
        "scraped_at"   : datetime.now().isoformat(),
        "source"       : BASE_URL,
        "total_products": len(products),
        "navigation"   : nav.get("categories", []),
        "products"     : [{
            "name"       : p["name"],
            "handle"     : p["handle"],
            "sku"        : p["sku"],
            "price"      : p["price"],
            "compare"    : p["compare"],
            "brand"      : p["brand"],
            "category"   : p["category"],
            "stock"      : p["stock"],
            "description": p["description"],
            "images"     : p["images"],
            "url"        : p["url"],
            "tags"       : p["tags"],
        } for p in products],
    }

    fname = f"{OUTPUT_DIR}/factory_all_products_{TIMESTAMP}.json"
    with open(fname, "w", encoding="utf-8") as f:
        json.dump(backup, f, ensure_ascii=False, indent=2)

    print(f"  ✓ JSON backup     → {fname}")
    return fname


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    start = time.time()

    print("\n" + "═"*62)
    print("  COMPLETE WEBSITE SCRAPER")
    print(f"  {BASE_URL}")
    print("  Scraping: Navigation + Categories + Products + Images")
    print("═"*62)

    # ── Step 1: Navigation ────────────────────────────────────────────────────
    nav = scrape_navigation()

    # ── Step 2 & 3: All Products ──────────────────────────────────────────────
    print("\n" + "─"*62)
    print("  STEP 2 & 3: Scraping all category pages...")
    print("─"*62)
    products = scrape_all_categories(nav)

    if not products:
        print("\n  ⚠ No products found")
        return

    # ── Step 4: Save outputs ──────────────────────────────────────────────────
    print("\n" + "─"*62)
    print("  STEP 4: Saving output files...")
    print("─"*62)

    products_csv    = save_shopify_products_csv(products)
    collections_csv = save_shopify_collections_csv(nav)
    json_file       = save_json_backup(products, nav)

    # ── Summary ───────────────────────────────────────────────────────────────
    elapsed  = round(time.time() - start)
    with_img = sum(1 for p in products if any(is_real_image(i) for i in p.get("images",[])))
    priced   = sum(1 for p in products if norm_price(p.get("price","")))
    cats     = len(set(p["category"] for p in products))

    print(f"\n{'═'*62}")
    print(f"  ✅ COMPLETE SCRAPE DONE in {elapsed//60}m {elapsed%60}s")
    print(f"{'─'*62}")
    print(f"  Total products    : {len(products)}")
    print(f"  With real images  : {with_img}")
    print(f"  With prices       : {priced}")
    print(f"  Categories found  : {cats}")
    print(f"\n  OUTPUT FILES:")
    print(f"  📦 {products_csv}")
    print(f"  📁 {collections_csv}")
    print(f"  💾 {json_file}")
    print(f"\n  HOW TO IMPORT TO SHOPIFY:")
    print(f"  1. Products → Import → factory_shopify_products_*.csv")
    print(f"  2. Products → Collections → Import → factory_shopify_collections_*.csv")
    print("═"*62)


if __name__ == "__main__":
    main()
