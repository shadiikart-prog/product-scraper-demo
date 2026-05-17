"""
complete_scraper.py
════════════════════════════════════════════════════════════════
Complete scraper for factory-direct-flooring.co.uk

SCRAPES:
  ✓ Full navigation + category tree
  ✓ All categories + subcategories
  ✓ All products (name, SKU, price, sale price, description)
  ✓ All product images (real CDN URLs only)
  ✓ Product codes
  ✓ Stock status

OUTPUT FILES:
  output/shopify_products.csv     → Shopify product import
  output/shopify_collections.csv  → Shopify collections/categories
  output/navigation_structure.json → Full site map
  output/all_products_raw.json    → Raw backup

IMPORT ORDER IN SHOPIFY:
  1. Upload shopify_collections.csv  (creates categories)
  2. Upload shopify_products.csv     (creates products)
"""

import requests
import json
import csv
import time
import re
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from html import unescape

# ── Config ────────────────────────────────────────────────────────────────────
BASE_URL   = "https://www.factory-direct-flooring.co.uk"
OUTPUT_DIR = "output"
TIMESTAMP  = datetime.now().strftime("%Y%m%d_%H%M%S")
THREADS    = 6      # parallel product page requests
PAGE_DELAY = 0.8    # seconds between category page requests
PROD_DELAY = 0.3    # seconds between product requests

REAL_CDN   = "imagely.factory-direct-flooring.co.uk/media/catalog/product/cache/"
PLACEHOLDER= "placeholder"

HEADERS = {
    "User-Agent"     : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept"         : "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}

# Valid Shopify taxonomy
TAXONOMY = {
    "Solid Wood"      : "Home & Garden > Decor > Flooring",
    "Engineered"      : "Home & Garden > Decor > Flooring",
    "Laminate"        : "Home & Garden > Decor > Flooring",
    "LVT"             : "Home & Garden > Decor > Flooring",
    "Vinyl"           : "Home & Garden > Decor > Flooring",
    "Herringbone"     : "Home & Garden > Decor > Flooring",
    "Carpet"          : "Home & Garden > Decor > Rugs",
    "Underlay"        : "Home & Garden > Decor > Flooring",
    "Wood"            : "Home & Garden > Decor > Flooring",
    "Flooring"        : "Home & Garden > Decor > Flooring",
}

# Shopify Products CSV columns
PRODUCT_COLS = [
    "Handle","Title","Body (HTML)","Vendor","Product Category","Type","Tags",
    "Published","Option1 Name","Option1 Value","Variant SKU","Variant Grams",
    "Variant Inventory Tracker","Variant Inventory Qty","Variant Inventory Policy",
    "Variant Fulfillment Service","Variant Price","Variant Compare At Price",
    "Variant Requires Shipping","Variant Taxable","Image Src","Image Position",
    "Image Alt Text","SEO Title","SEO Description","Status","Google Shopping / Google Product Category",
    "Collection",
]

# Shopify Collections/Smart Collections CSV columns
COLLECTION_COLS = [
    "Handle","Title","Body (HTML)","Image Src","Image Alt Text",
    "Must Match","Condition: Column","Condition: Relation","Condition: Condition",
    "Sort Order","Published",
]
# ─────────────────────────────────────────────────────────────────────────────


# ══ UTILITIES ═════════════════════════════════════════════════════════════════

def fetch(url, timeout=25):
    for i in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            if r.status_code == 200:
                return r.text
            if r.status_code in (404, 410):
                return ""
        except Exception as e:
            time.sleep(2)
    return ""

def clean(text):
    if not text: return ""
    text = unescape(str(text))
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()

def make_handle(title):
    h = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
    return h[:200]

def norm_price(p):
    try:
        return f"{float(str(p).replace(',','').replace('£','').strip()):.2f}"
    except: return ""

def is_real_img(url):
    if not url: return False
    if PLACEHOLDER in url.lower(): return False
    return REAL_CDN in str(url)

def fix_img(url):
    if not url: return ""
    url = str(url).strip().split("?")[0]
    return url if is_real_img(url) else ""

def get_taxonomy(cat_name):
    for key, val in TAXONOMY.items():
        if key.lower() in cat_name.lower():
            return val
    return "Home & Garden > Decor > Flooring"


# ══ STEP 1 — SCRAPE NAVIGATION ════════════════════════════════════════════════

def get_navigation():
    """
    Scrape the main navigation menu from homepage.
    Returns: list of {name, url, children:[{name,url}]}
    """
    print("\n[1/4] Scraping navigation structure...")
    html = fetch(BASE_URL)
    if not html:
        print("  ✗ Could not fetch homepage")
        return []

    nav_items = []

    # Pattern 1: Nav menu with nested ul/li (Hyva/Magento pattern)
    # Main nav items
    nav_block = re.search(
        r'<nav[^>]+(?:id|class)="[^"]*(?:navigation|main-nav|menu)[^"]*"[^>]*>(.*?)</nav>',
        html, re.DOTALL | re.IGNORECASE
    )
    nav_html = nav_block.group(1) if nav_block else html

    # Top-level menu items
    top_items = re.findall(
        r'<li[^>]+class="[^"]*(?:level0|nav-item|menu-item)[^"]*"[^>]*>'
        r'.*?<a[^>]+href="(' + re.escape(BASE_URL) + r'/[^"?#]+)"[^>]*>'
        r'.*?<span[^>]*>([^<]+)</span>',
        nav_html, re.DOTALL | re.IGNORECASE
    )

    if not top_items:
        # Fallback: find all nav links
        top_items = re.findall(
            r'<a[^>]+href="(' + re.escape(BASE_URL) + r'/(?!.*(?:blog|contact|about|account|cart|checkout|wishlist|compare|search|media|static|customer|index|privacy|cookie|delivery|returns|faq))[a-z][a-z0-9-]+)"[^>]*>'
            r'\s*<span[^>]*>([^<]{3,40})</span>',
            nav_html, re.DOTALL | re.IGNORECASE
        )

    seen_urls = set()
    for url, name in top_items:
        name = clean(name)
        if not name or url in seen_urls: continue
        if len(name) < 3 or len(name) > 60: continue
        seen_urls.add(url)
        nav_items.append({"name": name, "url": url, "children": []})

    # Get subcategories for each top item
    for item in nav_items:
        sub_html = fetch(item["url"])
        if not sub_html:
            continue

        # Look for subcategory links on the category page
        subs = re.findall(
            r'<a[^>]+href="(' + re.escape(BASE_URL) + r'/[a-z][a-z0-9-]+-(?:flooring|carpet|underlay|wood|laminate|vinyl|lvt|herringbone)[^"?#]*)"[^>]*>'
            r'([^<]{3,60})</a>',
            sub_html, re.IGNORECASE
        )
        seen_sub = set()
        for sub_url, sub_name in subs:
            sub_name = clean(sub_name)
            if sub_url not in seen_sub and sub_url != item["url"] and len(sub_name) > 3:
                seen_sub.add(sub_url)
                item["children"].append({"name": sub_name, "url": sub_url})

        time.sleep(0.5)

    # Filter — keep only flooring-related items
    flooring_keywords = ["floor","carpet","underlay","wood","laminate","vinyl","lvt","herringbone","rug","tile"]
    nav_items = [i for i in nav_items if any(k in i["name"].lower() or k in i["url"].lower() for k in flooring_keywords)]

    print(f"  ✓ {len(nav_items)} top categories found")
    for item in nav_items:
        print(f"    • {item['name']} ({len(item['children'])} subcategories)")

    return nav_items


# ══ STEP 2 — COLLECT ALL CATEGORY URLs ═══════════════════════════════════════

def collect_all_categories(nav_items):
    """
    Build flat list of all categories to scrape.
    Returns: [{name, url, parent}]
    """
    categories = []
    seen = set()

    for item in nav_items:
        if item["url"] not in seen:
            seen.add(item["url"])
            categories.append({
                "name"  : item["name"],
                "url"   : item["url"],
                "parent": "",
                "handle": make_handle(item["name"]),
            })
        for child in item.get("children", []):
            if child["url"] not in seen:
                seen.add(child["url"])
                categories.append({
                    "name"  : child["name"],
                    "url"   : child["url"],
                    "parent": item["name"],
                    "handle": make_handle(child["name"]),
                })

    # Add hardcoded categories as fallback
    fallback_cats = [
        {"name":"Solid Wood Flooring",      "url":f"{BASE_URL}/solid-wood-flooring",       "parent":"","handle":"solid-wood-flooring"},
        {"name":"Engineered Wood Flooring",  "url":f"{BASE_URL}/engineered-wood-flooring",  "parent":"","handle":"engineered-wood-flooring"},
        {"name":"Laminate Flooring",         "url":f"{BASE_URL}/laminate-flooring",         "parent":"","handle":"laminate-flooring"},
        {"name":"LVT Flooring",              "url":f"{BASE_URL}/lvt-flooring",              "parent":"","handle":"lvt-flooring"},
        {"name":"Herringbone Flooring",      "url":f"{BASE_URL}/herringbone-flooring",      "parent":"","handle":"herringbone-flooring"},
        {"name":"Vinyl Flooring",            "url":f"{BASE_URL}/vinyl-flooring",            "parent":"","handle":"vinyl-flooring"},
        {"name":"Carpet",                    "url":f"{BASE_URL}/carpet",                    "parent":"","handle":"carpet"},
        {"name":"Underlay",                  "url":f"{BASE_URL}/underlay",                  "parent":"","handle":"underlay"},
    ]
    for fc in fallback_cats:
        if fc["url"] not in seen:
            seen.add(fc["url"])
            categories.append(fc)

    print(f"\n  Total categories to scrape: {len(categories)}")
    return categories


# ══ STEP 3 — SCRAPE PRODUCT LISTING PAGES ════════════════════════════════════

def scrape_category_products(cat):
    """Scrape all products from one category (all pages)."""
    products = []
    page     = 1
    url      = cat["url"]

    while True:
        html = fetch(url)
        if not html: break

        # ── Extract products from card-image divs ─────────────────────────────
        page_products = extract_products_from_listing(html, cat["name"])

        new = [p for p in page_products if p.get("url")]
        products.extend(new)
        print(f"      Page {page}: +{len(new)} products")

        if not new and page > 1: break

        # Next page
        next_url = find_next_page(html, url, page)
        if not next_url: break
        url  = next_url
        page += 1
        time.sleep(PAGE_DELAY)

    return products


def extract_products_from_listing(html, category):
    """Extract all products from a category listing page HTML."""
    products = []
    seen     = set()

    # ── From card-image divs (exact Hyva structure) ───────────────────────────
    card_blocks = re.findall(
        r'<div[^>]+class="[^"]*card-image[^"]*"[^>]*>(.*?)</div>\s*</div>',
        html, re.DOTALL | re.IGNORECASE
    )

    for card in card_blocks:
        # Product URL
        url_m = re.search(
            r'href=["\'](' + re.escape(BASE_URL) + r'/[a-z0-9][a-z0-9-]+)["\']',
            card
        )
        if not url_m: continue
        prod_url = url_m.group(1)
        if prod_url in seen: continue
        seen.add(prod_url)

        # Images — only real CDN
        images = []
        for src in re.findall(r'src=["\']([^"\']+)["\']', card):
            img = fix_img(src)
            if img and img not in images:
                images.append(img)
        for src in re.findall(r'data-src=["\']([^"\']+)["\']', card):
            img = fix_img(src)
            if img and img not in images:
                images.append(img)

        # Name from alt attribute
        name = ""
        alt_m = re.search(r'alt=["\']([^"\']{5,})["\']', card)
        if alt_m:
            name = clean(alt_m.group(1))

        if not name:
            slug = prod_url.rstrip("/").split("/")[-1]
            name = slug.replace("-", " ").title()

        products.append({
            "name"       : name,
            "url"        : prod_url,
            "images"     : images,
            "category"   : category,
            "price"      : "",
            "compare"    : "",
            "sku"        : "",
            "description": "",
            "brand"      : "",
            "stock"      : "active",
            "tags"       : [],
        })

    # ── Prices + SKUs from JSON-LD ────────────────────────────────────────────
    price_map, sku_map, desc_map, brand_map, stock_map = {}, {}, {}, {}, {}

    for block in re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL
    ):
        try:
            data  = json.loads(block.strip())
            items = data if isinstance(data, list) else [data]
            for item in items:
                if not isinstance(item, dict): continue

                nodes = []
                if item.get("@type") == "ItemList":
                    nodes = [e.get("item", e) for e in item.get("itemListElement", [])]
                elif item.get("@type") == "Product":
                    nodes = [item]

                for node in nodes:
                    if not isinstance(node, dict): continue
                    nurl = str(node.get("url", ""))
                    if not nurl: continue

                    offers = node.get("offers", {})
                    if isinstance(offers, list): offers = offers[0] if offers else {}
                    if isinstance(offers, dict):
                        price_map[nurl]  = str(offers.get("price",""))
                        avail = str(offers.get("availability",""))
                        stock_map[nurl]  = "active" if "InStock" in avail or not avail else "draft"
                        hi = str(offers.get("highPrice",""))
                        price_map[nurl]  = str(offers.get("lowPrice","") or offers.get("price",""))
                        if hi and hi != price_map.get(nurl):
                            price_map[nurl + "_compare"] = hi

                    sku_map[nurl]   = str(node.get("sku",""))
                    desc_map[nurl]  = clean(node.get("description",""))
                    b = node.get("brand",{})
                    brand_map[nurl] = clean(b.get("name","") if isinstance(b,dict) else b)

                    # Also get images from JSON-LD
                    imgs = node.get("image",[])
                    if isinstance(imgs, str): imgs = [imgs]
                    if isinstance(imgs, dict): imgs = [imgs.get("url","")]
                    for img in imgs:
                        fixed = fix_img(str(img))
                        if fixed:
                            for p in products:
                                if p["url"] == nurl and fixed not in p["images"]:
                                    p["images"].append(fixed)

        except Exception:
            pass

    # Merge JSON-LD data into products
    for p in products:
        u = p["url"]
        if not p["price"]:    p["price"]       = price_map.get(u, "")
        if not p["sku"]:      p["sku"]          = sku_map.get(u, "")
        if not p["description"]: p["description"]= desc_map.get(u,"")
        if not p["brand"]:    p["brand"]        = brand_map.get(u,"Factory Direct Flooring")
        p["compare"] = price_map.get(u+"_compare","")
        p["stock"]   = stock_map.get(u,"active")

    # ── Fallback price from HTML ──────────────────────────────────────────────
    if products:
        all_prices = re.findall(r'£\s*([\d,]+\.?\d*)', html)
        if all_prices:
            for i, p in enumerate(products):
                if not p["price"] and i < len(all_prices):
                    p["price"] = all_prices[i].replace(",","")

    return products


def find_next_page(html, current_url, current_page):
    """Find next pagination URL."""
    # rel=next (most reliable)
    rel = re.search(r'<link[^>]+rel=["\']next["\'][^>]+href=["\']([^"\']+)["\']', html)
    if rel:
        nxt = rel.group(1)
        return nxt if nxt.startswith("http") else BASE_URL + nxt

    # ?p= parameter
    pg = re.search(
        r'href=["\']([^"\']*[?&]p=' + str(current_page+1) + r'[^"\']*)["\']', html
    )
    if pg:
        nxt = pg.group(1)
        return nxt if nxt.startswith("http") else BASE_URL + nxt

    return None


# ══ STEP 4 — SCRAPE INDIVIDUAL PRODUCT PAGES (for full description) ══════════

def scrape_product_details(product):
    """Scrape individual product page for full description + extra images."""
    url  = product.get("url","")
    if not url: return product

    html = fetch(url)
    if not html: return product

    # Full description from product page
    if not product.get("description"):
        # Try JSON-LD first
        for block in re.findall(
            r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            html, re.DOTALL
        ):
            try:
                data = json.loads(block.strip())
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if isinstance(item,dict) and item.get("@type")=="Product":
                        desc = item.get("description","")
                        if desc: product["description"] = clean(desc)
                        # SKU
                        if not product["sku"]:
                            product["sku"] = str(item.get("sku",""))
                        # More images
                        imgs = item.get("image",[])
                        if isinstance(imgs,str): imgs=[imgs]
                        if isinstance(imgs,dict): imgs=[imgs.get("url","")]
                        for img in imgs:
                            fixed = fix_img(str(img))
                            if fixed and fixed not in product["images"]:
                                product["images"].append(fixed)
            except: pass

        # HTML description fallback
        if not product.get("description"):
            desc_m = re.search(
                r'<div[^>]+(?:class|id)="[^"]*(?:description|product-description|overview)[^"]*"[^>]*>(.*?)</div>',
                html, re.DOTALL | re.IGNORECASE
            )
            if desc_m:
                product["description"] = clean(desc_m.group(1))[:800]

    # Extra images from product page
    extra_imgs = re.findall(
        r'(?:src|data-src)=["\']('
        r'https://imagely\.factory-direct-flooring\.co\.uk'
        r'/media/catalog/product/cache/[^"\'?\s]+)["\']',
        html
    )
    for img in extra_imgs:
        clean_img = img.split("?")[0]
        if is_real_img(clean_img) and clean_img not in product["images"]:
            product["images"].append(clean_img)

    return product


def enrich_products(products):
    """Visit individual product pages for full details — parallel."""
    total   = len(products)
    done    = 0
    enriched = []

    print(f"\n  Enriching {total} products (descriptions + extra images)...")

    with ThreadPoolExecutor(max_workers=THREADS) as ex:
        futures = {ex.submit(scrape_product_details, p): i for i,p in enumerate(products)}
        for future in as_completed(futures):
            try:
                result = future.result()
                enriched.append(result)
            except Exception:
                enriched.append(products[futures[future]])
            done += 1
            if done % 50 == 0 or done == total:
                with_img  = sum(1 for p in enriched if p.get("images"))
                with_desc = sum(1 for p in enriched if p.get("description"))
                print(f"    {done}/{total} | Images: {with_img} | Descriptions: {with_desc}")
            time.sleep(PROD_DELAY)

    return enriched


# ══ STEP 5 — SAVE OUTPUTS ════════════════════════════════════════════════════

def save_shopify_products(products):
    """Save Shopify-ready products CSV."""
    rows = []

    for p in products:
        name    = p.get("name","")
        if not name: continue

        handle  = make_handle(name)
        cat     = p.get("category","Flooring")
        body    = p.get("description","") or ""
        if body and not body.startswith("<"):
            body = f"<p>{body}</p>"

        vendor  = p.get("brand","") or "Factory Direct Flooring"
        tax_cat = get_taxonomy(cat)
        tags    = ", ".join(filter(None,[
            cat, p.get("brand",""),
            "flooring", "UK flooring",
        ]))
        images  = [i for i in p.get("images",[]) if is_real_img(i)]
        status  = p.get("stock","active")
        price   = norm_price(p.get("price","")) or "0.00"
        compare = norm_price(p.get("compare",""))
        sku     = p.get("sku","")
        first   = images[0] if images else ""

        # Main product row
        rows.append({
            "Handle"                     : handle,
            "Title"                      : name,
            "Body (HTML)"                : body,
            "Vendor"                     : vendor,
            "Product Category"           : tax_cat,
            "Type"                       : cat,
            "Tags"                       : tags,
            "Published"                  : "TRUE",
            "Option1 Name"               : "Title",
            "Option1 Value"              : "Default Title",
            "Variant SKU"                : sku,
            "Variant Grams"              : "0",
            "Variant Inventory Tracker"  : "shopify",
            "Variant Inventory Qty"      : "100" if status=="active" else "0",
            "Variant Inventory Policy"   : "deny",
            "Variant Fulfillment Service": "manual",
            "Variant Price"              : price,
            "Variant Compare At Price"   : compare,
            "Variant Requires Shipping"  : "TRUE",
            "Variant Taxable"            : "TRUE",
            "Image Src"                  : first,
            "Image Position"             : "1" if first else "",
            "Image Alt Text"             : name,
            "SEO Title"                  : name[:255],
            "SEO Description"            : clean(body)[:320],
            "Status"                     : status,
            "Google Shopping / Google Product Category": tax_cat,
            "Collection"                 : make_handle(cat),
        })

        # Extra image rows
        for i, img in enumerate(images[1:], 2):
            blank = {k:"" for k in PRODUCT_COLS}
            blank.update({
                "Handle"        : handle,
                "Image Src"     : img,
                "Image Position": str(i),
                "Image Alt Text": name,
            })
            rows.append(blank)

    csv_file = f"{OUTPUT_DIR}/shopify_products_{TIMESTAMP}.csv"
    with open(csv_file, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=PRODUCT_COLS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    print(f"  ✓ Products CSV  → {csv_file} ({len(rows)} rows)")
    return csv_file


def save_shopify_collections(categories):
    """Save Shopify collections CSV."""
    rows = []
    seen = set()

    for cat in categories:
        name   = cat["name"]
        handle = cat["handle"]
        if handle in seen: continue
        seen.add(handle)

        rows.append({
            "Handle"              : handle,
            "Title"               : name,
            "Body (HTML)"         : f"<p>Browse our range of {name}.</p>",
            "Image Src"           : "",
            "Image Alt Text"      : name,
            "Must Match"          : "all",
            "Condition: Column"   : "type",
            "Condition: Relation" : "equals",
            "Condition: Condition": name,
            "Sort Order"          : "best-selling",
            "Published"           : "TRUE",
        })

    csv_file = f"{OUTPUT_DIR}/shopify_collections_{TIMESTAMP}.csv"
    with open(csv_file, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLLECTION_COLS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    print(f"  ✓ Collections CSV → {csv_file} ({len(rows)} collections)")
    return csv_file


def save_navigation(nav_items):
    """Save navigation structure JSON."""
    nav_file = f"{OUTPUT_DIR}/navigation_structure_{TIMESTAMP}.json"
    with open(nav_file, "w", encoding="utf-8") as f:
        json.dump(nav_items, f, ensure_ascii=False, indent=2)
    print(f"  ✓ Navigation JSON → {nav_file}")
    return nav_file


def save_raw_json(products):
    """Save raw products JSON backup."""
    json_file = f"{OUTPUT_DIR}/all_products_raw_{TIMESTAMP}.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump([{
            "Name"        : p.get("name",""),
            "SKU"         : p.get("sku",""),
            "Price"       : p.get("price",""),
            "Sale Price"  : p.get("compare",""),
            "Category"    : p.get("category",""),
            "Brand"       : p.get("brand",""),
            "Stock"       : p.get("stock",""),
            "Description" : p.get("description","")[:400],
            "Images"      : p.get("images",[]),
            "URL"         : p.get("url",""),
        } for p in products], f, ensure_ascii=False, indent=2)
    print(f"  ✓ Raw JSON backup → {json_file}")
    return json_file


# ══ MAIN ══════════════════════════════════════════════════════════════════════

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    start = time.time()

    print("\n" + "═"*62)
    print("  Factory Direct Flooring — Complete Scraper")
    print("  Scraping: Navigation + Categories + Products + Images")
    print("═"*62)

    # ── 1. Navigation ─────────────────────────────────────────────────────────
    nav_items  = get_navigation()
    categories = collect_all_categories(nav_items)

    # ── 2. Scrape all category listing pages ──────────────────────────────────
    print(f"\n[2/4] Scraping {len(categories)} category pages...")
    all_products = []
    seen_urls    = set()

    for i, cat in enumerate(categories, 1):
        print(f"\n  [{i}/{len(categories)}] {cat['name']}")
        prods = scrape_category_products(cat)

        new = []
        for p in prods:
            if p["url"] not in seen_urls:
                seen_urls.add(p["url"])
                new.append(p)

        all_products.extend(new)
        print(f"  → +{len(new)} new products (total: {len(all_products)})")
        time.sleep(PAGE_DELAY)

    print(f"\n  ✓ Total unique products found: {len(all_products)}")

    # ── 3. Enrich with individual page details ─────────────────────────────────
    print(f"\n[3/4] Fetching full descriptions + extra images...")
    # Only enrich products missing description or with no images
    needs_enrich = [p for p in all_products if not p.get("description") or not p.get("images")]
    already_ok   = [p for p in all_products if p.get("description") and p.get("images")]

    print(f"  Products needing enrichment : {len(needs_enrich)}")
    print(f"  Products already complete   : {len(already_ok)}")

    if needs_enrich:
        enriched   = enrich_products(needs_enrich)
        all_products = already_ok + enriched

    # ── 4. Save all outputs ────────────────────────────────────────────────────
    print(f"\n[4/4] Saving files...")

    products_csv    = save_shopify_products(all_products)
    collections_csv = save_shopify_collections(categories)
    nav_json        = save_navigation(nav_items)
    raw_json        = save_raw_json(all_products)

    # ── Summary ───────────────────────────────────────────────────────────────
    elapsed   = round(time.time() - start)
    with_img  = sum(1 for p in all_products if any(is_real_img(i) for i in p.get("images",[])))
    with_desc = sum(1 for p in all_products if p.get("description"))
    with_sku  = sum(1 for p in all_products if p.get("sku"))
    with_price= sum(1 for p in all_products if p.get("price"))

    print(f"\n{'═'*62}")
    print(f"  ✅ COMPLETE in {elapsed//60}m {elapsed%60}s")
    print(f"{'─'*62}")
    print(f"  Total Products     : {len(all_products)}")
    print(f"  With Images        : {with_img}")
    print(f"  With Descriptions  : {with_desc}")
    print(f"  With SKU           : {with_sku}")
    print(f"  With Price         : {with_price}")
    print(f"  Categories         : {len(categories)}")
    print(f"{'─'*62}")
    print(f"\n  FILES SAVED:")
    print(f"  📦 {products_csv}")
    print(f"  📁 {collections_csv}")
    print(f"  🗺  {nav_json}")
    print(f"  💾 {raw_json}")
    print(f"\n  HOW TO IMPORT IN SHOPIFY:")
    print(f"  Step 1: Products → Import → shopify_products_*.csv")
    print(f"  Step 2: Products → Collections → Import → shopify_collections_*.csv")
    print("═"*62)


if __name__ == "__main__":
    main()
