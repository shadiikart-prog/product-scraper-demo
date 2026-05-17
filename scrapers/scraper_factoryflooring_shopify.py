"""
scraper_factoryflooring_shopify.py
════════════════════════════════════════════════════════════════
FAST scraper for factory-direct-flooring.co.uk
Strategy: Scrape CATEGORY LISTING pages only (not individual pages)
         → 1 request = 20-30 products = 10x faster!

Output: Shopify-ready CSV with images
"""

import requests
import json
import csv
import time
import re
import os
from datetime import datetime
from html import unescape

OUTPUT_DIR = "output"
BASE_URL   = "https://www.factory-direct-flooring.co.uk"
TIMESTAMP  = datetime.now().strftime("%Y%m%d_%H%M%S")
DELAY      = 0.8

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}

# All categories — show max products per page using ?product_list_limit=100
CATEGORIES = [
    "Solid Wood Flooring",
    "Engineered Wood Flooring",
    "Laminate Flooring",
    "LVT Flooring",
    "Herringbone Flooring",
    "Vinyl Flooring",
    "Carpet",
    "Underlay",
]

CATEGORY_URLS = [
    ("Solid Wood Flooring",      f"{BASE_URL}/solid-wood-flooring?product_list_limit=100"),
    ("Engineered Wood Flooring", f"{BASE_URL}/engineered-wood-flooring?product_list_limit=100"),
    ("Laminate Flooring",        f"{BASE_URL}/laminate-flooring?product_list_limit=100"),
    ("LVT Flooring",             f"{BASE_URL}/lvt-flooring?product_list_limit=100"),
    ("Herringbone Flooring",     f"{BASE_URL}/herringbone-flooring?product_list_limit=100"),
    ("Vinyl Flooring",           f"{BASE_URL}/vinyl-flooring?product_list_limit=100"),
    ("Carpet",                   f"{BASE_URL}/carpet?product_list_limit=100"),
    ("Underlay",                 f"{BASE_URL}/underlay?product_list_limit=100"),
]

SHOPIFY_COLS = [
    "Handle","Title","Body (HTML)","Vendor","Product Category",
    "Type","Tags","Published",
    "Option1 Name","Option1 Value",
    "Variant SKU","Variant Grams","Variant Inventory Tracker",
    "Variant Inventory Qty","Variant Inventory Policy",
    "Variant Fulfillment Service","Variant Price",
    "Variant Compare At Price","Variant Requires Shipping",
    "Variant Taxable","Image Src","Image Position","Image Alt Text",
    "SEO Title","SEO Description","Status",
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=25)
        if r.status_code == 200:
            return r.text
        return ""
    except Exception as e:
        print(f"    Error: {e}")
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
        return f"{float(str(p).replace(',','').replace('£','').strip()):.2f}"
    except Exception:
        return ""

# ── Parse products from category listing page ─────────────────────────────────

def parse_category_page(html, category):
    """
    Extract all products from a category listing page.
    No need to visit individual product pages!
    """
    products = []

    # ── Method 1: JSON-LD ItemList on category page ──────────────────────────
    json_ld_blocks = re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL
    )
    for block in json_ld_blocks:
        try:
            data = json.loads(block.strip())
            items = data if isinstance(data, list) else [data]
            for item in items:
                if not isinstance(item, dict):
                    continue
                # ItemList containing products
                if item.get("@type") == "ItemList":
                    for elem in item.get("itemListElement", []):
                        p = elem.get("item", elem)
                        parsed = extract_product_jsonld(p, category)
                        if parsed:
                            products.append(parsed)
                # Single product
                elif item.get("@type") == "Product":
                    parsed = extract_product_jsonld(item, category)
                    if parsed:
                        products.append(parsed)
        except Exception:
            pass

    # ── Method 2: HTML product cards (Hyva/Magento pattern) ──────────────────
    if not products:
        products = parse_html_product_cards(html, category)

    # ── Method 3: Embedded JSON in page script ────────────────────────────────
    if not products:
        products = parse_embedded_json(html, category)

    return products


def extract_product_jsonld(item, category):
    """Extract product from JSON-LD Product object."""
    if not isinstance(item, dict):
        return None
    if item.get("@type") not in ("Product", "product"):
        return None

    name = clean(item.get("name", ""))
    if not name or len(name) < 3:
        return None

    # Price
    price = ""
    offers = item.get("offers", {})
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    if isinstance(offers, dict):
        price = str(offers.get("price", offers.get("lowPrice", "")))
        avail = offers.get("availability", "")
        stock = "active" if "InStock" in avail else "draft"
    else:
        stock = "active"

    # Images
    images = []
    imgs = item.get("image", [])
    if isinstance(imgs, str): imgs = [imgs]
    if isinstance(imgs, dict): imgs = [imgs.get("url", "")]
    for img in imgs:
        if img and str(img).startswith("http"):
            images.append(str(img))

    # Brand
    brand = ""
    b = item.get("brand", {})
    if isinstance(b, dict): brand = clean(b.get("name", ""))
    elif isinstance(b, str): brand = clean(b)

    return {
        "name"       : name,
        "sku"        : str(item.get("sku", "")),
        "price"      : price,
        "compare"    : "",
        "brand"      : brand,
        "category"   : category,
        "images"     : images,
        "description": item.get("description", ""),
        "tags"       : [],
        "stock"      : stock,
        "url"        : item.get("url", ""),
    }


def parse_html_product_cards(html, category):
    """Parse product cards directly from category page HTML."""
    products = []
    seen     = set()

    # Hyva/Magento product card patterns
    # Products are in <li> or <div> with class containing 'product-item'
    card_pattern = re.compile(
        r'<(?:li|div|article)[^>]+class="[^"]*product[^"]*item[^"]*"[^>]*>'
        r'(.*?)'
        r'</(?:li|div|article)>',
        re.DOTALL | re.IGNORECASE
    )

    for card_match in card_pattern.finditer(html):
        card = card_match.group(1)

        # Name
        name_m = re.search(
            r'class="[^"]*product[^"]*name[^"]*"[^>]*>.*?<a[^>]*>(.*?)</a>',
            card, re.DOTALL | re.IGNORECASE
        )
        if not name_m:
            name_m = re.search(r'<a[^>]+title="([^"]{5,})"', card)

        name = clean(name_m.group(1)) if name_m else ""
        if not name or name in seen:
            continue
        seen.add(name)

        # Price
        price_m = re.search(r'£\s*([\d,]+\.?\d*)', card)
        price   = price_m.group(1).replace(",", "") if price_m else ""

        # Image — CDN
        img_m = re.search(
            r'(?:src|data-src)=["\']'
            r'(https://imagely\.factory-direct-flooring\.co\.uk/media/catalog/product[^"\'?\s]+)',
            card
        )
        image = img_m.group(1).split("?")[0] if img_m else ""

        # URL
        url_m = re.search(
            r'href="(https://www\.factory-direct-flooring\.co\.uk/[a-z0-9][^"?#]+)"',
            card
        )
        prod_url = url_m.group(1) if url_m else ""

        products.append({
            "name"       : name,
            "sku"        : "",
            "price"      : price,
            "compare"    : "",
            "brand"      : "Factory Direct Flooring",
            "category"   : category,
            "images"     : [image] if image else [],
            "description": "",
            "tags"       : [category.lower().replace(" ", "-")],
            "stock"      : "active",
            "url"        : prod_url,
        })

    return products


def parse_embedded_json(html, category):
    """Try to find product data in embedded JS objects."""
    products = []
    seen     = set()

    # Some Magento themes embed product data as JS objects
    # Pattern: {"id":123,"name":"Product Name","price":29.99,"image":"..."}
    pattern = re.compile(
        r'\{[^{}]*"name"\s*:\s*"([^"]{5,})"[^{}]*"price"\s*:\s*([\d.]+)[^{}]*\}',
        re.IGNORECASE
    )
    for m in pattern.finditer(html):
        name  = clean(m.group(1))
        price = m.group(2)
        if name and name not in seen and len(name) > 5:
            seen.add(name)
            products.append({
                "name"       : name,
                "sku"        : "",
                "price"      : price,
                "compare"    : "",
                "brand"      : "Factory Direct Flooring",
                "category"   : category,
                "images"     : [],
                "description": "",
                "tags"       : [category.lower().replace(" ", "-")],
                "stock"      : "active",
                "url"        : "",
            })

    return products


# ── Scrape all category pages ─────────────────────────────────────────────────

def scrape_all_categories():
    all_products = []
    seen_names   = set()

    for cat_name, base_cat_url in CATEGORY_URLS:
        print(f"\n  [{cat_name}]")
        page     = 1
        cat_url  = base_cat_url

        while True:
            print(f"    Page {page}: {cat_url}")
            html = fetch(cat_url)
            if not html:
                print(f"    ✗ Failed to fetch")
                break

            products = parse_category_page(html, cat_name)

            # Deduplicate
            new = []
            for p in products:
                key = p["name"].lower().strip()
                if key and key not in seen_names:
                    seen_names.add(key)
                    new.append(p)

            all_products.extend(new)
            print(f"    +{len(new)} new products (total: {len(all_products)})")

            if not new and page > 1:
                print(f"    No new products — done with this category")
                break

            # Find next page
            next_url = None

            # rel=next link tag
            rel = re.search(
                r'<link[^>]+rel=["\']next["\'][^>]+href=["\']([^"\']+)["\']', html
            )
            if rel:
                nxt = rel.group(1)
                next_url = nxt if nxt.startswith("http") else BASE_URL + nxt

            # Pagination ?p= link
            if not next_url:
                pag = re.search(
                    r'href=["\']([^"\']*[?&]p=' + str(page+1) + r'[^"\']*)["\']', html
                )
                if pag:
                    nxt = pag.group(1)
                    next_url = nxt if nxt.startswith("http") else BASE_URL + nxt

            if not next_url:
                print(f"    Last page reached")
                break

            cat_url = next_url
            page   += 1
            time.sleep(DELAY)

        print(f"  ✓ {cat_name}: done")
        time.sleep(DELAY)

    return all_products


# ── Convert to Shopify CSV ────────────────────────────────────────────────────

def to_shopify_rows(p):
    name   = p.get("name", "")
    if not name:
        return []

    handle  = make_handle(name)
    body    = p.get("description", "")
    if body and not body.strip().startswith("<"):
        body = f"<p>{body}</p>"

    vendor  = p.get("brand", "") or "Factory Direct Flooring"
    ptype   = p.get("category", "Flooring")
    tags    = ", ".join(p.get("tags", []))
    images  = [i for i in p.get("images", []) if i]
    status  = p.get("stock", "active")
    price   = norm_price(p.get("price", "0"))
    compare = norm_price(p.get("compare", ""))
    sku     = p.get("sku", "")

    rows = []

    # Row 1 — full product data
    first_img = images[0] if images else ""
    rows.append({
        "Handle"                    : handle,
        "Title"                     : name,
        "Body (HTML)"               : body,
        "Vendor"                    : vendor,
        "Product Category"          : ptype,
        "Type"                      : ptype,
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
        "Variant Price"             : price or "0.00",
        "Variant Compare At Price"  : compare,
        "Variant Requires Shipping" : "TRUE",
        "Variant Taxable"           : "TRUE",
        "Image Src"                 : first_img,
        "Image Position"            : "1" if first_img else "",
        "Image Alt Text"            : name,
        "SEO Title"                 : name[:255],
        "SEO Description"           : clean(body)[:320],
        "Status"                    : status,
    })

    # Extra image rows
    for i, img in enumerate(images[1:], 2):
        empty = {k: "" for k in SHOPIFY_COLS}
        empty.update({
            "Handle"        : handle,
            "Image Src"     : img,
            "Image Position": str(i),
            "Image Alt Text": name,
        })
        rows.append(empty)

    return rows


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    start = time.time()

    print("\n" + "═"*60)
    print("  Factory Direct Flooring → Shopify CSV (Fast Mode)")
    print("  Strategy: Category pages only — no individual pages!")
    print("═"*60)

    # Scrape
    products = scrape_all_categories()

    if not products:
        print("\n  ⚠ No products found!")
        print("  The site may be blocking automated requests.")
        # Save empty CSV so workflow doesn't crash
        out = f"{OUTPUT_DIR}/factory_flooring_shopify_{TIMESTAMP}.csv"
        with open(out, "w", encoding="utf-8-sig", newline="") as f:
            csv.DictWriter(f, fieldnames=SHOPIFY_COLS).writeheader()
        return

    # Save Shopify CSV
    shopify_rows = []
    for p in products:
        shopify_rows.extend(to_shopify_rows(p))

    csv_file = f"{OUTPUT_DIR}/factory_flooring_shopify_{TIMESTAMP}.csv"
    with open(csv_file, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SHOPIFY_COLS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(shopify_rows)

    # Save JSON
    json_file = f"{OUTPUT_DIR}/factory_flooring_{TIMESTAMP}.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump([{
            "Name"        : p["name"],
            "SKU"         : p["sku"],
            "Price (GBP)" : p["price"],
            "Brand"       : p["brand"],
            "Category"    : p["category"],
            "Stock"       : p["stock"],
            "Image"       : p["images"][0] if p["images"] else "",
            "All Images"  : " | ".join(p["images"]),
            "Description" : clean(p["description"])[:400],
            "URL"         : p["url"],
        } for p in products], f, ensure_ascii=False, indent=2)

    elapsed = round(time.time() - start)
    mins    = elapsed // 60
    secs    = elapsed % 60

    print(f"\n{'═'*60}")
    print(f"  ✅ COMPLETE in {mins}m {secs}s")
    print(f"  Products  : {len(products)}")
    print(f"  CSV rows  : {len(shopify_rows)}")
    print(f"  CSV file  : {csv_file}")
    print(f"\n  HOW TO IMPORT IN SHOPIFY:")
    print(f"  Products → Import → Upload CSV")
    print("═"*60)


if __name__ == "__main__":
    main()
