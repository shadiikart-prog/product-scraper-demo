"""
scraper_factoryflooring_shopify.py
════════════════════════════════════════════════════════════════
Factory Direct Flooring → Shopify CSV

FIXES:
  ✓ Valid Shopify product categories (official taxonomy)
  ✓ Real image URLs from CDN
  ✓ Fast — category pages only, no individual page visits
  ✓ Completes in under 20 minutes
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
DELAY      = 1.0

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept"         : "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection"     : "keep-alive",
}

# ── Official Shopify Taxonomy IDs for flooring ────────────────────────────────
# Source: https://help.shopify.com/manual/products/details/product-type
SHOPIFY_CATEGORY_MAP = {
    "Solid Wood Flooring"      : "Hardware > Flooring > Hardwood Flooring",
    "Engineered Wood Flooring" : "Hardware > Flooring > Engineered Wood Flooring",
    "Laminate Flooring"        : "Hardware > Flooring > Laminate Flooring",
    "LVT Flooring"             : "Hardware > Flooring > Vinyl Flooring",
    "Herringbone Flooring"     : "Hardware > Flooring > Hardwood Flooring",
    "Vinyl Flooring"           : "Hardware > Flooring > Vinyl Flooring",
    "Carpet"                   : "Hardware > Flooring > Carpet",
    "Underlay"                 : "Hardware > Flooring",
    "Accessories"              : "Hardware > Flooring",
    "Default"                  : "Hardware > Flooring",
}

# Categories to scrape — using ?product_list_limit=100 to get max per page
CATEGORIES = [
    ("Solid Wood Flooring",      "/solid-wood-flooring"),
    ("Engineered Wood Flooring", "/engineered-wood-flooring"),
    ("Laminate Flooring",        "/laminate-flooring"),
    ("LVT Flooring",             "/lvt-flooring"),
    ("Herringbone Flooring",     "/herringbone-flooring"),
    ("Vinyl Flooring",           "/vinyl-flooring"),
    ("Carpet",                   "/carpet"),
    ("Underlay",                 "/underlay"),
]

# Shopify official CSV columns
SHOPIFY_COLS = [
    "Handle", "Title", "Body (HTML)", "Vendor", "Product Category",
    "Type", "Tags", "Published",
    "Option1 Name", "Option1 Value",
    "Variant SKU", "Variant Grams", "Variant Inventory Tracker",
    "Variant Inventory Qty", "Variant Inventory Policy",
    "Variant Fulfillment Service", "Variant Price",
    "Variant Compare At Price", "Variant Requires Shipping",
    "Variant Taxable", "Image Src", "Image Position", "Image Alt Text",
    "SEO Title", "SEO Description", "Status",
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch(url):
    """Fetch a page with retry."""
    for attempt in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            if r.status_code == 200:
                return r.text
            time.sleep(2)
        except Exception as e:
            print(f"    Attempt {attempt+1} failed: {e}")
            time.sleep(3)
    return ""


def clean(text):
    if not text:
        return ""
    text = unescape(str(text))
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def make_handle(title):
    h = re.sub(r"[^a-z0-9]+", "-", title.lower().strip()).strip("-")
    return h[:200]


def norm_price(p):
    try:
        return f"{float(re.sub(r'[^0-9.]', '', str(p))):.2f}"
    except Exception:
        return ""


def get_shopify_category(cat_name):
    return SHOPIFY_CATEGORY_MAP.get(cat_name, SHOPIFY_CATEGORY_MAP["Default"])


# ── Image URL Builder ─────────────────────────────────────────────────────────

def build_full_image_url(img_path):
    """
    Factory Direct Flooring CDN pattern:
    https://imagely.factory-direct-flooring.co.uk/media/catalog/product/cache/.../image.jpg
    We want the full-size version without cache path
    """
    if not img_path:
        return ""

    # Already full URL
    if img_path.startswith("http"):
        # Remove cache/resize params to get full image
        # Pattern: /cache/abc123/200x200/ → remove this part
        img_clean = re.sub(r'/cache/[^/]+/\d+x\d+/', '/', img_path)
        img_clean = re.sub(r'/cache/[^/]+/', '/', img_clean)
        img_clean = img_clean.split("?")[0]
        return img_clean

    # Relative path
    if img_path.startswith("/media/"):
        return f"https://imagely.factory-direct-flooring.co.uk{img_path}"

    return img_path


def extract_images_from_html(html):
    """Extract all product CDN image URLs from page HTML."""
    images = []

    # Pattern 1: imagely CDN URLs (main product CDN)
    cdn_pattern = re.compile(
        r'(?:src|data-src|data-lazy-src|data-original)=["\']'
        r'(https://imagely\.factory-direct-flooring\.co\.uk/media/catalog/product[^"\'>\s?]+)',
        re.IGNORECASE
    )
    for m in cdn_pattern.finditer(html):
        url = build_full_image_url(m.group(1))
        if url and url not in images:
            images.append(url)

    # Pattern 2: srcset attribute
    srcset_pattern = re.compile(
        r'srcset=["\']([^"\']+)["\']', re.IGNORECASE
    )
    for m in srcset_pattern.finditer(html):
        parts = m.group(1).split(",")
        for part in parts:
            url_part = part.strip().split(" ")[0]
            if "imagely.factory-direct-flooring" in url_part and "catalog/product" in url_part:
                url = build_full_image_url(url_part.strip())
                if url and url not in images:
                    images.append(url)

    # Pattern 3: JSON data-gallery or x-data attributes
    gallery_pattern = re.compile(
        r'"(?:full|medium|image|src)":\s*"(https://imagely[^"]+)"',
        re.IGNORECASE
    )
    for m in gallery_pattern.finditer(html):
        url = build_full_image_url(m.group(1))
        if url and url not in images:
            images.append(url)

    # Pattern 4: og:image meta tag
    og_pattern = re.compile(
        r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
        re.IGNORECASE
    )
    for m in og_pattern.finditer(html):
        url = build_full_image_url(m.group(1))
        if url and "catalog" in url and url not in images:
            images.insert(0, url)  # og:image = main image, put first

    return images


# ── Product Parser ────────────────────────────────────────────────────────────

def parse_products_from_page(html, category):
    """Extract products from a category listing page."""
    products  = []
    seen_names = set()

    # ── Method 1: JSON-LD structured data ────────────────────────────────────
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

                # ItemList containing products
                if item.get("@type") == "ItemList":
                    for elem in item.get("itemListElement", []):
                        p = extract_from_jsonld(elem.get("item", elem), category, html)
                        if p and p["name"] not in seen_names:
                            seen_names.add(p["name"])
                            products.append(p)

                # Single Product
                elif item.get("@type") == "Product":
                    p = extract_from_jsonld(item, category, html)
                    if p and p["name"] not in seen_names:
                        seen_names.add(p["name"])
                        products.append(p)
        except Exception:
            pass

    # ── Method 2: HTML product cards ─────────────────────────────────────────
    if not products:
        products = parse_html_cards(html, category, seen_names)

    return products


def extract_from_jsonld(item, category, page_html=""):
    """Extract product data from a JSON-LD Product object."""
    if not isinstance(item, dict):
        return None
    if item.get("@type") not in ("Product",):
        return None

    name = clean(item.get("name", ""))
    if not name or len(name) < 4:
        return None

    # Price
    price   = ""
    compare = ""
    stock   = "active"
    offers  = item.get("offers", {})
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    if isinstance(offers, dict):
        price   = str(offers.get("price", offers.get("lowPrice", "")))
        compare = str(offers.get("highPrice", ""))
        avail   = offers.get("availability", "")
        stock   = "active" if "InStock" in avail else "draft"

    # Brand
    brand = ""
    b = item.get("brand", {})
    if isinstance(b, dict): brand = clean(b.get("name", ""))
    elif isinstance(b, str): brand = clean(b)

    # Description
    desc = item.get("description", "")

    # Images from JSON-LD
    images = []
    imgs = item.get("image", [])
    if isinstance(imgs, str): imgs = [imgs]
    if isinstance(imgs, dict): imgs = [imgs.get("url", "")]
    for img in imgs:
        url = build_full_image_url(str(img))
        if url and url not in images:
            images.append(url)

    # If no images from JSON-LD, try extracting from page HTML context
    if not images and page_html:
        all_imgs = extract_images_from_html(page_html)
        images = all_imgs[:5]  # Take first 5

    # SKU
    sku = str(item.get("sku", ""))

    # URL
    url = str(item.get("url", ""))

    # Tags
    tags = [category.lower().replace(" ", "-")]

    return {
        "name"       : name,
        "sku"        : sku,
        "price"      : price,
        "compare"    : compare if compare != price else "",
        "brand"      : brand or "Factory Direct Flooring",
        "category"   : category,
        "images"     : images,
        "description": desc,
        "tags"       : tags,
        "stock"      : stock,
        "url"        : url,
    }


def parse_html_cards(html, category, seen_names):
    """Fallback: parse product cards from HTML when JSON-LD unavailable."""
    products = []

    # Split HTML into product sections
    # Look for product links with names
    link_pattern = re.compile(
        r'<a\s+[^>]*href=["\']('
        r'https://www\.factory-direct-flooring\.co\.uk/[a-z0-9][a-z0-9-]+'
        r')["\'][^>]*>'
        r'(.*?)</a>',
        re.DOTALL | re.IGNORECASE
    )

    skip_keywords = [
        "solid-wood-flooring", "engineered-wood-flooring", "laminate-flooring",
        "lvt-flooring", "herringbone-flooring", "vinyl-flooring", "carpet",
        "underlay", "commercial", "blog", "about", "contact", "brand",
        "accessories", "advice", "faq", "sitemap", "account", "wishlist"
    ]

    for m in link_pattern.finditer(html):
        url   = m.group(1)
        inner = m.group(2)

        # Skip navigation/category links
        slug = url.replace(BASE_URL, "").strip("/")
        if any(kw in slug for kw in skip_keywords):
            continue

        # Extract name from link text
        name = clean(re.sub(r"<[^>]+>", " ", inner))
        if not name or len(name) < 5 or name in seen_names:
            continue

        # Find price near this link in surrounding HTML
        start = m.start()
        context = html[max(0, start-200):start+800]
        price_m = re.search(r'£\s*([\d,]+\.?\d*)', context)
        price   = price_m.group(1).replace(",", "") if price_m else ""

        # Find image near this link
        img_m = re.search(
            r'(?:src|data-src)=["\']'
            r'(https://imagely\.factory-direct-flooring\.co\.uk[^"\'>\s?]+)',
            context, re.IGNORECASE
        )
        img = build_full_image_url(img_m.group(1)) if img_m else ""

        seen_names.add(name)
        products.append({
            "name"       : name,
            "sku"        : "",
            "price"      : price,
            "compare"    : "",
            "brand"      : "Factory Direct Flooring",
            "category"   : category,
            "images"     : [img] if img else [],
            "description": f"<p>{name} — Premium {category} from Factory Direct Flooring.</p>",
            "tags"       : [category.lower().replace(" ", "-"), "flooring"],
            "stock"      : "active",
            "url"        : url,
        })

    return products


# ── Category Scraper ──────────────────────────────────────────────────────────

def scrape_category(cat_name, cat_path):
    """Scrape all pages of a single category."""
    all_products = []
    seen_names   = set()
    page         = 1
    url          = f"{BASE_URL}{cat_path}?product_list_limit=100"

    print(f"\n  ▶ {cat_name}")

    while True:
        print(f"    Page {page}: fetching...")
        html = fetch(url)
        if not html:
            print(f"    ✗ Failed")
            break

        products = parse_products_from_page(html, cat_name)

        # Deduplicate
        new = []
        for p in products:
            key = p["name"].lower().strip()
            if key and key not in seen_names:
                seen_names.add(key)
                new.append(p)

        all_products.extend(new)
        print(f"    Page {page}: +{len(new)} products → total {len(all_products)}")

        if not new and page > 1:
            break

        # Find next page link
        next_url = None

        # rel=next (most reliable)
        rel = re.search(
            r'<link[^>]+rel=["\']next["\'][^>]+href=["\']([^"\']+)["\']', html
        )
        if rel:
            nxt = rel.group(1)
            next_url = nxt if nxt.startswith("http") else BASE_URL + nxt

        # ?p= pagination
        if not next_url:
            pag = re.search(
                rf'href=["\']([^"\']*[?&]p={page+1}[^"\']*)["\']', html
            )
            if pag:
                nxt = pag.group(1)
                next_url = nxt if nxt.startswith("http") else BASE_URL + nxt

        if not next_url:
            print(f"    Last page.")
            break

        url  = next_url
        page += 1
        time.sleep(DELAY)

    print(f"  ✓ {cat_name}: {len(all_products)} products")
    return all_products


# ── Shopify CSV Row Builder ───────────────────────────────────────────────────

def to_shopify_rows(p):
    """Convert product dict to Shopify CSV rows (1 per image)."""
    name = p.get("name", "").strip()
    if not name:
        return []

    handle   = make_handle(name)
    body     = p.get("description", "")
    if body and not body.strip().startswith("<"):
        body = f"<p>{body}</p>"

    vendor   = p.get("brand", "") or "Factory Direct Flooring"
    cat_name = p.get("category", "Default")
    shopify_cat = get_shopify_category(cat_name)
    tags     = ", ".join(p.get("tags", []))
    images   = [i for i in p.get("images", []) if i and i.startswith("http")]
    price    = norm_price(p.get("price", "")) or "0.00"
    compare  = norm_price(p.get("compare", ""))
    sku      = p.get("sku", "")
    status   = p.get("stock", "active")

    rows = []

    # Row 1 — full product
    first_img = images[0] if images else ""
    rows.append({
        "Handle"                    : handle,
        "Title"                     : name,
        "Body (HTML)"               : body,
        "Vendor"                    : vendor,
        "Product Category"          : shopify_cat,
        "Type"                      : cat_name,
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
        "Image Src"                 : first_img,
        "Image Position"            : "1" if first_img else "",
        "Image Alt Text"            : name,
        "SEO Title"                 : name[:255],
        "SEO Description"           : clean(body)[:320],
        "Status"                    : status,
    })

    # Extra image rows (same handle, only image fields)
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
    print("  Factory Direct Flooring → Shopify CSV")
    print(f"  Target: {BASE_URL}")
    print("═"*60)

    # Scrape all categories
    all_products = []
    seen_global  = set()

    for cat_name, cat_path in CATEGORIES:
        products = scrape_category(cat_name, cat_path)
        for p in products:
            key = p["name"].lower().strip()
            if key not in seen_global:
                seen_global.add(key)
                all_products.append(p)
        time.sleep(DELAY)

    print(f"\n  Total unique products: {len(all_products)}")

    if not all_products:
        print("  ⚠ No products found! Saving empty CSV.")
        with open(f"{OUTPUT_DIR}/factory_flooring_shopify_{TIMESTAMP}.csv",
                  "w", encoding="utf-8-sig", newline="") as f:
            csv.DictWriter(f, fieldnames=SHOPIFY_COLS).writeheader()
        return

    # Build Shopify rows
    shopify_rows = []
    for p in all_products:
        shopify_rows.extend(to_shopify_rows(p))

    # Save Shopify CSV
    csv_file = f"{OUTPUT_DIR}/factory_flooring_shopify_{TIMESTAMP}.csv"
    with open(csv_file, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SHOPIFY_COLS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(shopify_rows)

    # Save JSON backup
    json_file = f"{OUTPUT_DIR}/factory_flooring_{TIMESTAMP}.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump([{
            "Name"        : p["name"],
            "SKU"         : p["sku"],
            "Price"       : p["price"],
            "Compare"     : p["compare"],
            "Brand"       : p["brand"],
            "Category"    : p["category"],
            "Shopify Cat" : get_shopify_category(p["category"]),
            "Stock"       : p["stock"],
            "Images"      : p["images"],
            "Description" : clean(p.get("description", ""))[:400],
            "URL"         : p["url"],
        } for p in all_products], f, ensure_ascii=False, indent=2)

    elapsed = time.time() - start
    mins    = int(elapsed // 60)
    secs    = int(elapsed % 60)

    # Summary
    with_images    = sum(1 for p in all_products if p["images"])
    without_images = len(all_products) - with_images
    total_images   = sum(len(p["images"]) for p in all_products)

    print(f"\n{'═'*60}")
    print(f"  ✅ DONE in {mins}m {secs}s")
    print(f"  Products     : {len(all_products)}")
    print(f"  With images  : {with_images}")
    print(f"  No images    : {without_images}")
    print(f"  Total images : {total_images}")
    print(f"  CSV rows     : {len(shopify_rows)}")
    print(f"  CSV file     : {csv_file}")
    print(f"\n  IMPORT TO SHOPIFY:")
    print(f"  Products → Import → Upload CSV → {os.path.basename(csv_file)}")
    print("═"*60)


if __name__ == "__main__":
    main()
