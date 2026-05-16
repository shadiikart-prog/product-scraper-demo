"""
scraper_factoryflooring_shopify.py
═══════════════════════════════════════════════════════════════════
Scrapes ALL products from factory-direct-flooring.co.uk
Outputs official Shopify product import CSV (with full images)

HOW TO RUN:
    pip install requests beautifulsoup4
    python scraper_factoryflooring_shopify.py

OUTPUT:
    output/factory_flooring_shopify_YYYYMMDD.csv  ← Import into Shopify
    output/factory_flooring_YYYYMMDD.json         ← Raw data backup
"""

import requests
import json
import csv
import time
import re
import os
from datetime import datetime
from html import unescape

try:
    from bs4 import BeautifulSoup
    BS4_OK = True
except ImportError:
    BS4_OK = False
    print("⚠ Install beautifulsoup4: pip install beautifulsoup4")

BASE_URL   = "https://www.factory-direct-flooring.co.uk"
OUTPUT_DIR = "output"
DELAY      = 1.2
TIMESTAMP  = datetime.now().strftime("%Y%m%d_%H%M%S")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}

# All category pages to scrape
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

# ── Official Shopify CSV columns ──────────────────────────────────────────────
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
# ─────────────────────────────────────────────────────────────────────────────


def fetch(url, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            if r.status_code == 200:
                return r.text
            elif r.status_code == 404:
                return ""
            time.sleep(2)
        except Exception as e:
            print(f"    Retry {attempt+1}: {e}")
            time.sleep(3)
    return ""


def clean_text(html_text):
    if not html_text:
        return ""
    text = unescape(str(html_text))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def make_handle(title):
    handle = title.lower().strip()
    handle = re.sub(r"[^a-z0-9]+", "-", handle)
    handle = handle.strip("-")
    return handle[:200]


def get_product_urls_from_sitemap():
    """Try to get all product URLs from XML sitemap."""
    product_urls = set()
    sitemap_urls = [
        f"{BASE_URL}/sitemap.xml",
        f"{BASE_URL}/pub/sitemap.xml",
        f"{BASE_URL}/sitemap_products_1.xml",
    ]
    for sm_url in sitemap_urls:
        print(f"  Trying sitemap: {sm_url}")
        html = fetch(sm_url)
        if not html:
            continue
        # Extract product URLs
        urls = re.findall(r'<loc>([^<]+)</loc>', html)
        for u in urls:
            # Product URLs usually don't have deep paths
            if u.startswith(BASE_URL) and u.count("/") == 3:
                product_urls.add(u)
        # Also look for nested sitemaps
        nested = re.findall(r'<loc>(https?://[^<]+\.xml[^<]*)</loc>', html)
        for ns_url in nested:
            ns_html = fetch(ns_url)
            if ns_html:
                ns_urls = re.findall(r'<loc>([^<]+)</loc>', ns_html)
                for u in ns_urls:
                    if u.startswith(BASE_URL) and u.count("/") == 3:
                        product_urls.add(u)
        if product_urls:
            break
    return list(product_urls)


def get_product_urls_from_category(cat_url):
    """Scrape category pages to collect product URLs."""
    product_urls = []
    page = 1
    url = BASE_URL + cat_url

    while True:
        html = fetch(url)
        if not html:
            break

        # Extract product links
        # Magento product URLs: domain.com/product-name (no subfolder)
        found = re.findall(
            r'href="(https://www\.factory-direct-flooring\.co\.uk/[a-z0-9][a-z0-9-]+)"',
            html
        )
        before = len(product_urls)
        for u in found:
            # Filter out category/nav pages
            skip = any(s in u for s in [
                "/solid-wood", "/engineered", "/laminate", "/lvt",
                "/herringbone", "/vinyl", "/carpet", "/underlay",
                "/blog", "/about", "/contact", "/brand", "/advice",
                "/customer", "/faqs", "?", "#"
            ])
            if not skip and u not in product_urls:
                product_urls.append(u)

        # Next page
        next_match = re.search(
            r'<a[^>]+href="([^"]*(?:\?p=|&p=)(\d+)[^"]*)"[^>]*>',
            html, re.IGNORECASE
        )
        rel_next = re.search(r'<link[^>]+rel=["\']next["\'][^>]+href=["\']([^"\']+)["\']', html)

        next_url = None
        if rel_next:
            next_url = rel_next.group(1)
            if not next_url.startswith("http"):
                next_url = BASE_URL + next_url
        elif next_match:
            next_url = next_match.group(1)
            if not next_url.startswith("http"):
                next_url = BASE_URL + next_url

        added = len(product_urls) - before
        print(f"    Page {page}: +{added} URLs (total: {len(product_urls)})")

        if not next_url or next_url == url or added == 0:
            break

        url = next_url
        page += 1
        time.sleep(DELAY)

    return product_urls


def scrape_product_page(url, category):
    """Scrape a single product page and return product dict."""
    html = fetch(url)
    if not html:
        return None

    product = {
        "url"         : url,
        "category"    : category,
        "name"        : "",
        "sku"         : "",
        "price"       : "",
        "compare_price": "",
        "description" : "",
        "images"      : [],
        "brand"       : "",
        "tags"        : [],
        "stock"       : "active",
    }

    # ── 1. JSON-LD (most reliable) ──────────────────────────────────────────
    json_ld_blocks = re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL
    )
    for block in json_ld_blocks:
        try:
            data = json.loads(block.strip())
            items = data if isinstance(data, list) else [data]
            for item in items:
                if item.get("@type") not in ("Product", "product"):
                    continue

                product["name"]  = clean_text(item.get("name", ""))
                product["sku"]   = item.get("sku", "")
                product["description"] = item.get("description", "")

                # Brand
                brand = item.get("brand", {})
                if isinstance(brand, dict):
                    product["brand"] = clean_text(brand.get("name", ""))
                elif isinstance(brand, str):
                    product["brand"] = clean_text(brand)

                # Price & availability
                offers = item.get("offers", {})
                if isinstance(offers, list):
                    offers = offers[0] if offers else {}
                if isinstance(offers, dict):
                    product["price"] = str(offers.get("price", ""))
                    product["compare_price"] = str(offers.get("highPrice", ""))
                    avail = offers.get("availability", "")
                    product["stock"] = "active" if "InStock" in avail else "draft"

                # Images from JSON-LD
                imgs = item.get("image", [])
                if isinstance(imgs, str):
                    imgs = [imgs]
                elif isinstance(imgs, dict):
                    imgs = [imgs.get("url", "")]
                for img in imgs:
                    if img and img not in product["images"]:
                        product["images"].append(img)

        except Exception:
            pass

    # ── 2. HTML fallback for name ────────────────────────────────────────────
    if not product["name"]:
        title_match = re.search(
            r'<h1[^>]*(?:class="[^"]*(?:page-title|product-name|title)[^"]*")?[^>]*>'
            r'\s*<span[^>]*>(.*?)</span>',
            html, re.DOTALL | re.IGNORECASE
        )
        if title_match:
            product["name"] = clean_text(title_match.group(1))

    # ── 3. HTML fallback for price ───────────────────────────────────────────
    if not product["price"]:
        price_match = re.search(
            r'<span[^>]*class="[^"]*price[^"]*"[^>]*>\s*£\s*([\d,]+\.?\d*)',
            html, re.IGNORECASE
        )
        if price_match:
            product["price"] = price_match.group(1).replace(",", "")

    # ── 4. Extract ALL images from HTML ─────────────────────────────────────
    img_urls = re.findall(
        r'(?:src|data-src|data-lazy-src)=["\']'
        r'(https://imagely\.factory-direct-flooring\.co\.uk/media/catalog/product[^"\'?\s]+)',
        html
    )
    for img in img_urls:
        # Clean up size params if any
        img_clean = img.split("?")[0]
        if img_clean not in product["images"]:
            product["images"].append(img_clean)

    # Also catch from og:image
    og_img = re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)', html)
    if og_img:
        img = og_img.group(1)
        if "catalog/product" in img and img not in product["images"]:
            product["images"].insert(0, img)

    # ── 5. Tags from breadcrumb / meta keywords ──────────────────────────────
    meta_kw = re.search(r'<meta[^>]+name=["\']keywords["\'][^>]+content=["\']([^"\']+)', html)
    if meta_kw:
        product["tags"] = [k.strip() for k in meta_kw.group(1).split(",") if k.strip()]

    return product if product["name"] else None


def product_to_shopify_rows(product):
    """Convert one product dict into Shopify CSV rows (multiple rows for multiple images)."""
    if not product or not product["name"]:
        return []

    handle   = make_handle(product["name"])
    title    = product["name"]
    body     = product.get("description", "")
    if body and not body.startswith("<"):
        body = f"<p>{body}</p>"

    vendor   = product.get("brand", "Factory Direct Flooring")
    ptype    = product.get("category", "Flooring")
    tags     = ", ".join(product.get("tags", []))
    price    = product.get("price", "0.00")
    compare  = product.get("compare_price", "")
    sku      = product.get("sku", "")
    status   = product.get("stock", "active")
    images   = product.get("images", [])

    # Ensure price is numeric
    try:
        price = f"{float(str(price).replace(',','')):.2f}"
    except Exception:
        price = "0.00"

    try:
        compare = f"{float(str(compare).replace(',','')):.2f}" if compare else ""
    except Exception:
        compare = ""

    rows = []

    # First row — full product data + first image
    first_img = images[0] if images else ""
    rows.append({
        "Handle"                    : handle,
        "Title"                     : title,
        "Body (HTML)"               : body,
        "Vendor"                    : vendor or "Factory Direct Flooring",
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
        "Variant Price"             : price,
        "Variant Compare At Price"  : compare,
        "Variant Requires Shipping" : "TRUE",
        "Variant Taxable"           : "TRUE",
        "Image Src"                 : first_img,
        "Image Position"            : "1",
        "Image Alt Text"            : title,
        "SEO Title"                 : title[:255],
        "SEO Description"           : clean_text(body)[:320],
        "Status"                    : status,
    })

    # Additional rows for extra images (same handle, no product data)
    for i, img_url in enumerate(images[1:], 2):
        rows.append({
            "Handle"       : handle,
            "Image Src"    : img_url,
            "Image Position": str(i),
            "Image Alt Text": title,
            # All other fields empty for image-only rows
            **{k: "" for k in SHOPIFY_COLS if k not in (
                "Handle", "Image Src", "Image Position", "Image Alt Text"
            )},
        })

    return rows


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("\n" + "═"*62)
    print("  Factory Direct Flooring → Shopify CSV Scraper")
    print(f"  {BASE_URL}")
    print("═"*62)

    # ── Step 1: Collect all product URLs ─────────────────────────────────────
    all_urls = {}  # url → category

    # Try sitemap first
    print("\n[1/3] Getting product URLs from sitemap...")
    sitemap_urls = get_product_urls_from_sitemap()
    if sitemap_urls:
        print(f"  ✓ {len(sitemap_urls)} URLs from sitemap")
        for u in sitemap_urls:
            all_urls[u] = "Flooring"

    # Then scrape categories
    print("\n[2/3] Scraping category pages...")
    for cat_name, cat_path in CATEGORIES:
        print(f"\n  Category: {cat_name}")
        urls = get_product_urls_from_category(cat_path)
        for u in urls:
            if u not in all_urls:
                all_urls[u] = cat_name
        print(f"  → {len(urls)} URLs found")
        time.sleep(DELAY)

    print(f"\n  Total unique product URLs: {len(all_urls)}")

    if not all_urls:
        print("\n  ⚠ No product URLs found.")
        print("  The website may require JavaScript to load products.")
        print("  Try running with Selenium or Playwright for JS rendering.")
        # Save empty file so pipeline doesn't crash
        with open(f"{OUTPUT_DIR}/factory_flooring_shopify_{TIMESTAMP}.csv", "w",
                  encoding="utf-8-sig", newline="") as f:
            csv.DictWriter(f, fieldnames=SHOPIFY_COLS).writeheader()
        return

    # ── Step 2: Scrape each product page ─────────────────────────────────────
    print("\n[3/3] Scraping product pages...")
    all_products = []
    raw_data     = []

    for i, (url, category) in enumerate(all_urls.items(), 1):
        print(f"  [{i:>4}/{len(all_urls)}] {url.split('/')[-1][:50]}")
        product = scrape_product_page(url, category)
        if product:
            all_products.append(product)
            raw_data.append({
                "Site"           : "Factory Direct Flooring",
                "Name"           : product["name"],
                "SKU"            : product["sku"],
                "Category"       : product["category"],
                "Vendor / Brand" : product["brand"],
                "Price (GBP)"    : product["price"],
                "Compare At Price": product["compare_price"],
                "Stock Status"   : "In Stock" if product["stock"] == "active" else "Out of Stock",
                "Description"    : clean_text(product["description"])[:400],
                "Main Image URL" : product["images"][0] if product["images"] else "",
                "All Image URLs" : " | ".join(product["images"]),
                "Product URL"    : url,
            })
        time.sleep(DELAY)

    print(f"\n  ✓ {len(all_products)} products scraped successfully")

    # ── Step 3: Save Shopify CSV ──────────────────────────────────────────────
    shopify_rows = []
    for product in all_products:
        shopify_rows.extend(product_to_shopify_rows(product))

    shopify_file = f"{OUTPUT_DIR}/factory_flooring_shopify_{TIMESTAMP}.csv"
    with open(shopify_file, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SHOPIFY_COLS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(shopify_rows)

    print(f"\n  Shopify CSV → {shopify_file}")
    print(f"  ({len(shopify_rows)} rows including image rows)")

    # ── Step 4: Save JSON backup ──────────────────────────────────────────────
    json_file = f"{OUTPUT_DIR}/factory_flooring_{TIMESTAMP}.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(raw_data, f, ensure_ascii=False, indent=2)
    print(f"  JSON backup → {json_file}")

    print(f"\n{'═'*62}")
    print(f"  ✅ DONE!")
    print(f"  {len(all_products)} products | {len(shopify_rows)} CSV rows")
    print(f"\n  TO IMPORT INTO SHOPIFY:")
    print(f"  Products → Import → Upload CSV → factory_flooring_shopify_*.csv")
    print(f"{'═'*62}\n")


if __name__ == "__main__":
    main()
