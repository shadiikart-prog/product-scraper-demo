"""
scraper_factoryflooring_shopify.py
════════════════════════════════════════════════════════════════
Scraper for factory-direct-flooring.co.uk
FIXED: Exact image extraction from card-image div
       Skips placeholder images
       Valid Shopify taxonomy
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

# Real image CDN — only these are valid
REAL_CDN   = "imagely.factory-direct-flooring.co.uk/media/catalog/product/cache/"

# Placeholder to SKIP
PLACEHOLDER = "placeholder"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept"         : "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}

# Valid Shopify taxonomy
SHOPIFY_TAXONOMY = {
    "Solid Wood Flooring"      : "Home & Garden > Decor > Flooring",
    "Engineered Wood Flooring" : "Home & Garden > Decor > Flooring",
    "Laminate Flooring"        : "Home & Garden > Decor > Flooring",
    "LVT Flooring"             : "Home & Garden > Decor > Flooring",
    "Herringbone Flooring"     : "Home & Garden > Decor > Flooring",
    "Vinyl Flooring"           : "Home & Garden > Decor > Flooring",
    "Carpet"                   : "Home & Garden > Decor > Rugs",
    "Underlay"                 : "Home & Garden > Decor > Flooring",
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
    for attempt in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 200:
                return r.text
            if r.status_code == 404:
                return ""
        except Exception as e:
            print(f"    Retry {attempt+1}: {e}")
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
        val = str(p).replace(",","").replace("£","").strip()
        return f"{float(val):.2f}" if val else ""
    except Exception:
        return ""


def is_real_image(url):
    """Check if URL is a real product image (not placeholder)."""
    if not url:
        return False
    if PLACEHOLDER in url.lower():
        return False
    if REAL_CDN in url:
        return True
    return False


def extract_real_images(html_block):
    """
    Extract ONLY real product images from HTML block.
    Pattern from inspect: imagely CDN with /cache/ path
    Example: https://imagely.factory-direct-flooring.co.uk/media/catalog/product/cache/HASH/p/r/name.jpg
    """
    images = []

    # Find all img src — exact pattern from inspect element
    all_srcs = re.findall(
        r'<img[^>]+src=["\']([^"\']+)["\']',
        html_block, re.IGNORECASE
    )
    for src in all_srcs:
        src = src.strip()
        if is_real_image(src):
            # Remove query params
            clean_url = src.split("?")[0]
            if clean_url not in images:
                images.append(clean_url)

    # Also check data-src (lazy loading)
    lazy_srcs = re.findall(
        r'data-src=["\']([^"\']+)["\']',
        html_block, re.IGNORECASE
    )
    for src in lazy_srcs:
        src = src.strip()
        if is_real_image(src):
            clean_url = src.split("?")[0]
            if clean_url not in images:
                images.append(clean_url)

    return images


# ── Parse products from page ──────────────────────────────────────────────────

def parse_page(html, category):
    """
    Extract products from category page.
    Uses card-image div structure found via inspect.
    """
    products = []
    seen     = set()

    # ── Method 1: card-image divs (exact structure from inspect) ─────────────
    # Find all card-image blocks which contain the product images + link
    card_pattern = re.compile(
        r'<div[^>]+class="[^"]*card-image[^"]*"[^>]*>(.*?)</div>\s*</div>',
        re.DOTALL | re.IGNORECASE
    )

    for card_m in card_pattern.finditer(html):
        card_html = card_m.group(0)

        # Get product URL from the anchor inside card
        url_m = re.search(
            r'href=["\'](' + re.escape(BASE_URL) + r'/[a-z0-9][a-z0-9-]+)["\']',
            card_html
        )
        if not url_m:
            continue

        prod_url = url_m.group(1)
        if prod_url in seen:
            continue
        seen.add(prod_url)

        # Real images only
        images = extract_real_images(card_html)

        # Name from img alt
        name = ""
        alt_m = re.search(r'<img[^>]+alt=["\']([^"\']+)["\']', card_html)
        if alt_m:
            name = clean(alt_m.group(1))

        if not name:
            # Derive name from URL slug
            slug = prod_url.rstrip("/").split("/")[-1]
            name = slug.replace("-", " ").title()

        if name:
            products.append({
                "name"    : name,
                "url"     : prod_url,
                "images"  : images,
                "category": category,
            })

    # ── Method 2: JSON-LD for prices + SKUs ──────────────────────────────────
    # Build URL→price map from JSON-LD
    price_map = {}
    sku_map   = {}
    desc_map  = {}
    brand_map = {}

    json_ld_blocks = re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL
    )
    for block in json_ld_blocks:
        try:
            data  = json.loads(block.strip())
            items = data if isinstance(data, list) else [data]
            for item in items:
                if not isinstance(item, dict):
                    continue

                # ItemList
                if item.get("@type") == "ItemList":
                    for elem in item.get("itemListElement", []):
                        p = elem.get("item", elem)
                        _map_jsonld_product(p, price_map, sku_map, desc_map, brand_map)
                # Single product
                elif item.get("@type") == "Product":
                    _map_jsonld_product(item, price_map, sku_map, desc_map, brand_map)
        except Exception:
            pass

    # If JSON-LD didn't give us products from card-image, extract them directly
    if not products:
        for block in json_ld_blocks:
            try:
                data  = json.loads(block.strip())
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    if item.get("@type") == "Product":
                        name = clean(item.get("name",""))
                        url  = str(item.get("url",""))
                        if not name or url in seen:
                            continue
                        seen.add(url)

                        # Get images — prefer CDN images
                        imgs = []
                        raw_imgs = item.get("image",[])
                        if isinstance(raw_imgs, str): raw_imgs = [raw_imgs]
                        if isinstance(raw_imgs, dict): raw_imgs = [raw_imgs.get("url","")]
                        for img in raw_imgs:
                            if is_real_image(str(img)):
                                imgs.append(str(img).split("?")[0])

                        offers = item.get("offers",{})
                        if isinstance(offers, list): offers = offers[0] if offers else {}
                        price = str(offers.get("price","")) if isinstance(offers,dict) else ""

                        products.append({
                            "name"    : name,
                            "url"     : url,
                            "images"  : imgs,
                            "category": category,
                            "price"   : price,
                            "sku"     : str(item.get("sku","")),
                            "description": clean(item.get("description","")),
                            "brand"   : "",
                        })
            except Exception:
                pass

    # ── Merge price/sku/desc data into products ───────────────────────────────
    for p in products:
        url = p.get("url","")
        if "price" not in p:
            p["price"] = price_map.get(url,"")
        if "sku" not in p:
            p["sku"] = sku_map.get(url,"")
        if "description" not in p:
            p["description"] = desc_map.get(url,"")
        if "brand" not in p:
            p["brand"] = brand_map.get(url,"Factory Direct Flooring")
        if "stock" not in p:
            p["stock"] = "active"
        if "compare" not in p:
            p["compare"] = ""
        if "tags" not in p:
            p["tags"] = [category.lower().replace(" ","-")]

    # ── Method 3: fallback price from HTML £ signs ────────────────────────────
    # Find product name+price pairs near each other
    price_pairs = re.findall(
        r'class="[^"]*product[^"]*name[^"]*"[^>]*>.*?'
        r'<a[^>]*>([^<]{5,})</a>.*?'
        r'£\s*([\d,]+\.?\d*)',
        html, re.DOTALL | re.IGNORECASE
    )
    pair_map = {clean(n).lower(): p.replace(",","") for n,p in price_pairs}

    for p in products:
        if not p.get("price"):
            p["price"] = pair_map.get(p["name"].lower(), "")

    return products


def _map_jsonld_product(item, price_map, sku_map, desc_map, brand_map):
    if not isinstance(item, dict) or item.get("@type") != "Product":
        return
    url = str(item.get("url",""))
    if not url:
        return
    offers = item.get("offers",{})
    if isinstance(offers, list): offers = offers[0] if offers else {}
    if isinstance(offers, dict):
        price_map[url] = str(offers.get("price",""))
    sku_map[url]  = str(item.get("sku",""))
    desc_map[url] = clean(item.get("description",""))
    b = item.get("brand",{})
    brand_map[url] = clean(b.get("name","") if isinstance(b,dict) else b)


# ── Scrape all categories ─────────────────────────────────────────────────────

def scrape_all():
    all_products = []
    seen         = set()

    for cat_name, base_url in CATEGORY_URLS:
        print(f"\n  [{cat_name}]")
        page    = 1
        cur_url = base_url

        while True:
            print(f"    Page {page}...")
            html = fetch(cur_url)
            if not html:
                print(f"    ✗ Failed")
                break

            products = parse_page(html, cat_name)

            # Global dedup
            new = []
            for p in products:
                key = (p.get("url") or p["name"]).lower()
                if key and key not in seen:
                    seen.add(key)
                    new.append(p)

            # Count images
            with_img = sum(1 for p in new if p.get("images"))
            all_products.extend(new)
            print(f"    +{len(new)} products ({with_img} with images) | Total: {len(all_products)}")

            if not new and page > 1:
                break

            # Next page
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


# ── Convert to Shopify CSV ────────────────────────────────────────────────────

def to_shopify_rows(p):
    name = p.get("name","")
    if not name:
        return []

    cat     = p.get("category","Flooring")
    handle  = make_handle(name)
    body    = p.get("description","") or ""
    if body and not body.strip().startswith("<"):
        body = f"<p>{body}</p>"

    vendor      = p.get("brand","") or "Factory Direct Flooring"
    shopify_cat = SHOPIFY_TAXONOMY.get(cat, "Home & Garden > Decor > Flooring")
    tags        = ", ".join(p.get("tags",[cat.lower().replace(" ","-")]))
    images      = [i for i in p.get("images",[]) if i and is_real_image(i)]
    status      = p.get("stock","active")
    price       = norm_price(p.get("price","")) or "0.00"
    compare     = norm_price(p.get("compare",""))
    sku         = p.get("sku","")
    first       = images[0] if images else ""

    rows = []

    # Main product row
    rows.append({
        "Handle"                    : handle,
        "Title"                     : name,
        "Body (HTML)"               : body,
        "Vendor"                    : vendor,
        "Product Category"          : shopify_cat,
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
        blank = {k:"" for k in SHOPIFY_COLS}
        blank.update({
            "Handle"        : handle,
            "Image Src"     : img,
            "Image Position": str(i),
            "Image Alt Text": name,
        })
        rows.append(blank)

    return rows


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    start = time.time()

    print("\n" + "═"*62)
    print("  Factory Direct Flooring → Shopify CSV")
    print("  ✓ Real imagely CDN images (no placeholders)")
    print("  ✓ Valid Shopify taxonomy")
    print("═"*62)

    products = scrape_all()

    if not products:
        print("\n  ⚠ No products found — saving empty CSV")
        out = f"{OUTPUT_DIR}/factory_flooring_shopify_{TIMESTAMP}.csv"
        with open(out, "w", encoding="utf-8-sig", newline="") as f:
            csv.DictWriter(f, fieldnames=SHOPIFY_COLS).writeheader()
        return

    # Stats
    with_img = sum(1 for p in products if any(is_real_image(i) for i in p.get("images",[])))
    print(f"\n  ✓ Products with real images : {with_img}/{len(products)}")

    # Shopify CSV
    shopify_rows = []
    for p in products:
        shopify_rows.extend(to_shopify_rows(p))

    csv_file = f"{OUTPUT_DIR}/factory_flooring_shopify_{TIMESTAMP}.csv"
    with open(csv_file, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SHOPIFY_COLS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(shopify_rows)

    # JSON backup
    json_file = f"{OUTPUT_DIR}/factory_flooring_{TIMESTAMP}.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump([{
            "Name"       : p["name"],
            "SKU"        : p["sku"],
            "Price (GBP)": p.get("price",""),
            "Category"   : p["category"],
            "Images"     : p.get("images",[]),
            "URL"        : p.get("url",""),
        } for p in products], f, ensure_ascii=False, indent=2)

    elapsed = round(time.time() - start)
    print(f"\n{'═'*62}")
    print(f"  ✅ DONE in {elapsed//60}m {elapsed%60}s")
    print(f"  Products : {len(products)}")
    print(f"  CSV rows : {len(shopify_rows)}")
    print(f"\n  IMPORT TO SHOPIFY:")
    print(f"  Products → Import → Upload → factory_flooring_shopify_*.csv")
    print("═"*62)


if __name__ == "__main__":
    main()
