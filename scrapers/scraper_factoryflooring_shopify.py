"""
FACTORY DIRECT FLOORING — SHOPIFY SCRAPER (IMAGE + DESC FIXED)
✓ Images from HTML cards (imagely CDN)
✓ Auto descriptions in Body (HTML) column
✓ Max 3 images per product
✓ Type = category for Smart Collections
✓ Zero taxonomy errors
"""

import requests, json, csv, re, os, time
from datetime import datetime
from html import unescape

BASE_URL   = "https://www.factory-direct-flooring.co.uk"
OUTPUT_DIR = "output"
TIMESTAMP  = datetime.now().strftime("%Y%m%d_%H%M%S")
MAX_IMAGES = 3

HEADERS = {
    "User-Agent"      : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept"          : "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language" : "en-GB,en;q=0.9",
    "Accept-Encoding" : "gzip, deflate, br",
    "Connection"      : "keep-alive",
    "Referer"         : BASE_URL,
}

CATEGORIES = [
    ("Solid Wood Flooring",      f"{BASE_URL}/solid-wood-flooring"),
    ("Engineered Wood Flooring", f"{BASE_URL}/engineered-wood-flooring"),
    ("Laminate Flooring",        f"{BASE_URL}/laminate-flooring"),
    ("LVT Flooring",             f"{BASE_URL}/lvt-flooring"),
    ("Herringbone Flooring",     f"{BASE_URL}/herringbone-flooring"),
    ("Vinyl Flooring",           f"{BASE_URL}/vinyl-flooring"),
    ("Carpet",                   f"{BASE_URL}/carpet"),
    ("Underlay",                 f"{BASE_URL}/underlay"),
    ("Accessories",              f"{BASE_URL}/accessories"),
]

DESC_TEMPLATES = {
    "Solid Wood Flooring"      : "Premium solid wood flooring available from Factory Direct Flooring. Real wood construction for a natural, timeless finish. Suitable for residential and commercial use. Can be sanded and refinished multiple times for a lasting, beautiful floor.",
    "Engineered Wood Flooring" : "High-quality engineered wood flooring from Factory Direct Flooring. Multi-layer construction for enhanced stability and durability. Compatible with underfloor heating systems. A beautiful and practical choice for any room in your home.",
    "Laminate Flooring"        : "Durable laminate flooring from Factory Direct Flooring. Scratch-resistant surface with a realistic wood or stone effect. Easy click-fit installation suitable for DIY fitting. Ideal for busy family homes and high-traffic areas throughout the home.",
    "LVT Flooring"             : "Luxury Vinyl Tile flooring from Factory Direct Flooring. 100% waterproof with a highly durable wear layer. Perfect for kitchens, bathrooms and hallways. Comfortable underfoot with stunning realistic wood and stone designs.",
    "Herringbone Flooring"     : "Elegant herringbone pattern flooring from Factory Direct Flooring. A classic design that adds character and timeless style to any room. Available in wood and LVT options to suit your budget. Perfect for both traditional and contemporary interiors.",
    "Vinyl Flooring"           : "Quality vinyl flooring from Factory Direct Flooring. Fully waterproof and extremely easy to clean and maintain. Ideal for kitchens, bathrooms and utility rooms. Available in a wide range of on-trend colours and realistic styles.",
    "Carpet"                   : "Soft and stylish carpet from Factory Direct Flooring. Warm and comfortable underfoot with excellent sound insulation properties. Available in a variety of colours, textures and pile heights to suit your home. Perfect for bedrooms and living rooms.",
    "Underlay"                 : "Professional-grade underlay from Factory Direct Flooring. Provides cushioning, sound insulation and thermal comfort underfoot. Compatible with all floor types including underfloor heating systems. Essential for a perfect and long-lasting flooring installation.",
    "Accessories"              : "Quality flooring accessories and installation products from Factory Direct Flooring. Everything you need for a professional flooring installation. High-quality trims, adhesives and fitting tools to perfectly complement your new floor.",
}

SHOPIFY_COLS = [
    "Handle","Title","Body (HTML)","Vendor","Product Category","Type",
    "Tags","Published","Option1 Name","Option1 Value","Variant SKU",
    "Variant Grams","Variant Inventory Tracker","Variant Inventory Qty",
    "Variant Inventory Policy","Variant Fulfillment Service","Variant Price",
    "Variant Compare At Price","Variant Requires Shipping","Variant Taxable",
    "Image Src","Image Position","Image Alt Text","SEO Title","SEO Description","Status",
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch(url):
    for _ in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            if r.status_code == 200: return r.text
            if r.status_code == 404: return ""
        except: time.sleep(1)
    return ""

def clean(t):
    if not t: return ""
    t = unescape(str(t))
    t = re.sub(r"<[^>]+>", " ", t)
    return re.sub(r"\s+", " ", t).strip()

def price_fmt(p):
    try:
        f = float(str(p).replace(",","").replace("£","").strip())
        return f"{f:.2f}" if f > 0 else ""
    except: return ""

def make_handle(url, name):
    slug = url.rstrip("/").split("/")[-1].replace(".html","")
    slug = re.sub(r"[^a-z0-9\-]","",slug.lower()).strip("-")
    if slug and len(slug) > 4: return slug[:200]
    h = re.sub(r"[^a-z0-9]+","-",name.lower()).strip("-")
    return h[:200]

def build_desc(name, cat):
    t = DESC_TEMPLATES.get(cat, "Quality flooring from Factory Direct Flooring.")
    return f"<h2>{name}</h2><p>{t}</p>"

# ── Image extraction from ONE product card ────────────────────────────────────

def card_images(card_html):
    """
    Extract images from a single product card HTML.
    Tries multiple patterns used by Magento/Hyva themes.
    Returns max MAX_IMAGES clean URLs.
    """
    found = []
    seen  = set()

    def add(u):
        u = str(u).strip()
        # Remove everything after ? (query params / cache busters)
        u = u.split("?")[0]
        if (u.startswith("http")
                and len(u) > 40
                and u not in seen):
            seen.add(u)
            found.append(u)

    # Pattern 1: data-src (lazy load — most common in Magento/Hyva)
    for m in re.finditer(r'data-src=["\']([^"\'>\s]+)["\']', card_html, re.I):
        add(m.group(1))

    # Pattern 2: src attribute on img tag
    for m in re.finditer(r'<img[^>]+src=["\']([^"\'>\s]+)["\']', card_html, re.I):
        u = m.group(1)
        # Skip tiny placeholder/spinner images
        if "placeholder" in u or "spinner" in u or u.endswith(".gif"):
            continue
        add(u)

    # Pattern 3: data-original (another lazy load attr)
    for m in re.finditer(r'data-original=["\']([^"\'>\s]+)["\']', card_html, re.I):
        add(m.group(1))

    # Pattern 4: content= (some themes use this for image meta)
    for m in re.finditer(r'content=["\']([^"\'>\s]+\.(?:jpg|jpeg|png|webp))["\']', card_html, re.I):
        add(m.group(1))

    # Pattern 5: background-image style
    for m in re.finditer(r'background-image\s*:\s*url\(["\']?([^"\')\s]+)["\']?\)', card_html, re.I):
        add(m.group(1))

    # Filter: prefer imagely CDN URLs, then any product image
    imagely = [u for u in found if "imagely" in u or "factory-direct-flooring" in u]
    if imagely:
        return imagely[:MAX_IMAGES]
    return found[:MAX_IMAGES]


# ── Parse JSON-LD for product data (name/price/sku) ──────────────────────────

def parse_jsonld_products(html):
    """Extract product name/price/sku/url from JSON-LD on the page."""
    products_data = {}  # name → dict

    for block in re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL
    ):
        try:
            data  = json.loads(block.strip())
            items = data if isinstance(data, list) else [data]
            for item in items:
                if not isinstance(item, dict): continue

                def process(p):
                    if not isinstance(p, dict) or p.get("@type") != "Product": return
                    name = clean(p.get("name",""))
                    if not name or len(name) < 3: return

                    sku  = str(p.get("sku",""))
                    url  = p.get("url","")
                    brand = ""
                    b = p.get("brand",{})
                    if isinstance(b,dict): brand = clean(b.get("name",""))
                    elif isinstance(b,str): brand = clean(b)

                    price=""; compare=""; stock="active"
                    offers = p.get("offers",{})
                    if isinstance(offers,list): offers = offers[0] if offers else {}
                    if isinstance(offers,dict):
                        price   = str(offers.get("price",offers.get("lowPrice","")))
                        hp      = offers.get("highPrice","")
                        compare = str(hp) if hp and hp!=price else ""
                        avail   = offers.get("availability","")
                        stock   = "active" if "InStock" in avail else "draft"

                    products_data[name] = {
                        "name":name,"sku":sku,"url":url,"brand":brand,
                        "price":price,"compare":compare,"stock":stock,
                    }

                if item.get("@type") == "ItemList":
                    for el in item.get("itemListElement",[]):
                        process(el.get("item",el))
                elif item.get("@type") == "Product":
                    process(item)
        except: pass

    return products_data


# ── Parse HTML cards for images ───────────────────────────────────────────────

def parse_html_cards(html):
    """
    Parse all product cards from HTML.
    Returns list of {name, url, price, sku, images}
    """
    results = []
    seen    = set()

    # Split page into individual product cards
    card_pattern = re.compile(
        r'<(?:li|div|article)([^>]+class="[^"]*product[^"]*item[^"]*"[^>]*)>(.*?)</(?:li|div|article)>',
        re.DOTALL | re.I
    )

    for m in card_pattern.finditer(html):
        card = m.group(2)

        # Name
        nm = (re.search(r'class="[^"]*(?:product[_-]name|product[_-]title)[^"]*"[^>]*>.*?<a[^>]*>(.*?)</a>', card, re.DOTALL|re.I)
              or re.search(r'<a[^>]+class="[^"]*product[^"]*[^"]*"[^>]+title="([^"]{5,})"', card)
              or re.search(r'<a[^>]+title="([^"]{5,})"', card)
              or re.search(r'class="[^"]*name[^"]*"[^>]*>(.*?)</\w+>', card, re.DOTALL|re.I))
        name = clean(nm.group(1)) if nm else ""
        if not name or len(name) < 4 or name in seen: continue
        seen.add(name)

        # URL
        url_m    = re.search(r'href=["\'](' + re.escape(BASE_URL) + r'/[^"\'?#]+)["\']', card)
        prod_url = url_m.group(1) if url_m else ""

        # Price
        pm    = re.search(r'£\s*([\d,]+\.?\d*)', card)
        price = pm.group(1).replace(",","") if pm else ""

        # SKU
        sku_m = re.search(r'data-(?:sku|product-sku)=["\']([^"\']+)["\']', card)
        sku   = sku_m.group(1) if sku_m else ""

        # Images — from this card only
        images = card_images(card)

        results.append({
            "name":name,"url":prod_url,"price":price,"sku":sku,"images":images
        })

    return results


# ── Main parse: combine JSON-LD data + HTML card images ───────────────────────

def parse_page(html, cat_name):
    """
    Best of both worlds:
    - JSON-LD gives: accurate name, price, sku, stock, brand
    - HTML cards give: images (JSON-LD rarely has images in ItemList)
    Merge them by matching product names.
    """
    # Get structured data
    jsonld_data = parse_jsonld_products(html)

    # Get HTML cards (has images)
    html_cards  = parse_html_cards(html)

    products = []
    seen     = set()

    # If we have HTML cards, use them as primary source
    if html_cards:
        for card in html_cards:
            name = card["name"]
            if name in seen: continue
            seen.add(name)

            # Merge with JSON-LD data if available
            jd = jsonld_data.get(name, {})

            products.append({
                "name"    : name,
                "sku"     : jd.get("sku","") or card.get("sku",""),
                "price"   : jd.get("price","") or card.get("price",""),
                "compare" : jd.get("compare",""),
                "brand"   : jd.get("brand","") or "Factory Direct Flooring",
                "category": cat_name,
                "images"  : card["images"],
                "stock"   : jd.get("stock","active"),
                "url"     : jd.get("url","") or card.get("url",""),
            })

    # Fallback: use JSON-LD only (no images but at least products)
    elif jsonld_data:
        for name, jd in jsonld_data.items():
            if name in seen: continue
            seen.add(name)
            products.append({
                "name"    : name,
                "sku"     : jd.get("sku",""),
                "price"   : jd.get("price",""),
                "compare" : jd.get("compare",""),
                "brand"   : jd.get("brand","") or "Factory Direct Flooring",
                "category": cat_name,
                "images"  : [],
                "stock"   : jd.get("stock","active"),
                "url"     : jd.get("url",""),
            })

    return products


# ── Scrape all categories ─────────────────────────────────────────────────────

def scrape_all():
    all_products = []
    seen_handles = set()

    for cat_name, cat_base_url in CATEGORIES:
        print(f"\n  [{cat_name}]")
        page    = 1
        cur_url = f"{cat_base_url}?product_list_limit=100"

        while True:
            print(f"    Page {page}...", end=" ", flush=True)
            html = fetch(cur_url)
            if not html: print("FAILED"); break

            products = parse_page(html, cat_name)
            new = 0
            for p in products:
                handle = make_handle(p.get("url",""), p["name"])
                if handle in seen_handles: continue
                seen_handles.add(handle)
                p["handle"] = handle
                all_products.append(p)
                new += 1

            print(f"+{new} (total:{len(all_products)}) imgs:{sum(len(p['images']) for p in all_products)}")
            if new == 0 and page > 1: break

            next_url = None
            rel = re.search(r'<link[^>]+rel=["\']next["\'][^>]+href=["\']([^"\']+)["\']', html)
            if rel:
                n = rel.group(1)
                next_url = n if n.startswith("http") else BASE_URL + n
            else:
                pg = re.search(r'href=["\']([^"\']*[?&]p=' + str(page+1) + r'[^"\']*)["\']', html)
                if pg:
                    n = pg.group(1)
                    next_url = n if n.startswith("http") else BASE_URL + n

            if not next_url: break
            cur_url = next_url
            page   += 1
            time.sleep(0.6)

    return all_products


# ── Build Shopify CSV rows ────────────────────────────────────────────────────

def build_rows(p):
    name = (p.get("name") or "").strip()
    if not name: return []

    handle    = p.get("handle") or make_handle(p.get("url",""), name)
    cat_raw   = p.get("category","Flooring")
    body      = build_desc(name, cat_raw)          # Always has description
    vendor    = p.get("brand","").strip() or "Factory Direct Flooring"
    tags      = cat_raw.lower().replace(" ","-")
    images    = [i for i in p.get("images",[]) if i and i.startswith("http")][:MAX_IMAGES]
    price     = price_fmt(p.get("price","")) or "0.00"
    compare   = price_fmt(p.get("compare",""))
    first_img = images[0] if images else ""
    seo_desc  = DESC_TEMPLATES.get(cat_raw,"")[:320]

    rows = []

    rows.append({
        "Handle"                     : handle,
        "Title"                      : name,
        "Body (HTML)"                : body,           # Description here
        "Vendor"                     : vendor,
        "Product Category"           : "",             # Blank = no taxonomy error
        "Type"                       : cat_raw,        # Category here
        "Tags"                       : tags,
        "Published"                  : "TRUE",
        "Option1 Name"               : "Title",
        "Option1 Value"              : "Default Title",
        "Variant SKU"                : p.get("sku",""),
        "Variant Grams"              : "0",
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
        "SEO Title"                  : name[:255],
        "SEO Description"            : seo_desc,
        "Status"                     : p.get("stock","active"),
    })

    for i, img in enumerate(images[1:], 2):
        empty = {k:"" for k in SHOPIFY_COLS}
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
    t0 = time.time()

    print("\n" + "="*65)
    print("  FACTORY DIRECT FLOORING — SHOPIFY SCRAPER")
    print("  Images from HTML cards | Descriptions auto-generated")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*65)

    products = scrape_all()

    if not products:
        print("  No products found")
        with open(f"{OUTPUT_DIR}/factory_flooring_shopify_{TIMESTAMP}.csv",
                  "w", encoding="utf-8-sig", newline="") as f:
            csv.DictWriter(f, fieldnames=SHOPIFY_COLS).writeheader()
        return

    all_rows = []
    for p in products:
        all_rows.extend(build_rows(p))

    csv_file = f"{OUTPUT_DIR}/factory_flooring_shopify_{TIMESTAMP}.csv"
    with open(csv_file, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SHOPIFY_COLS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    json_file = f"{OUTPUT_DIR}/factory_flooring_{TIMESTAMP}.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump([{
            "name"     : p["name"],
            "handle"   : p.get("handle",""),
            "sku"      : p["sku"],
            "price"    : p["price"],
            "category" : p["category"],
            "images"   : p["images"],
            "img_count": len(p["images"]),
            "url"      : p.get("url",""),
        } for p in products], f, ensure_ascii=False, indent=2)

    elapsed  = round(time.time() - t0)
    with_img = len([p for p in products if p["images"]])
    with_desc = len(products)  # All have auto desc
    cats = {}
    for p in products:
        cats[p["category"]] = cats.get(p["category"],0)+1

    print(f"\n{'='*65}")
    print(f"  DONE in {elapsed//60}m {elapsed%60:02d}s")
    print(f"  Products    : {len(products)}")
    print(f"  CSV rows    : {len(all_rows)}")
    print(f"  With images : {with_img} ({round(with_img/len(products)*100) if products else 0}%)")
    print(f"  With desc   : {with_desc} (100% — auto generated)")
    print(f"\n  By category:")
    for cat, count in sorted(cats.items(), key=lambda x:-x[1]):
        print(f"    {cat:<35} {count:>3}")
    print(f"\n  CSV  : {csv_file}")
    print(f"  Shopify → Products → Import → Upload CSV")
    print("="*65)


if __name__ == "__main__":
    main()
