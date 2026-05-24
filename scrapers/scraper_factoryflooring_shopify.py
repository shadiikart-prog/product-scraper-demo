"""
╔══════════════════════════════════════════════════════════════════════╗
║   FACTORY DIRECT FLOORING — FINAL SCRAPER (SLUG-MATCH STRATEGY)     ║
╠══════════════════════════════════════════════════════════════════════╣
║  Strategy:                                                           ║
║   1. Collect ALL imagely URLs from the page                          ║
║   2. Get product list with URL slugs from JSON-LD                    ║
║   3. Match images to products by slug name (no mixing!)              ║
║   4. Each product gets only its OWN images (max 2)                   ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import requests, json, csv, re, os, time
from datetime import datetime
from html import unescape

BASE_URL    = "https://www.factory-direct-flooring.co.uk"
OUTPUT_DIR  = "output"
TIMESTAMP   = datetime.now().strftime("%Y%m%d_%H%M%S")
MAX_IMAGES  = 2
DELAY       = 0.6

HEADERS = {
    "User-Agent"      : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept"          : "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language" : "en-GB,en;q=0.9",
    "Accept-Encoding" : "gzip, deflate, br",
    "Connection"      : "keep-alive",
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

SHOPIFY_COLS = [
    "Handle","Title","Body (HTML)","Vendor","Product Category","Type",
    "Tags","Published","Option1 Name","Option1 Value","Variant SKU",
    "Variant Grams","Variant Inventory Tracker","Variant Inventory Qty",
    "Variant Inventory Policy","Variant Fulfillment Service","Variant Price",
    "Variant Compare At Price","Variant Requires Shipping","Variant Taxable",
    "Image Src","Image Position","Image Alt Text","SEO Title","SEO Description","Status",
]

# ──────────────────────────────────────────────────────────────────────
#  AUTO DESCRIPTION TEMPLATES
# ──────────────────────────────────────────────────────────────────────

def extract_hints(title):
    tl = title.lower()
    brands = ['karndean','kahrs','quick-step','quickstep','amtico','ted todd','elka',
              'wicanders','moduleo','polyflor','tarkett','egger','krono','berry alloc',
              'balterio','camaro','boen','hakwood','woodpecker','lifestyle','rhinofloor',
              'gerflor','forbo','altro','armstrong','pergo','kronoswiss']
    brand  = next((b.title() for b in brands if b in tl), "")
    thick  = re.search(r'(\d+(?:\.\d+)?)\s*mm', title, re.I)
    ts     = thick.group(1)+"mm" if thick else ""
    cols   = ['oak','walnut','pine','ash','maple','birch','cherry','white','grey','gray',
              'black','brown','beige','cream','ivory','natural','smoked','rustic','aged',
              'antique','vintage','blond','golden','silver','slate','stone','marble']
    colour = next((c.title() for c in cols if c in tl), "")
    return brand, ts, colour


def auto_desc(title, cat):
    brand, thick, colour = extract_hints(title)
    bl = f" by <strong>{brand}</strong>" if brand else " from Factory Direct Flooring"
    cl = f" in a beautiful <strong>{colour}</strong> finish" if colour else ""
    tl = f" with a <strong>{thick}</strong> thickness" if thick else ""

    intros = {
        "Solid Wood Flooring": f"<h2>{title}</h2><p>Introducing the <strong>{title}</strong>{bl}{cl}{tl}. Crafted from 100% genuine solid timber, this floor delivers unmatched natural beauty and authentic character that only improves with age.</p>",
        "Engineered Wood Flooring": f"<h2>{title}</h2><p>Discover the <strong>{title}</strong>{bl}{cl}{tl}. Engineered wood combines a real wood top layer with multi-layer construction for warmth and stability.</p>",
        "Laminate Flooring": f"<h2>{title}</h2><p>Meet the <strong>{title}</strong>{bl}{cl}{tl}. Outstanding laminate delivers the look of real wood or stone at a fraction of the cost.</p>",
        "LVT Flooring": f"<h2>{title}</h2><p>Introducing the <strong>{title}</strong>{bl}{cl}{tl}. Luxury Vinyl Tile — 100% waterproof with hyper-realistic designs for any room.</p>",
        "Herringbone Flooring": f"<h2>{title}</h2><p>Make a statement with the <strong>{title}</strong>{bl}{cl}{tl}. The iconic herringbone pattern adds timeless elegance.</p>",
        "Vinyl Flooring": f"<h2>{title}</h2><p>Presenting the <strong>{title}</strong>{bl}{cl}{tl}. Premium vinyl — fully waterproof, ideal for kitchens, bathrooms and high-traffic areas.</p>",
        "Carpet": f"<h2>{title}</h2><p>Transform your home with the <strong>{title}</strong>{bl}{cl}. Luxuriously soft — perfect for bedrooms and living rooms.</p>",
        "Underlay": f"<h2>{title}</h2><p>The <strong>{title}</strong>{bl} — professional underlay for comfort, sound insulation and floor longevity.</p>",
        "Accessories": f"<h2>{title}</h2><p>Complete your installation with the <strong>{title}</strong>{bl}. Quality finishing accessories.</p>",
    }
    features = {
        "Solid Wood Flooring": "<h3>Key Features</h3><ul><li><strong>100% Real Solid Timber</strong></li><li><strong>Sand &amp; Refinish</strong> up to 5 times</li><li><strong>Natural Insulator</strong></li><li><strong>Adds Property Value</strong></li><li><strong>Sustainably Sourced</strong></li></ul>",
        "Engineered Wood Flooring": "<h3>Key Features</h3><ul><li><strong>Real Wood Top Layer</strong></li><li><strong>Stable Multi-Layer Core</strong></li><li><strong>UFH Compatible</strong></li><li><strong>All-Level Installation</strong></li><li><strong>Flexible Fitting</strong></li></ul>",
        "Laminate Flooring": "<h3>Key Features</h3><ul><li><strong>HD Print Layer</strong></li><li><strong>Scratch-Resistant</strong></li><li><strong>Easy Click Installation</strong></li><li><strong>Bevelled V-Groove Edges</strong></li><li><strong>Low Maintenance</strong></li></ul>",
        "LVT Flooring": "<h3>Key Features</h3><ul><li><strong>100% Waterproof</strong></li><li><strong>Commercial-Grade Wear</strong></li><li><strong>Hyper-Realistic</strong></li><li><strong>Warmer Than Tile</strong></li><li><strong>UFH Compatible</strong></li></ul>",
        "Herringbone Flooring": "<h3>Key Features</h3><ul><li><strong>Iconic 45° Pattern</strong></li><li><strong>Creates Space</strong></li><li><strong>Wood &amp; LVT Options</strong></li><li><strong>Unique Character</strong></li><li><strong>Suits All Rooms</strong></li></ul>",
        "Vinyl Flooring": "<h3>Key Features</h3><ul><li><strong>Fully Waterproof</strong></li><li><strong>Tough Wear Surface</strong></li><li><strong>Cushioned</strong></li><li><strong>Easy to Clean</strong></li><li><strong>Realistic Designs</strong></li></ul>",
        "Carpet": "<h3>Key Features</h3><ul><li><strong>Luxuriously Soft</strong></li><li><strong>Sound Insulation</strong></li><li><strong>Thermal Properties</strong></li><li><strong>Wide Colour Range</strong></li><li><strong>Durable</strong></li></ul>",
        "Underlay": "<h3>Key Features</h3><ul><li><strong>Superior Cushioning</strong></li><li><strong>Sound Reduction</strong></li><li><strong>Thermal Insulation</strong></li><li><strong>Moisture Protection</strong></li><li><strong>Extends Floor Life</strong></li></ul>",
        "Accessories": "<h3>Key Features</h3><ul><li><strong>Professional Quality</strong></li><li><strong>Precision Engineered</strong></li><li><strong>Wide Compatibility</strong></li><li><strong>Easy Installation</strong></li><li><strong>Excellent Value</strong></li></ul>",
    }
    install = {
        "Solid Wood Flooring": "<h3>Installation</h3><p>Secret-nail or glue to suitable subfloor. Acclimatise 48-72 hours. Leave 15mm expansion gap. Professional installation recommended.</p>",
        "Engineered Wood Flooring": "<h3>Installation</h3><p>Floating click, secret-nail or glue-down. UFH compatible (max 27°C). Suitable over concrete or timber subfloors.</p>",
        "Laminate Flooring": "<h3>Installation</h3><p>Click-lock floating system — DIY-friendly. 10mm expansion gap around perimeter. Quality underlay required.</p>",
        "LVT Flooring": "<h3>Installation</h3><p>Click-lock, loose-lay or glue-down. Subfloor must be clean, dry, flat. UFH compatible. No acclimatisation required.</p>",
        "Herringbone Flooring": "<h3>Installation</h3><p>Requires careful planning — centre line and 45° angle must be calculated. Professional installation recommended.</p>",
        "Vinyl Flooring": "<h3>Installation</h3><p>Loose-laid, adhered or click-lock. Subfloor must be clean, dry, smooth. Expansion gap for floating installations.</p>",
        "Carpet": "<h3>Installation</h3><p>Professional fitting recommended. Always fit over quality underlay. Allow for pattern matching.</p>",
        "Underlay": "<h3>Installation</h3><p>Lay smooth-side down. Butt edges tightly. Tape all joins. Replace when new flooring is fitted.</p>",
        "Accessories": "<h3>Installation</h3><p>Refer to product packaging for guidelines. Contact our team for advice.</p>",
    }
    care = {
        "Solid Wood Flooring": "<h3>Care &amp; Maintenance</h3><p>Sweep or vacuum regularly. Clean with wood-specific cleaner on damp mop. Wipe spills immediately. Felt pads under furniture.</p>",
        "Engineered Wood Flooring": "<h3>Care &amp; Maintenance</h3><p>Sweep or vacuum regularly. Wood floor cleaner with damp mop. Avoid harsh chemicals and steam cleaners.</p>",
        "Laminate Flooring": "<h3>Care &amp; Maintenance</h3><p>Sweep or vacuum with soft brush. Damp mop with laminate cleaner. Never use steam cleaners.</p>",
        "LVT Flooring": "<h3>Care &amp; Maintenance</h3><p>Sweep or vacuum regularly. Mop with warm water and LVT cleaner. Avoid abrasive pads and solvents.</p>",
        "Herringbone Flooring": "<h3>Care &amp; Maintenance</h3><p>Care depends on material — wood needs wood cleaner, LVT can be mopped with warm water.</p>",
        "Vinyl Flooring": "<h3>Care &amp; Maintenance</h3><p>Sweep or vacuum regularly. Mop with warm water and mild cleaner. Resistant to most stains.</p>",
        "Carpet": "<h3>Care &amp; Maintenance</h3><p>Vacuum twice weekly in high-traffic areas. Blot (never rub) spills. Professional cleaning every 12-18 months.</p>",
        "Underlay": "<h3>Care &amp; Maintenance</h3><p>No ongoing maintenance once installed. Always replace when new flooring is fitted.</p>",
        "Accessories": "<h3>Care &amp; Maintenance</h3><p>Maintenance-free once installed. Wipe with damp cloth and mild detergent.</p>",
    }
    why_blk = "<h3>Why Factory Direct Flooring?</h3><p>UK's leading flooring specialist with competitive prices, free samples and expert advice. Full manufacturer's warranty and fast UK delivery on all orders.</p>"

    i = intros.get(cat,   f"<h2>{title}</h2><p>{title}. Quality flooring from Factory Direct Flooring.</p>")
    f = features.get(cat, "<h3>Features</h3><ul><li>High quality</li><li>Stylish design</li><li>Easy maintenance</li><li>Suitable for residential use</li><li>Excellent value</li></ul>")
    n = install.get(cat,  "<h3>Installation</h3><p>Refer to product specification.</p>")
    c = care.get(cat,     "<h3>Care</h3><p>Clean regularly with appropriate products.</p>")
    return f"{i}\n{f}\n{n}\n{c}\n{why_blk}"


# ──────────────────────────────────────────────────────────────────────
#  HELPERS
# ──────────────────────────────────────────────────────────────────────

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
    slug = re.sub(r"[^a-z0-9\-]", "", slug.lower()).strip("-")
    if slug and len(slug) > 4: return slug[:200]
    h = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return h[:200]


# ──────────────────────────────────────────────────────────────────────
#  CRITICAL: SLUG-BASED IMAGE MATCHING
# ──────────────────────────────────────────────────────────────────────

def get_all_page_images(html):
    """Extract ALL imagely image URLs from entire page HTML."""
    all_imgs = []
    seen     = set()

    patterns = [
        # imagely.factory-direct-flooring.co.uk URLs
        r'https://imagely\.factory-direct-flooring\.co\.uk/media/catalog/product/[^\s"\'<>,\)\\]+',
        # www.factory-direct-flooring.co.uk/media/catalog URLs
        r'https://www\.factory-direct-flooring\.co\.uk/media/catalog/product/[^\s"\'<>,\)\\]+',
        # Any /media/catalog/product/ URL (fallback)
        r'https?://[^\s"\'<>]+/media/catalog/product/[^\s"\'<>,\?]+',
    ]

    for pat in patterns:
        for m in re.finditer(pat, html, re.IGNORECASE):
            url = m.group(0).split("?")[0]  # strip query string
            # Only keep image files
            if not url.lower().endswith(('.jpg','.jpeg','.png','.webp','.gif')):
                continue
            if "placeholder" in url.lower():
                continue
            if url in seen: continue
            seen.add(url)
            all_imgs.append(url)

    return all_imgs


def extract_slug_keywords(slug):
    """Pull meaningful keywords from a product slug (URL ending)."""
    # e.g. "stoutland-oak-engineered" → ["stoutland", "oak", "engineered"]
    parts = slug.replace("-", " ").replace("_", " ").lower().split()
    return [p for p in parts if len(p) > 3]


def match_image_to_product(slug, all_images, used_images):
    """
    Find images that match this product's slug.
    Returns up to MAX_IMAGES matching images.
    """
    if not slug:
        return []

    keywords = extract_slug_keywords(slug)
    if not keywords:
        return []

    matched = []

    # Strategy 1: Image URL contains the FULL slug or major parts
    for img in all_images:
        if img in used_images: continue
        img_lower = img.lower()

        # Count how many keywords appear in the URL
        match_count = sum(1 for kw in keywords if kw in img_lower)

        # Require at least 1 keyword match for the FIRST keyword (most specific)
        if keywords[0] in img_lower or match_count >= 2:
            matched.append((match_count, img))

    # Sort by best match first
    matched.sort(key=lambda x: -x[0])
    result = [img for _, img in matched[:MAX_IMAGES]]

    return result


# ──────────────────────────────────────────────────────────────────────
#  PARSE CATEGORY PAGE
# ──────────────────────────────────────────────────────────────────────

def parse_category_page(html, cat_name):
    products = []
    seen     = set()

    # Get ALL images on the page first
    all_page_images = get_all_page_images(html)

    # Extract product list from JSON-LD
    products_lookup = []

    for block in re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL
    ):
        try:
            data  = json.loads(block.strip())
            items = data if isinstance(data, list) else [data]

            def process(p):
                if not isinstance(p, dict): return
                if p.get("@type") != "Product": return
                name = clean(p.get("name", ""))
                if not name or len(name) < 3: return

                real_desc = p.get("description", "")
                if real_desc: real_desc = clean(real_desc)

                brand = ""
                b = p.get("brand", {})
                if isinstance(b, dict):  brand = clean(b.get("name", ""))
                elif isinstance(b, str): brand = clean(b)

                price = ""; compare = ""; stock = "active"
                offers = p.get("offers", {})
                if isinstance(offers, list): offers = offers[0] if offers else {}
                if isinstance(offers, dict):
                    price   = str(offers.get("price", offers.get("lowPrice", "")))
                    hp      = offers.get("highPrice", "")
                    compare = str(hp) if hp and hp != price else ""
                    avail   = offers.get("availability", "")
                    stock   = "active" if "InStock" in avail else "draft"

                # JSON-LD might have image field too
                json_imgs = []
                imgs = p.get("image", [])
                if isinstance(imgs, str): imgs = [imgs]
                if isinstance(imgs, dict): imgs = [imgs.get("url","")]
                for img in imgs:
                    img_clean = str(img).split("?")[0]
                    if img_clean.startswith("http") and "catalog" in img_clean:
                        json_imgs.append(img_clean)

                url_str = p.get("url", "")
                slug    = url_str.rstrip("/").split("/")[-1].replace(".html","").lower() if url_str else ""

                products_lookup.append({
                    "name"      : name,
                    "sku"       : str(p.get("sku", "")),
                    "url"       : url_str,
                    "slug"      : slug,
                    "brand"     : brand,
                    "price"     : price,
                    "compare"   : compare,
                    "stock"     : stock,
                    "real_desc" : real_desc,
                    "json_imgs" : json_imgs,
                })

            for item in items:
                if not isinstance(item, dict): continue
                if item.get("@type") == "ItemList":
                    for el in item.get("itemListElement", []):
                        process(el.get("item", el))
                elif item.get("@type") == "Product":
                    process(item)
        except Exception:
            pass

    # Also find any other product URLs in the HTML (anchors)
    extra_slugs = {}
    for m in re.finditer(
        r'href=["\'](' + re.escape(BASE_URL) + r'/([a-z0-9][a-z0-9\-]+(?:\.html)?))["\']',
        html
    ):
        url  = m.group(1).rstrip("/")
        slug = m.group(2).replace(".html","").lower()
        # Skip category/navigation URLs
        skip = ['solid-wood-flooring','engineered-wood-flooring','laminate-flooring',
                'lvt-flooring','herringbone-flooring','vinyl-flooring','carpet',
                'underlay','accessories','blog','about','contact','search','cart',
                'account','wishlist','sitemap','privacy','terms','delivery']
        if slug in skip: continue
        if len(slug) < 5: continue
        extra_slugs[url] = slug

    # ── Now match images to products ──────────────────────────────────
    used_images = set()

    print(f"      Found {len(all_page_images)} images, {len(products_lookup)} products in JSON-LD")

    for p in products_lookup:
        if p["name"] in seen: continue
        seen.add(p["name"])

        images = []
        # First try JSON-LD images (most accurate)
        for img in p["json_imgs"]:
            if img not in used_images and img not in images:
                images.append(img)
                used_images.add(img)
                if len(images) >= MAX_IMAGES: break

        # Then try slug matching from page images
        if len(images) < MAX_IMAGES:
            matched = match_image_to_product(p["slug"], all_page_images, used_images)
            for img in matched:
                if img not in images:
                    images.append(img)
                    used_images.add(img)
                    if len(images) >= MAX_IMAGES: break

        products.append({
            "name"     : p["name"],
            "sku"      : p["sku"],
            "price"    : p["price"],
            "compare"  : p["compare"],
            "brand"    : p["brand"] or "Factory Direct Flooring",
            "category" : cat_name,
            "images"   : images,
            "stock"    : p["stock"],
            "url"      : p["url"],
            "real_desc": p["real_desc"],
        })

    return products


# ──────────────────────────────────────────────────────────────────────
#  SCRAPE ALL CATEGORIES
# ──────────────────────────────────────────────────────────────────────

def scrape_all():
    all_products = []
    seen_handles = set()

    for cat_name, cat_url in CATEGORIES:
        print(f"\n  [{cat_name}]")
        page    = 1
        cur_url = f"{cat_url}?product_list_limit=100"

        while True:
            print(f"    Page {page}...", end=" ", flush=True)
            html = fetch(cur_url)
            if not html:
                print("FAILED")
                break

            products = parse_category_page(html, cat_name)
            new = 0
            for p in products:
                handle = make_handle(p.get("url", ""), p["name"])
                if handle in seen_handles: continue
                seen_handles.add(handle)
                p["handle"] = handle
                all_products.append(p)
                new += 1

            imgs_total = sum(len(x["images"]) for x in all_products)
            print(f"+{new} prods | imgs:{imgs_total} | total:{len(all_products)}")

            if new == 0 and page > 1: break

            next_url = None
            rel = re.search(
                r'<link[^>]+rel=["\']next["\'][^>]+href=["\']([^"\']+)["\']', html
            )
            if rel:
                n = rel.group(1)
                next_url = n if n.startswith("http") else BASE_URL + n
            else:
                pg = re.search(
                    r'href=["\']([^"\']*[?&]p=' + str(page+1) + r'[^"\']*)["\']', html
                )
                if pg:
                    n = pg.group(1)
                    next_url = n if n.startswith("http") else BASE_URL + n

            if not next_url: break
            cur_url = next_url
            page   += 1
            time.sleep(DELAY)

    return all_products


# ──────────────────────────────────────────────────────────────────────
#  BUILD SHOPIFY CSV
# ──────────────────────────────────────────────────────────────────────

def build_rows(p):
    name = (p.get("name") or "").strip()
    if not name: return []

    handle  = p.get("handle") or make_handle(p.get("url", ""), name)
    cat     = p.get("category", "Flooring")

    real_desc = p.get("real_desc", "")
    if real_desc and len(real_desc) > 50:
        body = f"<h2>{name}</h2><p>{real_desc}</p>"
    else:
        body = auto_desc(name, cat)

    vendor  = p.get("brand", "").strip() or "Factory Direct Flooring"
    tags    = cat.lower().replace(" ", "-")
    images  = [i for i in p.get("images", []) if i and i.startswith("http")][:MAX_IMAGES]
    price   = price_fmt(p.get("price", "")) or "0.00"
    compare = price_fmt(p.get("compare", ""))
    seo_d   = f"Buy {name} at Factory Direct Flooring. {cat} at competitive prices. Free UK delivery and free samples. Order today."[:320]

    rows = []
    first_img = images[0] if images else ""

    row1 = {
        "Handle":handle,"Title":name,"Body (HTML)":body,"Vendor":vendor,
        "Product Category":"","Type":cat,"Tags":tags,"Published":"TRUE",
        "Option1 Name":"Title","Option1 Value":"Default Title",
        "Variant SKU":p.get("sku",""),"Variant Grams":"0",
        "Variant Inventory Tracker":"shopify","Variant Inventory Qty":"100",
        "Variant Inventory Policy":"deny","Variant Fulfillment Service":"manual",
        "Variant Price":price,"Variant Compare At Price":compare,
        "Variant Requires Shipping":"TRUE","Variant Taxable":"TRUE",
        "Image Src":"","Image Position":"","Image Alt Text":"",
        "SEO Title":f"{name} | Factory Direct Flooring"[:255],
        "SEO Description":seo_d,"Status":p.get("stock","active"),
    }
    if first_img and first_img.startswith("http"):
        row1["Image Src"]      = first_img
        row1["Image Position"] = "1"
        row1["Image Alt Text"] = name
    rows.append(row1)

    for i, img in enumerate(images[1:], 2):
        if not img or not img.startswith("http"): continue
        e = {k:"" for k in SHOPIFY_COLS}
        e.update({"Handle":handle,"Image Src":img,"Image Position":str(i),"Image Alt Text":name})
        rows.append(e)

    return rows


# ──────────────────────────────────────────────────────────────────────
#  MAIN
# ──────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    t0 = time.time()

    print("\n" + "="*65)
    print("  FACTORY DIRECT FLOORING — FINAL SCRAPER (SLUG-MATCH)")
    print(f"  Strategy: Match imagely images to products by URL slug")
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
            "name":p["name"],"handle":p.get("handle",""),"sku":p["sku"],
            "price":p["price"],"category":p["category"],
            "images":p["images"],"img_count":len(p["images"]),
            "url":p.get("url",""),
        } for p in products], f, ensure_ascii=False, indent=2)

    elapsed  = round(time.time() - t0)
    with_img = len([p for p in products if p["images"]])
    cats = {}
    for p in products:
        cats[p["category"]] = cats.get(p["category"], 0) + 1

    print(f"\n{'='*65}")
    print(f"  DONE in {elapsed//60}m {elapsed%60:02d}s")
    print(f"  Products    : {len(products)}")
    print(f"  CSV rows    : {len(all_rows)}")
    print(f"  With images : {with_img} ({round(with_img/len(products)*100) if products else 0}%)")
    print(f"\n  By category (use in Smart Collections → 'Product type is equal to'):")
    for cat,count in sorted(cats.items(),key=lambda x:-x[1]):
        print(f"    {cat:<35} {count:>3}")
    print(f"\n  CSV → {csv_file}")
    print(f"  Shopify → Products → Import → Upload CSV")
    print("="*65)


if __name__ == "__main__":
    main()
