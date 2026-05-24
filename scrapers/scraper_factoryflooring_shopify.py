"""
FACTORY DIRECT FLOORING — COMPLETE FINAL SCRAPER v2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Phase 1: Collect product URLs from category pages
Phase 2: Fetch EACH product page individually (parallel)
         → each page has its OWN images in JSON-LD + HTML
Phase 3: Download images from imagely CDN → local files
Phase 4: Build Shopify CSV with GitHub Pages image URLs
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import requests, json, csv, re, os, time
from datetime import datetime
from html import unescape
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL      = "https://www.factory-direct-flooring.co.uk"
OUTPUT_DIR    = "output"
IMAGES_DIR    = "output/images"
TIMESTAMP     = datetime.now().strftime("%Y%m%d_%H%M%S")
MAX_IMAGES    = 3   # max images per product
THREADS       = 8   # parallel product page fetches
DELAY         = 0.3

GITHUB_USER   = "shadiikart-prog"
GITHUB_REPO   = "product-scraper-demo"
IMAGES_BASE   = f"https://{GITHUB_USER}.github.io/{GITHUB_REPO}/scrapers/output/images"

HEADERS = {
    "User-Agent"      : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept"          : "text/html,application/xhtml+xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language" : "en-GB,en;q=0.9",
    "Accept-Encoding" : "gzip, deflate, br",
    "Connection"      : "keep-alive",
    "Cache-Control"   : "no-cache",
}
IMG_HEADERS = {
    "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept"     : "image/webp,image/apng,image/*,*/*;q=0.8",
    "Referer"    : "https://www.factory-direct-flooring.co.uk/",
}

CATEGORIES = [
    ("Solid Wood",      f"{BASE_URL}/solid-wood-flooring"),
    ("Engineered Wood", f"{BASE_URL}/engineered-wood-flooring"),
    ("Laminate",        f"{BASE_URL}/laminate-flooring"),
    ("LVT",             f"{BASE_URL}/lvt-flooring"),
    ("Herringbone",     f"{BASE_URL}/herringbone-flooring"),
    ("Vinyl",           f"{BASE_URL}/vinyl-flooring"),
    ("Carpet",          f"{BASE_URL}/carpet"),
    ("Underlay",        f"{BASE_URL}/underlay"),
    ("Accessories",     f"{BASE_URL}/accessories"),
]

SHOPIFY_COLS = [
    "Handle","Title","Body (HTML)","Vendor","Product Category","Type",
    "Tags","Published","Option1 Name","Option1 Value","Variant SKU",
    "Variant Grams","Variant Inventory Tracker","Variant Inventory Qty",
    "Variant Inventory Policy","Variant Fulfillment Service","Variant Price",
    "Variant Compare At Price","Variant Requires Shipping","Variant Taxable",
    "Image Src","Image Position","Image Alt Text","SEO Title","SEO Description","Status",
]

# ── Descriptions ──────────────────────────────────────────────────────────────

def hints(title):
    tl = title.lower()
    brands = ['karndean','kahrs','quick-step','quickstep','amtico','ted todd','elka',
              'wicanders','moduleo','polyflor','tarkett','egger','krono','balterio',
              'camaro','boen','hakwood','woodpecker','lifestyle','rhinofloor','altro']
    brand  = next((b.title() for b in brands if b in tl), "")
    thick  = re.search(r'(\d+(?:\.\d+)?)\s*mm', title, re.I)
    ts     = thick.group(1)+"mm" if thick else ""
    cols   = ['oak','walnut','pine','ash','maple','birch','cherry','white','grey','gray',
              'black','brown','beige','cream','ivory','natural','smoked','rustic','aged',
              'blond','golden','silver','slate','stone','marble','teak','ebony']
    colour = next((c.title() for c in cols if c in tl), "")
    return brand, ts, colour

def build_desc(title, cat):
    brand, thick, colour = hints(title)
    bl = f" by <strong>{brand}</strong>" if brand else " from Factory Direct Flooring"
    cl = f" in a beautiful <strong>{colour}</strong> finish" if colour else ""
    tl = f" with a <strong>{thick}</strong> thickness" if thick else ""

    data = {
        "Solid Wood": (
            f"<h2>{title}</h2><p>Introducing the <strong>{title}</strong>{bl}{cl}{tl}. Crafted from 100% genuine solid timber, this floor delivers unmatched natural beauty and authentic character that only improves with age. A true lifetime investment.</p>",
            "<h3>Key Features</h3><ul><li><strong>100% Real Solid Timber</strong> — unique grain in every plank</li><li><strong>Sand &amp; Refinish 5x</strong> — lasts a lifetime</li><li><strong>Natural Insulator</strong> — warm underfoot, lower bills</li><li><strong>Adds Property Value</strong> — proven ROI</li><li><strong>Sustainably Sourced</strong> — certified forests</li></ul>",
            "<h3>Installation</h3><p>Secret-nail or glue to suitable subfloor. Acclimatise 48–72 hrs. Leave 15mm expansion gap. Professional installation recommended.</p>",
            "<h3>Care &amp; Maintenance</h3><p>Sweep or vacuum with soft brush. Wood-specific cleaner on lightly damp mop. Wipe spills immediately. Felt pads under furniture.</p>",
        ),
        "Engineered Wood": (
            f"<h2>{title}</h2><p>Discover the <strong>{title}</strong>{bl}{cl}{tl}. Real wood top layer with multi-layer core — authentic timber beauty with superior stability. Compatible with underfloor heating throughout your home.</p>",
            "<h3>Key Features</h3><ul><li><strong>Real Wood Surface</strong> — genuine timber veneer</li><li><strong>Stable Core</strong> — resists warping &amp; shrinking</li><li><strong>UFH Compatible</strong> — wet and electric systems</li><li><strong>All Floor Levels</strong> — ground to basement</li><li><strong>Flexible Fitting</strong> — click, nail or glue</li></ul>",
            "<h3>Installation</h3><p>Floating click, secret-nail or glue-down. UFH max 27°C. Over concrete or timber. Acclimatise 48 hrs.</p>",
            "<h3>Care &amp; Maintenance</h3><p>Sweep regularly. Wood floor cleaner on damp mop. No steam cleaners or excess water. Wipe spills promptly.</p>",
        ),
        "Laminate": (
            f"<h2>{title}</h2><p>Meet the <strong>{title}</strong>{bl}{cl}{tl}. Authentic wood or stone look at a fraction of the cost — scratch-resistant and practical for busy family homes.</p>",
            "<h3>Key Features</h3><ul><li><strong>HD Surface Layer</strong> — photorealistic appearance</li><li><strong>AC-Rated Scratch Resistance</strong> — handles heavy use</li><li><strong>Easy Click Fit</strong> — DIY-friendly</li><li><strong>V-Groove Edges</strong> — realistic depth</li><li><strong>Low Maintenance</strong> — sweep and mop</li></ul>",
            "<h3>Installation</h3><p>Click-lock floating over most existing floors. 10mm expansion gap. Quality underlay required unless pre-attached.</p>",
            "<h3>Care &amp; Maintenance</h3><p>Soft brush vacuum. Well-wrung damp mop with laminate cleaner. No steam or excess water.</p>",
        ),
        "LVT": (
            f"<h2>{title}</h2><p>Introducing the <strong>{title}</strong>{bl}{cl}{tl}. 100% waterproof luxury vinyl — hyper-realistic wood and stone designs with commercial-grade durability for every room.</p>",
            "<h3>Key Features</h3><ul><li><strong>100% Waterproof</strong> — all rooms including bathrooms</li><li><strong>Commercial Wear Layer</strong> — resists heavy traffic</li><li><strong>Realistic Embossed</strong> — wood and stone replica</li><li><strong>Warmer Than Tile</strong> — comfortable underfoot</li><li><strong>UFH Compatible</strong> — max 27°C</li></ul>",
            "<h3>Installation</h3><p>Click-lock, loose-lay or glue-down. Subfloor clean, dry, flat. No acclimatisation needed.</p>",
            "<h3>Care &amp; Maintenance</h3><p>Sweep to remove grit. Warm water and LVT cleaner. No abrasive pads or solvents.</p>",
        ),
        "Herringbone": (
            f"<h2>{title}</h2><p>Make a bold statement with the <strong>{title}</strong>{bl}{cl}{tl}. The iconic herringbone pattern adds instant elegance and depth — available in wood and LVT for every budget.</p>",
            "<h3>Key Features</h3><ul><li><strong>Iconic 45° Pattern</strong> — instant elegance</li><li><strong>Creates Space</strong> — enlarges any room visually</li><li><strong>Wood &amp; LVT Options</strong> — real or waterproof</li><li><strong>Unique Character</strong> — every floor is individual</li><li><strong>All Rooms</strong> — hall to bedroom</li></ul>",
            "<h3>Installation</h3><p>Mark centre line and 45° angle precisely before laying. Professional installation recommended.</p>",
            "<h3>Care &amp; Maintenance</h3><p>Wood type: wood cleaner on damp mop. LVT type: warm water and LVT cleaner.</p>",
        ),
        "Vinyl": (
            f"<h2>{title}</h2><p>Presenting the <strong>{title}</strong>{bl}{cl}{tl}. Premium fully waterproof vinyl — ideal for kitchens, bathrooms, hallways and any high-traffic area in your home.</p>",
            "<h3>Key Features</h3><ul><li><strong>Fully Waterproof</strong> — kitchens, bathrooms, wet rooms</li><li><strong>Tough Surface</strong> — resists scratches and scuffs</li><li><strong>Cushioned Underfoot</strong> — warm and quiet</li><li><strong>Easy Clean</strong> — resistant to most stains</li><li><strong>Realistic Designs</strong> — HD wood and stone effects</li></ul>",
            "<h3>Installation</h3><p>Loose-laid, adhered or click-lock. Clean dry smooth subfloor. Expansion gap for floating.</p>",
            "<h3>Care &amp; Maintenance</h3><p>Sweep regularly. Warm water and mild cleaner. Resistant to bacteria and stains.</p>",
        ),
        "Carpet": (
            f"<h2>{title}</h2><p>Transform your home with the <strong>{title}</strong>{bl}{cl}. Luxuriously soft and warm underfoot — perfect for bedrooms, living rooms and stairs with excellent sound insulation.</p>",
            "<h3>Key Features</h3><ul><li><strong>Soft Underfoot</strong> — warm and comfortable</li><li><strong>Sound Insulation</strong> — reduces noise between floors</li><li><strong>Thermal Properties</strong> — lowers energy costs</li><li><strong>Wide Range</strong> — colours and textures to suit all décor</li><li><strong>Durable</strong> — built for family life</li></ul>",
            "<h3>Installation</h3><p>Professional fitting recommended. Always fit over quality underlay. Allow for pattern matching.</p>",
            "<h3>Care &amp; Maintenance</h3><p>Vacuum twice weekly. Blot (never rub) spills. Professional cleaning every 12–18 months.</p>",
        ),
        "Underlay": (
            f"<h2>{title}</h2><p>The <strong>{title}</strong>{bl} — professional underlay for enhanced comfort, sound insulation and extended floor life. The right underlay is as important as the floor itself.</p>",
            "<h3>Key Features</h3><ul><li><strong>Superior Cushioning</strong></li><li><strong>Sound Reduction</strong></li><li><strong>Thermal Insulation</strong></li><li><strong>Moisture Protection</strong></li><li><strong>Extends Floor Life</strong></li></ul>",
            "<h3>Installation</h3><p>Lay smooth-side down. Butt edges tightly. Tape all joins. Replace when fitting new flooring.</p>",
            "<h3>Care</h3><p>No maintenance once installed. Always replace with new when fitting new floors.</p>",
        ),
        "Accessories": (
            f"<h2>{title}</h2><p>Complete your installation with the <strong>{title}</strong>{bl}. Quality finishing accessories for a truly professional result.</p>",
            "<h3>Key Features</h3><ul><li><strong>Professional Quality</strong></li><li><strong>Wide Compatibility</strong></li><li><strong>Easy Installation</strong></li><li><strong>Excellent Value</strong></li></ul>",
            "<h3>Installation</h3><p>Refer to packaging guidelines. Contact our team for advice.</p>",
            "<h3>Care</h3><p>Maintenance-free once installed.</p>",
        ),
    }
    default = (
        f"<h2>{title}</h2><p>{title}{bl}. Quality flooring from Factory Direct Flooring.</p>",
        "<h3>Features</h3><ul><li>High quality</li><li>Stylish design</li><li>Easy maintenance</li></ul>",
        "<h3>Installation</h3><p>Refer to product specification.</p>",
        "<h3>Care</h3><p>Clean with appropriate products.</p>",
    )
    i, f, n, c = data.get(cat, default)
    why = "<h3>Why Factory Direct Flooring?</h3><p>UK's trusted flooring specialist — premium quality at factory direct prices. Free samples, expert advice, full warranties and fast UK delivery.</p>"
    return f"{i}\n{f}\n{n}\n{c}\n{why}"

# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch(url, timeout=20):
    for _ in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
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
    return re.sub(r"[^a-z0-9]+","-",name.lower()).strip("-")[:200]

def safe_fname(url, idx):
    fname = os.path.basename(urlparse(url).path)
    fname = re.sub(r'[^a-zA-Z0-9\._\-]','_',fname)
    if not fname or len(fname) < 4: fname = f"img_{idx}.jpg"
    return fname

IMAGELY_RE = re.compile(
    r'https://imagely\.factory-direct-flooring\.co\.uk'
    r'/media/catalog/product/[^\s"\'<>\)\\,]+',
    re.I
)

def extract_images_from_html(html):
    """Extract ALL imagely CDN image URLs from any HTML page."""
    found = []
    seen  = set()

    # 1. Direct imagely URL regex — catches URLs in any context
    for m in IMAGELY_RE.finditer(html):
        u = m.group(0).split("?")[0].strip()
        if u not in seen and len(u) > 60:
            seen.add(u)
            found.append(u)

    # 2. JSON-LD image fields
    for block in re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL
    ):
        try:
            data = json.loads(block)
            def walk(obj):
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        if k in ("image","thumbnail","url") and isinstance(v, str):
                            if "imagely" in v and "catalog/product" in v:
                                u = v.split("?")[0]
                                if u not in seen and len(u)>60:
                                    seen.add(u); found.append(u)
                        else:
                            walk(v)
                elif isinstance(obj, list):
                    for item in obj: walk(item)
            walk(data)
        except: pass

    # 3. srcset parsing
    for m in re.finditer(r'srcset=["\']([^"\']+)["\']', html):
        for part in m.group(1).split(","):
            u = part.strip().split(" ")[0]
            if "imagely" in u and "catalog/product" in u:
                u = u.split("?")[0]
                if u not in seen and len(u) > 60:
                    seen.add(u); found.append(u)

    return found

# ── Phase 1: Collect product URLs from category pages ────────────────────────

def collect_urls():
    """Collect all product page URLs from all category pages."""
    all_urls = {}  # url -> category

    SKIP = {
        'solid-wood-flooring','engineered-wood-flooring','laminate-flooring',
        'lvt-flooring','herringbone-flooring','vinyl-flooring','carpet',
        'underlay','accessories','blog','about','contact','brands',
        'advice','advice-centre','search','checkout','cart','account',
        'wishlist','compare','sitemap','privacy','terms','delivery',
        'returns','finance','trade','samples','customer-service',
    }

    for cat, base_url in CATEGORIES:
        print(f"  [{cat}] collecting URLs...", end=" ", flush=True)
        page    = 1
        cur_url = f"{base_url}?product_list_limit=100"
        found   = 0

        while True:
            html = fetch(cur_url, timeout=25)
            if not html:
                # Try without limit param
                html = fetch(base_url, timeout=25)
                if not html:
                    print(f"FAILED")
                    break

            # Extract product URLs from anchor tags
            for m in re.finditer(
                r'href=["\'](' + re.escape(BASE_URL) + r'/([a-z0-9][a-z0-9\-]+(?:\.html)?))["\']',
                html
            ):
                url  = m.group(1).rstrip("/")
                slug = m.group(2).replace(".html","").lower()
                if slug in SKIP or url in all_urls: continue
                if len(slug) < 4: continue
                all_urls[url] = cat
                found += 1

            # Next page?
            nxt = None
            rel = re.search(r'<link[^>]+rel=["\']next["\'][^>]+href=["\']([^"\']+)["\']', html)
            if rel:
                n = rel.group(1)
                nxt = n if n.startswith("http") else BASE_URL + n
            else:
                pg = re.search(r'href=["\']([^"\']*[?&]p='+str(page+1)+r'[^"\']*)["\']', html)
                if pg:
                    n = pg.group(1)
                    nxt = n if n.startswith("http") else BASE_URL + n

            if not nxt: break
            cur_url = next
            page   += 1
            time.sleep(DELAY)

        print(f"+{found} URLs (total: {len(all_urls)})")

    return all_urls  # {url: category}

# ── Phase 2: Scrape each product page individually ───────────────────────────

def scrape_product_page(url, cat):
    """Fetch ONE product page and extract all its data."""
    html = fetch(url, timeout=15)
    if not html: return None

    product = {
        "url"   : url,
        "name"  : "",
        "sku"   : "",
        "price" : "",
        "compare":"",
        "brand" : "",
        "cat"   : cat,
        "stock" : "active",
        "images": [],
        "desc"  : "",
    }

    # JSON-LD — most reliable source
    for block in re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL
    ):
        try:
            data  = json.loads(block)
            items = data if isinstance(data, list) else [data]
            for item in items:
                if not isinstance(item, dict): continue
                if item.get("@type") != "Product": continue

                product["name"] = clean(item.get("name",""))
                product["sku"]  = str(item.get("sku",""))
                product["desc"] = item.get("description","")

                b = item.get("brand",{})
                if isinstance(b,dict): product["brand"] = clean(b.get("name",""))
                elif isinstance(b,str): product["brand"] = clean(b)

                offers = item.get("offers",{})
                if isinstance(offers,list): offers = offers[0] if offers else {}
                if isinstance(offers,dict):
                    product["price"]   = str(offers.get("price",offers.get("lowPrice","")))
                    hp                 = offers.get("highPrice","")
                    product["compare"] = str(hp) if hp and hp!=product["price"] else ""
                    avail              = offers.get("availability","")
                    product["stock"]   = "active" if "InStock" in avail else "draft"
        except: pass

    # HTML fallback for name
    if not product["name"]:
        m = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
        if m: product["name"] = clean(m.group(1))

    # HTML fallback for price
    if not product["price"]:
        m = re.search(r'<span[^>]*class="[^"]*price[^"]*"[^>]*>\s*£\s*([\d,]+\.?\d*)', html, re.I)
        if m: product["price"] = m.group(1).replace(",","")

    # Extract ALL imagely images from this page
    product["images"] = extract_images_from_html(html)[:MAX_IMAGES]

    return product if product["name"] else None


def scrape_all_products(url_map):
    """Scrape all product pages in parallel."""
    results = []
    failed  = 0
    done    = 0
    total   = len(url_map)
    url_list = list(url_map.items())

    print(f"\n  Scraping {total} product pages ({THREADS} threads)...")

    with ThreadPoolExecutor(max_workers=THREADS) as ex:
        futures = {ex.submit(scrape_product_page, url, cat): url
                   for url, cat in url_list}

        for future in as_completed(futures):
            done += 1
            try:
                r = future.result()
                if r: results.append(r)
                else: failed += 1
            except: failed += 1

            if done % 30 == 0 or done == total:
                imgs = sum(len(p["images"]) for p in results)
                print(f"  [{done:>4}/{total}] OK:{len(results)} Fail:{failed} Images:{imgs}")

            time.sleep(0.1)

    return results

# ── Phase 3: Download images ─────────────────────────────────────────────────

def download_images(products):
    os.makedirs(IMAGES_DIR, exist_ok=True)
    dl = 0; fail = 0; idx = 0

    print(f"\n  Downloading images to {IMAGES_DIR}/...")

    for p in products:
        local_urls = []
        for img_url in p["images"]:
            idx    += 1
            fname   = safe_fname(img_url, idx)
            lpath   = os.path.join(IMAGES_DIR, fname)
            gh_url  = f"{IMAGES_BASE}/{fname}"

            if os.path.exists(lpath) and os.path.getsize(lpath) > 1000:
                local_urls.append(gh_url)
                dl += 1
                continue

            try:
                r = requests.get(img_url, headers=IMG_HEADERS, timeout=15, stream=True)
                if r.status_code == 200:
                    with open(lpath, 'wb') as f:
                        for chunk in r.iter_content(8192): f.write(chunk)
                    if os.path.getsize(lpath) > 1000:
                        local_urls.append(gh_url)
                        dl += 1
                    else:
                        os.remove(lpath); fail += 1
                else:
                    fail += 1
            except:
                fail += 1
            time.sleep(0.15)

        p["local_images"] = local_urls

    print(f"  Downloaded: {dl} | Failed: {fail}")
    return products

# ── Phase 4: Build Shopify CSV ────────────────────────────────────────────────

def build_rows(p):
    name = (p.get("name") or "").strip()
    if not name: return []

    handle  = make_handle(p.get("url",""), name)
    cat     = p.get("cat","Flooring")
    real_d  = p.get("desc","")
    if real_d and len(clean(real_d)) > 80:
        body = f"<h2>{name}</h2><p>{clean(real_d)}</p>"
    else:
        body = build_desc(name, cat)

    vendor  = p.get("brand","").strip() or "Factory Direct Flooring"
    tags    = cat.lower().replace(" ","-")
    images  = [i for i in p.get("local_images",[]) if i and i.startswith("http")]
    price   = price_fmt(p.get("price","")) or "0.00"
    compare = price_fmt(p.get("compare",""))
    first   = images[0] if images else ""
    seo_d   = f"Buy {name} at Factory Direct Flooring. {cat} at competitive prices. Free UK delivery and expert advice."[:320]

    rows = []
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
    if first:
        row1["Image Src"]      = first
        row1["Image Position"] = "1"
        row1["Image Alt Text"] = name
    rows.append(row1)

    for i, img in enumerate(images[1:], 2):
        e = {k:"" for k in SHOPIFY_COLS}
        e.update({"Handle":handle,"Image Src":img,
                  "Image Position":str(i),"Image Alt Text":name})
        rows.append(e)
    return rows

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(IMAGES_DIR, exist_ok=True)
    t0 = time.time()

    print("\n" + "="*65)
    print("  FACTORY DIRECT FLOORING — COMPLETE SCRAPER v2")
    print("  Each product page scraped individually for its own images")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*65)

    # Phase 1 — collect URLs
    print("\n[Phase 1] Collecting product URLs from category pages...")
    url_map = collect_urls()
    print(f"  Total unique product URLs: {len(url_map)}")

    if not url_map:
        print("  No URLs found!"); return

    # Phase 2 — scrape each product page
    print("\n[Phase 2] Scraping individual product pages...")
    products = scrape_all_products(url_map)
    print(f"  Scraped: {len(products)} products")

    # Phase 3 — download images
    print("\n[Phase 3] Downloading images from imagely CDN...")
    products = download_images(products)

    # Phase 4 — build CSV
    print("\n[Phase 4] Building Shopify CSV...")
    all_rows = []
    for p in products:
        all_rows.extend(build_rows(p))

    csv_file = f"{OUTPUT_DIR}/factory_flooring_shopify_{TIMESTAMP}.csv"
    with open(csv_file,"w",encoding="utf-8-sig",newline="") as f:
        writer = csv.DictWriter(f,fieldnames=SHOPIFY_COLS,extrasaction="ignore")
        writer.writeheader(); writer.writerows(all_rows)

    json_file = f"{OUTPUT_DIR}/factory_flooring_{TIMESTAMP}.json"
    with open(json_file,"w",encoding="utf-8") as f:
        json.dump([{
            "name":p["name"],"sku":p["sku"],"price":p["price"],
            "cat":p["cat"],"images":p.get("local_images",[]),
            "img_count":len(p.get("local_images",[])),
            "url":p.get("url",""),
        } for p in products], f, ensure_ascii=False, indent=2)

    elapsed  = round(time.time()-t0)
    with_img = len([p for p in products if p.get("local_images")])
    cats = {}
    for p in products: cats[p["cat"]] = cats.get(p["cat"],0)+1

    print(f"\n{'='*65}")
    print(f"  DONE in {elapsed//60}m {elapsed%60:02d}s")
    print(f"  Products      : {len(products)}")
    print(f"  CSV rows      : {len(all_rows)}")
    print(f"  With images   : {with_img} ({round(with_img/len(products)*100) if products else 0}%)")
    print(f"\n  By category (use in Smart Collections → Product type is equal to):")
    for cat,count in sorted(cats.items(),key=lambda x:-x[1]):
        print(f"    {cat:<20} {count:>3}")
    print(f"\n  CSV  → {csv_file}")
    print(f"  Imgs → {IMAGES_DIR}/")
    print(f"  Shopify → Products → Import → Upload CSV")
    print("="*65)

if __name__=="__main__":
    main()
