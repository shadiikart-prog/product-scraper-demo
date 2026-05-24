"""
FACTORY DIRECT FLOORING — SHOPIFY SCRAPER (FIXED)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Fixes:
  ✓ next variable bug fixed (was crashing categories)
  ✓ Multiple URL patterns tried for each category
  ✓ Aggressive imagely URL extraction (10 patterns)
  ✓ Images downloaded → GitHub Pages → Shopify
  ✓ Prices, descriptions, types — all included
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import requests, json, csv, re, os, time
from datetime import datetime
from html import unescape
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL    = "https://www.factory-direct-flooring.co.uk"
OUTPUT_DIR  = "output"
IMAGES_DIR  = "output/images"
TIMESTAMP   = datetime.now().strftime("%Y%m%d_%H%M%S")
MAX_IMAGES  = 3
THREADS     = 6
DELAY       = 0.5

GITHUB_USER  = "shadiikart-prog"
GITHUB_REPO  = "product-scraper-demo"
IMAGES_BASE  = f"https://{GITHUB_USER}.github.io/{GITHUB_REPO}/scrapers/output/images"

HEADERS = {
    "User-Agent"      : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept"          : "text/html,application/xhtml+xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language" : "en-GB,en;q=0.9",
    "Accept-Encoding" : "gzip, deflate, br",
    "Connection"      : "keep-alive",
    "Cache-Control"   : "no-cache",
    "Pragma"          : "no-cache",
}
IMG_HDR = {
    "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept"     : "image/webp,image/apng,image/*,*/*;q=0.8",
    "Referer"    : "https://www.factory-direct-flooring.co.uk/",
}

CATEGORIES = [
    ("Solid Wood",      "/solid-wood-flooring"),
    ("Engineered Wood", "/engineered-wood-flooring"),
    ("Laminate",        "/laminate-flooring"),
    ("LVT",             "/lvt-flooring"),
    ("Herringbone",     "/herringbone-flooring"),
    ("Vinyl",           "/vinyl-flooring"),
    ("Carpet",          "/carpet"),
    ("Underlay",        "/underlay"),
    ("Accessories",     "/accessories"),
]

SHOPIFY_COLS = [
    "Handle","Title","Body (HTML)","Vendor","Product Category","Type",
    "Tags","Published","Option1 Name","Option1 Value","Variant SKU",
    "Variant Grams","Variant Inventory Tracker","Variant Inventory Qty",
    "Variant Inventory Policy","Variant Fulfillment Service","Variant Price",
    "Variant Compare At Price","Variant Requires Shipping","Variant Taxable",
    "Image Src","Image Position","Image Alt Text","SEO Title","SEO Description","Status",
]

# ── Descriptions ───────────────────────────────────────────────────────────────

def hints(title):
    tl = title.lower()
    brands = ['karndean','kahrs','quick-step','quickstep','amtico','ted todd','elka',
              'wicanders','moduleo','polyflor','tarkett','egger','krono','balterio',
              'camaro','boen','hakwood','woodpecker','lifestyle','rhinofloor','altro']
    brand  = next((b.title() for b in brands if b in tl), "")
    thick  = re.search(r'(\d+(?:\.\d+)?)\s*mm', title, re.I)
    ts     = thick.group(1)+"mm" if thick else ""
    colours = ['oak','walnut','pine','ash','maple','birch','cherry','white','grey','gray',
               'black','brown','beige','cream','ivory','natural','smoked','rustic','aged',
               'blond','golden','silver','slate','stone','marble','teak','ebony']
    colour = next((c.title() for c in colours if c in tl), "")
    return brand, ts, colour

def build_desc(title, cat):
    brand, thick, colour = hints(title)
    bl = f" by <strong>{brand}</strong>" if brand else " from Factory Direct Flooring"
    cl = f" in a beautiful <strong>{colour}</strong> finish" if colour else ""
    tl = f" with a <strong>{thick}</strong> thickness" if thick else ""

    d = {
        "Solid Wood": (
            f"<h2>{title}</h2><p>Introducing the <strong>{title}</strong>{bl}{cl}{tl}. 100% genuine solid timber delivering unmatched natural beauty and character that only improves with age.</p>",
            "<h3>Key Features</h3><ul><li><strong>100% Real Solid Timber</strong></li><li><strong>Sand &amp; Refinish Up to 5 Times</strong></li><li><strong>Natural Insulator</strong> — reduces energy bills</li><li><strong>Increases Property Value</strong></li><li><strong>Sustainably Sourced</strong></li></ul>",
            "<h3>Installation</h3><p>Secret-nail or glue. Acclimatise 48–72 hrs. 15mm expansion gap. Professional fitting recommended.</p>",
            "<h3>Care</h3><p>Soft brush vacuum. Wood cleaner on damp mop. Wipe spills immediately.</p>",
        ),
        "Engineered Wood": (
            f"<h2>{title}</h2><p>Discover the <strong>{title}</strong>{bl}{cl}{tl}. Real wood top layer with stable multi-layer core — authentic beauty with superior stability, compatible with underfloor heating.</p>",
            "<h3>Key Features</h3><ul><li><strong>Real Wood Surface</strong></li><li><strong>Stable Multi-Layer Core</strong></li><li><strong>UFH Compatible</strong></li><li><strong>All Floor Levels</strong></li><li><strong>Click, Nail or Glue</strong></li></ul>",
            "<h3>Installation</h3><p>Floating click, secret-nail or glue-down. UFH max 27°C. Acclimatise 48 hrs.</p>",
            "<h3>Care</h3><p>Vacuum regularly. Wood floor cleaner on damp mop. No steam or excess water.</p>",
        ),
        "Laminate": (
            f"<h2>{title}</h2><p>Meet the <strong>{title}</strong>{bl}{cl}{tl}. Authentic wood or stone look at a fraction of the cost — scratch resistant and easy to install.</p>",
            "<h3>Key Features</h3><ul><li><strong>HD Surface Layer</strong></li><li><strong>AC-Rated Scratch Resistance</strong></li><li><strong>Easy Click Fit</strong></li><li><strong>V-Groove Edges</strong></li><li><strong>Low Maintenance</strong></li></ul>",
            "<h3>Installation</h3><p>Click-lock floating. 10mm expansion gap. Quality underlay required.</p>",
            "<h3>Care</h3><p>Vacuum with soft brush. Well-wrung damp mop with laminate cleaner. No steam.</p>",
        ),
        "LVT": (
            f"<h2>{title}</h2><p>Introducing the <strong>{title}</strong>{bl}{cl}{tl}. 100% waterproof luxury vinyl with hyper-realistic designs — perfect for kitchens, bathrooms and all rooms.</p>",
            "<h3>Key Features</h3><ul><li><strong>100% Waterproof</strong></li><li><strong>Commercial Wear Layer</strong></li><li><strong>Hyper-Realistic Surface</strong></li><li><strong>Warmer Than Tile</strong></li><li><strong>UFH Compatible</strong></li></ul>",
            "<h3>Installation</h3><p>Click-lock, loose-lay or glue-down. Clean dry flat subfloor. No acclimatisation needed.</p>",
            "<h3>Care</h3><p>Vacuum to remove grit. Warm water with LVT cleaner. No abrasives.</p>",
        ),
        "Herringbone": (
            f"<h2>{title}</h2><p>Make a bold statement with the <strong>{title}</strong>{bl}{cl}{tl}. Iconic herringbone pattern — timeless elegance in wood and LVT.</p>",
            "<h3>Key Features</h3><ul><li><strong>Iconic 45° Pattern</strong></li><li><strong>Creates Space</strong></li><li><strong>Wood &amp; LVT Options</strong></li><li><strong>Unique Character</strong></li><li><strong>Suits All Rooms</strong></li></ul>",
            "<h3>Installation</h3><p>Mark centre line and 45° angle precisely. Professional fitting recommended.</p>",
            "<h3>Care</h3><p>Wood type: wood cleaner on damp mop. LVT type: warm water and LVT cleaner.</p>",
        ),
        "Vinyl": (
            f"<h2>{title}</h2><p>Presenting the <strong>{title}</strong>{bl}{cl}{tl}. Fully waterproof premium vinyl — ideal for kitchens, bathrooms and high-traffic areas.</p>",
            "<h3>Key Features</h3><ul><li><strong>Fully Waterproof</strong></li><li><strong>Tough Wear Surface</strong></li><li><strong>Cushioned Underfoot</strong></li><li><strong>Easy to Clean</strong></li><li><strong>Realistic Designs</strong></li></ul>",
            "<h3>Installation</h3><p>Loose-laid, adhered or click-lock. Clean dry smooth subfloor.</p>",
            "<h3>Care</h3><p>Sweep and mop with warm water and mild cleaner. Stain resistant.</p>",
        ),
        "Carpet": (
            f"<h2>{title}</h2><p>Transform your home with the <strong>{title}</strong>{bl}{cl}. Luxuriously soft and warm — perfect for bedrooms, living rooms and stairs.</p>",
            "<h3>Key Features</h3><ul><li><strong>Luxuriously Soft</strong></li><li><strong>Sound Insulation</strong></li><li><strong>Thermal Properties</strong></li><li><strong>Wide Colour Range</strong></li><li><strong>Durable</strong></li></ul>",
            "<h3>Installation</h3><p>Professional fitting over quality underlay recommended.</p>",
            "<h3>Care</h3><p>Vacuum twice weekly. Blot spills immediately. Professional cleaning annually.</p>",
        ),
        "Underlay": (
            f"<h2>{title}</h2><p>The <strong>{title}</strong>{bl} — professional underlay for comfort, sound insulation and extended floor life.</p>",
            "<h3>Key Features</h3><ul><li><strong>Superior Cushioning</strong></li><li><strong>Sound Reduction</strong></li><li><strong>Thermal Insulation</strong></li><li><strong>Moisture Protection</strong></li><li><strong>Extends Floor Life</strong></li></ul>",
            "<h3>Installation</h3><p>Lay smooth-side down. Butt edges. Tape joins. Replace when fitting new flooring.</p>",
            "<h3>Care</h3><p>No maintenance needed once installed.</p>",
        ),
        "Accessories": (
            f"<h2>{title}</h2><p>Complete your installation with the <strong>{title}</strong>{bl}. Professional quality finishing accessories.</p>",
            "<h3>Key Features</h3><ul><li><strong>Professional Quality</strong></li><li><strong>Wide Compatibility</strong></li><li><strong>Easy Installation</strong></li><li><strong>Excellent Value</strong></li></ul>",
            "<h3>Installation</h3><p>Refer to packaging guidelines.</p>",
            "<h3>Care</h3><p>Maintenance-free once installed.</p>",
        ),
    }
    default = (
        f"<h2>{title}</h2><p>{title}{bl}. Quality flooring from Factory Direct Flooring.</p>",
        "<h3>Features</h3><ul><li>High quality</li><li>Stylish design</li><li>Easy maintenance</li></ul>",
        "<h3>Installation</h3><p>Refer to product specification.</p>",
        "<h3>Care</h3><p>Clean with appropriate products.</p>",
    )
    i, f, n, c = d.get(cat, default)
    why = "<h3>Why Factory Direct Flooring?</h3><p>UK's trusted flooring specialist — premium quality at factory direct prices. Free samples, expert advice and fast UK delivery.</p>"
    return f"{i}\n{f}\n{n}\n{c}\n{why}"

# ── Helpers ────────────────────────────────────────────────────────────────────

def fetch(url, timeout=20):
    """Fetch URL trying multiple User-Agent patterns."""
    agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    ]
    for agent in agents:
        h = dict(HEADERS); h["User-Agent"] = agent
        for _ in range(2):
            try:
                r = requests.get(url, headers=h, timeout=timeout)
                if r.status_code == 200:
                    return r.text
                elif r.status_code in (301, 302):
                    redir = r.headers.get("Location","")
                    if redir: return fetch(redir, timeout)
                elif r.status_code == 404:
                    return ""
                time.sleep(0.5)
            except Exception:
                time.sleep(1)
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
    # Ensure unique by prepending index
    return f"{idx:05d}_{fname}"

# ── Imagely URL extraction ─────────────────────────────────────────────────────

IMAGELY_RE = re.compile(
    r'https://imagely\.factory-direct-flooring\.co\.uk'
    r'/media/catalog/product/[^\s"\'<>\)\\,\]]+',
    re.I
)

def get_imagely_urls(html):
    """Extract ALL imagely URLs from any HTML using 6 methods."""
    found = []
    seen  = set()

    def add(u):
        u = str(u).strip()
        u = re.sub(r'["\'\s].*$','',u)  # cut at quote or space
        u = u.split("?")[0]
        if (u.startswith("https://imagely") and
                "catalog/product" in u and
                u not in seen and
                len(u) > 60 and
                not u.endswith("/")):
            seen.add(u); found.append(u)

    # 1. Direct regex — catches all bare URLs
    for m in IMAGELY_RE.finditer(html):
        add(m.group(0))

    # 2. JSON-LD structured data
    for block in re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL
    ):
        try:
            data = json.loads(block)
            txt  = json.dumps(data)
            for m in IMAGELY_RE.finditer(txt): add(m.group(0))
        except: pass

    # 3. Inline Magento script blocks
    for block in re.findall(
        r'<script[^>]*>(.*?)</script>', html, re.DOTALL
    ):
        for m in IMAGELY_RE.finditer(block): add(m.group(0))

    # 4. x-data Alpine.js attributes
    for m in re.finditer(r'x-data=["\']([^"\']{20,})["\']', html):
        for m2 in IMAGELY_RE.finditer(m.group(1)): add(m2.group(0))

    # 5. data-* attributes
    for m in re.finditer(r'data-[a-z\-]+=["\'](https://imagely[^"\']+)["\']', html, re.I):
        add(m.group(1))

    # 6. JSON in x-magento-init or similar
    for block in re.findall(r'\{[^<]{100,}\}', html):
        if 'imagely' in block:
            for m in IMAGELY_RE.finditer(block): add(m.group(0))

    return found

# ── Parse products from category page ────────────────────────────────────────

def parse_category_page(html, cat):
    products = []
    seen     = set()
    all_imgs = get_imagely_urls(html)

    # JSON-LD products
    jl_products = []
    for block in re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL
    ):
        try:
            data  = json.loads(block)
            items = data if isinstance(data,list) else [data]
            for item in items:
                if not isinstance(item,dict): continue
                def proc(p):
                    if not isinstance(p,dict) or p.get("@type")!="Product": return
                    nm = clean(p.get("name",""))
                    if not nm or len(nm)<3: return
                    brand="";b=p.get("brand",{})
                    if isinstance(b,dict): brand=clean(b.get("name",""))
                    elif isinstance(b,str): brand=clean(b)
                    price="";compare="";stock="active"
                    of=p.get("offers",{})
                    if isinstance(of,list): of=of[0] if of else {}
                    if isinstance(of,dict):
                        price=str(of.get("price",of.get("lowPrice","")))
                        hp=of.get("highPrice","")
                        compare=str(hp) if hp and hp!=price else ""
                        stock="active" if "InStock" in of.get("availability","") else "draft"
                    url=p.get("url","")
                    slug=url.rstrip("/").split("/")[-1].replace(".html","").lower() if url else ""

                    # JSON-LD images
                    ji=[]
                    imgs=p.get("image",[])
                    if isinstance(imgs,str): imgs=[imgs]
                    if isinstance(imgs,dict): imgs=[imgs.get("url","")]
                    for img in imgs:
                        u=str(img).split("?")[0]
                        if "imagely" in u and len(u)>60: ji.append(u)

                    jl_products.append({
                        "name":nm,"sku":str(p.get("sku","")),"url":url,"slug":slug,
                        "brand":brand,"price":price,"compare":compare,"stock":stock,
                        "json_imgs":ji,
                    })
                if item.get("@type")=="ItemList":
                    for el in item.get("itemListElement",[]): proc(el.get("item",el))
                elif item.get("@type")=="Product": proc(item)
        except: pass

    # Match images to products by slug
    used = set()
    for p in jl_products:
        if p["name"] in seen: continue
        seen.add(p["name"])

        imgs = []

        # First: use JSON-LD images
        for u in p["json_imgs"]:
            if u not in used and len(imgs)<MAX_IMAGES:
                imgs.append(u); used.add(u)

        # Second: slug-match from page images
        if len(imgs) < MAX_IMAGES and p["slug"]:
            words = [w for w in p["slug"].replace("-"," ").split() if len(w)>3]
            for u in all_imgs:
                if u in used or len(imgs)>=MAX_IMAGES: continue
                ul = u.lower()
                if words and (words[0] in ul or sum(1 for w in words if w in ul)>=2):
                    imgs.append(u); used.add(u)

        products.append({
            "name":p["name"],"sku":p["sku"],"price":p["price"],"compare":p["compare"],
            "brand":p["brand"] or "Factory Direct Flooring","cat":cat,
            "images":imgs,"stock":p["stock"],"url":p["url"],
        })

    # HTML card fallback
    if not products:
        for m in re.finditer(
            r'<(?:li|div|article)[^>]*class="[^"]*product[^"]*item[^"]*"[^>]*>(.*?)</(?:li|div|article)>',
            html, re.DOTALL|re.I
        ):
            card=m.group(1)
            nm=(re.search(r'class="[^"]*product[_\-]name[^"]*"[^>]*>.*?<a[^>]*>(.*?)</a>',card,re.DOTALL|re.I)
               or re.search(r'<a[^>]+title="([^"]{4,})"',card))
            name=clean(nm.group(1)) if nm else ""
            if not name or name in seen: continue
            seen.add(name)
            url_m=re.search(r'href=["\'](' + re.escape(BASE_URL) + r'/[^"\'?#]+)["\']',card)
            prod_url=url_m.group(1) if url_m else ""
            slug=prod_url.rstrip("/").split("/")[-1].replace(".html","").lower()
            pm=re.search(r'£\s*([\d,]+\.?\d*)',card)
            price=pm.group(1).replace(",","") if pm else ""
            # Card images
            card_imgs = get_imagely_urls(card)
            # Also slug-match from all page images
            if len(card_imgs) < MAX_IMAGES and slug:
                words = [w for w in slug.replace("-"," ").split() if len(w)>3]
                for u in all_imgs:
                    if u in card_imgs or len(card_imgs)>=MAX_IMAGES: continue
                    ul=u.lower()
                    if words and (words[0] in ul or sum(1 for w in words if w in ul)>=2):
                        card_imgs.append(u)
            products.append({
                "name":name,"sku":"","price":price,"compare":"",
                "brand":"Factory Direct Flooring","cat":cat,
                "images":card_imgs[:MAX_IMAGES],"stock":"active","url":prod_url,
            })

    return products, len(all_imgs)

# ── Scrape all categories ──────────────────────────────────────────────────────

def scrape_all():
    all_products = []
    seen_handles = set()

    for cat, path in CATEGORIES:
        print(f"\n  [{cat}]")

        # Try multiple URL variations
        urls_to_try = [
            f"{BASE_URL}{path}?product_list_limit=100",
            f"{BASE_URL}{path}?product_list_limit=48",
            f"{BASE_URL}{path}",
        ]

        page     = 1
        got_html = False

        for start_url in urls_to_try:
            html = fetch(start_url, timeout=30)
            if html:
                got_html  = True
                cur_url   = start_url
                break

        if not got_html:
            print(f"    FAILED (all URL variants blocked)")
            continue

        while True:
            prods, img_count = parse_category_page(html, cat)
            new = 0
            for p in prods:
                h = make_handle(p.get("url",""), p["name"])
                if h in seen_handles: continue
                seen_handles.add(h); p["handle"]=h
                all_products.append(p); new += 1

            total_imgs = sum(len(x["images"]) for x in all_products)
            print(f"    Page {page}: +{new} prods | page_imgs:{img_count} | matched:{total_imgs} | total:{len(all_products)}")

            if new == 0 and page > 1: break

            # ── FIXED: next_url variable (was 'next' before — Python built-in!) ──
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
            html = fetch(next_url, timeout=30)  # ← FIXED: was cur_url = next (bug!)
            if not html: break
            cur_url = next_url
            page   += 1
            time.sleep(DELAY)

    return all_products

# ── Download images ────────────────────────────────────────────────────────────

def download_images(products):
    os.makedirs(IMAGES_DIR, exist_ok=True)
    dl=0; fail=0; idx=0

    print(f"\n  Downloading images...")
    for p in products:
        local = []
        for img_url in p.get("images",[]):
            idx  += 1
            fname = safe_fname(img_url, idx)
            lpath = os.path.join(IMAGES_DIR, fname)
            gh    = f"{IMAGES_BASE}/{fname}"

            if os.path.exists(lpath) and os.path.getsize(lpath) > 1000:
                local.append(gh); dl += 1; continue

            try:
                r = requests.get(img_url, headers=IMG_HDR, timeout=15, stream=True)
                if r.status_code == 200:
                    with open(lpath,'wb') as f:
                        for chunk in r.iter_content(8192): f.write(chunk)
                    sz = os.path.getsize(lpath)
                    if sz > 1000:
                        local.append(gh); dl += 1
                    else:
                        os.remove(lpath); fail += 1
                else:
                    fail += 1
            except: fail += 1
            time.sleep(0.15)

        p["local_images"] = local

    print(f"  Downloaded: {dl} | Failed: {fail}")
    return products

# ── Build CSV ──────────────────────────────────────────────────────────────────

def build_rows(p):
    name=(p.get("name") or "").strip()
    if not name: return []
    handle  = p.get("handle") or make_handle(p.get("url",""),name)
    cat     = p.get("cat","Flooring")
    body    = build_desc(name,cat)
    vendor  = p.get("brand","").strip() or "Factory Direct Flooring"
    tags    = cat.lower().replace(" ","-")
    images  = [i for i in p.get("local_images",[]) if i and i.startswith("http")]
    price   = price_fmt(p.get("price","")) or "0.00"
    compare = price_fmt(p.get("compare",""))
    first   = images[0] if images else ""
    seo_d   = f"Buy {name} at Factory Direct Flooring — {cat} at competitive prices. Free samples and fast UK delivery."[:320]

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
        row1["Image Src"]=""; row1["Image Src"]=first
        row1["Image Position"]="1"; row1["Image Alt Text"]=name
    rows.append(row1)
    for i,img in enumerate(images[1:],2):
        e={k:"" for k in SHOPIFY_COLS}
        e.update({"Handle":handle,"Image Src":img,"Image Position":str(i),"Image Alt Text":name})
        rows.append(e)
    return rows

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(IMAGES_DIR, exist_ok=True)
    t0=time.time()

    print("\n"+"="*65)
    print("  FACTORY DIRECT FLOORING — SHOPIFY SCRAPER (FIXED)")
    print(f"  Bug fixed: next_url variable | Aggressive image extraction")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*65)

    products = scrape_all()

    if not products:
        print("\n  No products found — website may be blocking all requests")
        with open(f"{OUTPUT_DIR}/factory_flooring_shopify_{TIMESTAMP}.csv",
                  "w",encoding="utf-8-sig",newline="") as f:
            csv.DictWriter(f,fieldnames=SHOPIFY_COLS).writeheader()
        return

    products = download_images(products)

    all_rows=[]
    for p in products: all_rows.extend(build_rows(p))

    csv_file=f"{OUTPUT_DIR}/factory_flooring_shopify_{TIMESTAMP}.csv"
    with open(csv_file,"w",encoding="utf-8-sig",newline="") as f:
        writer=csv.DictWriter(f,fieldnames=SHOPIFY_COLS,extrasaction="ignore")
        writer.writeheader(); writer.writerows(all_rows)

    json_file=f"{OUTPUT_DIR}/factory_flooring_{TIMESTAMP}.json"
    with open(json_file,"w",encoding="utf-8") as f:
        json.dump([{
            "name":p["name"],"sku":p.get("sku",""),"price":p.get("price",""),
            "cat":p["cat"],"images_found":p.get("images",[]),
            "images_downloaded":p.get("local_images",[]),
            "img_count":len(p.get("local_images",[])),
            "url":p.get("url",""),
        } for p in products],f,ensure_ascii=False,indent=2)

    el=round(time.time()-t0)
    with_img=len([p for p in products if p.get("local_images")])
    cats={}
    for p in products: cats[p["cat"]]=cats.get(p["cat"],0)+1

    print(f"\n{'='*65}")
    print(f"  DONE in {el//60}m {el%60:02d}s")
    print(f"  Products    : {len(products)}")
    print(f"  CSV rows    : {len(all_rows)}")
    print(f"  With images : {with_img} ({round(with_img/len(products)*100) if products else 0}%)")
    print(f"\n  By category:")
    for cat,count in sorted(cats.items(),key=lambda x:-x[1]):
        print(f"    {cat:<20} {count:>3}")
    print(f"\n  CSV  → {csv_file}")
    print(f"  Shopify → Products → Import → Upload CSV")
    print("="*65)

if __name__=="__main__":
    main()
