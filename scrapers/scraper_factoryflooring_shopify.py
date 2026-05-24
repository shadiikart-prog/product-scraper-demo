"""
FACTORY DIRECT FLOORING — COMPLETE FINAL SCRAPER
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✓ Products + prices from category pages
✓ Downloads images from imagely CDN → saves locally
✓ CSV uses GitHub Pages URLs (Shopify accepts 100%)
✓ Rich auto-descriptions (5 sections)
✓ Type = short category name
✓ No taxonomy errors
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import requests, json, csv, re, os, time
from datetime import datetime
from html import unescape
from urllib.parse import urlparse

BASE_URL       = "https://www.factory-direct-flooring.co.uk"
OUTPUT_DIR     = "output"
IMAGES_DIR     = "output/images"
TIMESTAMP      = datetime.now().strftime("%Y%m%d_%H%M%S")
MAX_IMAGES     = 2
DELAY          = 0.5

# ── GitHub Pages base URL ─────────────────────────────────────────────
# Images saved to output/images/ → served via GitHub Pages
GITHUB_USER    = "shadiikart-prog"
GITHUB_REPO    = "product-scraper-demo"
GITHUB_BRANCH  = "main"
IMAGES_BASE    = f"https://{GITHUB_USER}.github.io/{GITHUB_REPO}/scrapers/output/images"

HEADERS = {
    "User-Agent"     : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept"         : "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection"     : "keep-alive",
}
IMG_HEADERS = {
    "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept"     : "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    "Referer"    : "https://www.factory-direct-flooring.co.uk/",
}

CATEGORIES = [
    ("Solid Wood",       f"{BASE_URL}/solid-wood-flooring"),
    ("Engineered Wood",  f"{BASE_URL}/engineered-wood-flooring"),
    ("Laminate",         f"{BASE_URL}/laminate-flooring"),
    ("LVT",              f"{BASE_URL}/lvt-flooring"),
    ("Herringbone",      f"{BASE_URL}/herringbone-flooring"),
    ("Vinyl",            f"{BASE_URL}/vinyl-flooring"),
    ("Carpet",           f"{BASE_URL}/carpet"),
    ("Underlay",         f"{BASE_URL}/underlay"),
    ("Accessories",      f"{BASE_URL}/accessories"),
]

SHOPIFY_COLS = [
    "Handle","Title","Body (HTML)","Vendor","Product Category","Type",
    "Tags","Published","Option1 Name","Option1 Value","Variant SKU",
    "Variant Grams","Variant Inventory Tracker","Variant Inventory Qty",
    "Variant Inventory Policy","Variant Fulfillment Service","Variant Price",
    "Variant Compare At Price","Variant Requires Shipping","Variant Taxable",
    "Image Src","Image Position","Image Alt Text","SEO Title","SEO Description","Status",
]

# ── Descriptions ──────────────────────────────────────────────────────

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

    blocks = {
        "Solid Wood": (
            f"<h2>{title}</h2><p>Introducing the <strong>{title}</strong>{bl}{cl}{tl}. Crafted from 100% genuine solid timber, this floor delivers unmatched natural beauty and authentic character that only improves with age. Every plank is completely unique with its own natural grain, knots and colour variation — a true lifetime investment for your home.</p>",
            "<h3>Key Features</h3><ul><li><strong>100% Real Solid Timber</strong> — authentic grain and character in every plank</li><li><strong>Sand &amp; Refinish Up to 5 Times</strong> — lasts a lifetime</li><li><strong>Natural Thermal Insulator</strong> — retains warmth, reduces energy bills</li><li><strong>Increases Property Value</strong> — proven to add value</li><li><strong>Sustainably Sourced</strong> — from certified responsible forests</li></ul>",
            "<h3>Installation</h3><p>Secret-nail or glue to suitable subfloor. Acclimatise 48–72 hours before fitting. Leave 15mm expansion gap around all fixed objects. Professional installation recommended.</p>",
            "<h3>Care &amp; Maintenance</h3><p>Sweep or vacuum regularly with a soft brush. Clean with wood-specific cleaner on a lightly damp mop — never use excess water. Wipe spills immediately. Use felt pads under furniture.</p>",
        ),
        "Engineered Wood": (
            f"<h2>{title}</h2><p>Discover the <strong>{title}</strong>{bl}{cl}{tl}. Engineered wood combines a genuine real wood top layer with a highly stable multi-layer core — giving you the authentic warmth of real timber with superior resistance to temperature and humidity changes. Fully compatible with underfloor heating.</p>",
            "<h3>Key Features</h3><ul><li><strong>Genuine Real Wood Surface</strong> — timber veneer for authentic appearance</li><li><strong>Stable Multi-Layer Core</strong> — resists warping and shrinking</li><li><strong>Underfloor Heating Compatible</strong> — wet and electric UFH</li><li><strong>All Floor Levels</strong> — ground, first floor and basement</li><li><strong>Flexible Fitting</strong> — floating click, secret nail or glue-down</li></ul>",
            "<h3>Installation</h3><p>Floating click, secret-nail or full glue-down options. UFH compatible — max 27°C surface temperature. Over concrete or timber subfloors. Acclimatise 48 hours.</p>",
            "<h3>Care &amp; Maintenance</h3><p>Sweep or vacuum regularly. Wood floor cleaner on a damp mop. Avoid excess moisture, steam cleaners and harsh chemicals. Wipe spills immediately.</p>",
        ),
        "Herringbone": (
            f"<h2>{title}</h2><p>Make a bold statement with the <strong>{title}</strong>{bl}{cl}{tl}. The iconic herringbone pattern adds instant elegance, depth and timeless character — an interlocking design that has graced grand homes for centuries. Available in wood and LVT for every budget and requirement.</p>",
            "<h3>Key Features</h3><ul><li><strong>Iconic 45° Pattern</strong> — instant elegance and visual depth</li><li><strong>Creates Space</strong> — diagonal design visually enlarges rooms</li><li><strong>Wood &amp; LVT Options</strong> — real wood or waterproof vinyl</li><li><strong>Unique Character</strong> — no two floors ever look the same</li><li><strong>All Rooms</strong> — hallways, living rooms, kitchens, bedrooms</li></ul>",
            "<h3>Installation</h3><p>Precise planning essential — mark centre line and 45° angle before laying. Professional installation recommended. Acclimatise wood products 48–72 hours.</p>",
            "<h3>Care &amp; Maintenance</h3><p>Wood type: wood-specific cleaner on damp mop. LVT type: warm water and LVT cleaner. Door mats at entrances and felt pads under furniture.</p>",
        ),
        "Vinyl": (
            f"<h2>{title}</h2><p>Presenting the <strong>{title}</strong>{bl}{cl}{tl}. Premium vinyl with total waterproof protection and beautiful realistic designs — the ideal solution for kitchens, bathrooms, hallways and any room where performance matters as much as appearance.</p>",
            "<h3>Key Features</h3><ul><li><strong>Fully Waterproof</strong> — safe for bathrooms, kitchens and wet rooms</li><li><strong>Tough Wear Surface</strong> — resists scratches and heavy traffic</li><li><strong>Cushioned Underfoot</strong> — warm, comfortable and quiet</li><li><strong>Easy to Clean</strong> — resistant to most household stains</li><li><strong>Realistic Designs</strong> — high-definition wood and stone effects</li></ul>",
            "<h3>Installation</h3><p>Loose-laid, adhered or click-lock depending on product. Subfloor must be clean, dry and smooth. Can go over existing well-bonded coverings. Expansion gap required for floating.</p>",
            "<h3>Care &amp; Maintenance</h3><p>Sweep or vacuum regularly. Mop with warm water and mild cleaner. Resistant to most stains and bacteria. Clean spills promptly.</p>",
        ),
        "Laminate": (
            f"<h2>{title}</h2><p>Meet the <strong>{title}</strong>{bl}{cl}{tl}. Outstanding laminate delivers the authentic look of real wood or stone at a fraction of the cost — with a tough scratch-resistant surface designed for busy family homes and virtually indistinguishable from the real thing.</p>",
            "<h3>Key Features</h3><ul><li><strong>HD Surface Layer</strong> — photorealistic wood or stone appearance</li><li><strong>AC-Rated Scratch Resistance</strong> — handles heavy domestic use</li><li><strong>Easy Click Installation</strong> — DIY-friendly, no glue needed</li><li><strong>Bevelled V-Groove Edges</strong> — realistic depth and detail</li><li><strong>Low Maintenance</strong> — sweep, vacuum and damp mop</li></ul>",
            "<h3>Installation</h3><p>Simple click-lock floating system — ideal for DIY. Over most existing floors. 10mm expansion gap around perimeter. Quality underlay required unless pre-attached.</p>",
            "<h3>Care &amp; Maintenance</h3><p>Sweep or vacuum. Well-wrung damp mop with laminate cleaner. No steam cleaners or excess water. Wipe spills immediately.</p>",
        ),
        "LVT": (
            f"<h2>{title}</h2><p>Introducing the <strong>{title}</strong>{bl}{cl}{tl}. Luxury Vinyl Tile — 100% waterproof with hyper-realistic wood and stone designs and outstanding durability. The ultimate choice for any room where style and performance go hand in hand.</p>",
            "<h3>Key Features</h3><ul><li><strong>100% Waterproof</strong> — safe for all rooms including bathrooms</li><li><strong>Commercial-Grade Wear Layer</strong> — resists heavy traffic</li><li><strong>Hyper-Realistic Surface</strong> — faithful wood and stone replica</li><li><strong>Warmer Than Real Tile</strong> — comfortable underfoot</li><li><strong>UFH Compatible</strong> — wet and electric underfloor heating</li></ul>",
            "<h3>Installation</h3><p>Click-lock floating, loose-lay or glue-down. Subfloor clean, dry, flat and level. UFH compatible — max 27°C. No acclimatisation required.</p>",
            "<h3>Care &amp; Maintenance</h3><p>Sweep or vacuum to remove grit. Mop with warm water and LVT cleaner. Avoid abrasive pads, solvents and steam cleaners.</p>",
        ),
        "Carpet": (
            f"<h2>{title}</h2><p>Transform your home with the <strong>{title}</strong>{bl}{cl}. Luxuriously soft and warm — perfect for bedrooms, living rooms and stairs. Excellent sound insulation and thermal properties throughout your home.</p>",
            "<h3>Key Features</h3><ul><li><strong>Luxuriously Soft</strong> — warm and comfortable underfoot</li><li><strong>Sound Insulation</strong> — reduces impact noise between floors</li><li><strong>Thermal Properties</strong> — retains warmth, lowers energy costs</li><li><strong>Wide Colour Range</strong> — suits any décor</li><li><strong>Durable</strong> — built for busy family life</li></ul>",
            "<h3>Installation</h3><p>Professional fitting recommended. Always fit over quality underlay. Measure carefully and allow for pattern matching.</p>",
            "<h3>Care &amp; Maintenance</h3><p>Vacuum twice weekly. Always blot (never rub) spills. Carpet spot cleaner for stains. Professional cleaning every 12–18 months.</p>",
        ),
        "Underlay": (
            f"<h2>{title}</h2><p>The <strong>{title}</strong>{bl} — professional underlay for enhanced comfort, sound insulation and floor longevity. The right underlay is as important as the floor itself.</p>",
            "<h3>Key Features</h3><ul><li><strong>Superior Cushioning</strong></li><li><strong>Sound Reduction</strong></li><li><strong>Thermal Insulation</strong></li><li><strong>Moisture Protection</strong></li><li><strong>Extends Floor Life</strong></li></ul>",
            "<h3>Installation</h3><p>Lay smooth-side down. Butt edges — never overlap. Tape all joins. Replace when fitting new flooring.</p>",
            "<h3>Care</h3><p>No maintenance once installed. Always replace when fitting new flooring.</p>",
        ),
        "Accessories": (
            f"<h2>{title}</h2><p>Complete your installation with the <strong>{title}</strong>{bl}. Quality finishing accessories for a truly professional result.</p>",
            "<h3>Key Features</h3><ul><li><strong>Professional Quality</strong></li><li><strong>Wide Compatibility</strong></li><li><strong>Easy Installation</strong></li><li><strong>Excellent Value</strong></li></ul>",
            "<h3>Installation</h3><p>Refer to packaging guidelines. Contact our team for product advice.</p>",
            "<h3>Care</h3><p>Maintenance-free once installed. Wipe with damp cloth where needed.</p>",
        ),
    }

    default = (
        f"<h2>{title}</h2><p>{title}{bl}. Quality flooring from Factory Direct Flooring.</p>",
        "<h3>Features</h3><ul><li>High-quality construction</li><li>Stylish design</li><li>Easy maintenance</li><li>Excellent value</li></ul>",
        "<h3>Installation</h3><p>Refer to product specification for guidelines.</p>",
        "<h3>Care</h3><p>Clean with appropriate products for your floor type.</p>",
    )

    i, f, n, c = blocks.get(cat, default)
    why = "<h3>Why Factory Direct Flooring?</h3><p>The UK's trusted flooring specialist — premium quality at factory direct prices. Free samples, expert advice, full warranties and fast UK delivery on all orders.</p>"
    return f"{i}\n{f}\n{n}\n{c}\n{why}"

# ── Helpers ───────────────────────────────────────────────────────────

def fetch_html(url):
    for _ in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            if r.status_code == 200: return r.text
            if r.status_code == 404: return ""
        except: time.sleep(1)
    return ""

def download_image(img_url, local_path):
    """Download image from imagely CDN with browser headers."""
    try:
        r = requests.get(img_url, headers=IMG_HEADERS, timeout=20, stream=True)
        if r.status_code == 200:
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
            size = os.path.getsize(local_path)
            if size > 1000:  # at least 1KB — real image
                return True
            os.remove(local_path)
    except: pass
    return False

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
    return re.sub(r"[^a-z0-9]+","-",name.lower()).strip("-")[:200]

def safe_filename(url, idx):
    """Generate safe local filename from image URL."""
    parsed = urlparse(url)
    fname  = os.path.basename(parsed.path)
    fname  = re.sub(r'[^a-zA-Z0-9\._\-]', '_', fname)
    if not fname or len(fname) < 5:
        fname = f"product_image_{idx}.jpg"
    return fname

# ── Find ALL imagely URLs on a page ───────────────────────────────────
IMAGELY_RE = re.compile(
    r'https://imagely\.factory-direct-flooring\.co\.uk'
    r'/media/catalog/product/[^\s"\'<>,\)\\]+',
    re.I
)

def get_page_images(html):
    found = []
    seen  = set()
    for m in IMAGELY_RE.finditer(html):
        u = m.group(0).split("?")[0]
        if u not in seen and len(u) > 60:
            seen.add(u); found.append(u)
    return found

def match_images_to_slug(slug, all_imgs, used):
    """Match imagely URLs to a product by slug keywords."""
    if not slug: return []
    words = [w for w in slug.replace("-"," ").split() if len(w) > 3]
    if not words: return []
    scored = []
    for img in all_imgs:
        if img in used: continue
        il = img.lower()
        score = sum(1 for w in words if w in il)
        if words[0] in il or score >= 2:
            scored.append((score, img))
    scored.sort(key=lambda x: -x[0])
    return [img for _, img in scored[:MAX_IMAGES]]

# ── Parse category page ───────────────────────────────────────────────

def parse_page(html, cat):
    products = []
    seen     = set()

    # All imagely images on this page
    all_imgs = get_page_images(html)

    # JSON-LD: name, price, sku, stock, url
    jl = {}
    for block in re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL
    ):
        try:
            data = json.loads(block.strip())
            for item in (data if isinstance(data,list) else [data]):
                if not isinstance(item,dict): continue
                def proc(p):
                    if not isinstance(p,dict) or p.get("@type")!="Product": return
                    nm = clean(p.get("name",""))
                    if not nm: return
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
                        avail=of.get("availability","")
                        stock="active" if "InStock" in avail else "draft"
                    url=p.get("url","")
                    slug=url.rstrip("/").split("/")[-1].replace(".html","").lower() if url else ""
                    jl[nm]={"sku":str(p.get("sku","")),"url":url,"slug":slug,
                            "brand":brand,"price":price,"compare":compare,"stock":stock}
                if item.get("@type")=="ItemList":
                    for el in item.get("itemListElement",[]): proc(el.get("item",el))
                elif item.get("@type")=="Product": proc(item)
        except: pass

    used_imgs = set()

    for name, d in jl.items():
        if name in seen: continue
        seen.add(name)
        imgs = match_images_to_slug(d["slug"], all_imgs, used_imgs)
        for i in imgs: used_imgs.add(i)
        products.append({
            "name":name,"sku":d["sku"],"price":d["price"],"compare":d["compare"],
            "brand":d["brand"] or "Factory Direct Flooring","cat":cat,
            "imagely_urls":imgs,"local_images":[],"stock":d["stock"],"url":d["url"],
        })

    # HTML card fallback
    if not products:
        for m in re.finditer(
            r'<(?:li|div|article)[^>]*class="[^"]*product[^"]*item[^"]*"[^>]*>(.*?)</(?:li|div|article)>',
            html, re.DOTALL|re.I
        ):
            card=m.group(1)
            nm=(re.search(r'class="[^"]*(?:product[_\-]name)[^"]*"[^>]*>.*?<a[^>]*>(.*?)</a>',card,re.DOTALL|re.I)
               or re.search(r'<a[^>]+title="([^"]{4,})"',card))
            name=clean(nm.group(1)) if nm else ""
            if not name or name in seen: continue
            seen.add(name)
            url_m=re.search(r'href=["\'](' + re.escape(BASE_URL) + r'/[^"\'?#]+)["\']',card)
            prod_url=url_m.group(1) if url_m else ""
            slug=prod_url.rstrip("/").split("/")[-1].replace(".html","").lower()
            pm=re.search(r'£\s*([\d,]+\.?\d*)',card)
            price=pm.group(1).replace(",","") if pm else ""
            imgs=match_images_to_slug(slug, all_imgs, used_imgs)
            for i in imgs: used_imgs.add(i)
            products.append({
                "name":name,"sku":"","price":price,"compare":"",
                "brand":"Factory Direct Flooring","cat":cat,
                "imagely_urls":imgs,"local_images":[],"stock":"active","url":prod_url,
            })

    return products

# ── Download images & build local paths ──────────────────────────────

def download_all_images(products):
    os.makedirs(IMAGES_DIR, exist_ok=True)
    total_downloaded = 0
    total_failed     = 0
    img_counter      = 0

    print(f"\n  Downloading images to {IMAGES_DIR}/...")
    for p in products:
        local = []
        for img_url in p["imagely_urls"]:
            img_counter += 1
            fname      = safe_filename(img_url, img_counter)
            local_path = os.path.join(IMAGES_DIR, fname)
            gh_url     = f"{IMAGES_BASE}/{fname}"

            # Skip if already downloaded
            if os.path.exists(local_path) and os.path.getsize(local_path) > 1000:
                local.append(gh_url)
                total_downloaded += 1
                continue

            ok = download_image(img_url, local_path)
            if ok:
                local.append(gh_url)
                total_downloaded += 1
            else:
                total_failed += 1
            time.sleep(0.2)

        p["local_images"] = local

    print(f"  Downloaded: {total_downloaded} | Failed: {total_failed}")
    return products

# ── Scrape categories ─────────────────────────────────────────────────

def scrape_all():
    all_p=[]; seen_h=set()
    for cat, base_url in CATEGORIES:
        print(f"\n  [{cat}]")
        page=1; cur=f"{base_url}?product_list_limit=100"
        while True:
            print(f"    Page {page}...",end=" ",flush=True)
            html=fetch_html(cur)
            if not html: print("FAILED"); break
            prods=parse_page(html,cat)
            new=0
            for p in prods:
                h=make_handle(p.get("url",""),p["name"])
                if h in seen_h: continue
                seen_h.add(h); p["handle"]=h; all_p.append(p); new+=1
            print(f"+{new} | imagely URLs:{sum(len(x['imagely_urls']) for x in all_p)} | total:{len(all_p)}")
            if new==0 and page>1: break
            nxt=None
            rel=re.search(r'<link[^>]+rel=["\']next["\'][^>]+href=["\']([^"\']+)["\']',html)
            if rel:
                n=rel.group(1); nxt=n if n.startswith("http") else BASE_URL+n
            else:
                pg=re.search(r'href=["\']([^"\']*[?&]p='+str(page+1)+r'[^"\']*)["\']',html)
                if pg:
                    n=pg.group(1); nxt=n if n.startswith("http") else BASE_URL+n
            if not nxt: break
            cur=nxt; page+=1; time.sleep(DELAY)
    return all_p

# ── Build CSV ─────────────────────────────────────────────────────────

def build_rows(p):
    name=(p.get("name") or "").strip()
    if not name: return []
    handle  = p.get("handle") or make_handle(p.get("url",""),name)
    cat     = p.get("cat","Flooring")
    body    = build_desc(name,cat)
    vendor  = p.get("brand","").strip() or "Factory Direct Flooring"
    tags    = cat.lower().replace(" ","-")
    images  = [i for i in p.get("local_images",[]) if i and i.startswith("http")][:MAX_IMAGES]
    price   = price_fmt(p.get("price","")) or "0.00"
    compare = price_fmt(p.get("compare",""))
    first   = images[0] if images else ""
    seo_d   = f"Buy {name} at Factory Direct Flooring. {cat} flooring at competitive prices. Free UK delivery and expert advice."[:320]
    rows=[]
    row1={
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
    for i,img in enumerate(images[1:],2):
        e={k:"" for k in SHOPIFY_COLS}
        e.update({"Handle":handle,"Image Src":img,"Image Position":str(i),"Image Alt Text":name})
        rows.append(e)
    return rows

# ── Main ──────────────────────────────────────────────────────────────

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(IMAGES_DIR, exist_ok=True)
    t0=time.time()
    print("\n"+"="*65)
    print("  FACTORY DIRECT FLOORING — COMPLETE FINAL SCRAPER")
    print(f"  Images downloaded locally → served via GitHub Pages")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*65)

    # 1. Scrape product data + imagely URLs
    products = scrape_all()
    if not products:
        print("  No products found"); return

    # 2. Download images from imagely CDN
    products = download_all_images(products)

    # 3. Build CSV
    all_rows=[]
    for p in products: all_rows.extend(build_rows(p))

    csv_file=f"{OUTPUT_DIR}/factory_flooring_shopify_{TIMESTAMP}.csv"
    with open(csv_file,"w",encoding="utf-8-sig",newline="") as f:
        writer=csv.DictWriter(f,fieldnames=SHOPIFY_COLS,extrasaction="ignore")
        writer.writeheader(); writer.writerows(all_rows)

    el=round(time.time()-t0)
    with_img=len([p for p in products if p["local_images"]])
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
    print(f"  Imgs → {IMAGES_DIR}/")
    print(f"  Shopify → Products → Import → Upload CSV")
    print("="*65)

if __name__=="__main__":
    main()
