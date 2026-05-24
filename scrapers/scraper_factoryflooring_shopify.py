"""
FACTORY DIRECT FLOORING — FINAL WORKING SCRAPER
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Strategy (combines everything that worked):
  1. Playwright renders page + scrolls (lazy images load)
  2. JSON-LD extraction (proven: finds 295 products)
  3. Image matching by FILENAME vs product NAME (better than slug)
  4. Download imagely → GitHub Pages → Shopify 100%
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import json, csv, re, os, time, requests
from datetime import datetime
from html import unescape
from urllib.parse import urlparse

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_OK = True
except ImportError:
    PLAYWRIGHT_OK = False

BASE_URL   = "https://www.factory-direct-flooring.co.uk"
OUTPUT_DIR = "output"
IMAGES_DIR = "output/images"
TIMESTAMP  = datetime.now().strftime("%Y%m%d_%H%M%S")
MAX_IMAGES = 3

GITHUB_USER = "shadiikart-prog"
GITHUB_REPO = "product-scraper-demo"
IMAGES_BASE = f"https://{GITHUB_USER}.github.io/{GITHUB_REPO}/scrapers/output/images"

# Correct URLs from user's navigation
CATEGORIES = [
    ("Solid Wood",      f"{BASE_URL}/solid-wood-flooring"),
    ("Engineered Wood", f"{BASE_URL}/engineered-wood-flooring"),
    ("Laminate",        f"{BASE_URL}/laminate"),
    ("LVT",             f"{BASE_URL}/luxury-vinyl-tiles"),
    ("Herringbone",     f"{BASE_URL}/herringbone-flooring"),
    ("Vinyl",           f"{BASE_URL}/vinyl-flooring"),
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

IMG_HDR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept"    : "image/webp,image/apng,image/*,*/*;q=0.8",
    "Referer"   : "https://www.factory-direct-flooring.co.uk/",
}

REQ_HDR = {
    "User-Agent"      : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept"          : "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language" : "en-GB,en;q=0.9",
    "Accept-Encoding" : "gzip, deflate, br",
    "Connection"      : "keep-alive",
}

IMAGELY_RE = re.compile(
    r'https://imagely\.factory-direct-flooring\.co\.uk'
    r'/media/catalog/product/[^\s"\'<>\)\\,\]]+', re.I
)

# ── Descriptions ──────────────────────────────────────────────────────────────

def hints(title):
    tl = title.lower()
    brands = ['karndean','kahrs','quick-step','quickstep','amtico','ted todd','elka',
              'wicanders','moduleo','polyflor','tarkett','egger','balterio',
              'camaro','boen','hakwood','woodpecker','lifestyle','rhinofloor']
    brand  = next((b.title() for b in brands if b in tl), "")
    thick  = re.search(r'(\d+(?:\.\d+)?)\s*mm', title, re.I)
    ts     = thick.group(1)+"mm" if thick else ""
    cols   = ['oak','walnut','pine','ash','maple','birch','cherry','white','grey','gray',
              'black','brown','beige','cream','ivory','natural','smoked','rustic','aged',
              'blond','golden','silver','slate','stone','marble','teak']
    colour = next((c.title() for c in cols if c in tl), "")
    return brand, ts, colour

def build_desc(title, cat):
    brand, thick, colour = hints(title)
    bl = f" by <strong>{brand}</strong>" if brand else " from Factory Direct Flooring"
    cl = f" in a beautiful <strong>{colour}</strong> finish" if colour else ""
    tl = f" with a <strong>{thick}</strong> thickness" if thick else ""
    d = {
        "Solid Wood":      (f"<h2>{title}</h2><p><strong>{title}</strong>{bl}{cl}{tl}. 100% genuine solid timber — unmatched natural beauty that improves with age.</p>",      "<h3>Key Features</h3><ul><li>100% Real Solid Timber</li><li>Sand &amp; Refinish 5x</li><li>Natural Insulator</li><li>Adds Property Value</li><li>Sustainably Sourced</li></ul>",           "<h3>Installation</h3><p>Secret-nail or glue. Acclimatise 48–72 hrs. 15mm expansion gap.</p>", "<h3>Care</h3><p>Soft brush vacuum. Wood cleaner on damp mop. Wipe spills immediately.</p>"),
        "Engineered Wood": (f"<h2>{title}</h2><p><strong>{title}</strong>{bl}{cl}{tl}. Real wood surface with stable multi-layer core — beauty and durability. UFH compatible.</p>", "<h3>Key Features</h3><ul><li>Real Wood Surface</li><li>Stable Multi-Layer Core</li><li>UFH Compatible</li><li>All Floor Levels</li><li>Click/Nail/Glue Options</li></ul>",     "<h3>Installation</h3><p>Floating click, nail or glue. UFH max 27°C. Acclimatise 48 hrs.</p>", "<h3>Care</h3><p>Vacuum regularly. Wood cleaner on damp mop. No steam or excess water.</p>"),
        "Laminate":        (f"<h2>{title}</h2><p><strong>{title}</strong>{bl}{cl}{tl}. Authentic wood or stone look — scratch-resistant and easy to fit for busy homes.</p>",       "<h3>Key Features</h3><ul><li>HD Surface Layer</li><li>AC-Rated Scratch Resistance</li><li>Easy Click Fit</li><li>V-Groove Edges</li><li>Low Maintenance</li></ul>",               "<h3>Installation</h3><p>Click-lock floating. 10mm expansion gap. Quality underlay required.</p>", "<h3>Care</h3><p>Vacuum with soft brush. Well-wrung damp mop. No steam cleaners.</p>"),
        "LVT":             (f"<h2>{title}</h2><p><strong>{title}</strong>{bl}{cl}{tl}. 100% waterproof luxury vinyl — hyper-realistic designs for every room.</p>",                  "<h3>Key Features</h3><ul><li>100% Waterproof</li><li>Commercial Wear Layer</li><li>Hyper-Realistic Surface</li><li>Warmer Than Tile</li><li>UFH Compatible</li></ul>",           "<h3>Installation</h3><p>Click-lock, loose-lay or glue. Clean dry flat subfloor.</p>",             "<h3>Care</h3><p>Vacuum to remove grit. Warm water + LVT cleaner. No abrasives.</p>"),
        "Herringbone":     (f"<h2>{title}</h2><p><strong>{title}</strong>{bl}{cl}{tl}. Iconic herringbone pattern — timeless elegance in wood and LVT.</p>",                        "<h3>Key Features</h3><ul><li>Iconic 45° Pattern</li><li>Creates Space</li><li>Wood &amp; LVT Options</li><li>Unique Character</li><li>Suits All Rooms</li></ul>",                 "<h3>Installation</h3><p>Mark centre line and 45° carefully. Professional fitting recommended.</p>", "<h3>Care</h3><p>Wood type: wood cleaner. LVT type: warm water + LVT cleaner.</p>"),
        "Vinyl":           (f"<h2>{title}</h2><p><strong>{title}</strong>{bl}{cl}{tl}. Fully waterproof premium vinyl — ideal for kitchens, bathrooms and busy areas.</p>",          "<h3>Key Features</h3><ul><li>Fully Waterproof</li><li>Tough Wear Surface</li><li>Cushioned &amp; Quiet</li><li>Easy to Clean</li><li>Realistic HD Designs</li></ul>",             "<h3>Installation</h3><p>Loose-laid, adhered or click-lock. Clean dry smooth subfloor.</p>",        "<h3>Care</h3><p>Sweep and mop with warm water + mild cleaner.</p>"),
        "Accessories":     (f"<h2>{title}</h2><p><strong>{title}</strong>{bl}. Professional quality finishing accessories for a perfect installation.</p>",                           "<h3>Key Features</h3><ul><li>Professional Quality</li><li>Wide Compatibility</li><li>Easy Installation</li><li>Excellent Value</li></ul>",                                          "<h3>Installation</h3><p>Refer to packaging guidelines.</p>",                                      "<h3>Care</h3><p>Maintenance-free once installed.</p>"),
    }
    default = (f"<h2>{title}</h2><p>{title}{bl}. Quality flooring from Factory Direct Flooring.</p>","<h3>Features</h3><ul><li>High quality</li><li>Stylish design</li><li>Easy maintenance</li></ul>","<h3>Installation</h3><p>Refer to product specification.</p>","<h3>Care</h3><p>Clean with appropriate products.</p>")
    i,f,n,c = d.get(cat, default)
    why = "<h3>Why Factory Direct Flooring?</h3><p>UK's trusted flooring specialist — premium quality at factory direct prices. Free samples, full warranties and fast UK delivery.</p>"
    return f"{i}\n{f}\n{n}\n{c}\n{why}"

# ── Helpers ────────────────────────────────────────────────────────────────────

def clean(t):
    if not t: return ""
    t = unescape(str(t)); t = re.sub(r"<[^>]+>"," ",t)
    return re.sub(r"\s+"," ",t).strip()

def price_fmt(p):
    try:
        f = float(str(p).replace(",","").replace("£","").strip())
        return f"{f:.2f}" if f > 0 else ""
    except: return ""

def make_handle(url, name):
    slug = url.rstrip("/").split("/")[-1].replace(".html","")
    slug = re.sub(r"[^a-z0-9\-]","",slug.lower()).strip("-")
    if slug and len(slug)>4: return slug[:200]
    return re.sub(r"[^a-z0-9]+","-",name.lower()).strip("-")[:200]

def safe_fname(url, idx):
    fname = os.path.basename(urlparse(url).path)
    fname = re.sub(r'[^a-zA-Z0-9\._\-]','_',fname)
    if not fname or len(fname)<4: fname = f"img_{idx}.jpg"
    return f"{idx:05d}_{fname}"

def get_imagely_urls(text):
    found = []; seen = set()
    for m in IMAGELY_RE.finditer(text):
        u = m.group(0).split("?")[0]
        if u not in seen and len(u)>60 and not u.endswith("/"):
            seen.add(u); found.append(u)
    return found

# ── Name-based image matching ─────────────────────────────────────────────────

def name_keywords(name):
    """
    Extract meaningful keywords from product name for image matching.
    'Stoutland Oak 18mm Natural Matt' → ['stoutland', 'oak', 'natural', 'matt']
    """
    stop = {'flooring','floor','mm','cm','wood','the','and','for',
            'with','from','solid','engineered','laminate','vinyl','lvt',
            'herringbone','click','pack','per','sqm','plank','wide','long',
            'collection','series','range','grade','class','style'}
    words = re.sub(r'[^a-z0-9\s]','',name.lower()).split()
    return [w for w in words if len(w)>3 and w not in stop]

def match_images_to_products(products, all_imgs):
    """
    Match imagely CDN images to products by comparing
    product name keywords against image FILENAME (not URL path).
    """
    used = set()

    # Sort products: prefer those with more specific names
    sorted_prods = sorted(products.values(), key=lambda p: -len(name_keywords(p["name"])))

    for p in sorted_prods:
        kws = name_keywords(p["name"])
        if not kws: continue

        scored = []
        for img_url in all_imgs:
            if img_url in used: continue
            # Match against filename only (most specific part)
            fname = os.path.basename(urlparse(img_url).path).lower()
            fname = re.sub(r'[^a-z0-9]', ' ', fname)
            score = sum(1 for kw in kws if kw in fname)
            # Bonus: first keyword (most specific, usually product name)
            if kws[0] in fname: score += 3
            if score > 0:
                scored.append((score, img_url))

        scored.sort(key=lambda x: -x[0])
        matched = []
        for score, img_url in scored:
            if img_url in used or len(matched) >= MAX_IMAGES: continue
            matched.append(img_url)
            used.add(img_url)

        p["images"] = matched

    return products

# ── Fetch page HTML (Playwright or requests) ──────────────────────────────────

def fetch_rendered(page, url):
    """Fetch URL with Playwright, scroll to load all lazy images, return HTML."""
    try:
        resp = page.goto(url, wait_until="domcontentloaded", timeout=30000)
        if not resp or resp.status >= 400:
            return ""

        # Wait for any product content
        try:
            page.wait_for_selector("[class*='product']", timeout=8000)
        except: pass

        # Scroll slowly to trigger ALL lazy loading
        page.evaluate("window.scrollTo(0,0)")
        height = page.evaluate("document.body.scrollHeight")
        pos = 0
        while pos < height:
            page.evaluate(f"window.scrollTo(0, {pos})")
            time.sleep(0.2)
            pos += 400
            height = page.evaluate("document.body.scrollHeight")

        # Final scroll + wait
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(2)

        # Grab ALL img src values directly from DOM
        extra_imgs = page.evaluate("""
            () => Array.from(document.images)
                       .map(i => [i.src, i.getAttribute('data-src'),
                                  i.getAttribute('data-original'),
                                  i.getAttribute('data-lazy')])
                       .flat()
                       .filter(u => u && u.includes('imagely') && u.includes('catalog'))
        """)

        html = page.content()

        # Inject extra imagely URLs as hidden text so regex finds them
        extra_block = "\n".join(f"<!-- {u} -->" for u in (extra_imgs or []) if u)
        return html + "\n" + extra_block

    except Exception as e:
        print(f"      Playwright error: {e}")
        return ""

def fetch_requests(url):
    for _ in range(3):
        try:
            r = requests.get(url, headers=REQ_HDR, timeout=25)
            if r.status_code == 200: return r.text
            if r.status_code == 404: return ""
        except: time.sleep(1)
    return ""

# ── Parse JSON-LD from HTML ───────────────────────────────────────────────────

def parse_jsonld(html, cat):
    """Extract products from JSON-LD — proven to work for 295 products."""
    products = {}
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
                    brand=""; b=p.get("brand",{})
                    if isinstance(b,dict): brand=clean(b.get("name",""))
                    elif isinstance(b,str): brand=clean(b)
                    price=""; compare=""; stock="active"
                    of=p.get("offers",{})
                    if isinstance(of,list): of=of[0] if of else {}
                    if isinstance(of,dict):
                        price=str(of.get("price",of.get("lowPrice","")))
                        hp=of.get("highPrice","")
                        compare=str(hp) if hp and hp!=price else ""
                        stock="active" if "InStock" in of.get("availability","") else "draft"
                    url=p.get("url","")
                    products[nm]={
                        "name":nm,"sku":str(p.get("sku","")),"url":url,
                        "brand":brand,"price":price,"compare":compare,
                        "stock":stock,"cat":cat,"images":[],
                    }
                if item.get("@type")=="ItemList":
                    for el in item.get("itemListElement",[]): proc(el.get("item",el))
                elif item.get("@type")=="Product": proc(item)
        except: pass
    return products

# ── Main scraper ───────────────────────────────────────────────────────────────

def scrape_all():
    all_products = {}

    if PLAYWRIGHT_OK:
        pw_ctx = sync_playwright().__enter__()
        browser = pw_ctx.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx     = browser.new_context(
            viewport={"width":1920,"height":1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
        )
        page_pw = ctx.new_page()
        page_pw.set_default_timeout(30000)
        using_pw = True
    else:
        page_pw = None
        using_pw = False

    try:
        for cat, base_url in CATEGORIES:
            print(f"\n  [{cat}]")
            page_num = 1
            cur_url  = f"{base_url}?product_list_limit=100"

            while True:
                print(f"    Page {page_num}...", end=" ", flush=True)

                # Fetch HTML
                if using_pw:
                    html = fetch_rendered(page_pw, cur_url)
                    if not html:
                        html = fetch_requests(cur_url)
                else:
                    html = fetch_requests(cur_url)

                if not html:
                    print("FAILED")
                    break

                # Extract products (JSON-LD — proven working)
                prods    = parse_jsonld(html, cat)

                # Extract ALL imagely images from rendered HTML
                all_imgs = get_imagely_urls(html)

                # Add new products
                new = 0
                for nm, p in prods.items():
                    if nm not in all_products:
                        all_products[nm] = p; new += 1

                # Match images to products found so far (name-based)
                all_products = match_images_to_products(all_products, all_imgs)
                with_imgs = sum(1 for p in all_products.values() if p.get("images"))

                print(f"+{new} prods | imgs_on_page:{len(all_imgs)} | with_imgs:{with_imgs}/{len(all_products)}")
                if new == 0 and page_num > 1: break

                # Next page
                next_url = None
                if using_pw:
                    try:
                        nxt = page_pw.query_selector("link[rel='next']")
                        if nxt: next_url = nxt.get_attribute("href")
                    except: pass
                if not next_url:
                    m = re.search(r'href=["\']([^"\']*[?&]p='+str(page_num+1)+r'[^"\']*)["\']', html)
                    if m:
                        n = m.group(1)
                        next_url = n if n.startswith("http") else BASE_URL+n
                if not next_url: break
                cur_url  = next_url
                page_num += 1
                time.sleep(0.5)

    finally:
        if using_pw:
            try: browser.close()
            except: pass
            try: pw_ctx.__exit__(None,None,None)
            except: pass

    return list(all_products.values())

# ── Download images ────────────────────────────────────────────────────────────

def download_images(products):
    os.makedirs(IMAGES_DIR, exist_ok=True)
    total = sum(len(p.get("images",[])) for p in products)
    print(f"\n  Downloading {total} images from imagely CDN...")
    dl=0; fail=0; idx=0

    for p in products:
        local=[]
        for img_url in p.get("images",[]):
            idx+=1
            fname = safe_fname(img_url, idx)
            lpath = os.path.join(IMAGES_DIR, fname)
            gh    = f"{IMAGES_BASE}/{fname}"

            if os.path.exists(lpath) and os.path.getsize(lpath)>1000:
                local.append(gh); dl+=1; continue

            try:
                r = requests.get(img_url, headers=IMG_HDR, timeout=15, stream=True)
                if r.status_code==200:
                    with open(lpath,'wb') as f:
                        for chunk in r.iter_content(8192): f.write(chunk)
                    if os.path.getsize(lpath)>1000: local.append(gh); dl+=1
                    else: os.remove(lpath); fail+=1
                else: fail+=1
            except: fail+=1
            time.sleep(0.1)

        p["local_images"] = local

    print(f"  Downloaded:{dl} | Failed:{fail}")
    return products

# ── Build CSV ──────────────────────────────────────────────────────────────────

def build_rows(p):
    name=(p.get("name") or "").strip()
    if not name: return []
    handle  = make_handle(p.get("url",""),name)
    cat     = p.get("cat","Flooring")
    body    = build_desc(name,cat)
    vendor  = p.get("brand","").strip() or "Factory Direct Flooring"
    tags    = cat.lower().replace(" ","-")
    images  = [i for i in p.get("local_images",[]) if i and i.startswith("http")]
    price   = price_fmt(p.get("price","")) or "0.00"
    compare = price_fmt(p.get("compare",""))
    first   = images[0] if images else ""
    seo_d   = f"Buy {name} at Factory Direct Flooring — {cat} at competitive prices. Free samples and UK delivery."[:320]

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
        row1["Image Src"]=first; row1["Image Position"]="1"; row1["Image Alt Text"]=name
    rows.append(row1)
    for i,img in enumerate(images[1:],2):
        e={k:"" for k in SHOPIFY_COLS}
        e.update({"Handle":handle,"Image Src":img,"Image Position":str(i),"Image Alt Text":name})
        rows.append(e)
    return rows

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(OUTPUT_DIR,exist_ok=True); os.makedirs(IMAGES_DIR,exist_ok=True)
    t0=time.time()
    print("\n"+"="*65)
    print("  FACTORY DIRECT FLOORING — FINAL WORKING SCRAPER")
    print(f"  Mode: {'Playwright + requests' if PLAYWRIGHT_OK else 'requests only'}")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*65)

    products = scrape_all()

    if not products:
        print("  No products found")
        with open(f"{OUTPUT_DIR}/factory_flooring_shopify_{TIMESTAMP}.csv","w",encoding="utf-8-sig",newline="") as f:
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
        json.dump([{"name":p["name"],"price":p.get("price",""),"cat":p["cat"],
                    "img_count":len(p.get("local_images",[])),"url":p.get("url","")}
                   for p in products],f,ensure_ascii=False,indent=2)

    el       = round(time.time()-t0)
    with_img = len([p for p in products if p.get("local_images")])
    cats={}
    for p in products: cats[p.get("cat","?")]=cats.get(p.get("cat","?"),0)+1

    print(f"\n{'='*65}")
    print(f"  DONE in {el//60}m {el%60:02d}s")
    print(f"  Products    : {len(products)}")
    print(f"  CSV rows    : {len(all_rows)}")
    print(f"  With images : {with_img} ({round(with_img/len(products)*100) if products else 0}%)")
    print(f"\n  Categories (Smart Collections → Product type is equal to):")
    for cat,cnt in sorted(cats.items(),key=lambda x:-x[1]):
        print(f"    {cat:<20} {cnt:>3}")
    print(f"\n  CSV → {csv_file}")
    print(f"  Shopify → Products → Import → Upload CSV")
    print("="*65)

if __name__=="__main__":
    main()
