"""
FACTORY DIRECT FLOORING — PRODUCT PAGE SCRAPER (PLAYWRIGHT)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Phase 1: Get product URLs from category pages (JSON-LD)
Phase 2: Visit EACH product page with Playwright
         → intercept network → get ALL product images
Phase 3: Download images → GitHub Pages → Shopify
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import json, csv, re, os, time, requests
from datetime import datetime
from html import unescape
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright

BASE_URL   = "https://www.factory-direct-flooring.co.uk"
OUTPUT_DIR = "output"
IMAGES_DIR = "output/images"
TIMESTAMP  = datetime.now().strftime("%Y%m%d_%H%M%S")
MAX_IMAGES = 3

GITHUB_USER = "shadiikart-prog"
GITHUB_REPO = "product-scraper-demo"
IMAGES_BASE = f"https://{GITHUB_USER}.github.io/{GITHUB_REPO}/scrapers/output/images"

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

# ── Descriptions ──────────────────────────────────────────────────────────────

def hints(title):
    tl = title.lower()
    brands = ['karndean','kahrs','quick-step','quickstep','amtico','ted todd','elka',
              'wicanders','moduleo','polyflor','tarkett','egger','balterio','camaro',
              'boen','hakwood','woodpecker','lifestyle','rhinofloor']
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
        "Solid Wood":      (f"<h2>{title}</h2><p><strong>{title}</strong>{bl}{cl}{tl}. 100% genuine solid timber delivering unmatched natural beauty that only improves with age.</p>",      "<h3>Key Features</h3><ul><li>100% Real Solid Timber</li><li>Sand &amp; Refinish 5x</li><li>Natural Insulator</li><li>Adds Property Value</li><li>Sustainably Sourced</li></ul>",          "<h3>Installation</h3><p>Secret-nail or glue. Acclimatise 48–72 hrs. 15mm expansion gap.</p>","<h3>Care</h3><p>Soft brush vacuum. Wood cleaner on damp mop. Wipe spills immediately.</p>"),
        "Engineered Wood": (f"<h2>{title}</h2><p><strong>{title}</strong>{bl}{cl}{tl}. Real wood surface with stable multi-layer core — authentic beauty. UFH compatible.</p>",             "<h3>Key Features</h3><ul><li>Real Wood Surface</li><li>Stable Core</li><li>UFH Compatible</li><li>All Floor Levels</li><li>Click/Nail/Glue Options</li></ul>",                         "<h3>Installation</h3><p>Floating click, nail or glue. UFH max 27°C. Acclimatise 48 hrs.</p>","<h3>Care</h3><p>Vacuum regularly. Wood cleaner on damp mop. No steam or excess water.</p>"),
        "Laminate":        (f"<h2>{title}</h2><p><strong>{title}</strong>{bl}{cl}{tl}. Authentic wood or stone look at a fraction of the cost — scratch-resistant and easy to fit.</p>",   "<h3>Key Features</h3><ul><li>HD Surface Layer</li><li>AC-Rated Scratch Resistance</li><li>Easy Click Fit</li><li>V-Groove Edges</li><li>Low Maintenance</li></ul>",                      "<h3>Installation</h3><p>Click-lock floating. 10mm expansion gap. Quality underlay required.</p>","<h3>Care</h3><p>Vacuum with soft brush. Well-wrung damp mop. No steam cleaners.</p>"),
        "LVT":             (f"<h2>{title}</h2><p><strong>{title}</strong>{bl}{cl}{tl}. 100% waterproof luxury vinyl — hyper-realistic designs for every room in the home.</p>",             "<h3>Key Features</h3><ul><li>100% Waterproof</li><li>Commercial Wear Layer</li><li>Hyper-Realistic</li><li>Warmer Than Tile</li><li>UFH Compatible</li></ul>",                         "<h3>Installation</h3><p>Click-lock, loose-lay or glue. Clean dry flat subfloor.</p>","<h3>Care</h3><p>Vacuum to remove grit. Warm water + LVT cleaner. No abrasives.</p>"),
        "Herringbone":     (f"<h2>{title}</h2><p><strong>{title}</strong>{bl}{cl}{tl}. Iconic herringbone pattern — timeless elegance in wood and LVT for any room.</p>",                  "<h3>Key Features</h3><ul><li>Iconic 45° Pattern</li><li>Creates Space</li><li>Wood &amp; LVT Options</li><li>Unique Character</li><li>Suits All Rooms</li></ul>",                       "<h3>Installation</h3><p>Mark centre line and 45°. Professional fitting recommended.</p>","<h3>Care</h3><p>Wood type: wood cleaner. LVT type: warm water + LVT cleaner.</p>"),
        "Vinyl":           (f"<h2>{title}</h2><p><strong>{title}</strong>{bl}{cl}{tl}. Fully waterproof premium vinyl — ideal for kitchens, bathrooms and high-traffic areas.</p>",         "<h3>Key Features</h3><ul><li>Fully Waterproof</li><li>Tough Wear Surface</li><li>Cushioned &amp; Quiet</li><li>Easy to Clean</li><li>Realistic HD Designs</li></ul>",                  "<h3>Installation</h3><p>Loose-laid, adhered or click-lock. Clean dry smooth subfloor.</p>","<h3>Care</h3><p>Sweep and mop with warm water + mild cleaner.</p>"),
        "Accessories":     (f"<h2>{title}</h2><p><strong>{title}</strong>{bl}. Professional quality finishing accessories for a perfect flooring installation.</p>",                         "<h3>Key Features</h3><ul><li>Professional Quality</li><li>Wide Compatibility</li><li>Easy Installation</li><li>Excellent Value</li></ul>",                                              "<h3>Installation</h3><p>Refer to packaging guidelines.</p>","<h3>Care</h3><p>Maintenance-free once installed.</p>"),
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

def is_imagely(url):
    return ("imagely.factory-direct-flooring.co.uk" in url and
            "catalog/product" in url and len(url) > 60)

# ── Phase 1: Collect product URLs from category pages ────────────────────────

def collect_product_urls(page):
    """Use Playwright to get all product URLs from all category pages."""
    all_urls = {}  # url -> category
    SKIP = {'solid-wood-flooring','engineered-wood-flooring','laminate',
            'laminate-flooring','luxury-vinyl-tiles','lvt-flooring',
            'herringbone-flooring','vinyl-flooring','accessories','carpet',
            'underlay','blog','about','contact','brands','advice',
            'advice-centre','search','checkout','cart','account',
            'wishlist','compare','sitemap','privacy','terms','delivery',
            'returns','finance','trade','samples','customer-service',''}

    for cat, base_url in CATEGORIES:
        print(f"\n  [{cat}] collecting URLs...")
        page_num = 1
        cur_url  = f"{base_url}?product_list_limit=100"

        while True:
            try:
                resp = page.goto(cur_url, wait_until="domcontentloaded", timeout=30000)
                if not resp or resp.status >= 400:
                    print(f"    HTTP {resp.status if resp else '?'} skip")
                    break
                time.sleep(2)
                html = page.content()
            except Exception as e:
                print(f"    Error: {e}"); break

            # Extract all product anchor links from JSON-LD
            found_this_page = 0
            for block in re.findall(
                r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
                html, re.DOTALL
            ):
                try:
                    data = json.loads(block)
                    items = data if isinstance(data,list) else [data]
                    for item in items:
                        if not isinstance(item,dict): continue
                        def proc(p):
                            nonlocal found_this_page
                            if not isinstance(p,dict) or p.get("@type")!="Product": return
                            url = p.get("url","")
                            if not url or url in all_urls: return
                            slug = url.rstrip("/").split("/")[-1].replace(".html","").lower()
                            if slug in SKIP or len(slug)<3: return
                            all_urls[url] = cat
                            found_this_page += 1
                        if item.get("@type")=="ItemList":
                            for el in item.get("itemListElement",[]): proc(el.get("item",el))
                        elif item.get("@type")=="Product": proc(item)
                except: pass

            # Also from anchor hrefs
            for m in re.finditer(r'href=["\'](' + re.escape(BASE_URL) + r'/([^"\'?#]+))["\']', html):
                url  = m.group(1).rstrip("/")
                slug = m.group(2).replace(".html","").rstrip("/").lower()
                if slug in SKIP or url in all_urls or len(slug)<3: continue
                if "/" in slug: continue  # skip deep URLs
                all_urls[url] = cat
                found_this_page += 1

            print(f"    Page {page_num}: +{found_this_page} URLs (total: {len(all_urls)})")

            # Next page
            next_url = None
            m = re.search(r'href=["\']([^"\']*[?&]p='+str(page_num+1)+r'[^"\']*)["\']', html)
            if m:
                n = m.group(1)
                next_url = n if n.startswith("http") else BASE_URL+n
            if not next_url or found_this_page == 0: break
            cur_url  = next_url
            page_num += 1
            time.sleep(0.5)

    return all_urls

# ── Phase 2: Scrape each product page for images ──────────────────────────────

def scrape_product_page(page, url, cat):
    """Visit one product page, intercept ALL image requests, extract data."""
    intercepted = set()

    def on_request(req):
        u = req.url.split("?")[0]
        if is_imagely(u):
            intercepted.add(u)

    page.on("request", on_request)

    try:
        resp = page.goto(url, wait_until="domcontentloaded", timeout=25000)
        if not resp or resp.status >= 400:
            page.remove_listener("request", on_request)
            return None
        time.sleep(1.5)

        # Scroll to trigger gallery lazy loading
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(1)
        page.evaluate("window.scrollTo(0, 0)")
        time.sleep(0.5)

        html = page.content()
    except Exception as e:
        try: page.remove_listener("request", on_request)
        except: pass
        return None

    page.remove_listener("request", on_request)

    # Extract from JSON-LD
    product = {
        "url":url,"cat":cat,"name":"","sku":"",
        "price":"","compare":"","brand":"","stock":"active",
    }

    for block in re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL
    ):
        try:
            data = json.loads(block)
            items = data if isinstance(data,list) else [data]
            for item in items:
                if not isinstance(item,dict) or item.get("@type")!="Product": continue
                product["name"] = clean(item.get("name",""))
                product["sku"]  = str(item.get("sku",""))
                b = item.get("brand",{})
                if isinstance(b,dict): product["brand"] = clean(b.get("name",""))
                elif isinstance(b,str): product["brand"] = clean(b)
                of = item.get("offers",{})
                if isinstance(of,list): of = of[0] if of else {}
                if isinstance(of,dict):
                    product["price"]   = str(of.get("price",of.get("lowPrice","")))
                    hp                 = of.get("highPrice","")
                    product["compare"] = str(hp) if hp and hp!=product["price"] else ""
                    product["stock"]   = "active" if "InStock" in of.get("availability","") else "draft"
        except: pass

    # Fallback name from <h1>
    if not product["name"]:
        m = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
        if m: product["name"] = clean(m.group(1))

    if not product["name"]: return None

    # Also scan HTML for imagely URLs
    imagely_re = re.compile(
        r'https://imagely\.factory-direct-flooring\.co\.uk'
        r'/media/catalog/product/[^\s"\'<>\)\\,\]]+', re.I
    )
    for m in imagely_re.finditer(html):
        u = m.group(0).split("?")[0]
        if is_imagely(u): intercepted.add(u)

    # Deduplicate and limit
    product["images"] = list(intercepted)[:MAX_IMAGES]
    return product

# ── Main scraper ──────────────────────────────────────────────────────────────

def scrape_all():
    all_products = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=["--no-sandbox","--disable-setuid-sandbox",
                  "--disable-blink-features=AutomationControlled"]
        )
        ctx = browser.new_context(
            viewport={"width":1920,"height":1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            locale="en-GB",
        )

        # Phase 1: collect URLs
        print("\n[Phase 1] Collecting product URLs...")
        nav_page = ctx.new_page()
        url_map  = collect_product_urls(nav_page)
        nav_page.close()
        print(f"\n  Total product URLs: {len(url_map)}")

        # Phase 2: scrape each product page
        print(f"\n[Phase 2] Scraping {len(url_map)} product pages...")
        url_list = list(url_map.items())
        done = 0; ok = 0; fail = 0

        for url, cat in url_list:
            done += 1
            prod_page = ctx.new_page()
            result    = scrape_product_page(prod_page, url, cat)
            prod_page.close()

            if result:
                all_products.append(result)
                ok += 1
            else:
                fail += 1

            if done % 20 == 0 or done == len(url_list):
                with_img = sum(1 for p in all_products if p.get("images"))
                print(f"  [{done:>4}/{len(url_list)}] OK:{ok} Fail:{fail} With-images:{with_img}")

            time.sleep(0.3)

        browser.close()

    return all_products

# ── Download images ────────────────────────────────────────────────────────────

def download_images(products):
    os.makedirs(IMAGES_DIR, exist_ok=True)
    total = sum(len(p.get("images",[])) for p in products)
    print(f"\n[Phase 3] Downloading {total} images...")
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
    t0 = time.time()
    print("\n"+"="*65)
    print("  FACTORY DIRECT FLOORING — PRODUCT PAGE SCRAPER")
    print(f"  Visiting each product page individually for images")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*65)

    products = scrape_all()

    if not products:
        print("  No products found")
        with open(f"{OUTPUT_DIR}/factory_flooring_shopify_{TIMESTAMP}.csv","w",
                  encoding="utf-8-sig",newline="") as f:
            csv.DictWriter(f,fieldnames=SHOPIFY_COLS).writeheader()
        return

    products = download_images(products)
    all_rows=[]
    for p in products: all_rows.extend(build_rows(p))

    csv_file = f"{OUTPUT_DIR}/factory_flooring_shopify_{TIMESTAMP}.csv"
    with open(csv_file,"w",encoding="utf-8-sig",newline="") as f:
        writer = csv.DictWriter(f,fieldnames=SHOPIFY_COLS,extrasaction="ignore")
        writer.writeheader(); writer.writerows(all_rows)

    json_file = f"{OUTPUT_DIR}/factory_flooring_{TIMESTAMP}.json"
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
    print(f"\n  Categories:")
    for cat,cnt in sorted(cats.items(),key=lambda x:-x[1]):
        print(f"    {cat:<20} {cnt:>3}")
    print(f"\n  CSV → {csv_file}")
    print("="*65)

if __name__=="__main__":
    main()
