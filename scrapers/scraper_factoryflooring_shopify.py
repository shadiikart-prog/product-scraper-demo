"""
FACTORY DIRECT FLOORING — DEFINITIVE SHOPIFY SCRAPER
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FIXES:
  ✓ Correct category URLs (from user's own list)
  ✓ Card-level image extraction (no slug matching)
  ✓ Playwright scrolls page → all lazy images load
  ✓ Each product gets ONLY its own images
  ✓ Prices, SKU, descriptions, types
  ✓ GitHub Pages images → Shopify import 100%
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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

GITHUB_USER  = "shadiikart-prog"
GITHUB_REPO  = "product-scraper-demo"
IMAGES_BASE  = f"https://{GITHUB_USER}.github.io/{GITHUB_REPO}/scrapers/output/images"

# ── CORRECT category URLs (from user's own navigation menu) ──────────────────
CATEGORIES = [
    ("Solid Wood",       f"{BASE_URL}/solid-wood-flooring"),
    ("Engineered Wood",  f"{BASE_URL}/engineered-wood-flooring"),
    ("Laminate",         f"{BASE_URL}/laminate"),            # ← FIXED
    ("LVT",              f"{BASE_URL}/luxury-vinyl-tiles"),  # ← FIXED
    ("Herringbone",      f"{BASE_URL}/herringbone-flooring"),
    ("Vinyl",            f"{BASE_URL}/vinyl-flooring"),
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

IMG_HDR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept"    : "image/webp,image/apng,image/*,*/*;q=0.8",
    "Referer"   : "https://www.factory-direct-flooring.co.uk/",
}

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
        "Solid Wood":      (f"<h2>{title}</h2><p><strong>{title}</strong>{bl}{cl}{tl}. 100% genuine solid timber delivering unmatched natural beauty that only improves with age. A true lifetime investment.</p>", "<h3>Key Features</h3><ul><li><strong>100% Real Solid Timber</strong></li><li><strong>Sand &amp; Refinish 5x</strong></li><li><strong>Natural Insulator</strong></li><li><strong>Adds Property Value</strong></li><li><strong>Sustainably Sourced</strong></li></ul>", "<h3>Installation</h3><p>Secret-nail or glue. Acclimatise 48–72 hrs. 15mm expansion gap.</p>", "<h3>Care</h3><p>Soft brush vacuum. Wood cleaner on damp mop. Wipe spills immediately.</p>"),
        "Engineered Wood": (f"<h2>{title}</h2><p><strong>{title}</strong>{bl}{cl}{tl}. Real wood top layer with multi-layer stable core — authentic beauty with superior performance. UFH compatible.</p>", "<h3>Key Features</h3><ul><li><strong>Real Wood Surface</strong></li><li><strong>Stable Multi-Layer Core</strong></li><li><strong>UFH Compatible</strong></li><li><strong>All Floor Levels</strong></li><li><strong>Click/Nail/Glue Options</strong></li></ul>", "<h3>Installation</h3><p>Floating click, secret-nail or glue. UFH max 27°C. Acclimatise 48 hrs.</p>", "<h3>Care</h3><p>Vacuum regularly. Wood cleaner on damp mop. No steam or excess water.</p>"),
        "Laminate":        (f"<h2>{title}</h2><p><strong>{title}</strong>{bl}{cl}{tl}. Authentic wood or stone look at a fraction of the cost — scratch-resistant and easy to install for busy homes.</p>", "<h3>Key Features</h3><ul><li><strong>HD Surface Layer</strong></li><li><strong>AC-Rated Scratch Resistance</strong></li><li><strong>Easy Click Fit</strong></li><li><strong>V-Groove Edges</strong></li><li><strong>Low Maintenance</strong></li></ul>", "<h3>Installation</h3><p>Click-lock floating. 10mm expansion gap. Quality underlay required.</p>", "<h3>Care</h3><p>Vacuum with soft brush. Well-wrung damp mop. No steam cleaners.</p>"),
        "LVT":             (f"<h2>{title}</h2><p><strong>{title}</strong>{bl}{cl}{tl}. 100% waterproof luxury vinyl tile — hyper-realistic designs for any room in the home.</p>", "<h3>Key Features</h3><ul><li><strong>100% Waterproof</strong></li><li><strong>Commercial Wear Layer</strong></li><li><strong>Hyper-Realistic Surface</strong></li><li><strong>Warmer Than Tile</strong></li><li><strong>UFH Compatible</strong></li></ul>", "<h3>Installation</h3><p>Click-lock, loose-lay or glue. Clean dry flat subfloor. No acclimatisation.</p>", "<h3>Care</h3><p>Vacuum to remove grit. Warm water + LVT cleaner. No abrasives.</p>"),
        "Herringbone":     (f"<h2>{title}</h2><p><strong>{title}</strong>{bl}{cl}{tl}. The iconic herringbone pattern — timeless elegance in wood and LVT for any room.</p>", "<h3>Key Features</h3><ul><li><strong>Iconic 45° Pattern</strong></li><li><strong>Creates Space</strong></li><li><strong>Wood &amp; LVT Options</strong></li><li><strong>Unique Character</strong></li><li><strong>Suits All Rooms</strong></li></ul>", "<h3>Installation</h3><p>Mark centre line and 45° carefully. Professional fitting recommended.</p>", "<h3>Care</h3><p>Wood type: wood cleaner. LVT type: warm water + LVT cleaner.</p>"),
        "Vinyl":           (f"<h2>{title}</h2><p><strong>{title}</strong>{bl}{cl}{tl}. Fully waterproof premium vinyl — ideal for kitchens, bathrooms and all high-traffic areas.</p>", "<h3>Key Features</h3><ul><li><strong>Fully Waterproof</strong></li><li><strong>Tough Wear Surface</strong></li><li><strong>Cushioned &amp; Quiet</strong></li><li><strong>Easy to Clean</strong></li><li><strong>Realistic HD Designs</strong></li></ul>", "<h3>Installation</h3><p>Loose-laid, adhered or click-lock. Clean dry smooth subfloor.</p>", "<h3>Care</h3><p>Sweep and mop with warm water + mild cleaner.</p>"),
        "Accessories":     (f"<h2>{title}</h2><p><strong>{title}</strong>{bl}. Professional quality finishing accessories for a perfect installation.</p>", "<h3>Key Features</h3><ul><li><strong>Professional Quality</strong></li><li><strong>Wide Compatibility</strong></li><li><strong>Easy Installation</strong></li><li><strong>Excellent Value</strong></li></ul>", "<h3>Installation</h3><p>Refer to packaging guidelines. Contact our team for advice.</p>", "<h3>Care</h3><p>Maintenance-free once installed.</p>"),
    }
    default = (f"<h2>{title}</h2><p>{title}{bl}. Quality flooring from Factory Direct Flooring.</p>", "<h3>Features</h3><ul><li>High quality</li><li>Stylish design</li><li>Easy maintenance</li></ul>", "<h3>Installation</h3><p>Refer to product specification.</p>", "<h3>Care</h3><p>Clean with appropriate products.</p>")
    i,f,n,c = d.get(cat, default)
    why = "<h3>Why Factory Direct Flooring?</h3><p>UK's trusted flooring specialist — premium quality at factory direct prices. Free samples, full warranties and fast UK delivery on all orders.</p>"
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
    return "imagely.factory-direct-flooring.co.uk" in url and "catalog/product" in url

def clean_img_url(url):
    u = str(url or "").strip().split("?")[0]
    return u if is_imagely(u) and len(u)>60 else ""

# ── Playwright: card-level extraction ────────────────────────────────────────

def scrape_with_playwright():
    all_products = {}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=["--no-sandbox","--disable-setuid-sandbox"])
        ctx = browser.new_context(
            viewport={"width":1920,"height":1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            locale="en-GB",
        )
        page = ctx.new_page()
        page.set_default_timeout(30000)

        for cat, base_url in CATEGORIES:
            print(f"\n  [{cat}]")
            page_num = 1
            cur_url  = f"{base_url}?product_list_limit=100"

            while True:
                try:
                    print(f"    Page {page_num}...", end=" ", flush=True)
                    resp = page.goto(cur_url, wait_until="domcontentloaded", timeout=30000)
                    if not resp or resp.status >= 400:
                        print(f"HTTP {resp.status if resp else '?'} — skip")
                        break

                    # Wait for product grid
                    try:
                        page.wait_for_selector(".product-item, .product-card, [class*=product]", timeout=8000)
                    except: pass

                    # Scroll to trigger ALL lazy loading
                    scroll_h = page.evaluate("document.body.scrollHeight")
                    step     = 600
                    pos      = 0
                    while pos < scroll_h:
                        page.evaluate(f"window.scrollTo(0, {pos})")
                        time.sleep(0.3)
                        pos += step
                        scroll_h = page.evaluate("document.body.scrollHeight")
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    time.sleep(2)

                    # ── Extract products via JavaScript DOM ──────────────────
                    products_js = page.evaluate("""
                        () => {
                            const cards = document.querySelectorAll(
                                'li.product-item, div.product-item, ' +
                                'div[class*="product-item"], li[class*="product-item"]'
                            );
                            const results = [];
                            cards.forEach(card => {
                                // Name
                                const nameEl = card.querySelector(
                                    '[class*="product-name"] a, ' +
                                    '[class*="product-title"] a, ' +
                                    'h2 a, h3 a, .name a'
                                );
                                const name = nameEl ? nameEl.textContent.trim() : '';
                                if (!name) return;

                                // URL
                                const urlEl = card.querySelector('a[href]');
                                const url   = urlEl ? urlEl.href : '';

                                // Price
                                const priceEl = card.querySelector('[class*="price"]');
                                const priceText = priceEl ? priceEl.textContent : '';
                                const priceMatch = priceText.match(/[£]\\s*([\\d,]+\\.?\\d*)/);
                                const price = priceMatch ? priceMatch[1].replace(/,/g,'') : '';

                                // Images — ALL img tags in this card
                                const imgEls = card.querySelectorAll('img');
                                const images = [];
                                imgEls.forEach(img => {
                                    const urls = [
                                        img.src,
                                        img.getAttribute('data-src'),
                                        img.getAttribute('data-original'),
                                        img.getAttribute('data-lazy'),
                                        img.getAttribute('data-zoom-image'),
                                    ];
                                    urls.forEach(u => {
                                        if (u && u.includes('imagely') &&
                                            u.includes('catalog/product') &&
                                            !u.includes('placeholder')) {
                                            const clean = u.split('?')[0];
                                            if (clean.length > 60 && !images.includes(clean)) {
                                                images.push(clean);
                                            }
                                        }
                                    });
                                });

                                // Also check background-image styles
                                const divEls = card.querySelectorAll('[style*="imagely"]');
                                divEls.forEach(el => {
                                    const m = el.style.backgroundImage.match(
                                        /url\\(['"](https:\\/\\/imagely[^'"]+)['"]/
                                    );
                                    if (m) images.push(m[1].split('?')[0]);
                                });

                                results.push({name, url, price, images: images.slice(0, 3)});
                            });
                            return results;
                        }
                    """)

                    # Also get JSON-LD for better price/SKU/stock data
                    html    = page.content()
                    jl_data = {}
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
                                    if not isinstance(p,dict) or p.get("@type")!="Product": return
                                    nm = clean(p.get("name",""))
                                    if not nm: return
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
                                    jl_data[nm]={"sku":str(p.get("sku","")),"brand":brand,
                                                  "price":price,"compare":compare,"stock":stock}
                                if item.get("@type")=="ItemList":
                                    for el in item.get("itemListElement",[]): proc(el.get("item",el))
                                elif item.get("@type")=="Product": proc(item)
                        except: pass

                    new = 0
                    for item in products_js:
                        nm = item.get("name","").strip()
                        if not nm or nm in all_products: continue

                        jl     = jl_data.get(nm, {})
                        price  = jl.get("price","") or item.get("price","")
                        images = item.get("images",[])

                        all_products[nm] = {
                            "name"   : nm,
                            "url"    : item.get("url",""),
                            "cat"    : cat,
                            "sku"    : jl.get("sku",""),
                            "price"  : price,
                            "compare": jl.get("compare",""),
                            "brand"  : jl.get("brand","") or "Factory Direct Flooring",
                            "stock"  : jl.get("stock","active"),
                            "images" : [u for u in images if is_imagely(u)][:MAX_IMAGES],
                        }
                        new += 1

                    with_imgs = sum(1 for p in all_products.values() if p["images"])
                    print(f"+{new} prods | with_imgs:{with_imgs}/{len(all_products)} | total:{len(all_products)}")

                    if new == 0 and page_num > 1: break

                    # Next page
                    next_url = None
                    try:
                        nxt = page.query_selector("link[rel='next']")
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
                    time.sleep(1)

                except Exception as e:
                    print(f"ERROR: {e}")
                    break

        browser.close()

    return list(all_products.values())

# ── Download images ────────────────────────────────────────────────────────────

def download_images(products):
    os.makedirs(IMAGES_DIR, exist_ok=True)
    total_to_dl = sum(len(p.get("images",[])) for p in products)
    print(f"\n  Downloading {total_to_dl} images from imagely CDN...")
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
                if r.status_code == 200:
                    with open(lpath,'wb') as f:
                        for chunk in r.iter_content(8192): f.write(chunk)
                    if os.path.getsize(lpath)>1000:
                        local.append(gh); dl+=1
                    else:
                        os.remove(lpath); fail+=1
                else:
                    fail+=1
            except: fail+=1
            time.sleep(0.1)

        p["local_images"] = local

    print(f"  Downloaded:{dl} | Failed:{fail}")
    return products

# ── Build Shopify CSV ──────────────────────────────────────────────────────────

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
    seo_d   = f"Buy {name} at Factory Direct Flooring — {cat} at competitive prices. Free samples and fast UK delivery."[:320]

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
    print("  FACTORY DIRECT FLOORING — DEFINITIVE SCRAPER")
    print(f"  Playwright DOM extraction | Correct URLs | Card-level images")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*65)

    if not PLAYWRIGHT_OK:
        print("  ERROR: Playwright not installed! Run: pip install playwright && playwright install chromium")
        return

    products = scrape_with_playwright()

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
        json.dump([{"name":p["name"],"sku":p.get("sku",""),"price":p.get("price",""),
                    "cat":p["cat"],"img_count":len(p.get("local_images",[])),
                    "images":p.get("local_images",[]),"url":p.get("url","")}
                   for p in products],f,ensure_ascii=False,indent=2)

    el       = round(time.time()-t0)
    with_img = len([p for p in products if p.get("local_images")])
    cats={}
    for p in products: cats[p.get("cat","?")]=cats.get(p.get("cat","?"),0)+1

    print(f"\n{'='*65}")
    print(f"  DONE in {el//60}m {el%60:02d}s")
    print(f"  Products      : {len(products)}")
    print(f"  CSV rows      : {len(all_rows)}")
    print(f"  With images   : {with_img} ({round(with_img/len(products)*100) if products else 0}%)")
    print(f"\n  By category (Smart Collections → 'Product type is equal to'):")
    for cat,cnt in sorted(cats.items(),key=lambda x:-x[1]):
        print(f"    {cat:<20} {cnt:>3} products")
    print(f"\n  CSV → {csv_file}")
    print(f"  Shopify → Products → Import → Upload CSV")
    print("="*65)

if __name__=="__main__":
    main()
