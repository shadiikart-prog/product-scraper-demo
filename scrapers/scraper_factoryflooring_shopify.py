"""
FACTORY DIRECT FLOORING — FINAL CLEAN SHOPIFY SCRAPER
✓ Max 3 images per product (only product-specific, no mixing)
✓ Auto-generated descriptions
✓ Type = category (for Smart Collections)
✓ Product Category = blank (zero taxonomy errors)
✓ Clean CSV — no raw data overflow
"""

import requests, json, csv, re, os, time
from datetime import datetime
from html import unescape

BASE_URL   = "https://www.factory-direct-flooring.co.uk"
OUTPUT_DIR = "output"
TIMESTAMP  = datetime.now().strftime("%Y%m%d_%H%M%S")
MAX_IMAGES = 3  # Max images per product

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

DESC_TEMPLATES = {
    "Solid Wood Flooring"      : "Premium solid wood flooring from Factory Direct Flooring. Real wood construction for a natural, timeless finish. Suitable for residential and commercial use. Can be sanded and refinished multiple times for a lasting floor.",
    "Engineered Wood Flooring" : "High-quality engineered wood flooring from Factory Direct Flooring. Multi-layer construction for enhanced stability and durability. Compatible with underfloor heating systems. A beautiful and practical choice for any room.",
    "Laminate Flooring"        : "Durable laminate flooring from Factory Direct Flooring. Scratch-resistant surface with a realistic wood or stone effect. Easy click-fit installation suitable for DIY. Ideal for busy family homes and high-traffic areas.",
    "LVT Flooring"             : "Luxury Vinyl Tile flooring from Factory Direct Flooring. 100% waterproof and highly durable wear layer. Perfect for kitchens, bathrooms and hallways. Comfortable underfoot with stunning realistic designs.",
    "Herringbone Flooring"     : "Elegant herringbone pattern flooring from Factory Direct Flooring. Classic design that adds character and style to any room. Available in wood and LVT options. Suits both traditional and contemporary interiors.",
    "Vinyl Flooring"           : "Quality vinyl flooring from Factory Direct Flooring. Fully waterproof and extremely easy to clean. Ideal for kitchens, bathrooms and utility rooms. Available in a wide range of on-trend colours and styles.",
    "Carpet"                   : "Soft and stylish carpet from Factory Direct Flooring. Warm and comfortable underfoot with excellent sound insulation. Available in a variety of colours, textures and pile heights. Perfect for bedrooms and living areas.",
    "Underlay"                 : "Professional-grade underlay from Factory Direct Flooring. Provides cushioning, sound insulation and thermal comfort. Compatible with all floor types including underfloor heating. Essential for a perfect flooring installation.",
    "Accessories"              : "Quality flooring accessories from Factory Direct Flooring. Everything you need for a professional flooring installation. High-quality trims, adhesives and installation tools to complement your new floor.",
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
            r = requests.get(url, headers=HEADERS, timeout=20)
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

def clean_img(url):
    """Clean image URL — remove query params, ensure valid."""
    u = str(url).strip().split("?")[0]
    if u.startswith("http") and "catalog/product" in u and len(u) > 50:
        return u
    return ""

def get_product_images_only(item):
    """
    Get images ONLY from the JSON-LD product item itself.
    Do NOT scan whole page — avoids mixing images from other products.
    Returns max MAX_IMAGES clean URLs.
    """
    images = []
    seen   = set()

    def add(u):
        u = clean_img(u)
        if u and u not in seen:
            seen.add(u)
            images.append(u)

    imgs = item.get("image", [])
    if isinstance(imgs, str): imgs = [imgs]
    if isinstance(imgs, dict): imgs = [imgs.get("url","")]
    for img in imgs:
        add(img)

    # Also check offers.image
    offers = item.get("offers", {})
    if isinstance(offers, list): offers = offers[0] if offers else {}
    if isinstance(offers, dict):
        add(offers.get("image",""))

    return images[:MAX_IMAGES]


def get_card_image(card_html):
    """Get single image from an HTML product card."""
    for pat in [
        r'data-src=["\']([^"\']+/media/catalog/product/[^"\'?\s]+)',
        r'src=["\']([^"\']+/media/catalog/product/[^"\'?\s]+)',
        r'content=["\']([^"\']+/media/catalog/product/[^"\'?\s]+)',
    ]:
        m = re.search(pat, card_html, re.I)
        if m:
            u = clean_img(m.group(1))
            if u: return [u]
    return []

def build_description(name, cat_name):
    template = DESC_TEMPLATES.get(cat_name, "Quality flooring product from Factory Direct Flooring.")
    return f"<h2>{name}</h2><p>{template}</p>"

# ── Parse products from category page ────────────────────────────────────────

def parse_page(html, cat_name):
    products = []
    seen     = set()

    # ── JSON-LD ───────────────────────────────────────────────────────
    for block in re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL
    ):
        try:
            data  = json.loads(block.strip())
            items = data if isinstance(data, list) else [data]
            for item in items:
                if not isinstance(item, dict): continue

                if item.get("@type") == "ItemList":
                    for el in item.get("itemListElement", []):
                        p = el.get("item", el)
                        prod = extract_product(p, cat_name)
                        if prod and prod["name"] not in seen:
                            seen.add(prod["name"])
                            products.append(prod)

                elif item.get("@type") == "Product":
                    prod = extract_product(item, cat_name)
                    if prod and prod["name"] not in seen:
                        seen.add(prod["name"])
                        products.append(prod)
        except: pass

    # ── HTML cards fallback ───────────────────────────────────────────
    if not products:
        cards = re.findall(
            r'<(?:li|div|article)[^>]+class="[^"]*product[^"]*item[^"]*"[^>]*>(.*?)</(?:li|div|article)>',
            html, re.DOTALL|re.I
        )
        for card in cards:
            nm = (re.search(r'class="[^"]*(?:product.name|name)[^"]*"[^>]*>.*?<a[^>]*>(.*?)</a>', card, re.DOTALL|re.I)
                  or re.search(r'<a[^>]+title="([^"]{5,})"', card))
            name = clean(nm.group(1)) if nm else ""
            if not name or name in seen: continue
            seen.add(name)

            url_m    = re.search(r'href="(' + re.escape(BASE_URL) + r'/[^"]+)"', card)
            prod_url = url_m.group(1) if url_m else ""
            pm       = re.search(r'£\s*([\d,]+\.?\d*)', card)
            price    = pm.group(1).replace(",","") if pm else ""
            sku_m    = re.search(r'data-sku=["\']([^"\']+)["\']', card)
            sku      = sku_m.group(1) if sku_m else ""

            # Only card-specific image
            images   = get_card_image(card)

            products.append({
                "name"    : name,
                "sku"     : sku,
                "price"   : price,
                "compare" : "",
                "brand"   : "Factory Direct Flooring",
                "category": cat_name,
                "images"  : images,
                "stock"   : "active",
                "url"     : prod_url,
            })

    return products


def extract_product(item, cat_name):
    if not isinstance(item, dict) or item.get("@type") != "Product":
        return None
    name = clean(item.get("name",""))
    if not name or len(name) < 3: return None

    sku   = str(item.get("sku",""))
    brand = ""
    b     = item.get("brand",{})
    if isinstance(b,dict): brand = clean(b.get("name",""))
    elif isinstance(b,str): brand = clean(b)

    price=""; compare=""; stock="active"
    offers = item.get("offers",{})
    if isinstance(offers,list): offers = offers[0] if offers else {}
    if isinstance(offers,dict):
        price   = str(offers.get("price", offers.get("lowPrice","")))
        hp      = offers.get("highPrice","")
        compare = str(hp) if hp and hp != price else ""
        avail   = offers.get("availability","")
        stock   = "active" if "InStock" in avail else "draft"

    # Only get images from THIS product's JSON-LD — no page scanning
    images = get_product_images_only(item)

    return {
        "name"    : name,
        "sku"     : sku,
        "price"   : price,
        "compare" : compare,
        "brand"   : brand or "Factory Direct Flooring",
        "category": cat_name,
        "images"  : images,
        "stock"   : stock,
        "url"     : item.get("url",""),
    }

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

            print(f"+{new} (total:{len(all_products)})")
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
            time.sleep(0.5)

    return all_products

# ── Build Shopify rows ────────────────────────────────────────────────────────

def build_rows(p):
    name = (p.get("name") or "").strip()
    if not name: return []

    handle    = p.get("handle") or make_handle(p.get("url",""), name)
    cat_raw   = p.get("category","Flooring")
    body      = build_description(name, cat_raw)
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
        "Body (HTML)"                : body,
        "Vendor"                     : vendor,
        "Product Category"           : "",
        "Type"                       : cat_raw,
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
    print("  FACTORY DIRECT FLOORING — FINAL CLEAN SHOPIFY SCRAPER")
    print(f"  Max {MAX_IMAGES} images per product | Auto descriptions")
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
            "brand"    : p["brand"],
            "category" : p["category"],
            "stock"    : p["stock"],
            "images"   : p["images"],
            "img_count": len(p["images"]),
            "url"      : p.get("url",""),
        } for p in products], f, ensure_ascii=False, indent=2)

    elapsed    = round(time.time() - t0)
    with_img   = len([p for p in products if p["images"]])
    cats = {}
    for p in products:
        cats[p["category"]] = cats.get(p["category"], 0) + 1

    print(f"\n{'='*65}")
    print(f"  DONE in {elapsed//60}m {elapsed%60:02d}s")
    print(f"  Products   : {len(products)}")
    print(f"  CSV rows   : {len(all_rows)}")
    print(f"  With images: {with_img} ({round(with_img/len(products)*100) if products else 0}%)")
    print(f"\n  By category:")
    for cat, count in sorted(cats.items(), key=lambda x: -x[1]):
        print(f"    {cat:<35} {count:>3}")
    print(f"\n  CSV  : {csv_file}")
    print(f"  JSON : {json_file}")
    print(f"  Shopify → Products → Import → Upload CSV")
    print(f"  Smart Collections → Product type is equal to [category]")
    print("="*65)


if __name__ == "__main__":
    main()
