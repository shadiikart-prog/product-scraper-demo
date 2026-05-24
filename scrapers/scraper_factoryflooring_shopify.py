"""
╔══════════════════════════════════════════════════════════════════════╗
║   FACTORY DIRECT FLOORING — FINAL SCRAPER                           ║
║   Image URL pattern: imagely.factory-direct-flooring.co.uk          ║
║   /media/catalog/product/cache/{hash}/{a}/{b}/{filename}.jpg        ║
╠══════════════════════════════════════════════════════════════════════╣
║   ✓ All products from all categories                                 ║
║   ✓ Real images from imagely CDN (max 2 per product)                ║
║   ✓ Real descriptions if available, else auto-generated             ║
║   ✓ Type = category for Smart Collections                            ║
║   ✓ Product Category blank — no taxonomy errors                     ║
║   ✓ Valid Shopify CSV — no import errors                            ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import requests
import json
import csv
import re
import os
import time
from datetime import datetime
from html import unescape

# ──────────────────────────────────────────────────────────────────────
#  CONFIG
# ──────────────────────────────────────────────────────────────────────
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
#  AUTO DESCRIPTION TEMPLATES (rich, 3000+ chars per product)
# ──────────────────────────────────────────────────────────────────────

def extract_hints(title):
    tl = title.lower()
    brands = ['karndean','kahrs','quick-step','quickstep','amtico','ted todd','elka',
              'wicanders','moduleo','polyflor','tarkett','egger','krono','berry alloc',
              'balterio','camaro','boen','hakwood','woodpecker','lifestyle','rhinofloor',
              'gerflor','forbo','altro','armstrong','pergo','kronoswiss','stoutland']
    brand  = next((b.title() for b in brands if b in tl), "")
    thick  = re.search(r'(\d+(?:\.\d+)?)\s*mm', title, re.I)
    ts     = thick.group(1)+"mm" if thick else ""
    colours = ['oak','walnut','pine','ash','maple','birch','cherry','white','grey','gray',
               'black','brown','beige','cream','ivory','natural','smoked','rustic','aged',
               'antique','vintage','blond','golden','silver','slate','stone','marble']
    colour = next((c.title() for c in colours if c in tl), "")
    return brand, ts, colour

def auto_desc(title, cat):
    brand, thick, colour = extract_hints(title)
    bl = f" by <strong>{brand}</strong>" if brand else " from Factory Direct Flooring"
    cl = f" in a beautiful <strong>{colour}</strong> finish" if colour else ""
    tl = f" with a <strong>{thick}</strong> thickness" if thick else ""

    intros = {
        "Solid Wood Flooring": f"<h2>{title}</h2><p>Introducing the <strong>{title}</strong>{bl}{cl}{tl}. Crafted from 100% genuine solid timber, this floor delivers unmatched natural beauty and authentic character that only improves with age.</p>",
        "Engineered Wood Flooring": f"<h2>{title}</h2><p>Discover the <strong>{title}</strong>{bl}{cl}{tl}. Engineered wood combines a genuine real wood top layer with multi-layer construction — giving you the warmth of timber with enhanced stability.</p>",
        "Laminate Flooring": f"<h2>{title}</h2><p>Meet the <strong>{title}</strong>{bl}{cl}{tl}. This outstanding laminate delivers the look of real wood or stone at a fraction of the cost, with scratch resistance for busy family homes.</p>",
        "LVT Flooring": f"<h2>{title}</h2><p>Introducing the <strong>{title}</strong>{bl}{cl}{tl}. Luxury Vinyl Tile combines 100% waterproof performance with hyper-realistic designs — the ultimate choice for any room.</p>",
        "Herringbone Flooring": f"<h2>{title}</h2><p>Make a statement with the <strong>{title}</strong>{bl}{cl}{tl}. The iconic herringbone pattern adds timeless elegance and character to any space.</p>",
        "Vinyl Flooring": f"<h2>{title}</h2><p>Presenting the <strong>{title}</strong>{bl}{cl}{tl}. Premium vinyl with total waterproof protection — ideal for kitchens, bathrooms and high-traffic areas.</p>",
        "Carpet": f"<h2>{title}</h2><p>Transform your home with the <strong>{title}</strong>{bl}{cl}. Luxuriously soft carpet brings warmth, comfort and style to any room — perfect for bedrooms and living rooms.</p>",
        "Underlay": f"<h2>{title}</h2><p>The <strong>{title}</strong>{bl} is professional-grade underlay designed to enhance the comfort, sound insulation and longevity of your new floor.</p>",
        "Accessories": f"<h2>{title}</h2><p>Complete your installation with the <strong>{title}</strong>{bl}. Quality finishing accessories for a truly professional flooring result.</p>",
    }

    features = {
        "Solid Wood Flooring": "<h3>Key Features</h3><ul><li><strong>100% Real Solid Timber</strong> — authentic grain in every plank</li><li><strong>Sand &amp; Refinish</strong> — up to 5 times for a lifetime of use</li><li><strong>Natural Insulator</strong> — retains warmth, reduces energy bills</li><li><strong>Adds Property Value</strong> — proven to increase home value</li><li><strong>Sustainably Sourced</strong> — from responsibly managed forests</li></ul>",
        "Engineered Wood Flooring": "<h3>Key Features</h3><ul><li><strong>Real Wood Top Layer</strong> — genuine timber veneer for natural appearance</li><li><strong>Stable Multi-Layer Core</strong> — resists warping and shrinking</li><li><strong>Underfloor Heating Compatible</strong> — works with wet and electric UFH</li><li><strong>All-Level Installation</strong> — ground, first floor and basement</li><li><strong>Flexible Fitting</strong> — click, secret nail or glue-down</li></ul>",
        "Laminate Flooring": "<h3>Key Features</h3><ul><li><strong>HD Print Layer</strong> — photorealistic wood or stone appearance</li><li><strong>Scratch-Resistant Wear Layer</strong> — handles heavy domestic use</li><li><strong>Easy Click Installation</strong> — DIY-friendly, no glue needed</li><li><strong>Bevelled V-Groove Edges</strong> — adds depth and authenticity</li><li><strong>Low Maintenance</strong> — sweep, vacuum, damp mop</li></ul>",
        "LVT Flooring": "<h3>Key Features</h3><ul><li><strong>100% Waterproof</strong> — safe for all rooms including bathrooms</li><li><strong>Commercial-Grade Wear Layer</strong> — resists scratches and heavy traffic</li><li><strong>Hyper-Realistic Designs</strong> — convincing wood and stone replicas</li><li><strong>Warmer Than Tile</strong> — comfortable underfoot</li><li><strong>UFH Compatible</strong> — works with underfloor heating</li></ul>",
        "Herringbone Flooring": "<h3>Key Features</h3><ul><li><strong>Iconic 45° Pattern</strong> — adds instant elegance and depth</li><li><strong>Creates Space</strong> — diagonal pattern enlarges the room</li><li><strong>Wood &amp; LVT Options</strong> — real wood or waterproof LVT</li><li><strong>Unique Character</strong> — natural variation in every floor</li><li><strong>Suits All Rooms</strong> — hallways, living rooms, kitchens</li></ul>",
        "Vinyl Flooring": "<h3>Key Features</h3><ul><li><strong>Fully Waterproof</strong> — safe for bathrooms and kitchens</li><li><strong>Tough Wear Surface</strong> — resists scratches and heavy use</li><li><strong>Cushioned Underfoot</strong> — warm and quiet</li><li><strong>Easy to Clean</strong> — sweep and mop to maintain</li><li><strong>Realistic Designs</strong> — convincing wood and stone effects</li></ul>",
        "Carpet": "<h3>Key Features</h3><ul><li><strong>Luxuriously Soft</strong> — perfect for bedrooms and lounges</li><li><strong>Sound Insulation</strong> — reduces impact noise between floors</li><li><strong>Thermal Properties</strong> — retains warmth, lowers energy bills</li><li><strong>Wide Colour Range</strong> — suits any décor</li><li><strong>Durable Construction</strong> — built for family life</li></ul>",
        "Underlay": "<h3>Key Features</h3><ul><li><strong>Superior Cushioning</strong> — adds comfort underfoot</li><li><strong>Sound Reduction</strong> — reduces impact noise</li><li><strong>Thermal Insulation</strong> — improves floor warmth</li><li><strong>Moisture Protection</strong> — many grades include DPM</li><li><strong>Extends Floor Life</strong> — protects your floor investment</li></ul>",
        "Accessories": "<h3>Key Features</h3><ul><li><strong>Professional Quality</strong> — trade-specification construction</li><li><strong>Precision Engineered</strong> — neat, durable finish</li><li><strong>Wide Compatibility</strong> — works with all flooring types</li><li><strong>Easy Installation</strong> — straightforward fitting</li><li><strong>Excellent Value</strong> — quality at competitive price</li></ul>",
    }

    install = {
        "Solid Wood Flooring": "<h3>Installation</h3><p>Can be secret-nailed or glued to a suitable subfloor. Acclimatise for 48-72 hours before fitting. Leave a 15mm expansion gap around all fixed objects. Professional installation strongly recommended for best results and warranty validity.</p>",
        "Engineered Wood Flooring": "<h3>Installation</h3><p>Floating click, secret-nail or full glue-down options. Fully compatible with underfloor heating (max 27°C surface temperature). Suitable over concrete or timber subfloors. Acclimatise 48 hours before installation.</p>",
        "Laminate Flooring": "<h3>Installation</h3><p>Simple click-lock floating system — ideal for DIY. Can be laid over most existing floors. Maintain 10mm expansion gap around perimeter. Use quality underlay unless pre-attached. Not suitable for wet rooms.</p>",
        "LVT Flooring": "<h3>Installation</h3><p>Click-lock floating, loose-lay or glue-down depending on product. Subfloor must be clean, dry, flat and level — irregularities over 3mm/3m must be levelled. UFH compatible (max 27°C). No acclimatisation required.</p>",
        "Herringbone Flooring": "<h3>Installation</h3><p>Herringbone requires careful planning — centre line and 45° angle must be calculated precisely. Professional installation strongly recommended. Method depends on material type. Acclimatise wood products 48-72 hours.</p>",
        "Vinyl Flooring": "<h3>Installation</h3><p>Loose-laid, adhered or click-lock depending on product. Subfloor must be clean, dry and smooth. Can be installed over existing smooth, well-bonded coverings. Expansion gap required for floating installations.</p>",
        "Carpet": "<h3>Installation</h3><p>Professional fitting recommended for best finish and maximum lifespan. Always fit over quality underlay for comfort and insulation. Measure carefully — allow for pattern matching where applicable.</p>",
        "Underlay": "<h3>Installation</h3><p>Lay smooth-side down on clean, dry subfloor. Butt edges tightly — do not overlap. Tape all joins. Trim neatly to perimeter. Replace every time new flooring is fitted — never re-use old underlay.</p>",
        "Accessories": "<h3>Installation</h3><p>Refer to product packaging and manufacturer guidelines. Correct installation essential for appearance and performance. Contact our team for advice on selecting the right accessory.</p>",
    }

    care = {
        "Solid Wood Flooring": "<h3>Care &amp; Maintenance</h3><p>Sweep or vacuum regularly with a soft brush. Clean with wood-specific cleaner on a lightly damp mop — avoid excess water. Wipe spills immediately. Use felt pads under furniture. Place mats at entrances. Avoid prolonged direct sunlight.</p>",
        "Engineered Wood Flooring": "<h3>Care &amp; Maintenance</h3><p>Sweep or vacuum regularly. Use a wood floor cleaner with a damp mop — never excess water. Wipe spills immediately. Avoid harsh chemicals, steam cleaners and abrasive products. Rearrange rugs periodically for even light exposure.</p>",
        "Laminate Flooring": "<h3>Care &amp; Maintenance</h3><p>Sweep or vacuum with a soft brush. Mop with a well-wrung damp mop using laminate cleaner. Never use excess water or steam cleaners. Clean spills immediately. Avoid wax polishes and abrasive pads. Use felt pads under furniture.</p>",
        "LVT Flooring": "<h3>Care &amp; Maintenance</h3><p>Sweep or vacuum regularly to remove grit. Mop with warm water and LVT cleaner. Resistant to most household chemicals. Never use abrasive pads, solvents or steam cleaners. Use felt pads under furniture legs.</p>",
        "Herringbone Flooring": "<h3>Care &amp; Maintenance</h3><p>Care depends on material — wood herringbone needs wood-specific cleaner on damp mop; LVT herringbone can be mopped with warm water and LVT cleaner. All benefit from door mats and felt pads under furniture.</p>",
        "Vinyl Flooring": "<h3>Care &amp; Maintenance</h3><p>Sweep or vacuum regularly. Mop with warm water and mild pH-neutral cleaner. Avoid abrasive pads and harsh solvents. Highly resistant to stains and bacteria — ideal for families and pets. Clean spills promptly.</p>",
        "Carpet": "<h3>Care &amp; Maintenance</h3><p>Vacuum at least twice weekly in high-traffic areas. Always blot (never rub) spills with a clean white cloth. Use carpet spot cleaner for stains. Professional hot-water extraction every 12-18 months. Rotate furniture to prevent pile indentation.</p>",
        "Underlay": "<h3>Care &amp; Maintenance</h3><p>Underlay requires no ongoing maintenance once installed. Always replace when new flooring is fitted — never re-use old underlay. Ensure subfloor remains dry throughout the installation life.</p>",
        "Accessories": "<h3>Care &amp; Maintenance</h3><p>Most accessories are maintenance-free once installed. Wipe clean with damp cloth and mild detergent. Periodically check mechanical fixings. Accessories can usually be replaced independently without disturbing the floor.</p>",
    }

    why = {
        "Solid Wood Flooring": "<h3>Why Factory Direct Flooring?</h3><p>We work directly with leading manufacturers, cutting out the middleman to bring you premium solid wood at unbeatable prices. Free expert advice, full manufacturer's warranty and competitive UK delivery. Order free samples today.</p>",
        "Engineered Wood Flooring": "<h3>Why Factory Direct Flooring?</h3><p>One of the UK's most trusted engineered wood specialists. Extensive range, best quality at competitive prices, full manufacturer's guarantee. Free samples available — see the quality before you commit.</p>",
        "Laminate Flooring": "<h3>Why Factory Direct Flooring?</h3><p>UK's widest laminate range from leading brands at factory direct prices. Expert team helps you choose the perfect floor. Free samples, technical advice and competitive UK delivery on all orders.</p>",
        "LVT Flooring": "<h3>Why Factory Direct Flooring?</h3><p>One of the UK's leading LVT specialists with extensive, competitively priced ranges. Free samples, full technical support and fast UK delivery. Full manufacturer's warranty for peace of mind.</p>",
        "Herringbone Flooring": "<h3>Why Factory Direct Flooring?</h3><p>One of the UK's finest herringbone collections — solid wood, engineered wood and LVT. Expert pattern planning advice and quantity calculation. Order free samples today.</p>",
        "Vinyl Flooring": "<h3>Why Factory Direct Flooring?</h3><p>Huge range of vinyl from top manufacturers at factory direct prices. Expert team helps you find the perfect floor for your project. Free samples and fast UK delivery on all orders.</p>",
        "Carpet": "<h3>Why Factory Direct Flooring?</h3><p>One of the UK's most comprehensive carpet selections — from everyday ranges to premium options. Specialist team helps you find the perfect match. Order free samples — feel the quality before you buy.</p>",
        "Underlay": "<h3>Why Factory Direct Flooring?</h3><p>Comprehensive underlay range for all floor types and conditions. Expert specification advice. Competitive prices and fast UK delivery — everything needed for a professional installation.</p>",
        "Accessories": "<h3>Why Factory Direct Flooring?</h3><p>Complete range of trims, threshold strips, adhesives and tools to finish your installation professionally. Quality at competitive prices with fast UK delivery available.</p>",
    }

    default = "Quality flooring from Factory Direct Flooring at unbeatable prices."
    i = intros.get(cat,   f"<h2>{title}</h2><p>{title}. {default}</p>")
    f = features.get(cat, "<h3>Features</h3><ul><li>High-quality construction</li><li>Stylish design</li><li>Easy maintenance</li><li>Suitable for residential use</li><li>Excellent value</li></ul>")
    n = install.get(cat,  "<h3>Installation</h3><p>Refer to product specification for installation guidelines.</p>")
    c = care.get(cat,     "<h3>Care</h3><p>Clean regularly with appropriate products for your floor type.</p>")
    w = why.get(cat,      f"<h3>Why Factory Direct Flooring?</h3><p>{default}</p>")
    return f"{i}\n{f}\n{n}\n{c}\n{w}"


# ──────────────────────────────────────────────────────────────────────
#  HELPERS
# ──────────────────────────────────────────────────────────────────────

def fetch(url):
    for _ in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            if r.status_code == 200: return r.text
            if r.status_code == 404: return ""
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
    except Exception:
        return ""

def make_handle(url, name):
    slug = url.rstrip("/").split("/")[-1].replace(".html","")
    slug = re.sub(r"[^a-z0-9\-]", "", slug.lower()).strip("-")
    if slug and len(slug) > 4: return slug[:200]
    h = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return h[:200]


# ──────────────────────────────────────────────────────────────────────
#  IMAGE EXTRACTION — IMAGELY CDN
# ──────────────────────────────────────────────────────────────────────

# Exact pattern matching:
# https://imagely.factory-direct-flooring.co.uk/media/catalog/product/cache/{hash}/{a}/{b}/{filename}
IMAGELY_PATTERN = re.compile(
    r'https://imagely\.factory-direct-flooring\.co\.uk'
    r'/media/catalog/product/[^\s"\'<>,\)\\]+',
    re.IGNORECASE
)

def extract_card_images(card_html):
    """Extract imagely CDN images from one product card. Max 2."""
    found = []
    seen  = set()

    def add(url):
        url = url.strip().split("?")[0]  # remove query params
        if url not in seen and url.startswith("http") and len(url) > 60:
            seen.add(url)
            found.append(url)

    # Find all bare imagely URLs in the card HTML
    for m in IMAGELY_PATTERN.finditer(card_html):
        add(m.group(0))

    # Also check data-src, src, data-original, srcset attributes
    for attr in ["data-src", "src", "data-original", "data-lazy"]:
        pattern = re.compile(
            attr + r'=["\']([^"\'<>\s]+)["\']',
            re.IGNORECASE
        )
        for m in pattern.finditer(card_html):
            url = m.group(1)
            if "imagely" in url and "catalog/product" in url:
                add(url)

    # srcset
    for m in re.finditer(r'srcset=["\']([^"\']+)["\']', card_html):
        for part in m.group(1).split(","):
            url = part.strip().split(" ")[0]
            if "imagely" in url and "catalog/product" in url:
                add(url)

    return found[:MAX_IMAGES]


# ──────────────────────────────────────────────────────────────────────
#  PARSE CATEGORY PAGE
# ──────────────────────────────────────────────────────────────────────

def parse_category_page(html, cat_name):
    """Extract products from a category listing page."""
    products = []
    seen     = set()

    # Step 1: Get product data from JSON-LD (name, price, sku, stock, real description)
    jl_lookup = {}
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

                # Real description from JSON-LD
                real_desc = p.get("description", "")
                if real_desc:
                    real_desc = clean(real_desc)

                brand = ""
                b = p.get("brand", {})
                if isinstance(b, dict):   brand = clean(b.get("name", ""))
                elif isinstance(b, str):  brand = clean(b)

                price = ""; compare = ""; stock = "active"
                offers = p.get("offers", {})
                if isinstance(offers, list): offers = offers[0] if offers else {}
                if isinstance(offers, dict):
                    price = str(offers.get("price", offers.get("lowPrice", "")))
                    hp    = offers.get("highPrice", "")
                    compare = str(hp) if hp and hp != price else ""
                    avail = offers.get("availability", "")
                    stock = "active" if "InStock" in avail else "draft"

                jl_lookup[name] = {
                    "sku"      : str(p.get("sku", "")),
                    "url"      : p.get("url", ""),
                    "brand"    : brand,
                    "price"    : price,
                    "compare"  : compare,
                    "stock"    : stock,
                    "real_desc": real_desc,
                }

            for item in items:
                if not isinstance(item, dict): continue
                if item.get("@type") == "ItemList":
                    for el in item.get("itemListElement", []):
                        process(el.get("item", el))
                elif item.get("@type") == "Product":
                    process(item)
        except Exception:
            pass

    # Step 2: Find HTML product cards (for images)
    card_pattern = re.compile(
        r'<(?:li|div|article)([^>]*class="[^"]*product[^"]*item[^"]*"[^>]*)>(.*?)</(?:li|div|article)>',
        re.DOTALL | re.IGNORECASE
    )

    for m in card_pattern.finditer(html):
        card = m.group(2)

        # Get product name from card
        nm = (re.search(r'class="[^"]*(?:product[_\-]name|name)[^"]*"[^>]*>.*?<a[^>]*>(.*?)</a>',
                        card, re.DOTALL | re.IGNORECASE)
              or re.search(r'<a[^>]+title="([^"]{4,})"', card)
              or re.search(r'class="[^"]*name[^"]*"[^>]*>(.*?)</\w+>',
                           card, re.DOTALL | re.IGNORECASE))
        name = clean(nm.group(1)) if nm else ""
        if not name or len(name) < 4 or name in seen:
            continue
        seen.add(name)

        # Get product URL
        url_m = re.search(
            r'href=["\'](' + re.escape(BASE_URL) + r'/[^"\'?#]+)["\']', card
        )
        prod_url = url_m.group(1) if url_m else ""

        # Get images from this card only (imagely CDN)
        images = extract_card_images(card)

        # Get price from card (fallback)
        pm = re.search(r'£\s*([\d,]+\.?\d*)', card)
        card_price = pm.group(1).replace(",", "") if pm else ""

        # Merge with JSON-LD data
        jl = jl_lookup.get(name, {})

        products.append({
            "name"      : name,
            "sku"       : jl.get("sku", ""),
            "price"     : jl.get("price", "") or card_price,
            "compare"   : jl.get("compare", ""),
            "brand"     : jl.get("brand", "") or "Factory Direct Flooring",
            "category"  : cat_name,
            "images"    : images,
            "stock"     : jl.get("stock", "active"),
            "url"       : jl.get("url", "") or prod_url,
            "real_desc" : jl.get("real_desc", ""),
        })

    # Fallback: JSON-LD only if cards weren't found
    if not products:
        for name, jl in jl_lookup.items():
            if name in seen: continue
            seen.add(name)
            products.append({
                "name":name,"sku":jl["sku"],"price":jl["price"],
                "compare":jl["compare"],"brand":jl["brand"],"category":cat_name,
                "images":[],"stock":jl["stock"],"url":jl["url"],
                "real_desc":jl["real_desc"],
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

            if new == 0 and page > 1:
                break

            # Find next page
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
#  BUILD SHOPIFY CSV ROWS
# ──────────────────────────────────────────────────────────────────────

def build_rows(p):
    name = (p.get("name") or "").strip()
    if not name: return []

    handle  = p.get("handle") or make_handle(p.get("url", ""), name)
    cat     = p.get("category", "Flooring")

    # Use REAL description if available, else auto-generated
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
    seo_d   = f"Buy {name} at Factory Direct Flooring. {cat} at competitive prices, free UK delivery and expert advice. Order free samples today."[:320]

    rows = []
    first_img = images[0] if images else ""

    # Row 1 — full product data
    row1 = {
        "Handle"                    : handle,
        "Title"                     : name,
        "Body (HTML)"               : body,
        "Vendor"                    : vendor,
        "Product Category"          : "",          # blank = no taxonomy errors
        "Type"                      : cat,         # category here for Smart Collections
        "Tags"                      : tags,
        "Published"                 : "TRUE",
        "Option1 Name"              : "Title",
        "Option1 Value"             : "Default Title",
        "Variant SKU"               : p.get("sku", ""),
        "Variant Grams"             : "0",
        "Variant Inventory Tracker" : "shopify",
        "Variant Inventory Qty"     : "100",
        "Variant Inventory Policy"  : "deny",
        "Variant Fulfillment Service": "manual",
        "Variant Price"             : price,
        "Variant Compare At Price"  : compare,
        "Variant Requires Shipping" : "TRUE",
        "Variant Taxable"           : "TRUE",
        "Image Src"                 : "",
        "Image Position"            : "",
        "Image Alt Text"            : "",
        "SEO Title"                 : f"{name} | Factory Direct Flooring"[:255],
        "SEO Description"           : seo_d,
        "Status"                    : p.get("stock", "active"),
    }
    # Only set image fields if URL is valid
    if first_img and first_img.startswith("http"):
        row1["Image Src"]      = first_img
        row1["Image Position"] = "1"
        row1["Image Alt Text"] = name
    rows.append(row1)

    # Extra image rows
    for i, img in enumerate(images[1:], 2):
        if not img or not img.startswith("http"):
            continue
        e = {k: "" for k in SHOPIFY_COLS}
        e.update({
            "Handle"        : handle,
            "Image Src"     : img,
            "Image Position": str(i),
            "Image Alt Text": name,
        })
        rows.append(e)

    return rows


# ──────────────────────────────────────────────────────────────────────
#  MAIN
# ──────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    t0 = time.time()

    print("\n" + "="*65)
    print("  FACTORY DIRECT FLOORING — FINAL SHOPIFY SCRAPER")
    print(f"  Image source: imagely.factory-direct-flooring.co.uk")
    print(f"  Max {MAX_IMAGES} images per product")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*65)

    products = scrape_all()

    if not products:
        print("  No products found — saving empty CSV")
        with open(f"{OUTPUT_DIR}/factory_flooring_shopify_{TIMESTAMP}.csv",
                  "w", encoding="utf-8-sig", newline="") as f:
            csv.DictWriter(f, fieldnames=SHOPIFY_COLS).writeheader()
        return

    # Build all CSV rows
    all_rows = []
    for p in products:
        all_rows.extend(build_rows(p))

    # Save Shopify CSV
    csv_file = f"{OUTPUT_DIR}/factory_flooring_shopify_{TIMESTAMP}.csv"
    with open(csv_file, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SHOPIFY_COLS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    # Save JSON backup
    json_file = f"{OUTPUT_DIR}/factory_flooring_{TIMESTAMP}.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump([{
            "name"        : p["name"],
            "handle"      : p.get("handle", ""),
            "sku"         : p["sku"],
            "price"       : p["price"],
            "category"    : p["category"],
            "images"      : p["images"],
            "img_count"   : len(p["images"]),
            "has_real_desc": bool(p.get("real_desc")),
            "url"         : p.get("url", ""),
        } for p in products], f, ensure_ascii=False, indent=2)

    # Summary
    elapsed     = round(time.time() - t0)
    with_img    = len([p for p in products if p["images"]])
    with_real   = len([p for p in products if p.get("real_desc")])
    cats = {}
    for p in products:
        cats[p["category"]] = cats.get(p["category"], 0) + 1

    print(f"\n{'='*65}")
    print(f"  DONE in {elapsed//60}m {elapsed%60:02d}s")
    print(f"{'─'*65}")
    print(f"  Products       : {len(products)}")
    print(f"  CSV rows       : {len(all_rows)}")
    print(f"  With images    : {with_img} ({round(with_img/len(products)*100)}%)")
    print(f"  With real desc : {with_real}  |  Auto-generated: {len(products)-with_real}")
    print(f"\n  By category (use these in Smart Collections → 'Product type is equal to'):")
    for cat, count in sorted(cats.items(), key=lambda x: -x[1]):
        print(f"    {cat:<35} {count:>3} products")
    print(f"{'─'*65}")
    print(f"  CSV  → {csv_file}")
    print(f"  JSON → {json_file}")
    print(f"{'─'*65}")
    print(f"  IMPORT: Shopify Admin → Products → Import → Upload CSV")
    print(f"{'='*65}")


if __name__ == "__main__":
    main()
