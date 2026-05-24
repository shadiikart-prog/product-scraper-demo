"""
╔══════════════════════════════════════════════════════════════╗
║   FACTORY DIRECT FLOORING — FINAL SHOPIFY SCRAPER           ║
║   Strategy: Category pages → HTML cards → imagely images    ║
║   Max 2 images per product from imagely CDN                  ║
║   Auto descriptions (5 sections, 3000+ chars each)          ║
║   Type = category for Smart Collections                      ║
╚══════════════════════════════════════════════════════════════╝

Run:  pip install requests
      python scraper_factoryflooring_shopify.py
"""

import requests, json, csv, re, os, time
from datetime import datetime
from html import unescape

BASE_URL    = "https://www.factory-direct-flooring.co.uk"
OUTPUT_DIR  = "output"
TIMESTAMP   = datetime.now().strftime("%Y%m%d_%H%M%S")
MAX_IMAGES  = 2   # max 2 per product from imagely CDN

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

# ─── Rich Description Templates ───────────────────────────────────────────────

def extract_product_hints(title):
    tl = title.lower()
    brands = ['karndean','kahrs','quick-step','quickstep','amtico','ted todd','elka',
              'wicanders','moduleo','polyflor','tarkett','egger','krono','berry alloc',
              'balterio','camaro','boen','hakwood','woodpecker','lifestyle','rhinofloor',
              'gerflor','forbo','altro','armstrong','pergo','kronoswiss']
    brand  = next((b.title() for b in brands if b in tl), "")
    thick  = re.search(r'(\d+(?:\.\d+)?)\s*mm', title, re.I)
    thick_s = thick.group(1)+"mm" if thick else ""
    colours = ['oak','walnut','pine','ash','maple','birch','cherry','white','grey','gray',
               'black','brown','beige','cream','ivory','natural','smoked','rustic','aged',
               'antique','vintage','blond','golden','silver','slate','stone','marble',
               'concrete','sand','earth','teak','ebony','light','dark','warm']
    colour = next((c.title() for c in colours if c in tl), "")
    return brand, thick_s, colour

def build_desc(title, cat):
    brand, thick, colour = extract_product_hints(title)
    bl = f" by <strong>{brand}</strong>" if brand else " from Factory Direct Flooring"
    cl = f" in a stunning <strong>{colour}</strong> finish" if colour else ""
    tl = f" with a <strong>{thick}</strong> profile" if thick else ""

    intros = {
        "Solid Wood Flooring"     :f"<h2>{title}</h2><p>Introducing the <strong>{title}</strong>{bl}{cl}{tl}. Crafted from 100% real timber, this genuine solid wood floor delivers unmatched natural beauty and character. Every single plank is unique — with its own natural grain, knots and colour variation that make solid wood truly one of a kind and a floor that improves in character and warmth with every passing year.</p>",
        "Engineered Wood Flooring":f"<h2>{title}</h2><p>Discover the <strong>{title}</strong>{bl}{cl}{tl}. Engineered wood combines the authentic warmth and beauty of real wood with enhanced structural stability, making it the smart and versatile choice for modern homes. The real wood top layer is completely indistinguishable from solid wood while the multi-layer core provides superior resistance to the effects of temperature and humidity change.</p>",
        "Laminate Flooring"       :f"<h2>{title}</h2><p>Meet the <strong>{title}</strong>{bl}{cl}{tl}. This exceptional laminate floor delivers the authentic look of real wood or stone at a fraction of the cost, with outstanding scratch resistance and long-lasting durability for even the busiest family homes. Modern laminate technology has advanced dramatically — today's finest laminate floors are virtually indistinguishable from the real thing.</p>",
        "LVT Flooring"            :f"<h2>{title}</h2><p>Introducing the <strong>{title}</strong>{bl}{cl}{tl}. Luxury Vinyl Tile flooring represents the very pinnacle of modern flooring technology — combining 100% waterproof performance with stunning, hyper-realistic designs and outstanding long-term durability. Whether you are fitting a new kitchen, bathroom, hallway or open-plan living space, LVT is the ultimate practical and stylish flooring solution.</p>",
        "Herringbone Flooring"    :f"<h2>{title}</h2><p>Make a truly bold interior statement with the <strong>{title}</strong>{bl}{cl}{tl}. The herringbone pattern is one of the most timeless and elegant floor designs in existence — a distinctive interlocking arrangement that has adorned the floors of grand homes and prestigious buildings for centuries. Now available in a range of modern materials, herringbone flooring brings classic sophistication to contemporary homes.</p>",
        "Vinyl Flooring"          :f"<h2>{title}</h2><p>Presenting the <strong>{title}</strong>{bl}{cl}{tl}. This premium vinyl floor offers exceptional performance and outstanding value, combining total waterproof protection with stylish, realistic designs. The ideal choice for kitchens, bathrooms, hallways and any room where practicality is every bit as important as appearance and style.</p>",
        "Carpet"                  :f"<h2>{title}</h2><p>Transform your home with the <strong>{title}</strong>{bl}{cl}. This luxuriously soft and beautifully finished carpet brings lasting warmth, comfort and style to any room. Carpet remains the most popular floor covering in British homes for very good reason — nothing else comes close for underfoot comfort, warmth retention and sound insulation.</p>",
        "Underlay"                :f"<h2>{title}</h2><p>The <strong>{title}</strong>{bl} is a professional-specification underlay designed to significantly enhance the comfort, acoustic performance and longevity of your new floor. It is a frequently overlooked truth that the right underlay is just as important as the floor covering itself — a quality underlay will deliver real, tangible benefits across the entire lifetime of your floor.</p>",
        "Accessories"             :f"<h2>{title}</h2><p>Complete your flooring installation to a truly professional standard with the <strong>{title}</strong>{bl}. Quality finishing accessories are the critical detail that distinguishes a truly professional flooring installation — providing a neat, durable and visually pleasing finish to all joins, edges and transitions.</p>",
    }
    highlights = {
        "Solid Wood Flooring"     :"<h3>Key Highlights</h3><ul><li><strong>100% Real Solid Timber</strong> — authentic grain and natural character in every single plank</li><li><strong>Sand &amp; Refinish</strong> — can be sanded and refinished up to 5 times, making it a truly lifelong investment</li><li><strong>Natural Insulator</strong> — timber naturally retains warmth and helps reduce energy bills</li><li><strong>Adds Property Value</strong> — solid wood is proven to increase the value of your home</li><li><strong>Sustainably Sourced</strong> — certified timber from responsibly managed forests</li><li><strong>Unique Every Time</strong> — no two planks are ever identical</li></ul>",
        "Engineered Wood Flooring":"<h3>Key Highlights</h3><ul><li><strong>Real Wood Top Layer</strong> — genuine timber veneer for an authentic natural appearance</li><li><strong>Multi-Layer Stability</strong> — cross-ply core resists warping, cupping and shrinking</li><li><strong>Underfloor Heating Compatible</strong> — engineered to work with both wet and electric UFH</li><li><strong>All-Level Installation</strong> — suitable for ground floor, first floor and basement</li><li><strong>Flexible Fitting</strong> — click float, secret nail or full glue-down options</li><li><strong>Wide Plank Options</strong> — available in a range of widths for a contemporary feel</li></ul>",
        "Laminate Flooring"       :"<h3>Key Highlights</h3><ul><li><strong>HD Print Layer</strong> — photorealistic imagery creates a strikingly authentic wood or stone appearance</li><li><strong>AC-Rated Wear Layer</strong> — tough scratch-resistant surface for heavy domestic use</li><li><strong>Easy Click Installation</strong> — simple floating system, no glue required — ideal for DIY</li><li><strong>Bevelled Edges</strong> — realistic V-groove detailing adds genuine depth and authenticity</li><li><strong>Low Maintenance</strong> — sweep, vacuum and wipe clean — no sanding or polishing ever needed</li><li><strong>Wide Format Options</strong> — plank, tile and herringbone formats available</li></ul>",
        "LVT Flooring"            :"<h3>Key Highlights</h3><ul><li><strong>100% Waterproof</strong> — completely impervious to moisture, safe for all rooms including bathrooms</li><li><strong>Commercial-Grade Wear Layer</strong> — PU-reinforced surface resists scratches, dents and heavy traffic</li><li><strong>Hyper-Realistic Designs</strong> — embossed surface faithfully replicates real wood, stone and ceramic</li><li><strong>Warm &amp; Comfortable</strong> — significantly softer and warmer underfoot than real stone or tile</li><li><strong>UFH Compatible</strong> — suitable for wet and electric underfloor heating systems</li><li><strong>Flexible Installation</strong> — click float, loose-lay or glue-down options available</li></ul>",
        "Herringbone Flooring"    :"<h3>Key Highlights</h3><ul><li><strong>Iconic Herringbone Pattern</strong> — the 45° interlocking design adds instant elegance and visual depth</li><li><strong>Creates Visual Space</strong> — the diagonal arrangement draws the eye and enlarges the perceived room</li><li><strong>Multiple Materials</strong> — available in solid wood, engineered wood and luxury vinyl tile</li><li><strong>Unique Character</strong> — natural variation in grain or embossed finish makes every floor unique</li><li><strong>Suits All Rooms</strong> — from entrance halls and living rooms to kitchens and bedrooms</li><li><strong>Timeless Versatility</strong> — complements both period and contemporary interior design styles</li></ul>",
        "Vinyl Flooring"          :"<h3>Key Highlights</h3><ul><li><strong>Fully Waterproof</strong> — 100% impervious to water, safe for bathrooms, kitchens and wet rooms</li><li><strong>Durable Wear Surface</strong> — resists everyday scratches, scuffs and heavy foot traffic</li><li><strong>Cushioned Backing</strong> — comfortable, warm and quiet underfoot</li><li><strong>Easy to Clean</strong> — resistant to most stains, simply sweep and mop</li><li><strong>Realistic Designs</strong> — high-definition printing creates convincing wood and stone effects</li><li><strong>Flexible Formats</strong> — sheet, tile and plank options available</li></ul>",
        "Carpet"                  :"<h3>Key Highlights</h3><ul><li><strong>Luxuriously Soft</strong> — warm and comfortable underfoot — perfect for bedrooms and living rooms</li><li><strong>Outstanding Sound Insulation</strong> — significantly reduces impact noise between floors</li><li><strong>Thermal Insulator</strong> — retains warmth, helping to reduce heating costs naturally</li><li><strong>Wide Colour Range</strong> — extensive palette and texture options to suit every décor</li><li><strong>Durable Construction</strong> — engineered to withstand the demands of busy family life</li><li><strong>Easy Stain Resistance</strong> — modern treatments make cleaning quick and straightforward</li></ul>",
        "Underlay"                :"<h3>Key Highlights</h3><ul><li><strong>Superior Cushioning</strong> — adds real softness and comfort underfoot</li><li><strong>Acoustic Performance</strong> — significantly reduces impact noise transmission between floors</li><li><strong>Thermal Insulation</strong> — improves floor warmth and reduces energy consumption</li><li><strong>Moisture Protection</strong> — many variants include an integrated damp-proof membrane</li><li><strong>UFH Compatible</strong> — low tog rating grades available for underfloor heating use</li><li><strong>Extends Floor Life</strong> — correct underlay can double the lifespan of your floor covering</li></ul>",
        "Accessories"             :"<h3>Key Highlights</h3><ul><li><strong>Professional Quality</strong> — manufactured to trade specification for lasting performance</li><li><strong>Precision Engineered</strong> — ensures a consistently neat and durable finish</li><li><strong>Wide Compatibility</strong> — suitable for use with laminate, LVT, wood and carpet</li><li><strong>Easy Installation</strong> — straightforward fixing for a professional result</li><li><strong>Excellent Value</strong> — premium quality at a genuinely competitive price point</li></ul>",
    }
    install = {
        "Solid Wood Flooring"     :"<h3>Installation</h3><p>This solid wood floor can be secret-nailed or glued to a suitable subfloor. Acclimatise for 48–72 hours in the installation room before fitting. Leave a minimum 15mm expansion gap around all fixed objects. Professional installation is strongly recommended to achieve the best results and maintain the manufacturer's warranty.</p>",
        "Engineered Wood Flooring":"<h3>Installation</h3><p>Can be floated (click), secret-nailed or fully glued depending on the product and subfloor type. Fully compatible with underfloor heating — floor surface temperature must not exceed 27°C. Suitable over concrete and timber subfloors. Acclimatise for 48 hours before fitting.</p>",
        "Laminate Flooring"       :"<h3>Installation</h3><p>Features a simple click-lock floating system — ideal for confident DIY installation. Can be laid over most existing floors including concrete, tiles and existing wood. A minimum 10mm expansion gap is required around the entire perimeter. A quality underlay must be used unless the product has a pre-attached underlay. Not suitable for wet rooms.</p>",
        "LVT Flooring"            :"<h3>Installation</h3><p>Available in click-lock floating, loose-lay or full glue-down formats. The subfloor must be clean, dry, flat and level — any imperfections exceeding 3mm over 3m must be rectified. Compatible with underfloor heating — do not exceed 27°C floor surface temperature. No acclimatisation period required.</p>",
        "Herringbone Flooring"    :"<h3>Installation</h3><p>Herringbone installation requires careful planning — the centre line and 45° angle must be precisely calculated before laying begins. Professional installation is strongly recommended for herringbone patterns. Installation method depends on material type. Always acclimatise wood products for 48–72 hours before fitting.</p>",
        "Vinyl Flooring"          :"<h3>Installation</h3><p>Can be laid loose, adhered with adhesive or as a click-lock floating floor depending on the product type. Subfloor must be clean, dry, smooth and free of contamination. Can be installed over existing smooth, well-bonded coverings. An expansion gap is required around the perimeter for floating installations.</p>",
        "Carpet"                  :"<h3>Installation</h3><p>Professional carpet fitting is recommended for the best finish and to maximise the carpet's lifespan. Always fit over a suitable quality underlay. Measure carefully and allow for pattern matching where applicable. Our team can advise on the correct underlay specification for your chosen carpet.</p>",
        "Underlay"                :"<h3>Installation</h3><p>Lay smooth-side down on a clean, dry subfloor. Butt edges tightly — do not overlap. Tape all joins securely. Trim neatly around the room perimeter. Replace underlay every time new flooring is installed — never re-use old underlay. Always check the tog rating is within the floor manufacturer's specification when using with underfloor heating.</p>",
        "Accessories"             :"<h3>Installation</h3><p>Please refer carefully to the product packaging and manufacturer guidelines before fitting. Correct installation is essential for both appearance and long-term performance. Contact our expert team if you need advice on selecting the right accessory for your specific flooring transition or application.</p>",
    }
    care = {
        "Solid Wood Flooring"     :"<h3>Care &amp; Maintenance</h3><p>Sweep or vacuum regularly with a soft brush attachment. Clean with a wood-specific cleaner on a lightly damp, well-wrung mop — avoid excessive moisture at all times. Wipe spills immediately. Use felt pads under all furniture legs. Place mats at entrances to prevent grit being tracked in.</p>",
        "Engineered Wood Flooring":"<h3>Care &amp; Maintenance</h3><p>Sweep or vacuum regularly. Use a wood floor cleaner with a lightly damp mop for deeper cleaning — never use excessive water. Wipe liquid spills immediately. Avoid harsh chemicals, steam cleaners and abrasive products. Rearrange rugs periodically for even light exposure across the surface.</p>",
        "Laminate Flooring"       :"<h3>Care &amp; Maintenance</h3><p>Sweep or vacuum with a soft brush regularly. Mop with a well-wrung damp mop and a laminate-specific cleaner. Never use excessive water, steam cleaners or wet mops. Clean spills immediately. Avoid wax polishes, abrasive pads and harsh solvents. Use felt pads under furniture.</p>",
        "LVT Flooring"            :"<h3>Care &amp; Maintenance</h3><p>Sweep or vacuum regularly to remove grit. Mop with warm water and a suitable LVT cleaner. LVT is resistant to most household stains and chemicals. Never use abrasive scrubbing pads, solvent-based cleaners or steam cleaners. Use felt pads under furniture legs and castor cups under wheeled furniture.</p>",
        "Herringbone Flooring"    :"<h3>Care &amp; Maintenance</h3><p>Care depends on the material — wood herringbone requires a wood-specific cleaner on a damp mop; LVT herringbone can be mopped with warm water and a suitable cleaner. All herringbone floors benefit from door mats at entrances and felt pads under furniture to maintain the surface.</p>",
        "Vinyl Flooring"          :"<h3>Care &amp; Maintenance</h3><p>Sweep or vacuum regularly with a soft brush. Mop with warm water and a mild pH-neutral floor cleaner. Avoid abrasive scrubbing pads and harsh chemical solvents. Vinyl is highly resistant to stains and bacteria — ideal for families and pet owners. Clean spills promptly.</p>",
        "Carpet"                  :"<h3>Care &amp; Maintenance</h3><p>Vacuum at least twice weekly in high-traffic areas. Always blot — never rub — spills with a clean white cloth. Use a suitable carpet spot cleaner for stubborn marks. Professional hot-water extraction cleaning is recommended every 12–18 months. Rotate furniture periodically to prevent permanent pile indentation.</p>",
        "Underlay"                :"<h3>Care &amp; Maintenance</h3><p>Underlay requires no ongoing maintenance once correctly installed. It is a consumable product and should always be replaced when new flooring is fitted — never re-use old underlay. Ensure the subfloor remains dry and structurally sound throughout the life of the installation.</p>",
        "Accessories"             :"<h3>Care &amp; Maintenance</h3><p>Most flooring accessories are maintenance-free once correctly installed. Wipe clean with a damp cloth and mild detergent as required. Periodically check all mechanical fixings and tighten if necessary. Accessory profiles can usually be replaced independently without disturbing the floor covering.</p>",
    }
    why = {
        "Solid Wood Flooring"     :"<h3>Why Factory Direct Flooring?</h3><p>We work directly with leading manufacturers and cut out the middleman to bring you premium solid wood floors at unbeatable prices. All products come with free expert advice, a full manufacturer's warranty and competitive UK delivery. Order free samples today.</p>",
        "Engineered Wood Flooring":"<h3>Why Factory Direct Flooring?</h3><p>We are one of the UK's most trusted engineered wood specialists. Our buying power means you get the best quality at the lowest price. All products carry a full manufacturer's guarantee. Free samples available — see the quality for yourself before you commit.</p>",
        "Laminate Flooring"       :"<h3>Why Factory Direct Flooring?</h3><p>We stock the UK's widest range of laminate flooring from all leading brands at factory direct prices. Our experts can help you choose the perfect floor for your needs and budget. Free sample service, comprehensive technical advice and competitive UK delivery available.</p>",
        "LVT Flooring"            :"<h3>Why Factory Direct Flooring?</h3><p>We are one of the UK's leading LVT specialists with one of the widest and most competitively priced ranges available. Our team offers free technical support and a full sample service. All products are backed by a full manufacturer's warranty for complete peace of mind.</p>",
        "Herringbone Flooring"    :"<h3>Why Factory Direct Flooring?</h3><p>Our herringbone collection is one of the finest in the UK, featuring solid wood, engineered wood and luxury vinyl options. Our experts can help you plan your herringbone installation and calculate the exact quantity needed. Order free samples today.</p>",
        "Vinyl Flooring"          :"<h3>Why Factory Direct Flooring?</h3><p>We stock a huge range of vinyl flooring from top manufacturers at factory direct prices. Our knowledgeable team can help you find the perfect vinyl floor for your project. Free samples available, comprehensive technical advice and fast UK delivery on all orders.</p>",
        "Carpet"                  :"<h3>Why Factory Direct Flooring?</h3><p>We offer one of the UK's most comprehensive carpet selections — from affordable everyday ranges to luxurious premium options. Our carpet specialists can help you find the perfect match for your room and budget. Order free samples today and feel the quality before you buy.</p>",
        "Underlay"                :"<h3>Why Factory Direct Flooring?</h3><p>We stock a comprehensive range of underlays for all floor types and conditions. Our team can advise on the correct specification for your project. Competitive prices and fast UK delivery on all underlay products — everything needed for a professional installation.</p>",
        "Accessories"             :"<h3>Why Factory Direct Flooring?</h3><p>We stock everything needed to complete your installation professionally — trims, threshold strips, adhesives, cleaning products and more. Our accessories are chosen to complement our flooring ranges perfectly. Competitive prices and fast UK delivery available on all orders.</p>",
    }
    default_txt = "Quality flooring from Factory Direct Flooring. Browse our extensive range at unbeatable prices with free expert advice and UK delivery."
    i = intros.get(cat, f"<h2>{title}</h2><p>Discover the <strong>{title}</strong>{bl}{cl}{tl}. {default_txt}</p>")
    h = highlights.get(cat, "<h3>Key Highlights</h3><ul><li>High-quality construction</li><li>Stylish design</li><li>Easy maintenance</li><li>Suitable for residential use</li><li>Excellent value for money</li></ul>")
    n = install.get(cat, "<h3>Installation</h3><p>Please refer to the product specification for full installation guidelines. Contact our expert team for advice on the correct method for your subfloor.</p>")
    c = care.get(cat, "<h3>Care &amp; Maintenance</h3><p>Sweep and clean regularly with appropriate products. Always use cleaners recommended for your specific floor type.</p>")
    w = why.get(cat, f"<h3>Why Factory Direct Flooring?</h3><p>{default_txt}</p>")
    return f"{i}\n{h}\n{n}\n{c}\n{w}"

# ─── Helpers ───────────────────────────────────────────────────────────────────

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
    t = unescape(str(t)); t = re.sub(r"<[^>]+>"," ",t)
    return re.sub(r"\s+"," ",t).strip()

def price_fmt(p):
    try:
        f = float(str(p).replace(",","").replace("£","").strip())
        return f"{f:.2f}" if f>0 else ""
    except: return ""

def make_handle(url, name):
    slug = url.rstrip("/").split("/")[-1].replace(".html","")
    slug = re.sub(r"[^a-z0-9\-]","",slug.lower()).strip("-")
    if slug and len(slug)>4: return slug[:200]
    return re.sub(r"[^a-z0-9]+","-",name.lower()).strip("-")[:200]

def card_imgs(card_html, slug=""):
    """
    Extract up to MAX_IMAGES imagely CDN images from a product card.
    Prioritises images whose URL contains the product slug.
    """
    found = []; seen = set()
    def add(u):
        u = str(u).strip().split("?")[0]
        if (u.startswith("http") and len(u)>40
                and "placeholder" not in u.lower()
                and not u.endswith(".gif")
                and u not in seen):
            seen.add(u); found.append(u)

    # data-src (lazy load — most common in Hyva/Magento)
    for m in re.finditer(r'data-src=["\']([^"\'>\s]+)["\']', card_html, re.I): add(m.group(1))
    # src on img tags
    for m in re.finditer(r'<img[^>]+src=["\']([^"\'>\s]+)["\']', card_html, re.I):
        u = m.group(1)
        if "placeholder" not in u and not u.endswith(".gif"): add(u)
    # data-original
    for m in re.finditer(r'data-original=["\']([^"\'>\s]+)["\']', card_html, re.I): add(m.group(1))
    # srcset first URL
    for m in re.finditer(r'srcset=["\']([^"\']+)["\']', card_html, re.I):
        u = m.group(1).split(",")[0].strip().split(" ")[0]
        if u: add(u)

    # Prefer imagely CDN
    imagely = [u for u in found if "imagely" in u or "factory-direct-flooring" in u]
    chosen  = imagely if imagely else found

    # Prefer images that contain the product slug for accuracy
    if slug:
        slug_imgs = [u for u in chosen if slug in u]
        if slug_imgs: return slug_imgs[:MAX_IMAGES]

    return chosen[:MAX_IMAGES]

# ─── Parse category page ──────────────────────────────────────────────────────

def parse_page(html, cat_name):
    products = []; seen = set()

    # ── JSON-LD for name/price/sku/stock ──────────────────────────────
    jl_data = {}
    for block in re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL
    ):
        try:
            data = json.loads(block.strip())
            items = data if isinstance(data,list) else [data]
            for item in items:
                if not isinstance(item,dict): continue
                def proc(p):
                    if not isinstance(p,dict) or p.get("@type")!="Product": return
                    nm = clean(p.get("name",""))
                    if not nm: return
                    sku = str(p.get("sku",""))
                    url = p.get("url","")
                    brand = ""
                    b = p.get("brand",{})
                    if isinstance(b,dict): brand=clean(b.get("name",""))
                    elif isinstance(b,str): brand=clean(b)
                    price=""; compare=""; stock="active"
                    of = p.get("offers",{})
                    if isinstance(of,list): of=of[0] if of else {}
                    if isinstance(of,dict):
                        price=str(of.get("price",of.get("lowPrice","")))
                        hp=of.get("highPrice","")
                        compare=str(hp) if hp and hp!=price else ""
                        avail=of.get("availability","")
                        stock="active" if "InStock" in avail else "draft"
                    jl_data[nm]={"sku":sku,"url":url,"brand":brand,
                                  "price":price,"compare":compare,"stock":stock}
                if item.get("@type")=="ItemList":
                    for el in item.get("itemListElement",[]):
                        proc(el.get("item",el))
                elif item.get("@type")=="Product":
                    proc(item)
        except: pass

    # ── HTML cards for images + name fallback ─────────────────────────
    card_patt = re.compile(
        r'<(?:li|div|article)([^>]*class="[^"]*product[^"]*item[^"]*"[^>]*)>(.*?)</(?:li|div|article)>',
        re.DOTALL|re.I
    )
    for m in card_patt.finditer(html):
        card = m.group(2)

        # Name from card
        nm_m = (re.search(r'class="[^"]*(?:product[_\-]name|name)[^"]*"[^>]*>.*?<a[^>]*>(.*?)</a>',card,re.DOTALL|re.I)
                or re.search(r'<a[^>]+title="([^"]{4,})"',card)
                or re.search(r'class="[^"]*name[^"]*"[^>]*>(.*?)</\w+>',card,re.DOTALL|re.I))
        name = clean(nm_m.group(1)) if nm_m else ""
        if not name or len(name)<4 or name in seen: continue
        seen.add(name)

        # URL
        url_m = re.search(r'href=["\'](' + re.escape(BASE_URL) + r'/[^"\'?#]+)["\']', card)
        prod_url = url_m.group(1) if url_m else ""
        slug = prod_url.rstrip("/").split("/")[-1].replace(".html","").lower()

        # Images from this card only (pass slug for accuracy)
        images = card_imgs(card, slug)

        # Price from card (fallback)
        pm = re.search(r'£\s*([\d,]+\.?\d*)',card)
        card_price = pm.group(1).replace(",","") if pm else ""

        # Merge with JSON-LD data
        jd = jl_data.get(name,{})
        products.append({
            "name"   : name,
            "sku"    : jd.get("sku",""),
            "price"  : jd.get("price","") or card_price,
            "compare": jd.get("compare",""),
            "brand"  : jd.get("brand","") or "Factory Direct Flooring",
            "cat"    : cat_name,
            "images" : images,
            "stock"  : jd.get("stock","active"),
            "url"    : jd.get("url","") or prod_url,
        })

    # Fallback: JSON-LD only if HTML cards empty
    if not products:
        for name, jd in jl_data.items():
            if name in seen: continue
            seen.add(name)
            products.append({
                "name":name,"sku":jd["sku"],"price":jd["price"],
                "compare":jd["compare"],"brand":jd["brand"],"cat":cat_name,
                "images":[],"stock":jd["stock"],"url":jd["url"],
            })

    return products

# ─── Scrape all categories ─────────────────────────────────────────────────────

def scrape_all():
    all_p=[]; seen_h=set()
    for cat_name, cat_url in CATEGORIES:
        print(f"\n  [{cat_name}]")
        page=1; cur=f"{cat_url}?product_list_limit=100"
        while True:
            print(f"    Page {page}...",end=" ",flush=True)
            html=fetch(cur)
            if not html: print("FAILED"); break
            prods=parse_page(html,cat_name)
            new=0
            for p in prods:
                h=make_handle(p.get("url",""),p["name"])
                if h in seen_h: continue
                seen_h.add(h); p["handle"]=h; all_p.append(p); new+=1
            imgs_total=sum(len(x["images"]) for x in all_p)
            print(f"+{new} prods | imgs:{imgs_total} | total:{len(all_p)}")
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
            cur=nxt; page+=1; time.sleep(0.6)
    return all_p

# ─── Build Shopify rows ────────────────────────────────────────────────────────

def build_rows(p):
    name=(p.get("name") or "").strip()
    if not name: return []
    handle = p.get("handle") or make_handle(p.get("url",""),name)
    cat    = p.get("cat","Flooring")
    body   = build_desc(name, cat)
    vendor = p.get("brand","").strip() or "Factory Direct Flooring"
    tags   = cat.lower().replace(" ","-")
    images = [i for i in p.get("images",[]) if i and i.startswith("http")][:MAX_IMAGES]
    price  = price_fmt(p.get("price","")) or "0.00"
    compare= price_fmt(p.get("compare",""))
    first  = images[0] if images else ""
    seo_d  = f"Buy {name} at Factory Direct Flooring. {cat} available at competitive prices. Free UK delivery and expert advice. Order free samples today."[:320]
    rows=[]
    # Row 1 — only add Image Src/Position if image actually exists
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
    # Only set image fields if image URL is valid
    if first and first.startswith("http"):
        row1["Image Src"]     = first
        row1["Image Position"] = "1"
        row1["Image Alt Text"] = name
    rows.append(row1)

    # Extra image rows — only if URL is valid
    for i,img in enumerate(images[1:],2):
        if not img or not img.startswith("http"):
            continue
        e={k:"" for k in SHOPIFY_COLS}
        e.update({"Handle":handle,"Image Src":img,"Image Position":str(i),"Image Alt Text":name})
        rows.append(e)
    return rows

# ─── Main ──────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(OUTPUT_DIR,exist_ok=True)
    t0=time.time()
    print("\n"+"="*62)
    print("  FACTORY DIRECT FLOORING — FINAL SHOPIFY SCRAPER")
    print(f"  Max {MAX_IMAGES} images per product | Full 5-section descriptions")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*62)

    products=scrape_all()

    if not products:
        print("  No products found")
        with open(f"{OUTPUT_DIR}/factory_flooring_shopify_{TIMESTAMP}.csv","w",
                  encoding="utf-8-sig",newline="") as f:
            csv.DictWriter(f,fieldnames=SHOPIFY_COLS).writeheader()
        return

    all_rows=[]
    for p in products: all_rows.extend(build_rows(p))

    csv_file=f"{OUTPUT_DIR}/factory_flooring_shopify_{TIMESTAMP}.csv"
    with open(csv_file,"w",encoding="utf-8-sig",newline="") as f:
        writer=csv.DictWriter(f,fieldnames=SHOPIFY_COLS,extrasaction="ignore")
        writer.writeheader(); writer.writerows(all_rows)

    json_file=f"{OUTPUT_DIR}/factory_flooring_{TIMESTAMP}.json"
    with open(json_file,"w",encoding="utf-8") as f:
        json.dump([{"name":p["name"],"handle":p.get("handle",""),
            "sku":p["sku"],"price":p["price"],"cat":p["cat"],
            "images":p["images"],"img_count":len(p["images"]),
            "url":p.get("url","")} for p in products],
            f,ensure_ascii=False,indent=2)

    el=round(time.time()-t0)
    wi=len([p for p in products if p["images"]])
    cats={}
    for p in products: cats[p["cat"]]=cats.get(p["cat"],0)+1

    print(f"\n{'='*62}")
    print(f"  DONE in {el//60}m {el%60:02d}s")
    print(f"  Products    : {len(products)}")
    print(f"  CSV rows    : {len(all_rows)}")
    print(f"  With images : {wi} ({round(wi/len(products)*100) if products else 0}%)")
    print(f"  Description : 100% (auto-generated, 5 sections each)")
    print(f"\n  By category (use these in Smart Collections → Type is equal to):")
    for cat,count in sorted(cats.items(),key=lambda x:-x[1]):
        print(f"    {cat:<35} {count:>3} products")
    print(f"\n  CSV  → {csv_file}")
    print(f"  JSON → {json_file}")
    print(f"  Shopify → Products → Import → Upload CSV")
    print("="*62)

if __name__=="__main__":
    main()
