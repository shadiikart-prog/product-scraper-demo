"""
embed_data.py
─────────────────────────────────────────────────────
Reads the latest scraped JSON files from output/
and injects the real product data into index.html

Run automatically by GitHub Actions after scraping.
"""

import json
import glob
import os
import re
from datetime import datetime

OUTPUT_DIR = "output"

def load_latest(pattern):
    files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
    if not files:
        return []
    print(f"  Loading: {files[0]}")
    with open(files[0], encoding="utf-8") as f:
        return json.load(f)

def clean(text):
    if not text:
        return ""
    text = str(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    # Escape for JS string safety
    text = text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ").replace("\r", "")
    return text[:500]

def build_js_array(products):
    lines = ["const PRODUCTS = ["]
    for p in products:
        site    = clean(p.get("Site") or p.get("Source Site") or "")
        name    = clean(p.get("Name", ""))
        sku     = clean(p.get("SKU", ""))
        cat     = clean(p.get("Category", ""))
        brand   = clean(p.get("Vendor / Brand", ""))
        price   = clean(p.get("Price (GBP)", "0"))
        compare = clean(p.get("Compare At Price", ""))
        stock   = clean(p.get("Stock Status", "Unknown"))
        inv     = str(p.get("Inventory Qty", "")).strip()
        desc    = clean(p.get("Description", ""))[:300]
        img     = clean(p.get("Main Image URL", ""))
        imgs    = clean(p.get("All Image URLs", ""))
        tags    = clean(p.get("Tags", ""))
        url     = clean(p.get("Product URL", ""))
        pid     = clean(str(p.get("Product ID", "")))
        m2tag   = clean(p.get("Price Per m2 Tag", ""))

        if not name:
            continue

        line = (
            f'  {{Site:"{site}",Name:"{name}",SKU:"{sku}",'
            f'Category:"{cat}","Vendor / Brand":"{brand}",'
            f'"Price (GBP)":"{price}","Compare At Price":"{compare}",'
            f'"Stock Status":"{stock}","Inventory Qty":"{inv}",'
            f'Description:"{desc}",'
            f'"Main Image URL":"{img}","All Image URLs":"{imgs}",'
            f'Tags:"{tags}","Product URL":"{url}",'
            f'"Product ID":"{pid}","Price Per m2 Tag":"{m2tag}"}},'
        )
        lines.append(line)
    lines.append("];")
    return "\n".join(lines)

def main():
    print("\n  embed_data.py — Injecting real product data into index.html")

    # Load data — prefer combined, fallback to individual
    products = load_latest(f"{OUTPUT_DIR}/combined_all_products_*.json")
    if not products:
        hdew = load_latest(f"{OUTPUT_DIR}/hdew_cameras_*.json")
        ukfd = load_latest(f"{OUTPUT_DIR}/ukflooring_direct_*.json")
        products = hdew + ukfd

    if not products:
        print("  No product data found! Scraper may have failed.")
        return

    print(f"  {len(products)} products loaded")

    # Read current index.html
    with open("index.html", encoding="utf-8") as f:
        html = f.read()

    # Replace the PRODUCTS array
    js_array = build_js_array(products)
    new_html = re.sub(
        r"const PRODUCTS = \[.*?\];",
        js_array,
        html,
        flags=re.DOTALL
    )

    # Update last-updated timestamp in header badge
    ts = datetime.utcnow().strftime("%d %b %Y %H:%M UTC")
    new_html = re.sub(
        r'(id="hdr-count">)[^<]*(</span>)',
        f'\\g<1>{len(products)} Products · Updated {ts}\\2',
        new_html
    )

    # Write back
    with open("index.html", "w", encoding="utf-8") as f:
        f.write(new_html)

    print(f"  index.html updated with {len(products)} real products!")
    print(f"  Timestamp: {ts}")

if __name__ == "__main__":
    main()
