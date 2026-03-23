"""
bigcommerce_csv_converter.py
─────────────────────────────────────────────────────────────────────────────
Converts scraped product data (from scraper_hdew.py / scraper_ukflooring.py)
into BigCommerce's official Product Import CSV format.

HOW TO USE:
  1. Run the scrapers first to generate your JSON files:
         python run_all_scrapers.py

  2. Then run this converter:
         python bigcommerce_csv_converter.py

  3. Upload the output CSV in BigCommerce:
         Dashboard → Products → Import

BigCommerce CSV docs:
  https://support.bigcommerce.com/s/article/Importing-Exporting-Products

OUTPUT:
  output/bigcommerce_import_hdew_<timestamp>.csv
  output/bigcommerce_import_ukflooring_<timestamp>.csv
  output/bigcommerce_import_combined_<timestamp>.csv
"""

import csv
import json
import os
import glob
import re
from datetime import datetime
from html import unescape

# ── Config ────────────────────────────────────────────────────────────────────
OUTPUT_DIR      = "output"
TIMESTAMP       = datetime.now().strftime("%Y%m%d_%H%M%S")
DEFAULT_WEIGHT  = "0.5"    # kg — BigCommerce requires a weight value
DEFAULT_TAX_CLASS = "Tax_Class_Default"
FIXED_SHIPPING  = "0.00"   # 0 = use store shipping rules
# ─────────────────────────────────────────────────────────────────────────────


# BigCommerce official CSV column order (v2 Product Import)
BC_COLUMNS = [
    "Product Name",          # Required
    "Product Type",          # physical / digital
    "SKU",
    "Description",
    "Price",
    "Cost Price",
    "MSRP",
    "Weight",
    "Width",
    "Height",
    "Depth",
    "Categories",            # Separated by / for hierarchy e.g. "Cameras/DSLR"
    "Brand Name",
    "Stock Level",
    "Low Stock Level",
    "Allow Purchases",       # Y or N
    "Track Inventory",       # Y / N / by_variant
    "Current Stock",
    "Tax Class Name",
    "Product Visible",       # Y or N
    "Product URL",
    "Search Keywords",
    "Availability",          # available / disabled / preorder
    "Product Image URL - 1",
    "Product Image URL - 2",
    "Product Image URL - 3",
    "Image Alt Text - 1",
    "Image Alt Text - 2",
    "Image Alt Text - 3",
    "Free Shipping",         # Y or N
    "Product Condition",     # New / Used / Refurbished
    "Show Product Condition", # Y or N
    "Page Title",
    "Meta Description",
    "Page Search Keywords",
    "Option Set",
    "Stop Processing Rules",
    "Gift Wrapping",
    "Sort Order",
    "Product Code/SKU",      # Alias — same as SKU
    "Bin Picking Number",
    "Import Strategy",       # MERGE or CREATE
    "External ID",
]


def clean_text(text: str) -> str:
    """Remove HTML tags, decode entities, normalise whitespace."""
    if not text:
        return ""
    text = unescape(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def parse_price(price_str) -> str:
    """Ensure price is a plain decimal string like '29.99'."""
    if not price_str:
        return "0.00"
    price_str = str(price_str).replace("£", "").replace(",", "").strip()
    try:
        return f"{float(price_str):.2f}"
    except ValueError:
        return "0.00"


def stock_to_bc(stock_status: str, inv_qty) -> tuple[str, str, str]:
    """
    Returns (availability, allow_purchases, current_stock)
    """
    status = str(stock_status).lower()
    qty = str(inv_qty) if inv_qty not in (None, "") else "0"

    if "in stock" in status:
        return "available", "Y", qty
    elif "out of stock" in status:
        return "disabled", "N", "0"
    elif "backorder" in status:
        return "preorder", "Y", "0"
    else:
        return "available", "Y", qty  # Unknown → treat as available


def build_category_path(category: str, tags: str, site_label: str) -> str:
    """
    Build BigCommerce category hierarchy string.
    e.g. "Electronics/Cameras/Mirrorless"
    """
    parts = []

    # Top-level from site
    if "hdew" in site_label.lower() or "hdew" in (category or "").lower():
        parts.append("Electronics")
    elif "flooring" in site_label.lower():
        parts.append("Flooring")

    # Mid level from product_type
    if category:
        parts.append(category.strip().title())

    # If no category, try to infer from tags
    if not category and tags:
        tag_list = [t.strip() for t in tags.split(",")]
        if tag_list:
            parts.append(tag_list[0].title())

    return "/".join(parts) if parts else "Uncategorised"


def split_images(all_images_str: str) -> list[str]:
    """Split pipe-separated image URLs into a list of up to 3."""
    if not all_images_str:
        return []
    return [u.strip() for u in all_images_str.split("|") if u.strip()][:3]


def row_to_bc(product: dict) -> dict:
    """Transform one scraped product row into a BigCommerce CSV row dict."""

    name        = clean_text(product.get("Name", ""))
    sku         = product.get("SKU", "").strip()
    description = clean_text(product.get("Description", ""))
    price       = parse_price(product.get("Price (GBP)", "0"))
    compare     = parse_price(product.get("Compare At Price", ""))
    brand       = clean_text(product.get("Vendor / Brand", ""))
    category    = product.get("Category", "")
    tags        = product.get("Tags", "")
    site        = product.get("Site", product.get("Source Site", ""))
    product_url = product.get("Product URL", "")
    stock_status= product.get("Stock Status", "Unknown")
    inv_qty     = product.get("Inventory Qty", "")
    variant     = product.get("Variant", "")

    # Append variant to name if present
    if variant:
        name = f"{name} - {variant}"

    availability, allow_purchases, current_stock = stock_to_bc(stock_status, inv_qty)
    category_path = build_category_path(category, tags, site)

    # Images
    images = split_images(product.get("All Image URLs", ""))
    main_img = product.get("Main Image URL", "")
    if main_img and main_img not in images:
        images.insert(0, main_img)
    images = images[:3]
    while len(images) < 3:
        images.append("")

    # Meta / SEO
    meta_desc = description[:160] if description else name
    keywords  = ", ".join([t.strip() for t in tags.split(",") if t.strip()][:10])
    page_title = name[:80]

    bc_row = {
        "Product Name"          : name,
        "Product Type"          : "physical",
        "SKU"                   : sku,
        "Description"           : description,
        "Price"                 : price,
        "Cost Price"            : "",
        "MSRP"                  : compare if compare != "0.00" else "",
        "Weight"                : DEFAULT_WEIGHT,
        "Width"                 : "",
        "Height"                : "",
        "Depth"                 : "",
        "Categories"            : category_path,
        "Brand Name"            : brand,
        "Stock Level"           : current_stock,
        "Low Stock Level"       : "5",
        "Allow Purchases"       : allow_purchases,
        "Track Inventory"       : "Y",
        "Current Stock"         : current_stock,
        "Tax Class Name"        : DEFAULT_TAX_CLASS,
        "Product Visible"       : "Y",
        "Product URL"           : product_url,
        "Search Keywords"       : keywords,
        "Availability"          : availability,
        "Product Image URL - 1" : images[0],
        "Product Image URL - 2" : images[1],
        "Product Image URL - 3" : images[2],
        "Image Alt Text - 1"    : name if images[0] else "",
        "Image Alt Text - 2"    : name if images[1] else "",
        "Image Alt Text - 3"    : name if images[2] else "",
        "Free Shipping"         : "N",
        "Product Condition"     : "New",
        "Show Product Condition": "N",
        "Page Title"            : page_title,
        "Meta Description"      : meta_desc,
        "Page Search Keywords"  : keywords,
        "Option Set"            : "",
        "Stop Processing Rules" : "N",
        "Gift Wrapping"         : "N",
        "Sort Order"            : "0",
        "Product Code/SKU"      : sku,
        "Bin Picking Number"    : "",
        "Import Strategy"       : "MERGE",
        "External ID"           : product.get("Product ID", ""),
    }

    return bc_row


def load_latest_json(pattern: str) -> list[dict]:
    """Find the most recently created JSON file matching a glob pattern."""
    files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
    if not files:
        return []
    filepath = files[0]
    print(f"  Loading: {filepath}")
    with open(filepath, encoding="utf-8") as f:
        return json.load(f)


def save_bc_csv(rows: list[dict], filepath: str, label: str):
    if not rows:
        print(f"  [{label}] No rows to save.")
        return
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=BC_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"  [{label}] ✓ BigCommerce CSV → {filepath}  ({len(rows)} products)")


def convert_dataset(source_rows: list[dict], label: str, out_path: str):
    """Convert a list of scraped rows → BigCommerce CSV rows → save."""
    if not source_rows:
        print(f"  [{label}] No source data found.")
        return []

    bc_rows = []
    skipped = 0
    seen_skus = set()

    for row in source_rows:
        name = row.get("Name", "").strip()
        if not name:
            skipped += 1
            continue

        # Deduplicate by SKU (keep first occurrence)
        sku = row.get("SKU", "").strip()
        if sku and sku in seen_skus:
            continue
        if sku:
            seen_skus.add(sku)

        bc_rows.append(row_to_bc(row))

    print(f"  [{label}] Converted {len(bc_rows)} rows  (skipped {skipped} blank names)")
    save_bc_csv(bc_rows, out_path, label)
    return bc_rows


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("\n" + "═"*62)
    print("  BigCommerce CSV Converter")
    print("  Reads latest scraped JSON → outputs BC-ready import CSVs")
    print("═"*62)

    # ── HDEW ──
    print("\n[1/3] HDEW Cameras")
    hdew_data = load_latest_json(f"{OUTPUT_DIR}/hdew_cameras_*.json")
    hdew_bc = convert_dataset(
        hdew_data, "HDEW",
        f"{OUTPUT_DIR}/bigcommerce_import_hdew_{TIMESTAMP}.csv"
    )

    # ── UK Flooring ──
    print("\n[2/3] UK Flooring Direct")
    ukfd_data = load_latest_json(f"{OUTPUT_DIR}/ukflooring_direct_*.json")
    ukfd_bc = convert_dataset(
        ukfd_data, "UKFD",
        f"{OUTPUT_DIR}/bigcommerce_import_ukflooring_{TIMESTAMP}.csv"
    )

    # ── Combined ──
    print("\n[3/3] Combined")
    combined = hdew_bc + ukfd_bc
    if combined:
        out = f"{OUTPUT_DIR}/bigcommerce_import_combined_{TIMESTAMP}.csv"
        save_bc_csv(combined, out, "COMBINED")

    print(f"\n{'═'*62}")
    print("  ✅ Conversion complete!")
    print(f"  Upload any of the CSVs via:")
    print(f"  BigCommerce Dashboard → Products → Import")
    print(f"{'═'*62}\n")


if __name__ == "__main__":
    main()
