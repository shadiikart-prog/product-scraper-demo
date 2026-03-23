"""
bigcommerce_api_import.py
─────────────────────────────────────────────────────────────────────────────
Imports products into BigCommerce via the REST API (v3).

MODES:
  --dry-run   (default) Validates & previews payload. Does NOT touch your store.
  --live      Actually creates products via the API.

SETUP — add your credentials to config.json (see README) or set env vars:
  BC_STORE_HASH   e.g. "abc123xyz"
  BC_ACCESS_TOKEN e.g. "your_api_token_here"

HOW TO GET API CREDENTIALS:
  1. Go to BigCommerce Dashboard
  2. Settings → API Accounts → Create API Account
  3. Choose "Store-level API", enable: Products (Modify), Categories (Read-only)
  4. Copy Store Hash + Access Token

USAGE:
  python bigcommerce_api_import.py              # dry-run (safe, no changes)
  python bigcommerce_api_import.py --live       # actually imports (USE ON REAL STORE)
  python bigcommerce_api_import.py --limit 5    # only process first N products
  python bigcommerce_api_import.py --source hdew  # only import HDEW products

BigCommerce API docs: https://developer.bigcommerce.com/docs/rest-catalog/products
"""

import argparse
import json
import os
import glob
import re
import sys
import time
from datetime import datetime
from html import unescape

try:
    import requests
    REQUESTS_OK = True
except ImportError:
    REQUESTS_OK = False

# ── Config ────────────────────────────────────────────────────────────────────
OUTPUT_DIR = "output"
CONFIG_FILE = "bc_config.json"
LOG_FILE = f"{OUTPUT_DIR}/api_import_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

# Placeholder credentials — replace with real values or set env vars
DEFAULT_CONFIG = {
    "store_hash": "YOUR_STORE_HASH",        # e.g. "abc123xyz"
    "access_token": "YOUR_ACCESS_TOKEN",    # e.g. "xyzabc123..."
    "api_version": "v3",
}

RATE_LIMIT_DELAY = 0.3   # seconds between API calls
BATCH_SIZE = 10           # products per batch (BC allows up to 10 in bulk create)
# ─────────────────────────────────────────────────────────────────────────────


def load_config() -> dict:
    """Load BC credentials from config file or environment variables."""
    config = dict(DEFAULT_CONFIG)

    # Config file takes priority
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE) as f:
            config.update(json.load(f))

    # Environment variables override file
    if os.environ.get("BC_STORE_HASH"):
        config["store_hash"] = os.environ["BC_STORE_HASH"]
    if os.environ.get("BC_ACCESS_TOKEN"):
        config["access_token"] = os.environ["BC_ACCESS_TOKEN"]

    return config


def is_placeholder(config: dict) -> bool:
    return (
        config["store_hash"] == "YOUR_STORE_HASH"
        or config["access_token"] == "YOUR_ACCESS_TOKEN"
        or not config["store_hash"]
        or not config["access_token"]
    )


def get_headers(config: dict) -> dict:
    return {
        "X-Auth-Token": config["access_token"],
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def base_url(config: dict) -> str:
    return (
        f"https://api.bigcommerce.com/stores/"
        f"{config['store_hash']}/{config['api_version']}"
    )


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = unescape(str(text))
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def parse_price(price_str) -> float:
    if not price_str:
        return 0.0
    price_str = str(price_str).replace("£", "").replace(",", "").strip()
    try:
        return round(float(price_str), 2)
    except ValueError:
        return 0.0


def stock_info(stock_status: str, inv_qty) -> dict:
    status = str(stock_status).lower()
    try:
        qty = int(inv_qty) if inv_qty not in (None, "") else 0
    except (ValueError, TypeError):
        qty = 0

    if "out of stock" in status:
        return {"availability": "disabled", "inventory_level": 0, "is_visible": True}
    elif "backorder" in status:
        return {"availability": "preorder", "inventory_level": 0, "is_visible": True}
    else:
        return {"availability": "available", "inventory_level": qty, "is_visible": True}


def build_bc_payload(product: dict) -> dict:
    """
    Convert one scraped row into a BigCommerce v3 API product payload.
    https://developer.bigcommerce.com/docs/rest-catalog/products#create-a-product
    """
    name        = clean_text(product.get("Name", "")).strip()
    sku         = product.get("SKU", "").strip()
    description = clean_text(product.get("Description", ""))
    price       = parse_price(product.get("Price (GBP)", ""))
    msrp        = parse_price(product.get("Compare At Price", ""))
    brand_name  = clean_text(product.get("Vendor / Brand", ""))
    category    = product.get("Category", "")
    tags_raw    = product.get("Tags", "")
    stock_status= product.get("Stock Status", "Unknown")
    inv_qty     = product.get("Inventory Qty", "")
    variant     = product.get("Variant", "")
    ext_id      = str(product.get("Product ID", ""))

    if variant:
        name = f"{name} - {variant}"

    stock = stock_info(stock_status, inv_qty)
    tags  = [t.strip() for t in tags_raw.split(",") if t.strip()][:20]

    # Images
    images = []
    all_imgs_str = product.get("All Image URLs", "")
    main_img = product.get("Main Image URL", "")

    img_urls = [u.strip() for u in all_imgs_str.split("|") if u.strip()]
    if main_img and main_img not in img_urls:
        img_urls.insert(0, main_img)

    for idx, url in enumerate(img_urls[:5]):
        images.append({
            "image_url": url,
            "is_thumbnail": idx == 0,
            "sort_order": idx,
            "description": name,
        })

    payload = {
        "name": name or "Unnamed Product",
        "type": "physical",
        "sku": sku,
        "description": f"<p>{description}</p>" if description else "",
        "price": price if price > 0 else 0.01,   # BC requires price > 0
        "retail_price": msrp if msrp > 0 else None,
        "weight": 0.5,                            # Default weight (kg)
        "brand_name": brand_name,
        "categories": [],                         # Filled by get_or_create_category()
        "inventory_level": stock["inventory_level"],
        "inventory_tracking": "product",
        "availability": stock["availability"],
        "is_visible": stock["is_visible"],
        "search_keywords": ", ".join(tags[:10]),
        "meta_keywords": tags[:10],
        "meta_description": description[:255] if description else name,
        "page_title": name[:255],
        "images": images,
        "custom_fields": [
            {"name": "Source Site", "value": product.get("Site", product.get("Source Site", ""))[:255]},
            {"name": "External Product ID", "value": ext_id[:255]},
            {"name": "Original URL", "value": product.get("Product URL", "")[:255]},
        ],
    }

    # Remove None values (BC API rejects them)
    if payload["retail_price"] is None:
        del payload["retail_price"]

    return payload


# ── API Calls ──────────────────────────────────────────────────────────────────

_category_cache: dict[str, int] = {}


def get_or_create_category(name: str, config: dict, dry_run: bool) -> int:
    """Return BC category ID for a name, creating it if needed."""
    if not name:
        return 0

    if name in _category_cache:
        return _category_cache[name]

    if dry_run:
        fake_id = abs(hash(name)) % 9000 + 1000
        _category_cache[name] = fake_id
        return fake_id

    url = f"{base_url(config)}/catalog/categories"
    headers = get_headers(config)

    # Search first
    try:
        r = requests.get(url, headers=headers, params={"name": name}, timeout=15)
        if r.ok:
            data = r.json().get("data", [])
            if data:
                cat_id = data[0]["id"]
                _category_cache[name] = cat_id
                return cat_id
    except Exception:
        pass

    # Create
    try:
        r = requests.post(url, headers=headers,
                          json={"name": name, "parent_id": 0}, timeout=15)
        if r.ok:
            cat_id = r.json()["data"]["id"]
            _category_cache[name] = cat_id
            return cat_id
    except Exception:
        pass

    return 0


def create_product(payload: dict, config: dict, dry_run: bool) -> dict:
    """POST a single product to BigCommerce (or simulate in dry-run)."""
    if dry_run:
        return {
            "success": True,
            "dry_run": True,
            "simulated_id": abs(hash(payload["name"])) % 90000 + 10000,
            "name": payload["name"],
            "sku": payload.get("sku", ""),
            "price": payload["price"],
        }

    url = f"{base_url(config)}/catalog/products"
    try:
        r = requests.post(url, headers=get_headers(config),
                          json=payload, timeout=30)
        if r.ok:
            data = r.json().get("data", {})
            return {"success": True, "id": data.get("id"), "name": payload["name"]}
        else:
            return {
                "success": False,
                "name": payload["name"],
                "status_code": r.status_code,
                "error": r.text[:300],
            }
    except Exception as e:
        return {"success": False, "name": payload["name"], "error": str(e)}


# ── Main logic ────────────────────────────────────────────────────────────────

def load_latest_json(pattern: str) -> list[dict]:
    files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
    if not files:
        return []
    print(f"  Loading: {files[0]}")
    with open(files[0], encoding="utf-8") as f:
        return json.load(f)


def run_import(source_rows: list[dict], config: dict,
               dry_run: bool, limit: int) -> list[dict]:
    results = []
    errors  = []
    processed = 0

    rows_to_process = source_rows[:limit] if limit else source_rows
    total = len(rows_to_process)

    print(f"\n  Mode     : {'🔍 DRY RUN (no changes made)' if dry_run else '🚀 LIVE IMPORT'}")
    print(f"  Products : {total}")
    print(f"  {'─'*50}")

    for i, row in enumerate(rows_to_process, 1):
        name = row.get("Name", "").strip()
        if not name:
            continue

        # Build payload
        payload = build_bc_payload(row)

        # Resolve category
        category_name = row.get("Category", "Uncategorised").strip() or "Uncategorised"
        cat_id = get_or_create_category(category_name, config, dry_run)
        if cat_id:
            payload["categories"] = [cat_id]

        # Create product
        result = create_product(payload, config, dry_run)
        result["row_index"] = i

        status_icon = "✓" if result["success"] else "✗"
        dry_tag = " [DRY RUN]" if dry_run else ""
        print(f"  {status_icon} [{i:>4}/{total}] {name[:55]:<55} £{payload['price']:.2f}{dry_tag}")

        if result["success"]:
            results.append(result)
        else:
            errors.append(result)
            print(f"       ↳ Error: {result.get('error', 'Unknown')[:80]}")

        processed += 1
        if not dry_run:
            time.sleep(RATE_LIMIT_DELAY)

    return results, errors


def print_summary(results: list, errors: list, dry_run: bool):
    total = len(results) + len(errors)
    print(f"\n{'═'*62}")
    print(f"  IMPORT SUMMARY {'(DRY RUN)' if dry_run else '(LIVE)'}")
    print(f"{'─'*62}")
    print(f"  ✓ Successful : {len(results)}")
    print(f"  ✗ Failed     : {len(errors)}")
    print(f"  Total        : {total}")
    if dry_run:
        print(f"\n  ✅ Dry run complete — no data was sent to BigCommerce.")
        print(f"  Run with --live when you have real API credentials.")
    print(f"{'═'*62}\n")


def main():
    parser = argparse.ArgumentParser(description="BigCommerce Product Importer")
    parser.add_argument("--live",    action="store_true",
                        help="Actually import (default is dry-run)")
    parser.add_argument("--limit",   type=int, default=0,
                        help="Only process first N products (0 = all)")
    parser.add_argument("--source",  choices=["hdew", "ukfd", "combined"],
                        default="combined",
                        help="Which dataset to import (default: combined)")
    args = parser.parse_args()

    dry_run = not args.live
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    config = load_config()

    print("\n" + "═"*62)
    print("  BigCommerce API Importer")
    print("═"*62)
    print(f"  Store Hash   : {config['store_hash']}")
    print(f"  Token        : {'[PLACEHOLDER]' if is_placeholder(config) else '[SET ✓]'}")
    print(f"  Mode         : {'DRY RUN' if dry_run else '⚠️  LIVE'}")

    # Force dry-run if credentials are placeholders
    if args.live and is_placeholder(config):
        print("\n  ⚠️  Cannot run --live: credentials are still placeholders.")
        print("  Set your real store_hash and access_token in bc_config.json")
        print("  Falling back to DRY RUN.\n")
        dry_run = True

    if not REQUESTS_OK:
        print("\n  ✗ 'requests' not installed. Run: pip install requests")
        sys.exit(1)

    # Load data
    patterns = {
        "hdew"    : f"{OUTPUT_DIR}/hdew_cameras_*.json",
        "ukfd"    : f"{OUTPUT_DIR}/ukflooring_direct_*.json",
        "combined": f"{OUTPUT_DIR}/combined_all_products_*.json",
    }
    print(f"\n  Loading dataset: {args.source}")
    rows = load_latest_json(patterns[args.source])

    if not rows:
        print("  No data found. Run run_all_scrapers.py first.")
        sys.exit(1)

    # Run
    results, errors = run_import(rows, config, dry_run, args.limit or 0)
    print_summary(results, errors, dry_run)

    # Save log
    log = {
        "timestamp": datetime.now().isoformat(),
        "dry_run": dry_run,
        "source": args.source,
        "total_attempted": len(results) + len(errors),
        "successful": len(results),
        "failed": len(errors),
        "results": results,
        "errors": errors,
    }
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(log, f, indent=2, ensure_ascii=False)
    print(f"  Log saved → {LOG_FILE}")


if __name__ == "__main__":
    main()
