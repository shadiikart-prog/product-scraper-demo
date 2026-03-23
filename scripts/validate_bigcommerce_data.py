"""
validate_bigcommerce_data.py
─────────────────────────────────────────────────────────────────────────────
Validates scraped product data against BigCommerce's import rules BEFORE
you attempt any real import. Catches issues early so nothing fails silently.

Checks performed:
  ✓ Required fields present (Name, Price)
  ✓ Price is a valid positive number
  ✓ SKU uniqueness
  ✓ Description length
  ✓ Image URLs are reachable (optional, slow)
  ✓ Stock status values are valid
  ✓ Name / description not too long (BC limits)
  ✓ Blank / duplicate product detection
  ✓ Summary statistics per site

USAGE:
  python validate_bigcommerce_data.py
  python validate_bigcommerce_data.py --check-images   # also pings image URLs
  python validate_bigcommerce_data.py --source hdew     # validate one site only
"""

import json
import glob
import os
import re
import sys
import argparse
from collections import defaultdict
from datetime import datetime

try:
    import requests
    REQUESTS_OK = True
except ImportError:
    REQUESTS_OK = False

# ── Config ────────────────────────────────────────────────────────────────────
OUTPUT_DIR = "output"

# BigCommerce field limits
BC_LIMITS = {
    "name": 250,
    "sku": 250,
    "description": 65535,
    "meta_description": 255,
    "brand": 250,
    "page_title": 255,
}

VALID_STOCK_STATUSES = {
    "in stock", "out of stock", "available (backorder)", "unknown"
}
# ─────────────────────────────────────────────────────────────────────────────


class Validator:
    def __init__(self, rows: list[dict], label: str):
        self.rows   = rows
        self.label  = label
        self.issues = []
        self.warnings = []
        self.stats  = defaultdict(int)

    def add_issue(self, row_idx: int, product_name: str, field: str, msg: str):
        self.issues.append({
            "row": row_idx,
            "product": product_name[:60],
            "field": field,
            "issue": msg,
        })

    def add_warning(self, row_idx: int, product_name: str, field: str, msg: str):
        self.warnings.append({
            "row": row_idx,
            "product": product_name[:60],
            "field": field,
            "warning": msg,
        })

    def run(self) -> bool:
        seen_skus = {}
        seen_names = {}
        price_missing = 0
        price_zero    = 0

        print(f"\n  Validating {len(self.rows)} rows…")

        for i, row in enumerate(self.rows, 1):
            name  = str(row.get("Name", "")).strip()
            sku   = str(row.get("SKU", "")).strip()
            price_raw = str(row.get("Price (GBP)", "")).strip()
            desc  = str(row.get("Description", "")).strip()
            brand = str(row.get("Vendor / Brand", "")).strip()
            stock = str(row.get("Stock Status", "")).strip().lower()
            imgs  = str(row.get("Main Image URL", "")).strip()

            # ── Required: Name ───────────────────────────────────────
            if not name:
                self.add_issue(i, "(blank)", "Name", "Product name is missing")
                self.stats["missing_name"] += 1
                continue   # Can't validate further without a name

            # ── Name length ──────────────────────────────────────────
            if len(name) > BC_LIMITS["name"]:
                self.add_issue(i, name, "Name",
                    f"Too long ({len(name)} chars, BC max {BC_LIMITS['name']})")
                self.stats["name_too_long"] += 1

            # ── Duplicate name ───────────────────────────────────────
            if name in seen_names:
                self.add_warning(i, name, "Name",
                    f"Duplicate of row {seen_names[name]} — may overwrite in BC")
                self.stats["duplicate_names"] += 1
            else:
                seen_names[name] = i

            # ── SKU ──────────────────────────────────────────────────
            if not sku:
                self.add_warning(i, name, "SKU",
                    "Missing SKU — BigCommerce will auto-generate one")
                self.stats["missing_sku"] += 1
            elif sku in seen_skus:
                self.add_issue(i, name, "SKU",
                    f"Duplicate SKU '{sku}' (first seen at row {seen_skus[sku]})")
                self.stats["duplicate_skus"] += 1
            else:
                seen_skus[sku] = i

            if len(sku) > BC_LIMITS["sku"]:
                self.add_issue(i, name, "SKU",
                    f"Too long ({len(sku)} chars, max {BC_LIMITS['sku']})")

            # ── Price ────────────────────────────────────────────────
            price_clean = price_raw.replace("£", "").replace(",", "").strip()
            if not price_clean:
                self.add_issue(i, name, "Price", "Price is missing")
                price_missing += 1
                self.stats["missing_price"] += 1
            else:
                try:
                    price_val = float(price_clean)
                    if price_val <= 0:
                        self.add_warning(i, name, "Price",
                            f"Price is £{price_val:.2f} — BC requires > 0")
                        price_zero += 1
                        self.stats["zero_price"] += 1
                    else:
                        self.stats["total_price"] += price_val
                        self.stats["priced_count"] += 1
                except ValueError:
                    self.add_issue(i, name, "Price",
                        f"Not a valid number: '{price_raw}'")
                    self.stats["invalid_price"] += 1

            # ── Description ──────────────────────────────────────────
            if not desc:
                self.add_warning(i, name, "Description",
                    "No description — will import with empty description")
                self.stats["missing_desc"] += 1
            elif len(desc) > BC_LIMITS["description"]:
                self.add_issue(i, name, "Description",
                    f"Too long ({len(desc)} chars, BC max {BC_LIMITS['description']})")

            # ── Stock Status ─────────────────────────────────────────
            if stock and stock not in VALID_STOCK_STATUSES:
                self.add_warning(i, name, "Stock Status",
                    f"Unrecognised value: '{stock}' — will default to 'available'")
                self.stats["unknown_stock"] += 1

            # ── Images ───────────────────────────────────────────────
            if not imgs:
                self.add_warning(i, name, "Image",
                    "No product image — product will import without images")
                self.stats["missing_image"] += 1
            elif not imgs.startswith("http"):
                self.add_issue(i, name, "Image",
                    f"Image URL doesn't start with http: '{imgs[:60]}'")

            self.stats["valid_rows"] += 1

        return len(self.issues) == 0

    def check_images(self, max_check=20):
        """Optionally ping a sample of image URLs to verify they are reachable."""
        if not REQUESTS_OK:
            print("  [IMAGE CHECK] Skipped — 'requests' not installed.")
            return

        print(f"\n  Checking up to {max_check} image URLs…")
        checked = broken = 0

        for row in self.rows[:max_check]:
            url = str(row.get("Main Image URL", "")).strip()
            name = row.get("Name", "?")[:40]
            if not url:
                continue
            try:
                r = requests.head(url, timeout=8, allow_redirects=True)
                if r.status_code >= 400:
                    self.add_warning(0, name, "Image URL",
                        f"HTTP {r.status_code} for {url[:80]}")
                    broken += 1
            except Exception as e:
                self.add_warning(0, name, "Image URL",
                    f"Unreachable: {url[:60]} — {str(e)[:40]}")
                broken += 1
            checked += 1

        ok = checked - broken
        print(f"  Image check: {ok}/{checked} OK  |  {broken} broken")

    def print_report(self):
        label = self.label
        total = len(self.rows)
        valid = self.stats["valid_rows"]

        print(f"\n  {'─'*58}")
        print(f"  📊  {label}  —  Validation Report")
        print(f"  {'─'*58}")
        print(f"  Total rows         : {total}")
        print(f"  Valid rows         : {valid}")
        print(f"  ERRORS (blockers)  : {len(self.issues)}")
        print(f"  Warnings (minor)   : {len(self.warnings)}")
        print()

        # Stats
        if self.stats["priced_count"]:
            avg = self.stats["total_price"] / self.stats["priced_count"]
            print(f"  💷 Avg price        : £{avg:.2f}")
        if self.stats["missing_sku"]:
            print(f"  ⚠ Missing SKU       : {self.stats['missing_sku']}")
        if self.stats["duplicate_skus"]:
            print(f"  ✗ Duplicate SKUs    : {self.stats['duplicate_skus']}")
        if self.stats["missing_price"]:
            print(f"  ✗ Missing price     : {self.stats['missing_price']}")
        if self.stats["zero_price"]:
            print(f"  ⚠ Zero/neg price    : {self.stats['zero_price']}")
        if self.stats["missing_desc"]:
            print(f"  ⚠ No description    : {self.stats['missing_desc']}")
        if self.stats["missing_image"]:
            print(f"  ⚠ No image          : {self.stats['missing_image']}")
        if self.stats["duplicate_names"]:
            print(f"  ⚠ Duplicate names   : {self.stats['duplicate_names']}")

        # Errors
        if self.issues:
            print(f"\n  ❌  ERRORS  (must fix before import):")
            print(f"  {'─'*58}")
            for issue in self.issues[:30]:
                print(f"  Row {issue['row']:>5} | [{issue['field']}] {issue['product']}")
                print(f"           ↳ {issue['issue']}")
            if len(self.issues) > 30:
                print(f"  … and {len(self.issues) - 30} more errors.")

        # Warnings
        if self.warnings:
            print(f"\n  ⚠️   WARNINGS  (safe to import but worth reviewing):")
            print(f"  {'─'*58}")
            for warn in self.warnings[:20]:
                print(f"  Row {warn['row']:>5} | [{warn['field']}] {warn['product']}")
                print(f"           ↳ {warn['warning']}")
            if len(self.warnings) > 20:
                print(f"  … and {len(self.warnings) - 20} more warnings.")

        # Final verdict
        print(f"\n  {'─'*58}")
        if not self.issues:
            print(f"  ✅  {label}: READY TO IMPORT  ({valid} products)")
        else:
            print(f"  ❌  {label}: {len(self.issues)} ERRORS must be resolved first")
        print(f"  {'─'*58}")


def load_latest_json(pattern: str) -> list[dict]:
    files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
    if not files:
        return []
    print(f"  Loading: {files[0]}")
    with open(files[0], encoding="utf-8") as f:
        return json.load(f)


def main():
    parser = argparse.ArgumentParser(description="BigCommerce Data Validator")
    parser.add_argument("--check-images", action="store_true",
                        help="Also verify image URLs are reachable (slow)")
    parser.add_argument("--source", choices=["hdew", "ukfd", "combined", "all"],
                        default="all", help="Which dataset(s) to validate")
    args = parser.parse_args()

    print("\n" + "═"*62)
    print("  BigCommerce Data Validator")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("═"*62)

    all_ok = True

    sources = {
        "hdew"    : (f"{OUTPUT_DIR}/hdew_cameras_*.json",        "HDEW Cameras"),
        "ukfd"    : (f"{OUTPUT_DIR}/ukflooring_direct_*.json",   "UK Flooring Direct"),
        "combined": (f"{OUTPUT_DIR}/combined_all_products_*.json","Combined"),
    }

    target = list(sources.items()) if args.source == "all" else [(args.source, sources[args.source])]

    for key, (pattern, label) in target:
        rows = load_latest_json(pattern)
        if not rows:
            print(f"\n  [{label}] No data found — run scrapers first.")
            continue

        v = Validator(rows, label)
        ok = v.run()
        if args.check_images:
            v.check_images()
        v.print_report()
        if not ok:
            all_ok = False

    print(f"\n{'═'*62}")
    if all_ok:
        print("  ✅  All datasets passed validation!")
        print("  Next step: python bigcommerce_csv_converter.py")
        print("         or: python bigcommerce_api_import.py --dry-run")
    else:
        print("  ❌  Some datasets have errors. Fix them before importing.")
    print(f"{'═'*62}\n")


if __name__ == "__main__":
    main()
