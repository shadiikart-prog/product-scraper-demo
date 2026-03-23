# 🛒 Product Import Dashboard — BigCommerce Ready

> Automated product scraping pipeline from **HDEW Cameras** & **UK Flooring Direct** — validated and formatted for BigCommerce import.

## 🔴 Live Demo

**Open `index.html` in any browser** — No server, no login, no setup required.

## 📦 What This Does

```
Shopify Stores          Python Scrapers           BigCommerce
──────────────    →     ───────────────────   →   ─────────────────
hdewcameras.co.uk       /products.json API         CSV Import
ukflooringdirect.co.uk  Cursor pagination           REST API v3
```

1. Scrapes both stores via their public Shopify API
2. Extracts all product fields: name, SKU, price, description, images, stock, category, brand
3. Validates every product against BigCommerce import rules
4. Converts to BigCommerce CSV format and REST API JSON payload
5. Exports as CSV, JSON and Excel

## 🖥️ Dashboard Features

- Product table + grid view with images, search, sort, filter
- Validation report — BC compatibility score per product
- BC CSV Preview — downloadable import file
- BC API Payload — live JSON for every product
- Filter by source, stock status, category

## 🚀 Run Scrapers Locally

```bash
pip install requests openpyxl
python run_all_scrapers.py
python validate_bigcommerce_data.py
python bigcommerce_csv_converter.py
python bigcommerce_api_import.py --dry-run
```

*Built with Python + Shopify public API*
