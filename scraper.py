"""
Meta Commerce Catalog Feed Generator — Parks Automotive Group
Scrapes all dealership inventories via Firecrawl and outputs one CSV feed per dealership.
Runs daily via GitHub Actions — no local machine required.
"""

import csv
import os
from datetime import datetime, timezone
from firecrawl import FirecrawlApp

# ─── DEALERSHIPS ──────────────────────────────────────────────────────────────
# Each entry: (Dealer Name, Inventory URL, Output filename)
DEALERSHIPS = [
    ("Parks Buick GMC",               "https://www.parksbuickgmc.com/inventory",               "parks_buick_gmc.csv"),
    ("Parks Chevy Spartanburg",        "https://www.parkschevroletspartanburg.com/inventory",   "parks_chevy_spartanburg.csv"),
    ("Parks Chevy Charlotte",          "https://www.parkscharlotte.com/inventory",              "parks_chevy_charlotte.csv"),
    ("Parks Chevrolet Huntersville",   "https://www.parkschevrolethuntersville.com/inventory",  "parks_chevrolet_huntersville.csv"),
    ("Parks Chevy Kernersville",       "https://www.parkschevy.com/inventory",                 "parks_chevy_kernersville.csv"),
    ("Parks Ford Hendersonville",      "https://www.parksfordhendersonville.com/inventory",     "parks_ford_hendersonville.csv"),
    ("Parks Richmond",                 "https://www.parksrichmond.com/inventory",               "parks_richmond.csv"),
    ("Lake Norman CDJR",               "https://www.lakenormanchrysler.com/inventory",          "lake_norman_cdjr.csv"),
]

# ─── CONFIG ───────────────────────────────────────────────────────────────────
FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY", "YOUR_API_KEY_HERE")
CURRENCY = "USD"
OUTPUT_DIR = "feeds"

GOOGLE_PRODUCT_CATEGORY = "Vehicles & Parts > Vehicles > Motor Vehicles > Cars, Trucks & Vans"
FB_PRODUCT_CATEGORY     = "Vehicles & Parts > Vehicles > Cars, Trucks & Vans"

# Exact Meta catalog CSV column order
CSV_COLUMNS = [
    "id",
    "title",
    "description",
    "availability",
    "condition",
    "price",
    "link",
    "image_link",
    "brand",
    "google_product_category",
    "fb_product_category",
    "quantity_to_sell_on_facebook",
    "sale_price",
    "sale_price_effective_date",
    "item_group_id",
    "gender",
    "color",
    "size",
    "age_group",
    "material",
    "pattern",
    "shipping",
    "shipping_weight",
    "video[0].url",
    "video[0].tag[0]",
    "gtin",
    "product_tags[0]",
    "product_tags[1]",
    "style[0]",
]
# ─────────────────────────────────────────────────────────────────────────────


# ─── SCRAPE ───────────────────────────────────────────────────────────────────
def scrape_inventory(url: str, dealer_name: str) -> list[dict]:
    app = FirecrawlApp(api_key=FIRECRAWL_API_KEY)
    print(f"\n[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Scraping {dealer_name}: {url}")

    result = app.crawl_url(
        url,
        params={
            "limit": 500,
            "scrapeOptions": {
                "formats": ["extract"],
                "extract": {
                    "prompt": (
                        "Extract all vehicle listings from this dealership inventory page. "
                        "For each vehicle, extract all available details including stock number, "
                        "VIN, year, make, model, trim, price, mileage, condition (new/used), "
                        "exterior color, interior color, body style, transmission, drivetrain, "
                        "fuel type, engine, description, primary image URL, and the full URL "
                        "to the vehicle detail page."
                    ),
                    "schema": {
                        "type": "object",
                        "properties": {
                            "vehicles": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "stock_number":    {"type": "string"},
                                        "vin":             {"type": "string"},
                                        "year":            {"type": "string"},
                                        "make":            {"type": "string"},
                                        "model":           {"type": "string"},
                                        "trim":            {"type": "string"},
                                        "price":           {"type": "number"},
                                        "sale_price":      {"type": "number"},
                                        "mileage":         {"type": "string"},
                                        "condition":       {"type": "string"},
                                        "availability":    {"type": "string"},
                                        "exterior_color":  {"type": "string"},
                                        "interior_color":  {"type": "string"},
                                        "body_style":      {"type": "string"},
                                        "transmission":    {"type": "string"},
                                        "drivetrain":      {"type": "string"},
                                        "fuel_type":       {"type": "string"},
                                        "engine":          {"type": "string"},
                                        "description":     {"type": "string"},
                                        "image_url":       {"type": "string"},
                                        "video_url":       {"type": "string"},
                                        "detail_page_url": {"type": "string"},
                                    },
                                    "required": ["year", "make", "model", "price", "detail_page_url"]
                                }
                            }
                        },
                        "required": ["vehicles"]
                    }
                }
            }
        }
    )

    vehicles = []
    # firecrawl-py v1.x returns a Pydantic CrawlResponse; older versions return a dict
    data = result.data if hasattr(result, "data") else result.get("data", [])
    for page in data:
        if hasattr(page, "extract"):
            extracted = page.extract or {}
        else:
            extracted = page.get("extract", {})
        if isinstance(extracted, dict):
            vehicles.extend(extracted.get("vehicles", []))

    print(f"  → {len(vehicles)} vehicles scraped")
    return vehicles


# ─── TRANSFORM ────────────────────────────────────────────────────────────────
def normalize_condition(raw: str) -> str:
    if not raw:
        return "used"
    return "new" if "new" in raw.lower() else "used"


def normalize_availability(raw: str) -> str:
    if not raw:
        return "in stock"
    return "out of stock" if ("out" in raw.lower() or "sold" in raw.lower()) else "in stock"


def format_price(amount) -> str:
    try:
        return f"{float(amount):.2f} {CURRENCY}"
    except (TypeError, ValueError):
        return ""


def build_title(v: dict) -> str:
    parts = [v.get("year", ""), v.get("make", ""), v.get("model", ""), v.get("trim", "")]
    return " ".join(p for p in parts if p).strip()[:200]


def build_description(v: dict) -> str:
    if v.get("description"):
        return v["description"].strip()[:9999]
    parts = []
    if v.get("condition"):
        parts.append(v["condition"].title())
    if v.get("year") and v.get("make") and v.get("model"):
        parts.append(f"{v['year']} {v['make']} {v['model']}")
    if v.get("trim"):
        parts.append(v["trim"])
    if v.get("mileage"):
        parts.append(f"with {v['mileage']} miles")
    if v.get("exterior_color"):
        parts.append(f"in {v['exterior_color']}")
    if v.get("body_style"):
        parts.append(f"({v['body_style']})")
    if v.get("transmission"):
        parts.append(v["transmission"])
    if v.get("drivetrain"):
        parts.append(v["drivetrain"])
    if v.get("fuel_type"):
        parts.append(v["fuel_type"])
    return ". ".join(parts)[:9999]


def transform_vehicles(raw_vehicles: list[dict], dealer_name: str) -> list[dict]:
    transformed = []
    seen_ids = set()

    for v in raw_vehicles:
        vid = (v.get("vin") or v.get("stock_number") or "").strip()[:100]
        if not vid or vid in seen_ids:
            continue
        seen_ids.add(vid)

        title     = build_title(v)
        price_str = format_price(v.get("price"))
        image     = (v.get("image_url") or "").strip()
        link      = (v.get("detail_page_url") or "").strip()

        if not all([title, price_str, image, link]):
            continue

        tags = []
        for field in ["body_style", "drivetrain", "fuel_type", "engine"]:
            if v.get(field):
                tags.append(v[field])
        if v.get("mileage"):
            tags.append(f"{v['mileage']} miles")

        transformed.append({
            "id":                           vid,
            "title":                        title,
            "description":                  build_description(v),
            "availability":                 normalize_availability(v.get("availability")),
            "condition":                    normalize_condition(v.get("condition")),
            "price":                        price_str,
            "link":                         link,
            "image_link":                   image,
            "brand":                        (v.get("make") or dealer_name).strip()[:100],
            "google_product_category":      GOOGLE_PRODUCT_CATEGORY,
            "fb_product_category":          FB_PRODUCT_CATEGORY,
            "quantity_to_sell_on_facebook": "",
            "sale_price":                   format_price(v.get("sale_price")) if v.get("sale_price") else "",
            "sale_price_effective_date":    "",
            "item_group_id":                "",
            "gender":                       "",
            "color":                        (v.get("exterior_color") or "")[:200],
            "size":                         "",
            "age_group":                    "",
            "material":                     "",
            "pattern":                      "",
            "shipping":                     "",
            "shipping_weight":              "",
            "video[0].url":                 (v.get("video_url") or "").strip(),
            "video[0].tag[0]":              "",
            "gtin":                         "",
            "product_tags[0]":              tags[0] if len(tags) > 0 else "",
            "product_tags[1]":              tags[1] if len(tags) > 1 else "",
            "style[0]":                     "",
        })

    print(f"  → {len(transformed)} valid vehicles after filtering")
    return transformed


# ─── BUILD CSV ────────────────────────────────────────────────────────────────
def build_csv_feed(vehicles: list[dict], output_path: str):
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(vehicles)

    print(f"  → Feed written: {output_path} ({len(vehicles)} vehicles)")


# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("Parks Automotive Group — Meta Catalog Feed Generator")
    print(f"Run date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Dealerships: {len(DEALERSHIPS)}")
    print("=" * 60)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    summary = []

    for dealer_name, inventory_url, output_filename in DEALERSHIPS:
        output_path = os.path.join(OUTPUT_DIR, output_filename)
        try:
            raw      = scrape_inventory(inventory_url, dealer_name)
            vehicles = transform_vehicles(raw, dealer_name)
            build_csv_feed(vehicles, output_path)
            summary.append((dealer_name, len(vehicles), "OK"))
        except Exception as e:
            import traceback
            print(f"  ERROR scraping {dealer_name}: {e}")
            traceback.print_exc()
            summary.append((dealer_name, 0, f"FAILED: {e}"))

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for dealer_name, count, status in summary:
        print(f"  {dealer_name:<40} {count:>4} vehicles  [{status}]")
    print(f"\nAll feeds saved to: {os.path.abspath(OUTPUT_DIR)}/")


if __name__ == "__main__":
    main()
