"""
Meta Commerce Catalog Feed Generator — Parks Automotive Group
Uses Firecrawl map() to discover VDP URLs, then extract() all vehicles in one API call per dealer.
Runs daily via GitHub Actions — no local machine required.
"""

import csv
import os
import re
from datetime import datetime, timezone
from firecrawl import FirecrawlApp

# ─── DEALERSHIPS ──────────────────────────────────────────────────────────────
DEALERSHIPS = [
    ("Parks Buick GMC",               "https://www.parksbuickgmc.com",               "parks_buick_gmc.csv"),
    ("Parks Chevy Spartanburg",        "https://www.parkschevroletspartanburg.com",   "parks_chevy_spartanburg.csv"),
    ("Parks Chevy Charlotte",          "https://www.parkscharlotte.com",              "parks_chevy_charlotte.csv"),
    ("Parks Chevrolet Huntersville",   "https://www.parkschevrolethuntersville.com",  "parks_chevrolet_huntersville.csv"),
    ("Parks Chevy Kernersville",       "https://www.parkschevy.com",                 "parks_chevy_kernersville.csv"),
    ("Parks Ford Hendersonville",      "https://www.parksfordhendersonville.com",     "parks_ford_hendersonville.csv"),
    ("Parks Richmond",                 "https://www.parksrichmond.com",               "parks_richmond.csv"),
    ("Lake Norman CDJR",               "https://www.lakenormanchrysler.com",          "lake_norman_cdjr.csv"),
]

# ─── CONFIG ───────────────────────────────────────────────────────────────────
FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY", "YOUR_API_KEY_HERE")
CURRENCY   = "USD"
OUTPUT_DIR = "feeds"

GOOGLE_PRODUCT_CATEGORY = "Vehicles & Parts > Vehicles > Motor Vehicles > Cars, Trucks & Vans"
FB_PRODUCT_CATEGORY     = "Vehicles & Parts > Vehicles > Cars, Trucks & Vans"

CSV_COLUMNS = [
    "id", "title", "description", "availability", "condition", "price",
    "link", "image_link", "brand", "google_product_category", "fb_product_category",
    "quantity_to_sell_on_facebook", "sale_price", "sale_price_effective_date",
    "item_group_id", "gender", "color", "size", "age_group", "material", "pattern",
    "shipping", "shipping_weight", "video[0].url", "video[0].tag[0]",
    "gtin", "product_tags[0]", "product_tags[1]", "style[0]",
]

# VDP URLs end with a 17-char VIN slug
VDP_RE = re.compile(r'/inventory/[a-z]+-\d{4}-.+-([a-z0-9]{17})/?$', re.IGNORECASE)
# ─────────────────────────────────────────────────────────────────────────────


# ─── EXTRACTION SCHEMA ────────────────────────────────────────────────────────
VEHICLE_SCHEMA = {
    "type": "object",
    "properties": {
        "vehicles": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "vin":             {"type": "string"},
                    "stock_number":    {"type": "string"},
                    "year":            {"type": "string"},
                    "make":            {"type": "string"},
                    "model":           {"type": "string"},
                    "trim":            {"type": "string"},
                    "body_style":      {"type": "string"},
                    "condition":       {"type": "string"},
                    "price":           {"type": "number"},
                    "sale_price":      {"type": "number"},
                    "mileage":         {"type": "string"},
                    "exterior_color":  {"type": "string"},
                    "interior_color":  {"type": "string"},
                    "transmission":    {"type": "string"},
                    "drivetrain":      {"type": "string"},
                    "fuel_type":       {"type": "string"},
                    "engine":          {"type": "string"},
                    "mpg_city":        {"type": "string"},
                    "mpg_highway":     {"type": "string"},
                    "certified":       {"type": "boolean"},
                    "image_url":       {"type": "string"},
                    "detail_page_url": {"type": "string"},
                },
                "required": ["year", "make", "model"]
            }
        }
    },
    "required": ["vehicles"]
}

EXTRACT_PROMPT = (
    "For each URL provided, visit that vehicle detail page and extract: "
    "VIN, stock number, year, make, model, trim, body style, condition (New/Used/Certified Pre-Owned), "
    "price, sale price (if different from price), mileage, exterior color, interior color, "
    "transmission, drivetrain, fuel type, engine, MPG city, MPG highway, "
    "certified (true if CPO, false otherwise), primary image URL, and the detail page URL. "
    "Return all vehicles as an array."
)


# ─── SCRAPE ───────────────────────────────────────────────────────────────────
def discover_vdp_urls(app: FirecrawlApp, base_url: str) -> list[str]:
    """One map() call on the root domain to get all VDP URLs."""
    result = app.map(base_url, limit=500)
    all_urls = [lr.url for lr in result.links if hasattr(lr, "url")]
    seen, unique = set(), []
    for u in all_urls:
        if not VDP_RE.search(u):
            continue
        key = u.rstrip("/")
        if key not in seen:
            seen.add(key)
            unique.append(u)
    print(f"  → {len(unique)} VDP URLs found (from {len(all_urls)} total)")
    return unique


def extract_vehicles(app: FirecrawlApp, urls: list[str]) -> list[dict]:
    """One extract() call for all VDPs — Firecrawl handles parallelism internally."""
    if not urls:
        return []

    result = app.extract(
        urls,
        prompt=EXTRACT_PROMPT,
        schema=VEHICLE_SCHEMA,
    )

    data = result.data if hasattr(result, "data") else result
    if isinstance(data, dict):
        vehicles = data.get("vehicles", [])
    elif isinstance(data, list):
        vehicles = data
    else:
        vehicles = []

    print(f"  → {len(vehicles)} vehicles extracted")
    return vehicles


def scrape_inventory(base_url: str, dealer_name: str) -> list[dict]:
    app = FirecrawlApp(api_key=FIRECRAWL_API_KEY)
    print(f"\n[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {dealer_name}")
    vdp_urls = discover_vdp_urls(app, base_url)
    if not vdp_urls:
        print("  → No VDPs found, skipping")
        return []
    return extract_vehicles(app, vdp_urls)


# ─── TRANSFORM ────────────────────────────────────────────────────────────────
def normalize_condition(raw: str, certified: bool = False) -> str:
    if certified:
        return "used"
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
    parts = [v.get("year",""), v.get("make",""), v.get("model",""), v.get("trim","")]
    return " ".join(p for p in parts if p).strip()[:200]


def build_description(v: dict) -> str:
    specs = []
    condition_label = "Certified Pre-Owned" if v.get("certified") else (v.get("condition") or "")
    if condition_label:
        specs.append(condition_label)

    year, make, model, trim = v.get("year",""), v.get("make",""), v.get("model",""), v.get("trim","")
    if year and make and model:
        specs.append(f"{year} {make} {model}" + (f" {trim}" if trim else ""))

    for label, field in [
        ("Body",         "body_style"),
        ("Exterior",     "exterior_color"),
        ("Interior",     "interior_color"),
        ("Engine",       "engine"),
        ("Transmission", "transmission"),
        ("Drivetrain",   "drivetrain"),
        ("Fuel",         "fuel_type"),
        ("Doors",        "doors"),
        ("Stock #",      "stock_number"),
        ("VIN",          "vin"),
    ]:
        if v.get(field):
            specs.append(f"{label}: {v[field]}")

    if v.get("mileage"):
        specs.append(f"Mileage: {v['mileage']} miles")
    if v.get("mpg_city") and v.get("mpg_highway"):
        specs.append(f"MPG: {v['mpg_city']} city / {v['mpg_highway']} hwy")

    return " | ".join(specs)[:9999]


def build_item_group_id(v: dict) -> str:
    year  = (v.get("year")  or "").strip()
    make  = (v.get("make")  or "").strip().replace(" ", "-")
    model = (v.get("model") or "").strip().replace(" ", "-")
    if year and make and model:
        return f"{year}-{make}-{model}"[:100]
    return ""


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

        certified = bool(v.get("certified"))

        transformed.append({
            "id":                           vid,
            "title":                        title,
            "description":                  build_description(v),
            "availability":                 normalize_availability(v.get("availability", "")),
            "condition":                    normalize_condition(v.get("condition", ""), certified),
            "price":                        price_str,
            "link":                         link,
            "image_link":                   image,
            "brand":                        (v.get("make") or dealer_name).strip()[:100],
            "google_product_category":      GOOGLE_PRODUCT_CATEGORY,
            "fb_product_category":          FB_PRODUCT_CATEGORY,
            "quantity_to_sell_on_facebook": "",
            "sale_price":                   format_price(v.get("sale_price")) if v.get("sale_price") else "",
            "sale_price_effective_date":    "",
            "item_group_id":                build_item_group_id(v),
            "gender":                       "",
            "color":                        (v.get("exterior_color") or "")[:200],
            "size":                         "",
            "age_group":                    "",
            "material":                     "",
            "pattern":                      "",
            "shipping":                     "",
            "shipping_weight":              "",
            "video[0].url":                 "",
            "video[0].tag[0]":              "",
            "gtin":                         "",
            "product_tags[0]":              (v.get("body_style") or "")[:110],
            "product_tags[1]":              (v.get("drivetrain") or v.get("fuel_type") or "")[:110],
            "style[0]":                     "",
        })

    print(f"  → {len(transformed)} valid vehicles after deduplication")
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

    for dealer_name, base_url, output_filename in DEALERSHIPS:
        output_path = os.path.join(OUTPUT_DIR, output_filename)
        try:
            raw      = scrape_inventory(base_url, dealer_name)
            vehicles = transform_vehicles(raw, dealer_name)
            build_csv_feed(vehicles, output_path)
            summary.append((dealer_name, len(vehicles), "OK"))
        except Exception as e:
            import traceback
            print(f"  ERROR: {dealer_name}: {e}")
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
