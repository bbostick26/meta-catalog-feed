"""
Meta Commerce Catalog Feed Generator — Parks Automotive Group
Uses Firecrawl map() to discover all VDP URLs, then batch_scrape() to extract data.
Runs daily via GitHub Actions — no local machine required.
"""

import csv
import os
from datetime import datetime, timezone
from firecrawl import FirecrawlApp
from firecrawl.v2.types import JsonFormat

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

# Inventory section paths shared across all dealership sites (same platform)
INVENTORY_SECTIONS = [
    "/new-vehicles/",
    "/used-vehicles/",
    "/certified-pre-owned/",
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
# ─────────────────────────────────────────────────────────────────────────────


# ─── EXTRACTION SCHEMA ────────────────────────────────────────────────────────
VEHICLE_SCHEMA = {
    "type": "object",
    "properties": {
        "stock_number":    {"type": "string"},
        "vin":             {"type": "string"},
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
        "doors":           {"type": "string"},
        "mpg_city":        {"type": "string"},
        "mpg_highway":     {"type": "string"},
        "availability":    {"type": "string"},
        "certified":       {"type": "boolean"},
        "description":     {"type": "string"},
        "image_url":       {"type": "string"},
        "video_url":       {"type": "string"},
        "detail_page_url": {"type": "string"},
    },
    "required": ["year", "make", "model"]
}

EXTRACT_PROMPT = """
Extract details for the single vehicle shown on this Vehicle Detail Page (VDP).
Capture every available field:
- stock_number: dealer stock/unit number
- vin: full 17-character VIN
- year: model year (e.g. "2024")
- make: manufacturer (e.g. "Chevrolet")
- model: model name (e.g. "Silverado 1500")
- trim: trim level (e.g. "LTZ", "Z71", "Premier")
- body_style: (e.g. "Truck", "SUV", "Sedan", "Coupe", "Van", "Wagon")
- condition: exactly "New", "Used", or "Certified Pre-Owned"
- price: asking/internet price as a number (no symbols or commas)
- sale_price: discounted price if shown separately, as a number
- mileage: odometer as a string (e.g. "34,215")
- exterior_color: full exterior color name
- interior_color: interior/seat color name
- transmission: (e.g. "10-Speed Automatic")
- drivetrain: (e.g. "4WD", "AWD", "FWD", "RWD")
- fuel_type: (e.g. "Gasoline", "Electric", "Hybrid", "Diesel")
- engine: (e.g. "6.2L V8")
- doors: number of doors
- mpg_city: city MPG
- mpg_highway: highway MPG
- availability: "In Stock", "On Order", or "In Transit"
- certified: true if Certified Pre-Owned, false otherwise
- description: marketing description or feature highlights
- image_url: full URL of the primary vehicle photo
- video_url: full URL of any vehicle video
- detail_page_url: the current page URL
"""


# ─── SCRAPE ───────────────────────────────────────────────────────────────────
def is_vdp_url(url: str, base_url: str) -> bool:
    """
    Filter map() results to only individual vehicle detail pages.
    VDPs on this platform have a numeric segment or end with a VIN-like string.
    They always live under an inventory section path.
    """
    relative = url.replace(base_url, "")
    # Must be under an inventory section
    if not any(relative.startswith(p.rstrip("/")) for p in INVENTORY_SECTIONS):
        return False
    # SRP root pages have short paths — VDPs have at least 3 path segments
    segments = [s for s in relative.split("/") if s]
    return len(segments) >= 3


def discover_vdp_urls(app: FirecrawlApp, base_url: str) -> list[str]:
    """Use map() to discover all VDP URLs across inventory sections."""
    all_urls = []
    for section in INVENTORY_SECTIONS:
        section_url = base_url.rstrip("/") + section
        try:
            result = app.map(section_url, limit=500)
            raw_links = result.links if hasattr(result, "links") else []
            urls = [lr.url for lr in raw_links if hasattr(lr, "url")]
            vdps = [u for u in urls if is_vdp_url(u, base_url)]
            print(f"    {section:<30} {len(vdps)} VDPs found")
            all_urls.extend(vdps)
        except Exception as e:
            print(f"    {section:<30} SKIPPED ({e})")
    # Deduplicate
    unique = list(dict.fromkeys(all_urls))
    print(f"  → {len(unique)} total unique VDP URLs")
    return unique


def batch_scrape_vdps(app: FirecrawlApp, urls: list[str]) -> list[dict]:
    """Batch scrape all VDP URLs and extract vehicle data from each."""
    if not urls:
        return []

    result = app.batch_scrape(
        urls,
        formats=[JsonFormat(type="json", prompt=EXTRACT_PROMPT, schema=VEHICLE_SCHEMA)],
        wait_for=2000,
        only_main_content=False,
    )

    vehicles = []
    data = result.data if hasattr(result, "data") else []
    for page in data:
        extracted = page.json if hasattr(page, "json") and page.json else {}
        if isinstance(extracted, dict) and extracted.get("year"):
            # Inject the source URL as detail_page_url if not captured
            if not extracted.get("detail_page_url"):
                meta = page.metadata if hasattr(page, "metadata") else {}
                extracted["detail_page_url"] = (
                    meta.get("sourceURL") or meta.get("url") or ""
                )
            vehicles.append(extracted)

    print(f"  → {len(vehicles)} vehicles extracted from {len(data)} pages")
    return vehicles


def scrape_inventory(base_url: str, dealer_name: str) -> list[dict]:
    app = FirecrawlApp(api_key=FIRECRAWL_API_KEY)
    print(f"\n[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {dealer_name}")

    vdp_urls = discover_vdp_urls(app, base_url)
    if not vdp_urls:
        print("  → No VDP URLs found, skipping")
        return []

    return batch_scrape_vdps(app, vdp_urls)


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
    base = v.get("description", "").strip()
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

    spec_block = " | ".join(specs)
    combined = f"{base} | {spec_block}" if base and spec_block else (base or spec_block)
    return combined[:9999]


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
            "availability":                 normalize_availability(v.get("availability")),
            "condition":                    normalize_condition(v.get("condition"), certified),
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
            "video[0].url":                 (v.get("video_url") or "").strip(),
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
