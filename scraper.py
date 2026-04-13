"""
Meta Commerce Catalog Feed Generator — Parks Automotive Group
map() discovers all VDP URLs per dealer, then extract() in parallel batches of 10.
Runs daily via GitHub Actions — no local machine required.
"""

import csv
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from firecrawl import FirecrawlApp

# ─── DEALERSHIPS ──────────────────────────────────────────────────────────────
DEALERSHIPS = [
    (,
  
]

# ─── CONFIG ───────────────────────────────────────────────────────────────────
FIRECRAWL_API_KEY  = os.getenv("FIRECRAWL_API_KEY", "YOUR_API_KEY_HERE")
CURRENCY           = "USD"
OUTPUT_DIR         = "feeds"
EXTRACT_BATCH_SIZE = 10   # Firecrawl extract() beta limit
EXTRACT_WORKERS    = 4    # parallel extract() calls per dealer

GOOGLE_PRODUCT_CATEGORY = "Vehicles & Parts > Vehicles > Motor Vehicles > Cars, Trucks & Vans"
FB_PRODUCT_CATEGORY     = "Vehicles & Parts > Vehicles > Cars, Trucks & Vans"

CSV_COLUMNS = [
    "id", "title", "description", "availability", "condition", "price",
    "link", "image_link", "brand", "google_product_category", "fb_product_category",
    "quantity_to_sell_on_facebook", "sale_price", "sale_price_effective_date",
    "item_group_id", "gender", "color", "size", "age_group", "material", "pattern",
    "shipping", "shipping_weight",
    "additional_image_link[0]", "additional_image_link[1]", "additional_image_link[2]",
    "video[0].url", "video[0].tag[0]",
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
                    "vin":               {"type": "string"},
                    "stock_number":      {"type": "string"},
                    "year":              {"type": "string"},
                    "make":              {"type": "string"},
                    "model":             {"type": "string"},
                    "trim":              {"type": "string"},
                    "body_style":        {"type": "string"},
                    "condition":         {"type": "string"},
                    "price":             {"type": "number"},
                    "sale_price":        {"type": "number"},
                    "mileage":           {"type": "string"},
                    "exterior_color":    {"type": "string"},
                    "interior_color":    {"type": "string"},
                    "transmission":      {"type": "string"},
                    "drivetrain":        {"type": "string"},
                    "fuel_type":         {"type": "string"},
                    "engine":            {"type": "string"},
                    "mpg_city":          {"type": "string"},
                    "mpg_highway":       {"type": "string"},
                    "certified":         {"type": "boolean"},
                    "image_url":         {"type": "string"},
                    "additional_images": {"type": "array", "items": {"type": "string"}},
                    "detail_page_url":   {"type": "string"},
                },
                "required": ["year", "make", "model"]
            }
        }
    },
    "required": ["vehicles"]
}

EXTRACT_PROMPT = (
    "For each URL provided, visit that vehicle detail page and extract: "
    "VIN, stock number, year, make, model, full trim level, body style, "
    "condition (exactly: New / Used / Certified Pre-Owned), asking price, "
    "sale/internet price if different from asking price, mileage, exterior color, "
    "interior color, transmission, drivetrain, fuel type, engine description, "
    "MPG city, MPG highway, certified pre-owned flag (true/false), "
    "primary image URL, all additional vehicle photo URLs as additional_images array "
    "(include every gallery photo URL), and the detail page URL. "
    "Return all vehicles as an array."
)


# ─── SCRAPE ───────────────────────────────────────────────────────────────────
def discover_vdp_urls(app: FirecrawlApp, base_url: str) -> list[str]:
    """Single map() call to get all VDP URLs from the dealer's sitemap."""
    result = app.map(base_url, limit=500)
    seen, unique = set(), []
    for lr in result.links:
        u = lr.url if hasattr(lr, "url") else str(lr)
        if not VDP_RE.search(u):
            continue
        key = u.rstrip("/")
        if key not in seen:
            seen.add(key)
            unique.append(u)
    return unique


def extract_batch(api_key: str, urls: list[str], batch_num: int) -> list[dict]:
    """Extract one batch of up to 10 VDP URLs. Runs in a thread."""
    app = FirecrawlApp(api_key=api_key)
    for attempt in range(3):
        try:
            res = app.extract(urls, prompt=EXTRACT_PROMPT, schema=VEHICLE_SCHEMA)
            data = res.data if hasattr(res, "data") else res
            vehicles = data.get("vehicles", []) if isinstance(data, dict) else []
            return vehicles
        except Exception as e:
            if attempt < 2:
                time.sleep(5 * (attempt + 1))
            else:
                print(f"    Batch {batch_num} failed after 3 attempts: {e}")
                return []


def scrape_inventory(base_url: str, dealer_name: str) -> list[dict]:
    app = FirecrawlApp(api_key=FIRECRAWL_API_KEY)
    print(f"\n[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {dealer_name}")

    vdp_urls = discover_vdp_urls(app, base_url)
    if not vdp_urls:
        print("  No VDPs found, skipping")
        return []

    # Split into batches of 10
    batches = [vdp_urls[i:i + EXTRACT_BATCH_SIZE] for i in range(0, len(vdp_urls), EXTRACT_BATCH_SIZE)]
    print(f"  {len(vdp_urls)} VDPs found — {len(batches)} batches of {EXTRACT_BATCH_SIZE}, {EXTRACT_WORKERS} parallel")

    all_vehicles = []
    with ThreadPoolExecutor(max_workers=EXTRACT_WORKERS) as executor:
        futures = {
            executor.submit(extract_batch, FIRECRAWL_API_KEY, batch, i): i
            for i, batch in enumerate(batches)
        }
        completed = 0
        for future in as_completed(futures):
            vehicles = future.result()
            all_vehicles.extend(vehicles)
            completed += 1
            if completed % 5 == 0 or completed == len(batches):
                print(f"    {completed}/{len(batches)} batches done — {len(all_vehicles)} vehicles so far")

    print(f"  {len(all_vehicles)} total vehicles extracted")
    return all_vehicles


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
        extra_imgs = [u for u in (v.get("additional_images") or []) if u and u != image]

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
            "additional_image_link[0]":     extra_imgs[0] if len(extra_imgs) > 0 else "",
            "additional_image_link[1]":     extra_imgs[1] if len(extra_imgs) > 1 else "",
            "additional_image_link[2]":     extra_imgs[2] if len(extra_imgs) > 2 else "",
            "video[0].url":                 "",
            "video[0].tag[0]":              "",
            "gtin":                         "",
            "product_tags[0]":              (v.get("body_style") or "")[:110],
            "product_tags[1]":              (v.get("drivetrain") or v.get("fuel_type") or "")[:110],
            "style[0]":                     "",
        })

    print(f"  {len(transformed)} valid vehicles after deduplication")
    return transformed


# ─── BUILD CSV ────────────────────────────────────────────────────────────────
def build_csv_feed(vehicles: list[dict], output_path: str):
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(vehicles)
    print(f"  Feed written: {output_path} ({len(vehicles)} vehicles)")


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
