#!/usr/bin/env python3
"""
Builds a large ThomasFit food database from OpenFoodFacts dumps.

Why this script exists:
- The bundled baseline-foods.sqlite is currently small.
- This importer can build a much larger local SQLite database for offline use.
- It is optimized for multi-core Macs (ProcessPool parsing + batch inserts).

Notes:
- Uses open datasets and API-compatible sources only.
- Does not implement anti-bot or anti-detection bypass.

Example:
  python Scripts/build_massive_food_database.py \
    --download-openfoodfacts \
    --openfoodfacts-cache /tmp/openfoodfacts-products.jsonl.gz \
    --output ThomasFit/Resources/baseline-foods.sqlite \
    --workers 12 \
    --chunk-size 4000
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


OPENFOODFACTS_DUMP_URL = "https://static.openfoodfacts.org/data/openfoodfacts-products.jsonl.gz"
OPENFOODFACTS_SEARCH_BASE_URL = "https://world.openfoodfacts.org/cgi/search.pl"

APP_CATEGORIES = {
    "Milchprodukte",
    "Fleisch",
    "Fisch",
    "Gemüse",
    "Obst",
    "Getreide",
    "Hülsenfrüchte",
    "Nüsse & Samen",
    "Öle & Fette",
    "Süßigkeiten",
    "Getränke",
    "Backwaren",
    "Tiefkühl",
    "Gewürze & Soßen",
    "Supplements",
    "Eigene",
}


@dataclass(frozen=True)
class ParseConfig:
    require_calories: bool
    require_macros: bool
    keep_no_name: bool


# Keep category mapping intentionally broad for inconsistent source labels.
CATEGORY_KEYWORDS: list[tuple[str, str]] = [
    ("egg", "Milchprodukte"),
    ("eier", "Milchprodukte"),
    ("milk", "Milchprodukte"),
    ("milch", "Milchprodukte"),
    ("dairy", "Milchprodukte"),
    ("cheese", "Milchprodukte"),
    ("käse", "Milchprodukte"),
    ("yog", "Milchprodukte"),
    ("jogh", "Milchprodukte"),
    ("quark", "Milchprodukte"),
    ("beef", "Fleisch"),
    ("pork", "Fleisch"),
    ("lamb", "Fleisch"),
    ("ham", "Fleisch"),
    ("bacon", "Fleisch"),
    ("speck", "Fleisch"),
    ("wurst", "Fleisch"),
    ("fleisch", "Fleisch"),
    ("fish", "Fisch"),
    ("fisch", "Fisch"),
    ("seafood", "Fisch"),
    ("shrimp", "Fisch"),
    ("gemüse", "Gemüse"),
    ("vegetable", "Gemüse"),
    ("salad", "Gemüse"),
    ("spinat", "Gemüse"),
    ("broccoli", "Gemüse"),
    ("obst", "Obst"),
    ("fruit", "Obst"),
    ("apfel", "Obst"),
    ("banana", "Obst"),
    ("grain", "Getreide"),
    ("getreide", "Getreide"),
    ("pasta", "Getreide"),
    ("rice", "Getreide"),
    ("reis", "Getreide"),
    ("oat", "Getreide"),
    ("hafer", "Getreide"),
    ("bean", "Hülsenfrüchte"),
    ("lentil", "Hülsenfrüchte"),
    ("linse", "Hülsenfrüchte"),
    ("kicher", "Hülsenfrüchte"),
    ("nut", "Nüsse & Samen"),
    ("mandel", "Nüsse & Samen"),
    ("cashew", "Nüsse & Samen"),
    ("seed", "Nüsse & Samen"),
    ("oil", "Öle & Fette"),
    ("öl", "Öle & Fette"),
    ("butter", "Öle & Fette"),
    ("chocolate", "Süßigkeiten"),
    ("schoko", "Süßigkeiten"),
    ("cookie", "Süßigkeiten"),
    ("keks", "Süßigkeiten"),
    ("candy", "Süßigkeiten"),
    ("sweet", "Süßigkeiten"),
    ("getränk", "Getränke"),
    ("drink", "Getränke"),
    ("juice", "Getränke"),
    ("saft", "Getränke"),
    ("cola", "Getränke"),
    ("wasser", "Getränke"),
    ("water", "Getränke"),
    ("coffee", "Getränke"),
    ("kaffee", "Getränke"),
    ("tee", "Getränke"),
    ("bread", "Backwaren"),
    ("brot", "Backwaren"),
    ("bun", "Backwaren"),
    ("bröt", "Backwaren"),
    ("croissant", "Backwaren"),
    ("pizza", "Tiefkühl"),
    ("frozen", "Tiefkühl"),
    ("tiefkühl", "Tiefkühl"),
    ("spice", "Gewürze & Soßen"),
    ("gewürz", "Gewürze & Soßen"),
    ("sauce", "Gewürze & Soßen"),
    ("soße", "Gewürze & Soßen"),
    ("supplement", "Supplements"),
    ("protein powder", "Supplements"),
    ("creatine", "Supplements"),
]

LIQUID_KEYWORDS = {
    "drink", "juice", "saft", "cola", "water", "wasser", "milk", "milch",
    "coffee", "kaffee", "tea", "tee", "smoothie", "shake", "soup", "suppe",
}

NUMBER_RE = re.compile(r"(\d+(?:[.,]\d+)?)")

OPENFOODFACTS_API_CATEGORIES = [
    "eggs", "bacon", "meat", "fish", "dairy", "cheese", "yogurt", "quark",
    "vegetables", "fruits", "rice", "pasta", "bread", "cereal", "oats",
    "beans", "lentils", "nuts", "seeds", "oils", "sauces", "spices",
    "chocolate", "cookies", "snacks", "beverages", "juice", "water",
    "milch", "fleisch", "fisch", "gemuese", "obst", "getreide", "brot",
    "speck", "eier", "milchprodukte", "huelsenfruechte",
]

OPENFOODFACTS_FACET_CATEGORY_SLUGS = [
    "eggs",
    "bacon",
    "meat",
    "fish",
    "dairies",
    "cheeses",
    "yogurts",
    "fruits",
    "vegetables",
    "rice",
    "pasta",
    "breads",
    "breakfast-cereals",
    "legumes",
    "nuts",
    "oils",
    "beverages",
    "ready-meals",
]


def normalize_text(value: str) -> str:
    value = (value or "").strip().lower()
    value = re.sub(r"\s+", " ", value)
    return value


def parse_float(value) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        if math.isfinite(float(value)):
            return float(value)
        return 0.0
    try:
        cleaned = str(value).strip().replace(",", ".")
        if not cleaned:
            return 0.0
        parsed = float(cleaned)
        if not math.isfinite(parsed):
            return 0.0
        return parsed
    except Exception:
        return 0.0


def sanitize_macro(value: float) -> float:
    raw = max(0.0, parse_float(value))
    candidates = [raw, raw / 10.0, raw / 100.0, raw / 1000.0]

    def score(v: float, transformed: bool) -> float:
        penalty = 1.2 if transformed else 0.0
        if v > 100.0:
            penalty += (v - 100.0) * 50.0
        return penalty

    best = raw
    best_score = score(raw, False)
    for candidate in candidates[1:]:
        candidate_score = score(candidate, True)
        if candidate_score < best_score:
            best_score = candidate_score
            best = candidate
    return min(max(best, 0.0), 100.0)


def sanitize_sodium_mg(value: float) -> float:
    raw = max(0.0, parse_float(value))
    candidates = [raw, raw / 10.0, raw / 100.0, raw / 1000.0]

    def score(v: float, transformed: bool) -> float:
        penalty = 2.0 if transformed else 0.0
        if v > 100000.0:
            penalty += (v - 100000.0) * 100.0
        elif v > 10000.0:
            penalty += (v - 10000.0) * 0.5
        return penalty

    best = raw
    best_score = score(raw, False)
    for candidate in candidates[1:]:
        candidate_score = score(candidate, True)
        if candidate_score < best_score:
            best_score = candidate_score
            best = candidate
    return min(max(best, 0.0), 100000.0)


def sanitize_calories(raw_calories: float, protein: float, carbs: float, fat: float, fiber: float) -> float:
    macro_estimate = max(0.0, 4.0 * protein + 4.0 * carbs + 9.0 * fat + 2.0 * fiber)
    raw = max(0.0, parse_float(raw_calories))

    if raw == 0.0 and macro_estimate > 0.0:
        return macro_estimate
    if macro_estimate <= 0.0:
        return min(raw, 900.0)

    ratio = raw / max(macro_estimate, 1.0)
    suspicious = raw > 900.0 or raw > 100000.0 or ratio > 3.5 or ratio < 0.2
    if not suspicious:
        return min(raw, 900.0)

    candidates = [raw, raw / 4.184, raw / 10.0, raw / 100.0, raw / 1000.0]

    def score(v: float, transformed: bool) -> float:
        penalty = 25.0 if transformed else 0.0
        if v > 900.0:
            penalty += (v - 900.0) * 40.0
        if macro_estimate > 0.0:
            penalty += abs(v - macro_estimate)
            ratio = v / max(macro_estimate, 1.0)
            if ratio > 3.0 or ratio < 0.33:
                penalty += 250.0
        return penalty

    best = raw
    best_score = score(raw, False)
    for candidate in candidates[1:]:
        if candidate <= 0:
            continue
        candidate_score = score(candidate, True)
        if candidate_score < best_score:
            best_score = candidate_score
            best = candidate

    if best == 0.0 and macro_estimate > 0.0:
        best = macro_estimate

    return min(max(best, 0.0), 900.0)


def parse_portion(serving_size: str, is_liquid: bool) -> tuple[float, str]:
    raw = (serving_size or "").strip().lower()
    if not raw:
        return 100.0, "Portion"
    match = NUMBER_RE.search(raw)
    amount = parse_float(match.group(1)) if match else 100.0
    if amount <= 0:
        amount = 100.0
    if "ml" in raw:
        return amount, "ml"
    if "g" in raw:
        return amount, "g"
    if is_liquid:
        return amount, "ml"
    return amount, "g"


def choose_name(product: dict) -> str:
    candidates = [
        product.get("product_name_de"),
        product.get("product_name"),
        product.get("generic_name_de"),
        product.get("generic_name"),
        product.get("product_name_en"),
        product.get("generic_name_en"),
    ]
    for name in candidates:
        if name and str(name).strip():
            return str(name).strip()
    return ""


def choose_brand(product: dict) -> str:
    brand = str(product.get("brands", "") or "").strip()
    if "," in brand:
        brand = brand.split(",", 1)[0].strip()
    return brand


def map_category(name: str, categories_text: str) -> str:
    text = f"{name} {categories_text}".lower()
    words = set(re.findall(r"[a-z0-9äöüß]+", text))
    for keyword, target in CATEGORY_KEYWORDS:
        if len(keyword) <= 3:
            if keyword in words:
                return target
        elif keyword in text:
            return target
    return "Eigene"


def is_liquid(category: str, name: str) -> bool:
    if category == "Getränke":
        return True
    lower = name.lower()
    return any(keyword in lower for keyword in LIQUID_KEYWORDS)


def stable_food_id(name: str, brand: str) -> str:
    key = f"{normalize_text(name)}|{normalize_text(brand)}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def parse_openfoodfacts_product(product: dict, cfg: ParseConfig):
    name = choose_name(product)
    if not name and not cfg.keep_no_name:
        return None

    brand = choose_brand(product)
    nutriments = product.get("nutriments") or {}

    raw_kcal = parse_float(nutriments.get("energy-kcal_100g"))
    if raw_kcal <= 0:
        # Fallback conversion from kJ to kcal.
        kj = parse_float(nutriments.get("energy-kj_100g"))
        if kj > 0:
            raw_kcal = kj / 4.184

    protein = sanitize_macro(nutriments.get("proteins_100g"))
    carbs = sanitize_macro(nutriments.get("carbohydrates_100g"))
    fat = sanitize_macro(nutriments.get("fat_100g"))
    fiber = sanitize_macro(nutriments.get("fiber_100g"))
    sugar = min(sanitize_macro(nutriments.get("sugars_100g")), carbs if carbs > 0 else 100.0)
    sat_fat = min(sanitize_macro(nutriments.get("saturated-fat_100g")), fat if fat > 0 else 100.0)
    sodium = sanitize_sodium_mg(parse_float(nutriments.get("sodium_100g")) * 1000.0)  # g -> mg
    kcal = sanitize_calories(raw_kcal, protein, carbs, fat, fiber)

    if cfg.require_calories and kcal <= 0:
        return None

    if cfg.require_macros and (protein + carbs + fat <= 0):
        return None

    categories_tags = product.get("categories_tags") or []
    categories = product.get("categories") or ""
    cat_text = " ".join(categories_tags) + " " + str(categories)
    category = map_category(name, cat_text)
    liquid = is_liquid(category, name)

    default_portion, portion_unit = parse_portion(str(product.get("serving_size", "")), liquid)
    row = (
        stable_food_id(name, brand),
        name.strip(),
        brand.strip(),
        category,
        str(product.get("code", "") or "").strip(),
        round(kcal, 3),
        round(protein, 3),
        round(carbs, 3),
        round(fat, 3),
        round(fiber, 3),
        round(sugar, 3),
        round(sat_fat, 3),
        round(sodium, 3),
        round(default_portion, 3),
        portion_unit,
    )
    return row


def parse_json_lines_chunk(lines: list[str], cfg: ParseConfig) -> list[tuple]:
    rows: list[tuple] = []
    for line in lines:
        if not line:
            continue
        try:
            product = json.loads(line)
        except Exception:
            continue
        parsed = parse_openfoodfacts_product(product, cfg)
        if parsed:
            rows.append(parsed)
    return rows


def iter_lines(path: Path) -> Iterable[str]:
    if str(path).endswith(".gz"):
        with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                yield line
    else:
        with path.open("r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                yield line


def yield_chunks(lines: Iterable[str], chunk_size: int) -> Iterable[list[str]]:
    chunk: list[str] = []
    for line in lines:
        chunk.append(line)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def ensure_parent(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)


def download_file(url: str, destination: Path):
    ensure_parent(destination)
    with urllib.request.urlopen(url) as response, destination.open("wb") as out:
        total = int(response.headers.get("Content-Length", "0") or 0)
        copied = 0
        started = time.time()
        while True:
            chunk = response.read(1024 * 1024 * 8)
            if not chunk:
                break
            out.write(chunk)
            copied += len(chunk)
            if total > 0:
                pct = (copied / total) * 100
                elapsed = max(1.0, time.time() - started)
                speed = copied / elapsed / (1024 * 1024)
                print(f"\rDownload: {pct:6.2f}%  {speed:6.1f} MB/s", end="", flush=True)
    print()


def looks_like_valid_dump(path: Path, min_size_bytes: int = 1024 * 1024) -> bool:
    if not path.exists():
        return False
    if path.stat().st_size < min_size_bytes:
        return False
    try:
        with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as handle:
            first_line = handle.readline().strip()
            return first_line.startswith("{")
    except Exception:
        return False


def connect_db(path: Path) -> sqlite3.Connection:
    ensure_parent(path)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode = OFF;")
    conn.execute("PRAGMA synchronous = OFF;")
    conn.execute("PRAGMA temp_store = MEMORY;")
    conn.execute("PRAGMA cache_size = -200000;")
    conn.execute("PRAGMA mmap_size = 268435456;")
    return conn


def create_schema(conn: sqlite3.Connection):
    conn.executescript(
        """
        DROP TABLE IF EXISTS foods;
        DROP TABLE IF EXISTS foods_fts;

        CREATE TABLE foods (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            brand TEXT NOT NULL DEFAULT '',
            category TEXT NOT NULL,
            barcode TEXT DEFAULT '',
            calories_per_100g REAL NOT NULL,
            protein_per_100g REAL NOT NULL,
            carbs_per_100g REAL NOT NULL,
            fat_per_100g REAL NOT NULL,
            fiber_per_100g REAL DEFAULT 0,
            sugar_per_100g REAL DEFAULT 0,
            saturated_fat_per_100g REAL DEFAULT 0,
            sodium_per_100g REAL DEFAULT 0,
            default_portion_grams REAL DEFAULT 100,
            portion_name TEXT DEFAULT 'Portion'
        );
        """
    )
    conn.commit()


def upsert_rows(conn: sqlite3.Connection, rows: list[tuple]):
    if not rows:
        return
    conn.executemany(
        """
        INSERT INTO foods (
            id, name, brand, category, barcode,
            calories_per_100g, protein_per_100g, carbs_per_100g, fat_per_100g,
            fiber_per_100g, sugar_per_100g, saturated_fat_per_100g, sodium_per_100g,
            default_portion_grams, portion_name
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name = excluded.name,
            brand = excluded.brand,
            category = excluded.category,
            barcode = CASE WHEN excluded.barcode != '' THEN excluded.barcode ELSE foods.barcode END,
            calories_per_100g = excluded.calories_per_100g,
            protein_per_100g = excluded.protein_per_100g,
            carbs_per_100g = excluded.carbs_per_100g,
            fat_per_100g = excluded.fat_per_100g,
            fiber_per_100g = excluded.fiber_per_100g,
            sugar_per_100g = excluded.sugar_per_100g,
            saturated_fat_per_100g = excluded.saturated_fat_per_100g,
            sodium_per_100g = excluded.sodium_per_100g,
            default_portion_grams = excluded.default_portion_grams,
            portion_name = excluded.portion_name
        """,
        rows,
    )


def add_indexes_and_fts(conn: sqlite3.Connection):
    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_foods_name_nocase ON foods(name COLLATE NOCASE);
        CREATE INDEX IF NOT EXISTS idx_foods_brand_nocase ON foods(brand COLLATE NOCASE);
        CREATE INDEX IF NOT EXISTS idx_foods_category ON foods(category);
        CREATE INDEX IF NOT EXISTS idx_foods_barcode ON foods(barcode);
        CREATE INDEX IF NOT EXISTS idx_foods_calories ON foods(calories_per_100g);

        CREATE VIRTUAL TABLE IF NOT EXISTS foods_fts
        USING fts5(name, brand, category, content='foods', content_rowid='rowid', tokenize='unicode61 remove_diacritics 2');

        INSERT INTO foods_fts(foods_fts) VALUES('rebuild');
        ANALYZE;
        """
    )
    conn.commit()


ESSENTIAL_FOODS = [
    ("hartgekochtes Ei", "Basis", "Milchprodukte", "", 155, 13, 1.1, 11, 0, 1.1, 3.3, 124, 60, "g"),
    ("Rührei", "Basis", "Milchprodukte", "", 149, 10, 1.6, 11, 0, 1.5, 3.3, 150, 120, "g"),
    ("Spiegelei", "Basis", "Milchprodukte", "", 196, 13.6, 0.8, 15, 0, 0.8, 4.5, 190, 60, "g"),
    ("Eiweiß (Eiklar)", "Basis", "Milchprodukte", "", 52, 11, 0.7, 0.2, 0, 0.7, 0.0, 166, 100, "g"),
    ("Eigelb", "Basis", "Milchprodukte", "", 322, 15.9, 3.6, 26.5, 0, 0.6, 9.6, 48, 18, "g"),
    ("Bacon", "Basis", "Fleisch", "", 541, 37, 1.4, 42, 0, 1.2, 14, 1700, 30, "g"),
    ("Frühstücksspeck", "Basis", "Fleisch", "", 533, 35, 1.5, 42, 0, 1.2, 14, 1600, 30, "g"),
    ("Gebratener Speck", "Basis", "Fleisch", "", 533, 37, 1.4, 41, 0, 1.1, 13.5, 1700, 30, "g"),
    ("Hähnchenbrust, gebraten", "Basis", "Fleisch", "", 165, 31, 0, 3.6, 0, 0, 1.0, 74, 150, "g"),
    ("Lachs, gebraten", "Basis", "Fisch", "", 206, 22, 0, 12, 0, 0, 2.6, 59, 150, "g"),
]


def insert_essential_foods(conn: sqlite3.Connection):
    rows = []
    for entry in ESSENTIAL_FOODS:
        (
            name,
            brand,
            category,
            barcode,
            kcal,
            protein,
            carbs,
            fat,
            fiber,
            sugar,
            sat_fat,
            sodium,
            portion_grams,
            portion_name,
        ) = entry
        rows.append(
            (
                stable_food_id(name, brand),
                name,
                brand,
                category,
                barcode,
                kcal,
                protein,
                carbs,
                fat,
                fiber,
                sugar,
                sat_fat,
                sodium,
                portion_grams,
                portion_name,
            )
        )
    upsert_rows(conn, rows)
    conn.commit()


def build_from_openfoodfacts(
    conn: sqlite3.Connection,
    dump_path: Path,
    workers: int,
    chunk_size: int,
    max_items: int | None,
    parse_cfg: ParseConfig,
):
    if workers <= 1:
        build_from_openfoodfacts_single_process(conn, dump_path, chunk_size, max_items, parse_cfg)
        return

    inserted_batches = 0
    started = time.time()
    futures = set()

    def flush_completed(force: bool = False):
        nonlocal inserted_batches
        if not futures:
            return
        if force:
            done, pending = wait(futures)
        else:
            done, pending = wait(futures, return_when=FIRST_COMPLETED)
        futures.clear()
        futures.update(pending)
        for fut in done:
            rows = fut.result()
            if not rows:
                continue
            upsert_rows(conn, rows)
            inserted_batches += 1

    try:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            for chunk in yield_chunks(iter_lines(dump_path), chunk_size):
                futures.add(pool.submit(parse_json_lines_chunk, chunk, parse_cfg))
                if len(futures) >= workers * 2:
                    flush_completed(force=False)

                    if inserted_batches % 10 == 0:
                        conn.commit()
                        total = conn.execute("SELECT COUNT(*) FROM foods").fetchone()[0]
                        elapsed = max(1.0, time.time() - started)
                        speed = total / elapsed
                        print(f"Rows in DB: {total:,} ({speed:,.0f} rows/s)")
                        if max_items and total >= max_items:
                            print(f"Reached --max-items={max_items:,}, stopping import.")
                            break

            flush_completed(force=True)
    except PermissionError as err:
        print(f"Process pool unavailable ({err}), falling back to single-process import.")
        build_from_openfoodfacts_single_process(conn, dump_path, chunk_size, max_items, parse_cfg)
        return

    conn.commit()


def build_from_openfoodfacts_single_process(
    conn: sqlite3.Connection,
    dump_path: Path,
    chunk_size: int,
    max_items: int | None,
    parse_cfg: ParseConfig,
):
    started = time.time()
    processed_chunks = 0

    for chunk in yield_chunks(iter_lines(dump_path), chunk_size):
        rows = parse_json_lines_chunk(chunk, parse_cfg)
        if rows:
            upsert_rows(conn, rows)

        processed_chunks += 1
        if processed_chunks % 10 == 0:
            conn.commit()
            total = conn.execute("SELECT COUNT(*) FROM foods").fetchone()[0]
            elapsed = max(1.0, time.time() - started)
            speed = total / elapsed
            print(f"Rows in DB: {total:,} ({speed:,.0f} rows/s)")
            if max_items and total >= max_items:
                print(f"Reached --max-items={max_items:,}, stopping import.")
                break

    conn.commit()


def fetch_json(url: str, timeout_s: int = 8) -> dict | None:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "ThomasFit/1.0 (local data importer)",
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_s) as response:
            payload = response.read()
        if not payload:
            return None
        return json.loads(payload.decode("utf-8", errors="ignore"))
    except Exception:
        return None


def fetch_json_via_curl(url: str, timeout_s: int = 60) -> dict | None:
    cmd = [
        "curl",
        "-L",
        "--max-time",
        str(timeout_s),
        "-sS",
        url,
    ]
    try:
        result = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_s + 5,
        )
    except Exception:
        return None

    if result.returncode not in (0, 23):
        return None

    payload = result.stdout.strip()
    if not payload.startswith("{"):
        return None

    try:
        return json.loads(payload)
    except Exception:
        return None


def iter_openfoodfacts_api_products(
    max_pages_per_category: int,
    page_size: int = 100,
) -> Iterable[dict]:
    fields = ",".join([
        "product_name",
        "product_name_de",
        "product_name_en",
        "generic_name",
        "generic_name_de",
        "generic_name_en",
        "brands",
        "categories",
        "categories_tags",
        "nutriments",
        "serving_size",
        "code",
    ])

    consecutive_failures = 0
    categories_with_data = 0

    for category in OPENFOODFACTS_API_CATEGORIES:
        category_had_data = False
        for page in range(1, max_pages_per_category + 1):
            params = {
                "search_terms": category,
                "search_simple": 1,
                "action": "process",
                "json": 1,
                "page_size": page_size,
                "page": page,
                "fields": fields,
            }
            url = f"{OPENFOODFACTS_SEARCH_BASE_URL}?{urllib.parse.urlencode(params)}"
            data = fetch_json(url)
            if not data:
                consecutive_failures += 1
                if categories_with_data == 0 and consecutive_failures >= 8:
                    print("API fallback aborted early: repeated network failures.")
                    return
                break

            products = data.get("products") or []
            if not products:
                break

            consecutive_failures = 0
            category_had_data = True

            for product in products:
                yield product

            if len(products) < page_size:
                break

            time.sleep(0.15)

        if category_had_data:
            categories_with_data += 1


def build_from_openfoodfacts_api(
    conn: sqlite3.Connection,
    parse_cfg: ParseConfig,
    max_items: int | None,
    max_pages_per_category: int,
):
    started = time.time()
    batch: list[tuple] = []
    seen_codes: set[str] = set()
    processed = 0

    for product in iter_openfoodfacts_api_products(max_pages_per_category=max_pages_per_category):
        code = str(product.get("code", "") or "").strip()
        if code and code in seen_codes:
            continue
        if code:
            seen_codes.add(code)

        row = parse_openfoodfacts_product(product, parse_cfg)
        if not row:
            continue
        batch.append(row)
        processed += 1

        if len(batch) >= 3000:
            upsert_rows(conn, batch)
            batch.clear()
            conn.commit()

            total = conn.execute("SELECT COUNT(*) FROM foods").fetchone()[0]
            elapsed = max(1.0, time.time() - started)
            speed = total / elapsed
            print(f"Rows in DB (API): {total:,} ({speed:,.0f} rows/s)")

            if max_items and total >= max_items:
                print(f"Reached --max-items={max_items:,}, stopping API import.")
                break

    if batch:
        upsert_rows(conn, batch)
        conn.commit()

    return processed


def iter_openfoodfacts_facet_products(max_pages_per_slug: int, page_size: int = 100) -> Iterable[dict]:
    global_failures = 0
    successful_pages = 0

    for slug in OPENFOODFACTS_FACET_CATEGORY_SLUGS:
        page = 1
        seen_empty = 0
        while page <= max_pages_per_slug:
            params = urllib.parse.urlencode({"page": page, "page_size": page_size})
            url = f"https://world.openfoodfacts.org/facets/categories/{slug}.json?{params}"
            data = fetch_json_via_curl(url, timeout_s=60)
            if not data:
                global_failures += 1
                if successful_pages == 0 and global_failures >= 5:
                    print("Facet import aborted early: repeated network failures.")
                    return
                seen_empty += 1
                if seen_empty >= 2:
                    break
                page += 1
                continue

            products = data.get("products") or []
            if not products:
                break
            successful_pages += 1
            global_failures = 0

            for product in products:
                yield product

            page_count = int(data.get("page_count", 0) or 0)
            if page_count > 0 and page >= page_count:
                break
            page += 1
            time.sleep(0.1)


def build_from_openfoodfacts_facets(
    conn: sqlite3.Connection,
    parse_cfg: ParseConfig,
    max_items: int | None,
    max_pages_per_slug: int,
):
    started = time.time()
    batch: list[tuple] = []
    seen_codes: set[str] = set()
    processed = 0

    for product in iter_openfoodfacts_facet_products(max_pages_per_slug=max_pages_per_slug):
        code = str(product.get("code", "") or "").strip()
        if code and code in seen_codes:
            continue
        if code:
            seen_codes.add(code)

        row = parse_openfoodfacts_product(product, parse_cfg)
        if not row:
            continue
        batch.append(row)
        processed += 1

        if len(batch) >= 1000:
            upsert_rows(conn, batch)
            batch.clear()
            conn.commit()

            total = conn.execute("SELECT COUNT(*) FROM foods").fetchone()[0]
            elapsed = max(1.0, time.time() - started)
            speed = total / elapsed
            print(f"Rows in DB (Facets): {total:,} ({speed:,.0f} rows/s)", flush=True)

            if max_items and total >= max_items:
                print(f"Reached --max-items={max_items:,}, stopping Facet import.")
                break

    if batch:
        upsert_rows(conn, batch)
        conn.commit()

    return processed


def verify_queries(conn: sqlite3.Connection, queries: list[str]):
    print("\nVerification")
    print("-" * 40)
    for q in queries:
        count = conn.execute(
            "SELECT COUNT(*) FROM foods WHERE lower(name) LIKE ?",
            (f"%{q.lower()}%",),
        ).fetchone()[0]
        print(f"{q:<20} {count:>8,} Treffer")


def print_stats(conn: sqlite3.Connection, db_path: Path):
    total = conn.execute("SELECT COUNT(*) FROM foods").fetchone()[0]
    size_mb = db_path.stat().st_size / (1024 * 1024)
    print("\nDone")
    print("-" * 40)
    print(f"DB path:    {db_path}")
    print(f"Foods:      {total:,}")
    print(f"Size:       {size_mb:,.1f} MB")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a massive ThomasFit food database")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("ThomasFit/Resources/baseline-foods.sqlite"),
        help="Output SQLite path",
    )
    parser.add_argument(
        "--download-openfoodfacts",
        action="store_true",
        help="Download OpenFoodFacts dump before import",
    )
    parser.add_argument(
        "--openfoodfacts-cache",
        type=Path,
        default=Path(tempfile.gettempdir()) / "openfoodfacts-products.jsonl.gz",
        help="Path to OpenFoodFacts jsonl(.gz) file",
    )
    parser.add_argument("--workers", type=int, default=max(2, (os.cpu_count() or 8) - 2))
    parser.add_argument("--chunk-size", type=int, default=3000)
    parser.add_argument("--max-items", type=int, default=None)
    parser.add_argument("--api-fallback-max-pages", type=int, default=1)
    parser.add_argument("--keep-no-name", action="store_true")
    parser.add_argument("--allow-zero-calories", action="store_true")
    parser.add_argument("--allow-zero-macros", action="store_true")
    parser.add_argument(
        "--verify",
        nargs="*",
        default=["ei", "egg", "speck", "bacon", "hähnchen", "reis"],
        help="Name fragments to verify after import",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    output_path: Path = args.output.resolve()
    dump_path: Path = args.openfoodfacts_cache.resolve()

    if args.download_openfoodfacts:
        print(f"Downloading OpenFoodFacts dump to {dump_path} ...")
        download_file(OPENFOODFACTS_DUMP_URL, dump_path)
    elif not dump_path.exists():
        print(
            f"OpenFoodFacts dump not found: {dump_path}\n"
            "Use --download-openfoodfacts or provide --openfoodfacts-cache <path>."
        )
        print("No dump available, will use API fallback import.")

    ensure_parent(output_path)
    if output_path.exists():
        backup_path = output_path.with_suffix(output_path.suffix + ".pre_massive_backup")
        print(f"Backing up existing DB to {backup_path}")
        shutil.copy2(output_path, backup_path)

    conn = connect_db(output_path)
    try:
        create_schema(conn)
        insert_essential_foods(conn)

        parse_cfg = ParseConfig(
            require_calories=not args.allow_zero_calories,
            require_macros=not args.allow_zero_macros,
            keep_no_name=args.keep_no_name,
        )

        if looks_like_valid_dump(dump_path):
            print("Importing OpenFoodFacts dump ...")
            build_from_openfoodfacts(
                conn=conn,
                dump_path=dump_path,
                workers=max(1, args.workers),
                chunk_size=max(100, args.chunk_size),
                max_items=args.max_items,
                parse_cfg=parse_cfg,
            )
        else:
            print("Dump is missing/too small/invalid, switching to OpenFoodFacts Facet import ...")
            facet_processed = build_from_openfoodfacts_facets(
                conn=conn,
                parse_cfg=parse_cfg,
                max_items=args.max_items,
                max_pages_per_slug=max(1, args.api_fallback_max_pages),
            )
            if facet_processed == 0:
                print("Facet import returned 0 products, trying Search-API fallback ...")
                api_processed = build_from_openfoodfacts_api(
                    conn=conn,
                    parse_cfg=parse_cfg,
                    max_items=args.max_items,
                    max_pages_per_category=max(1, args.api_fallback_max_pages),
                )
                if api_processed == 0:
                    print("API fallback imported 0 products, keeping essential base foods only.")

        print("Creating indexes + FTS ...")
        add_indexes_and_fts(conn)
        verify_queries(conn, args.verify)
        print_stats(conn, output_path)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
