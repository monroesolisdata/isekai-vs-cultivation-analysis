"""
MAL ANIME SCRAPER - Official API v2
====================================
Collects all anime tagged with Isekai (62), Reincarnation (72),
and Martial Arts (17) from MyAnimeList using the official API v2.

STRATEGY:
  The MAL API v2 /anime endpoint only supports text search (q=) and does
  not allow filtering by genre ID. The correct approach is:

  Phase 1 — Pull anime from the MAL genre browse pages via the website
             URL pattern: myanimelist.net/anime/genre/{id}
             These pages are public HTML and paginate reliably.
             We extract MAL IDs from the href links on each page.

  Phase 2 — For each collected MAL ID, call the official API v2
             /anime/{id} endpoint to get the full structured data
             including producers, licensors, favorites, synonyms etc.

  This gives us both the genre-filtered coverage AND the clean API data.

FIELDS COLLECTED:
  mal_id, title_english, title_japanese, synonyms, media_type, status,
  num_episodes, start_date, mean, rank, popularity, num_scoring_users,
  members, favorites, source, rating, producers, licensors, studios,
  genres, themes, synopsis

RESUME SYSTEM:
  mal_scraped_ids.txt logs each completed MAL ID.
  Re-running skips already completed entries.

RATE LIMITING:
  1.1s delay between API calls. Genre page scraping uses 2s delay.

REQUIREMENTS:
  pip install beautifulsoup4
  Python 3.6+

HOW TO RUN:
  python mal_scraper.py
"""

import urllib.request
import urllib.parse
import urllib.error
import json
import csv
import time
import os
import sys
import re

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: beautifulsoup4 not installed. Run: pip install beautifulsoup4")
    sys.exit(1)

# ── SETTINGS ──────────────────────────────────────────────────────────────────

OUTPUT_CSV       = "isekai_dataset.csv"
CHECKPOINT_FILE  = "mal_scraped_ids.txt"
ID_CACHE_FILE    = "mal_collected_ids.json"
API_DELAY        = 1.1
PAGE_DELAY       = 2.0

THEMES = {
    62: "Isekai",
    72: "Reincarnation",
    17: "Martial Arts",
}

DETAIL_FIELDS = ",".join([
    "title", "alternative_titles", "media_type", "status",
    "num_episodes", "start_date", "mean", "rank", "popularity",
    "num_scoring_users", "num_list_users", "source",
    "rating", "studios", "genres", "synopsis",
])

CSV_FIELDS = [
    "mal_id", "title_english", "title_japanese", "synonyms",
    "media_type", "status", "num_episodes", "start_date",
    "mean", "rank", "popularity", "num_scoring_users",
    "members", "source", "rating",
    "studios", "genres", "themes", "synopsis",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

# ── CHECKPOINT HELPERS ────────────────────────────────────────────────────────

def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return set()
    with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())


def mark_as_done(mal_id):
    with open(CHECKPOINT_FILE, "a", encoding="utf-8") as f:
        f.write(str(mal_id) + "\n")


def save_id_cache(all_anime):
    """Save collected IDs and theme mappings to JSON so Phase 1 can be skipped on resume."""
    serializable = {
        str(mid): sorted(entry["matched_themes"])
        for mid, entry in all_anime.items()
    }
    with open(ID_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(serializable, f)


def load_id_cache():
    """Load previously collected IDs if cache exists."""
    if not os.path.exists(ID_CACHE_FILE):
        return None
    with open(ID_CACHE_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return {int(mid): {"matched_themes": set(themes)} for mid, themes in data.items()}


def ensure_csv_exists():
    if not os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()
        print(f"  Created new CSV: {OUTPUT_CSV}")
    else:
        print(f"  Appending to existing CSV: {OUTPUT_CSV}")


# ── PHASE 1: SCRAPE GENRE PAGES FOR MAL IDS ──────────────────────────────────

def fetch_ids_from_genre_page(theme_id, theme_name):
    """
    Scrape MAL genre browse pages to collect all anime IDs.
    Simply keeps incrementing pages until we get a 404 or no new IDs.
    The original broad regex is used since it was correctly finding ~100
    IDs per page — the only bug was pagination stopping too early.
    """
    ids          = set()
    page         = 1
    empty_streak = 0
    MAX_EMPTY    = 2

    print(f"\n  Scraping genre page: {theme_name} (ID: {theme_id})")

    while True:
        url = f"https://myanimelist.net/anime/genre/{theme_id}?page={page}"
        print(f"    Page {page}...", end=" ", flush=True)

        req = urllib.request.Request(url, headers=HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                html = resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                print("404 — no more pages.")
            else:
                print(f"HTTP {e.code}")
            break
        except Exception as e:
            print(f"Error: {e}")
            break

        soup = BeautifulSoup(html, "html.parser")

        # Broad match — same approach that found 109 per page originally
        found = set()
        for a in soup.find_all("a", href=True):
            match = re.search(r"/anime/(\d+)/", a["href"])
            if match:
                found.add(int(match.group(1)))

        new_ids = found - ids
        ids    |= found

        if new_ids:
            empty_streak = 0
            print(f"found {len(new_ids)} new (total: {len(ids)})")
        else:
            empty_streak += 1
            print(f"no new IDs (streak {empty_streak}/{MAX_EMPTY})")
            if empty_streak >= MAX_EMPTY:
                print(f"    Done — no new IDs for {MAX_EMPTY} pages.")
                break

        page += 1
        time.sleep(PAGE_DELAY)

    return ids


# ── PHASE 2: INDIVIDUAL DETAIL FETCH ─────────────────────────────────────────

def fetch_detail(mal_id, client_id):
    """Fetch full detail for a single anime by MAL ID via official API."""
    params = urllib.parse.urlencode({"fields": DETAIL_FIELDS})
    url    = f"https://api.myanimelist.net/v2/anime/{mal_id}?{params}"
    req    = urllib.request.Request(
        url, headers={"X-MAL-CLIENT-ID": client_id}
    )
    for attempt in range(1, 4):
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            print(f"    HTTP {e.code} (attempt {attempt}/3)")
            if e.code == 404:
                return None
            time.sleep(2)
        except Exception as e:
            print(f"    Error (attempt {attempt}/3): {e}")
            time.sleep(2)
    return None


# ── FIELD PARSERS ─────────────────────────────────────────────────────────────

def pipe_join(items, key="name"):
    return "|".join(i.get(key, "") for i in items) if items else "N/A"


def parse_anime(node, matched_themes):
    alt            = node.get("alternative_titles", {})
    synonyms       = "|".join(alt.get("synonyms", [])) or "N/A"
    title_english  = alt.get("en") or node.get("title", "N/A")
    title_japanese = alt.get("ja") or "N/A"

    return {
        "mal_id":            node.get("id"),
        "title_english":     title_english,
        "title_japanese":    title_japanese,
        "synonyms":          synonyms,
        "media_type":        node.get("media_type", "N/A"),
        "status":            node.get("status", "N/A"),
        "num_episodes":      node.get("num_episodes"),
        "start_date":        node.get("start_date", "N/A"),
        "mean":              node.get("mean"),
        "rank":              node.get("rank"),
        "popularity":        node.get("popularity"),
        "num_scoring_users": node.get("num_scoring_users"),
        "members":           node.get("num_list_users"),
        "source":            node.get("source", "N/A"),
        "rating":            node.get("rating", "N/A"),
        "studios":           pipe_join(node.get("studios", [])),
        "genres":            pipe_join(node.get("genres", [])),
        "themes":            "|".join(sorted(matched_themes)),
        "synopsis":          node.get("synopsis", "N/A"),
    }


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*55}")
    print(f"  MAL ANIME SCRAPER -- Isekai/Cultivation Dataset")
    print(f"{'='*55}")

    client_id = input("\n  Enter your MAL Client ID: ").strip()
    if not client_id:
        print("ERROR: No Client ID provided.")
        sys.exit(1)

    already_done = load_checkpoint()
    if already_done:
        print(f"  Resuming -- {len(already_done)} MAL IDs already saved.")

    # ── Phase 1: collect IDs (or load from cache) ─────────────────────────────
    all_anime = load_id_cache()

    if all_anime:
        print(f"\n  ID cache found -- skipping Phase 1.")
        print(f"  Loaded {len(all_anime)} anime IDs from cache.")
    else:
        print(f"\n{'─'*55}")
        print(f"  PHASE 1 — Collecting anime IDs from MAL genre pages")
        print(f"{'─'*55}")

        all_anime = {}

        for theme_id, theme_name in THEMES.items():
            ids = fetch_ids_from_genre_page(theme_id, theme_name)
            for mal_id in ids:
                if mal_id not in all_anime:
                    all_anime[mal_id] = {"matched_themes": set()}
                all_anime[mal_id]["matched_themes"].add(theme_name)
            time.sleep(PAGE_DELAY)

        save_id_cache(all_anime)
        print(f"\n  Total unique anime collected: {len(all_anime)}")
        print(f"  IDs saved to cache: {ID_CACHE_FILE}")

    # ── Phase 2: fetch details + write CSV ────────────────────────────────────
    remaining = [
        (mid, entry) for mid, entry in all_anime.items()
        if str(mid) not in already_done
    ]

    print(f"\n{'─'*55}")
    print(f"  PHASE 2 — Fetching full details via MAL API v2")
    print(f"  To process: {len(remaining)} | Already done: {len(already_done)}")
    print(f"{'─'*55}\n")

    ensure_csv_exists()

    success_count = 0
    fail_count    = 0
    failed_ids    = []

    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS)

        for i, (mal_id, entry) in enumerate(remaining, start=1):
            matched_themes = entry["matched_themes"]
            total_done     = len(already_done) + i
            pct            = (total_done / len(all_anime)) * 100

            print(f"  [{total_done}/{len(all_anime)} | {pct:.1f}%] ID {mal_id}...",
                  end=" ", flush=True)

            detail = fetch_detail(mal_id, client_id)

            if detail:
                row = parse_anime(detail, matched_themes)
                writer.writerow(row)
                csv_file.flush()
                mark_as_done(mal_id)
                success_count += 1
                print(f"{row['title_english']} | "
                      f"{row['media_type']} | {row['start_date']} | "
                      f"Score: {row['mean']} | Themes: {row['themes']}")
            else:
                fail_count += 1
                failed_ids.append(mal_id)
                print("FAILED")

            if i < len(remaining):
                time.sleep(API_DELAY)

    if failed_ids:
        with open("mal_failed_ids.txt", "w", encoding="utf-8") as f:
            for mid in failed_ids:
                f.write(str(mid) + "\n")
        print(f"\n  {fail_count} failures saved to: mal_failed_ids.txt")

    print(f"\n{'='*55}")
    print(f"  Complete!")
    print(f"  Written:  {success_count}")
    print(f"  Failed:   {fail_count}")
    print(f"  Dataset:  {OUTPUT_CSV}")
    print(f"{'='*55}")


if __name__ == "__main__":
    main()