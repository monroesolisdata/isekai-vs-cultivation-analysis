"""
SCRIPT 2 - NOVEL SCRAPER (nodriver edition, with Resume/Checkpoint)
====================================================================
Reads urls.txt, visits each novel page, extracts all fields,
and appends rows to a CSV immediately.

RESUME SYSTEM:
  Completed URLs are logged to scraped_urls.txt after each success.
  Re-running skips already-completed URLs automatically.

FIELDS COLLECTED:
  title, type, language, year, status, num_chapters,
  completely_translated, original_publisher, release_frequency,
  authors, genres, tags, rating, vote_count,
  reading_list_count, reading_list_monthly_rank, reading_list_alltime_rank,
  activity_weekly_rank, activity_monthly_rank, activity_alltime_rank,
  description, url

HOW THE EXTRACTORS WORK (confirmed April 2026 from live HTML):

  All fields are inside div.wpb_wrapper. Each field is preceded by an
  h5.seriesother label and followed by a specific div/span:

    Type        → div#showtype         a.genre[href*/ntype/]
    Genre       → div#seriesgenre      a.genre[href*/genre/]
    Tags        → div#showtags         a.genre[href*/stag/]
    Language    → div#showlang         a.genre[href*/language/]
    Authors     → div#showauthors      a.genre[id=authtag]
    Year        → div#edityear         text content
    Status/Ch   → div#editstatus       text e.g. "314 Chapters (Ongoing)"
    Translated  → div#showtranslated   text "Yes"/"No"
    Publisher   → div#showopublisher   a.genre[id=myopub]
    Rel. Freq   → h5 sibling text      "Every 0.7 Day(s)"
    Rating      → h5.seriesother       span.uvotes "(3.9 / 5.0, 29 votes)"
    Reading #   → b.rlist              text "201"
    Ranks       → span.userrate.rank   text "#6990" etc. (positional)
    Description → div#editdescription  text content

REQUIREMENTS:
  pip install nodriver beautifulsoup4
  Google Chrome must be installed.

HOW TO RUN:
  python 02_scraper.py
"""

import nodriver
from bs4 import BeautifulSoup
import csv
import asyncio
import os
import sys
import random
import re

# ── SETTINGS ──────────────────────────────────────────────────────────────────

URLS_FILE       = "urls.txt"
OUTPUT_CSV      = "cultivation_dataset.csv"
CHECKPOINT_FILE = "scraped_urls.txt"
DELAY_SECONDS   = 2
MAX_RETRIES     = 3

CSV_FIELDS = [
    "title", "type", "language", "year", "status", "num_chapters",
    "completely_translated", "original_publisher", "release_frequency",
    "authors", "genres", "tags", "rating", "vote_count",
    "reading_list_count", "reading_list_monthly_rank", "reading_list_alltime_rank",
    "activity_weekly_rank", "activity_monthly_rank", "activity_alltime_rank",
    "description", "url",
]

# ── CHECKPOINT HELPERS ────────────────────────────────────────────────────────

def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return set()
    with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
        scraped = set(line.strip() for line in f if line.strip())
    print(f"  Resuming -- {len(scraped)} URLs already scraped.")
    return scraped


def mark_as_done(url):
    with open(CHECKPOINT_FILE, "a", encoding="utf-8") as f:
        f.write(url + "\n")


def ensure_csv_exists():
    if not os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()
        print(f"  Created new CSV: {OUTPUT_CSV}")
    else:
        print(f"  Appending to existing CSV: {OUTPUT_CSV}")


# ── CLOUDFLARE HELPERS ────────────────────────────────────────────────────────

def is_cloudflare_challenge(html):
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.string if soup.title else ""
    return "just a moment" in title.lower() or "cloudflare" in title.lower()


async def wait_for_cloudflare(tab, timeout=60):
    """Poll every 3s until Cloudflare clears. Returns (html, success_bool)."""
    print(f"    Cloudflare detected. Polling every 3s (up to {timeout}s)...")
    elapsed = 0
    while elapsed < timeout:
        await asyncio.sleep(3)
        elapsed += 3
        html = await tab.get_content()
        if not is_cloudflare_challenge(html):
            print(f"    Cloudflare cleared after ~{elapsed}s.")
            return html, True
        print(f"    Still waiting... ({elapsed}s elapsed)")
    print(f"    Cloudflare did not clear after {timeout}s.")
    return await tab.get_content(), False


# ── FIELD EXTRACTORS ──────────────────────────────────────────────────────────

def extract_title(soup, url=""):
    tag = soup.find(class_="seriestitlenu")
    if tag:
        text = tag.get_text(strip=True)
        if text:
            return text
    if url:
        slug = url.rstrip("/").split("/series/")[-1]
        return slug.replace("-", " ").title()
    return "N/A"


def extract_type(soup):
    # div#showtype > a.genre[href*/ntype/]
    div = soup.find("div", id="showtype")
    if div:
        a = div.find("a", class_="genre")
        if a:
            return a.get_text(strip=True)
    return "N/A"


def extract_language(soup):
    # div#showlang > a.genre[href*/language/]
    div = soup.find("div", id="showlang")
    if div:
        a = div.find("a", class_="genre")
        if a:
            return a.get_text(strip=True)
    return "N/A"


def extract_year(soup):
    # div#edityear — plain text e.g. "2025"
    div = soup.find("div", id="edityear")
    if div:
        text = div.get_text(strip=True)
        if text:
            return text
    return "N/A"


def extract_status_and_chapters(soup):
    """
    div#editstatus contains text like "314 Chapters (Ongoing)"
    Returns (num_chapters, status) e.g. (314, "Ongoing")
    """
    div = soup.find("div", id="editstatus")
    if div:
        text = div.get_text(strip=True)
        ch_match = re.search(r'(\d+)\s+Chapter', text)
        num_chapters = int(ch_match.group(1)) if ch_match else None
        st_match = re.search(r'\((.*?)\)', text)
        status = st_match.group(1).strip() if st_match else "N/A"
        return num_chapters, status
    return None, "N/A"


def extract_completely_translated(soup):
    # div#showtranslated — "Yes" or "No"
    div = soup.find("div", id="showtranslated")
    if div:
        text = div.get_text(strip=True)
        if text:
            return text
    return "N/A"


def extract_original_publisher(soup):
    # div#showopublisher > a.genre[id=myopub] or span.seriesna
    div = soup.find("div", id="showopublisher")
    if div:
        a = div.find("a", id="myopub")
        if a:
            return a.get_text(strip=True)
        na = div.find("span", class_="seriesna")
        if na:
            return "N/A"
    return "N/A"


def extract_release_frequency(soup):
    """
    Release frequency is plain text immediately after its h5 label.
    Finds the h5 starting with "Release Frequency" and reads the
    next non-empty text sibling.
    """
    for h5 in soup.find_all("h5", class_="seriesother"):
        if h5.get_text(strip=True).startswith("Release Frequency"):
            sibling = h5.next_sibling
            while sibling:
                if isinstance(sibling, str):
                    text = sibling.strip()
                    if text:
                        return text
                sibling = sibling.next_sibling
    return "N/A"


def extract_authors(soup):
    # div#showauthors > a.genre[id=authtag]
    div = soup.find("div", id="showauthors")
    if div:
        authors = [
            a.get_text(strip=True)
            for a in div.find_all("a", id="authtag")
        ]
        return "|".join(authors) if authors else "N/A"
    return "N/A"


def extract_genres(soup):
    # div#seriesgenre > a.genre[href*/genre/]
    div = soup.find("div", id="seriesgenre")
    if div:
        genres = [
            a.get_text(strip=True)
            for a in div.find_all("a", class_="genre")
            if "/genre/" in a.get("href", "")
        ]
        return "|".join(genres) if genres else "N/A"
    return "N/A"


def extract_tags(soup):
    # div#showtags > a.genre[href*/stag/]
    div = soup.find("div", id="showtags")
    if div:
        tags = [
            a.get_text(strip=True)
            for a in div.find_all("a", class_="genre")
            if "/stag/" in a.get("href", "")
        ]
        return "|".join(tags) if tags else "N/A"
    return "N/A"


def extract_rating_and_votes(soup):
    """
    h5.seriesother > span.uvotes contains "(3.9 / 5.0, 29 votes)"
    Returns (rating_float, vote_count_int)
    """
    for h5 in soup.find_all("h5", class_="seriesother"):
        uvotes = h5.find("span", class_="uvotes")
        if uvotes:
            text = uvotes.get_text(strip=True)
            rating_match = re.search(r'(\d+\.\d+)\s*/\s*5', text)
            votes_match  = re.search(r',\s*(\d+)\s+vote', text)
            rating = float(rating_match.group(1)) if rating_match else None
            votes  = int(votes_match.group(1))    if votes_match  else None
            return rating, votes
    return None, None


def extract_reading_list(soup):
    """
    Searches the page directly for b.rlist and span.rank.
    All span.rank elements appear in this order:
      [0] activity weekly, [1] activity monthly, [2] activity alltime,
      [3] reading list monthly, [4] reading list alltime
    b.rlist is unique and holds the reading list count.
    """
    reading_count = None
    monthly_rank  = "N/A"
    alltime_rank  = "N/A"

    rlist = soup.find("b", class_="rlist")
    if rlist:
        try:
            reading_count = int(rlist.get_text(strip=True))
        except ValueError:
            pass

    all_ranks = soup.find_all("span", class_="rank")
    if len(all_ranks) >= 4:
        monthly_rank = all_ranks[3].get_text(strip=True)
    if len(all_ranks) >= 5:
        alltime_rank = all_ranks[4].get_text(strip=True)

    return reading_count, monthly_rank, alltime_rank


def extract_activity_stats(soup):
    """
    Activity stats are the first three span.rank elements on the page.
    Order: weekly, monthly, alltime.
    """
    weekly_rank  = "N/A"
    monthly_rank = "N/A"
    alltime_rank = "N/A"

    all_ranks = soup.find_all("span", class_="rank")
    if len(all_ranks) >= 1:
        weekly_rank  = all_ranks[0].get_text(strip=True)
    if len(all_ranks) >= 2:
        monthly_rank = all_ranks[1].get_text(strip=True)
    if len(all_ranks) >= 3:
        alltime_rank = all_ranks[2].get_text(strip=True)

    return weekly_rank, monthly_rank, alltime_rank


def extract_description(soup):
    desc_div = soup.find("div", id="editdescription")
    if desc_div:
        text = " ".join(desc_div.get_text(separator=" ").split())
        return text if text else "N/A"
    return "N/A"


# ── PAGE VALIDATION ───────────────────────────────────────────────────────────

def page_loaded_correctly(soup):
    return bool(soup.find(class_="seriestitlenu"))


# ── PAGE FETCH + PARSE ────────────────────────────────────────────────────────

async def fetch_and_parse(tab, url):
    """Navigate to a novel page and return a BeautifulSoup object, or None."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            await tab.get(url)
            await asyncio.sleep(3)

            html = await tab.get_content()

            if is_cloudflare_challenge(html):
                html, cleared = await wait_for_cloudflare(tab)
                if not cleared:
                    print(f"    Could not bypass Cloudflare on attempt {attempt}.")
                    continue

            soup = BeautifulSoup(html, "html.parser")

            if page_loaded_correctly(soup):
                return soup
            else:
                print(f"    Page did not load correctly (attempt {attempt}/{MAX_RETRIES})")
                await asyncio.sleep(3)

        except Exception as e:
            print(f"    Attempt {attempt}/{MAX_RETRIES} error: {e}")
            await asyncio.sleep(3)

    return None


# ── MAIN ──────────────────────────────────────────────────────────────────────

async def main():
    if not os.path.exists(URLS_FILE):
        print(f"ERROR: '{URLS_FILE}' not found. Run 01_url_collector.py first.")
        sys.exit(1)

    with open(URLS_FILE, "r", encoding="utf-8") as f:
        all_urls = [line.strip() for line in f if line.strip()]

    print(f"\n{'='*55}")
    print(f"  NOVEL SCRAPER -- Cultivation Dataset Builder")
    print(f"{'='*55}")
    print(f"  Total URLs: {len(all_urls)}")

    already_done = load_checkpoint()
    remaining = [u for u in all_urls if u not in already_done]

    print(f"  Already scraped: {len(already_done)}")
    print(f"  Remaining:       {len(remaining)}")
    print(f"  Chrome window will open -- this is normal.")
    print(f"{'='*55}\n")

    if not remaining:
        print("All done! Your dataset is at:", OUTPUT_CSV)
        sys.exit(0)

    ensure_csv_exists()

    browser = await nodriver.start(headless=False)
    tab = await browser.get(remaining[0])
    await asyncio.sleep(4)

    # Handle CF on very first page
    first_html = await tab.get_content()
    if is_cloudflare_challenge(first_html):
        _, cleared = await wait_for_cloudflare(tab)
        if not cleared:
            print("Could not bypass Cloudflare on first page. Exiting.")
            browser.stop()
            sys.exit(1)

    failed_urls = []

    try:
        with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS)

            for i, url in enumerate(remaining, start=1):
                total_done = len(already_done) + i
                pct = (total_done / len(all_urls)) * 100
                print(f"  [{total_done}/{len(all_urls)} | {pct:.1f}%] {url}")

                if i == 1:
                    html = await tab.get_content()
                    soup = BeautifulSoup(html, "html.parser")
                    if not page_loaded_correctly(soup):
                        soup = await fetch_and_parse(tab, url)
                else:
                    soup = await fetch_and_parse(tab, url)

                if soup:
                    num_chapters, status             = extract_status_and_chapters(soup)
                    rating, vote_count               = extract_rating_and_votes(soup)
                    rl_count, rl_monthly, rl_all     = extract_reading_list(soup)
                    act_weekly, act_monthly, act_all = extract_activity_stats(soup)

                    data = {
                        "title":                      extract_title(soup, url),
                        "type":                       extract_type(soup),
                        "language":                   extract_language(soup),
                        "year":                       extract_year(soup),
                        "status":                     status,
                        "num_chapters":               num_chapters,
                        "completely_translated":      extract_completely_translated(soup),
                        "original_publisher":         extract_original_publisher(soup),
                        "release_frequency":          extract_release_frequency(soup),
                        "authors":                    extract_authors(soup),
                        "genres":                     extract_genres(soup),
                        "tags":                       extract_tags(soup),
                        "rating":                     rating,
                        "vote_count":                 vote_count,
                        "reading_list_count":         rl_count,
                        "reading_list_monthly_rank":  rl_monthly,
                        "reading_list_alltime_rank":  rl_all,
                        "activity_weekly_rank":       act_weekly,
                        "activity_monthly_rank":      act_monthly,
                        "activity_alltime_rank":      act_all,
                        "description":                extract_description(soup),
                        "url":                        url,
                    }

                    writer.writerow(data)
                    csv_file.flush()
                    mark_as_done(url)
                    print(f"    '{data['title']}' | {data['language']} | "
                          f"Year: {data['year']} | Rating: {data['rating']} | "
                          f"Chapters: {data['num_chapters']} ({data['status']}) | "
                          f"Readers: {data['reading_list_count']}")
                else:
                    failed_urls.append(url)
                    print(f"    FAILED -- will be saved to failed_urls.txt")

                if i < len(remaining):
                    await asyncio.sleep(DELAY_SECONDS + random.uniform(0.5, 1.5))

    finally:
        browser.stop()

    if failed_urls:
        with open("failed_urls.txt", "w", encoding="utf-8") as f:
            for u in failed_urls:
                f.write(u + "\n")
        print(f"\n  {len(failed_urls)} failures saved to: failed_urls.txt")

    print(f"\n{'='*55}")
    print(f"  Scraping complete!")
    print(f"  Scraped: {len(remaining) - len(failed_urls)} | Failed: {len(failed_urls)}")
    print(f"  Dataset: {OUTPUT_CSV}")
    print(f"{'='*55}")


if __name__ == "__main__":
    nodriver.loop().run_until_complete(main())