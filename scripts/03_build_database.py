"""
SCRIPT 3 - BUILD NORMALIZED DATABASE
======================================
Reads cultivation_dataset.csv and isekai_dataset.csv and populates
a single combined SQLite database: isekai_vs_cultivation.db

SCHEMA OVERVIEW:

  NOVELS SIDE:
    novels              — one row per novel, all scalar fields
    novel_genres        — junction: novels <-> genres
    novel_tags          — junction: novels <-> tags
    novel_authors       — junction: novels <-> authors
    novel_publishers    — junction: novels <-> publishers

  ANIME SIDE:
    anime               — one row per anime, all scalar fields
    anime_genres        — junction: anime <-> genres
    anime_themes        — junction: anime <-> themes
    anime_studios       — junction: anime <-> studios

  SHARED LOOKUP TABLES:
    genres              — unique genre names (shared by both sides)
    tags                — unique tag names (novels only)
    authors             — unique author names with name_type
    publishers          — unique publisher names (novels only)
    themes              — unique theme names (anime only)
    studios             — unique studio names (anime only)

  AUTHOR NAME TYPES:
    original   — contains CJK characters (Chinese/Japanese)
    romanized  — latin script (romanized pinyin or English pen name)

REQUIREMENTS:
  Python 3.6+ stdlib only. No pip installs needed.

HOW TO RUN:
  python 03_build_database.py

  Place cultivation_dataset.csv and isekai_dataset.csv in the same
  folder as this script before running.
"""

import sqlite3
import csv
import os
import sys
import re
import unicodedata

# ── SETTINGS ──────────────────────────────────────────────────────────────────

NOVEL_CSV  = "cultivation_dataset.csv"
ANIME_CSV  = "isekai_dataset.csv"
DB_FILE    = "isekai_vs_cultivation.db"

# ── HELPERS ───────────────────────────────────────────────────────────────────

def pipe_split(value):
    """Split a pipe-separated string into a clean list, skipping N/A."""
    if not value or value.strip().upper() == "N/A":
        return []
    return [v.strip() for v in value.split("|") if v.strip()]


def is_cjk(text):
    """Return True if the string contains any CJK (Chinese/Japanese) characters."""
    for char in text:
        if unicodedata.category(char) in ("Lo",) and ord(char) > 0x2E7F:
            return True
    return False


def detect_name_type(name):
    """Classify an author name as 'original' (CJK) or 'romanized' (latin)."""
    return "original" if is_cjk(name) else "romanized"


def safe_int(value):
    """Convert to int, return None if blank or N/A."""
    if not value or str(value).strip().upper() in ("N/A", "NONE", ""):
        return None
    try:
        return int(str(value).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def safe_float(value):
    """Convert to float, return None if blank or N/A."""
    if not value or str(value).strip().upper() in ("N/A", "NONE", ""):
        return None
    try:
        return float(str(value).strip())
    except (ValueError, TypeError):
        return None


def safe_str(value):
    """Return None if blank or N/A, else stripped string."""
    if not value or str(value).strip().upper() in ("N/A", "NONE", ""):
        return None
    return str(value).strip()


# ── SCHEMA ────────────────────────────────────────────────────────────────────

SCHEMA = """
-- ── SHARED LOOKUP TABLES ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS genres (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS tags (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS authors (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL,
    name_type   TEXT    NOT NULL CHECK(name_type IN ('original', 'romanized')),
    UNIQUE(name, name_type)
);

CREATE TABLE IF NOT EXISTS publishers (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS themes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS studios (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL UNIQUE
);

-- ── NOVELS ────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS novels (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    title                       TEXT,
    type                        TEXT,
    language                    TEXT,
    year                        INTEGER,
    status                      TEXT,
    num_chapters                INTEGER,
    completely_translated       TEXT,
    release_frequency           TEXT,
    rating                      REAL,
    vote_count                  INTEGER,
    reading_list_count          INTEGER,
    reading_list_monthly_rank   TEXT,
    reading_list_alltime_rank   TEXT,
    activity_weekly_rank        TEXT,
    activity_monthly_rank       TEXT,
    activity_alltime_rank       TEXT,
    description                 TEXT,
    url                         TEXT    UNIQUE
);

CREATE TABLE IF NOT EXISTS novel_genres (
    novel_id    INTEGER NOT NULL REFERENCES novels(id),
    genre_id    INTEGER NOT NULL REFERENCES genres(id),
    PRIMARY KEY (novel_id, genre_id)
);

CREATE TABLE IF NOT EXISTS novel_tags (
    novel_id    INTEGER NOT NULL REFERENCES novels(id),
    tag_id      INTEGER NOT NULL REFERENCES tags(id),
    PRIMARY KEY (novel_id, tag_id)
);

CREATE TABLE IF NOT EXISTS novel_authors (
    novel_id    INTEGER NOT NULL REFERENCES novels(id),
    author_id   INTEGER NOT NULL REFERENCES authors(id),
    PRIMARY KEY (novel_id, author_id)
);

CREATE TABLE IF NOT EXISTS novel_publishers (
    novel_id        INTEGER NOT NULL REFERENCES novels(id),
    publisher_id    INTEGER NOT NULL REFERENCES publishers(id),
    PRIMARY KEY (novel_id, publisher_id)
);

-- ── ANIME ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS anime (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    mal_id              INTEGER UNIQUE,
    title_english       TEXT,
    title_japanese      TEXT,
    synonyms            TEXT,
    media_type          TEXT,
    status              TEXT,
    num_episodes        INTEGER,
    start_date          TEXT,
    mean                REAL,
    rank                INTEGER,
    popularity          INTEGER,
    num_scoring_users   INTEGER,
    members             INTEGER,
    source              TEXT,
    rating              TEXT,
    synopsis            TEXT
);

CREATE TABLE IF NOT EXISTS anime_genres (
    anime_id    INTEGER NOT NULL REFERENCES anime(id),
    genre_id    INTEGER NOT NULL REFERENCES genres(id),
    PRIMARY KEY (anime_id, genre_id)
);

CREATE TABLE IF NOT EXISTS anime_themes (
    anime_id    INTEGER NOT NULL REFERENCES anime(id),
    theme_id    INTEGER NOT NULL REFERENCES themes(id),
    PRIMARY KEY (anime_id, theme_id)
);

CREATE TABLE IF NOT EXISTS anime_studios (
    anime_id    INTEGER NOT NULL REFERENCES anime(id),
    studio_id   INTEGER NOT NULL REFERENCES studios(id),
    PRIMARY KEY (anime_id, studio_id)
);

-- ── INDEXES ───────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_novels_year       ON novels(year);
CREATE INDEX IF NOT EXISTS idx_novels_rating     ON novels(rating);
CREATE INDEX IF NOT EXISTS idx_novels_language   ON novels(language);
CREATE INDEX IF NOT EXISTS idx_anime_start_date  ON anime(start_date);
CREATE INDEX IF NOT EXISTS idx_anime_mean        ON anime(mean);
CREATE INDEX IF NOT EXISTS idx_anime_mal_id      ON anime(mal_id);
"""

# ── LOOKUP CACHE + UPSERT ─────────────────────────────────────────────────────

def get_or_create(cursor, table, column, value, extra_col=None, extra_val=None):
    """
    Get the ID of an existing row or insert and return the new ID.
    Optionally supports a second column for tables like authors (name + name_type).
    """
    if extra_col:
        cursor.execute(
            f"SELECT id FROM {table} WHERE {column} = ? AND {extra_col} = ?",
            (value, extra_val)
        )
    else:
        cursor.execute(f"SELECT id FROM {table} WHERE {column} = ?", (value,))

    row = cursor.fetchone()
    if row:
        return row[0]

    if extra_col:
        cursor.execute(
            f"INSERT INTO {table} ({column}, {extra_col}) VALUES (?, ?)",
            (value, extra_val)
        )
    else:
        cursor.execute(f"INSERT INTO {table} ({column}) VALUES (?)", (value,))

    return cursor.lastrowid


# ── NOVEL LOADER ──────────────────────────────────────────────────────────────

def load_novels(cursor, csv_path):
    print(f"\n  Loading novels from: {csv_path}")

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows   = list(reader)

    print(f"  Rows found: {len(rows)}")
    inserted = 0
    skipped  = 0

    for row in rows:
        # Insert main novel row
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO novels (
                    title, type, language, year, status, num_chapters,
                    completely_translated, release_frequency, rating, vote_count,
                    reading_list_count, reading_list_monthly_rank,
                    reading_list_alltime_rank, activity_weekly_rank,
                    activity_monthly_rank, activity_alltime_rank,
                    description, url
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                safe_str(row.get("title")),
                safe_str(row.get("type")),
                safe_str(row.get("language")),
                safe_int(row.get("year")),
                safe_str(row.get("status")),
                safe_int(row.get("num_chapters")),
                safe_str(row.get("completely_translated")),
                safe_str(row.get("release_frequency")),
                safe_float(row.get("rating")),
                safe_int(row.get("vote_count")),
                safe_int(row.get("reading_list_count")),
                safe_str(row.get("reading_list_monthly_rank")),
                safe_str(row.get("reading_list_alltime_rank")),
                safe_str(row.get("activity_weekly_rank")),
                safe_str(row.get("activity_monthly_rank")),
                safe_str(row.get("activity_alltime_rank")),
                safe_str(row.get("description")),
                safe_str(row.get("url")),
            ))
        except sqlite3.Error as e:
            print(f"    Error inserting novel '{row.get('title')}': {e}")
            skipped += 1
            continue

        if cursor.rowcount == 0:
            skipped += 1
            continue

        novel_id = cursor.lastrowid
        inserted += 1

        # Genres
        for genre in pipe_split(row.get("genres", "")):
            gid = get_or_create(cursor, "genres", "name", genre)
            cursor.execute(
                "INSERT OR IGNORE INTO novel_genres (novel_id, genre_id) VALUES (?,?)",
                (novel_id, gid)
            )

        # Tags
        for tag in pipe_split(row.get("tags", "")):
            tid = get_or_create(cursor, "tags", "name", tag)
            cursor.execute(
                "INSERT OR IGNORE INTO novel_tags (novel_id, tag_id) VALUES (?,?)",
                (novel_id, tid)
            )

        # Authors — detect name type per entry
        for author in pipe_split(row.get("authors", "")):
            name_type = detect_name_type(author)
            aid = get_or_create(cursor, "authors", "name", author,
                                extra_col="name_type", extra_val=name_type)
            cursor.execute(
                "INSERT OR IGNORE INTO novel_authors (novel_id, author_id) VALUES (?,?)",
                (novel_id, aid)
            )

        # Publishers
        for pub in pipe_split(row.get("original_publisher", "")):
            pid = get_or_create(cursor, "publishers", "name", pub)
            cursor.execute(
                "INSERT OR IGNORE INTO novel_publishers (novel_id, publisher_id) VALUES (?,?)",
                (novel_id, pid)
            )

    print(f"  Inserted: {inserted} | Skipped (duplicate/error): {skipped}")


# ── ANIME LOADER ──────────────────────────────────────────────────────────────

def load_anime(cursor, csv_path):
    print(f"\n  Loading anime from: {csv_path}")

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows   = list(reader)

    print(f"  Rows found: {len(rows)}")
    inserted = 0
    skipped  = 0

    for row in rows:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO anime (
                    mal_id, title_english, title_japanese, synonyms,
                    media_type, status, num_episodes, start_date,
                    mean, rank, popularity, num_scoring_users,
                    members, source, rating, synopsis
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                safe_int(row.get("mal_id")),
                safe_str(row.get("title_english")),
                safe_str(row.get("title_japanese")),
                safe_str(row.get("synonyms")),
                safe_str(row.get("media_type")),
                safe_str(row.get("status")),
                safe_int(row.get("num_episodes")),
                safe_str(row.get("start_date")),
                safe_float(row.get("mean")),
                safe_int(row.get("rank")),
                safe_int(row.get("popularity")),
                safe_int(row.get("num_scoring_users")),
                safe_int(row.get("members")),
                safe_str(row.get("source")),
                safe_str(row.get("rating")),
                safe_str(row.get("synopsis")),
            ))
        except sqlite3.Error as e:
            print(f"    Error inserting anime '{row.get('title_english')}': {e}")
            skipped += 1
            continue

        if cursor.rowcount == 0:
            skipped += 1
            continue

        anime_id = cursor.lastrowid
        inserted += 1

        # Genres (shared lookup table with novels)
        for genre in pipe_split(row.get("genres", "")):
            gid = get_or_create(cursor, "genres", "name", genre)
            cursor.execute(
                "INSERT OR IGNORE INTO anime_genres (anime_id, genre_id) VALUES (?,?)",
                (anime_id, gid)
            )

        # Themes
        for theme in pipe_split(row.get("themes", "")):
            tid = get_or_create(cursor, "themes", "name", theme)
            cursor.execute(
                "INSERT OR IGNORE INTO anime_themes (anime_id, theme_id) VALUES (?,?)",
                (anime_id, tid)
            )

        # Studios
        for studio in pipe_split(row.get("studios", "")):
            sid = get_or_create(cursor, "studios", "name", studio)
            cursor.execute(
                "INSERT OR IGNORE INTO anime_studios (anime_id, studio_id) VALUES (?,?)",
                (anime_id, sid)
            )

    print(f"  Inserted: {inserted} | Skipped (duplicate/error): {skipped}")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*55}")
    print(f"  DATABASE BUILDER -- Isekai vs Cultivation")
    print(f"{'='*55}")

    # Check CSVs exist
    missing = [f for f in [NOVEL_CSV, ANIME_CSV] if not os.path.exists(f)]
    if missing:
        for f in missing:
            print(f"  ERROR: '{f}' not found in current directory.")
        sys.exit(1)

    # Remove existing DB so we always build fresh
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        print(f"\n  Removed existing: {DB_FILE}")

    conn   = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Enable foreign keys and WAL mode for performance
    cursor.execute("PRAGMA foreign_keys = ON")
    cursor.execute("PRAGMA journal_mode = WAL")

    # Build schema
    print(f"\n  Creating schema...")
    for statement in SCHEMA.strip().split(";"):
        statement = statement.strip()
        if statement:
            cursor.execute(statement)
    print(f"  Schema created.")

    # Load data
    load_novels(cursor, NOVEL_CSV)
    load_anime(cursor, ANIME_CSV)

    conn.commit()

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'─'*55}")
    print(f"  DATABASE SUMMARY")
    print(f"{'─'*55}")

    tables = [
        "novels", "anime", "genres", "tags", "authors",
        "publishers", "themes", "studios",
        "novel_genres", "novel_tags", "novel_authors", "novel_publishers",
        "anime_genres", "anime_themes", "anime_studios",
    ]

    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table:<30} {count:>6} rows")

    conn.close()

    print(f"\n{'='*55}")
    print(f"  Done! Database saved to: {DB_FILE}")
    print(f"{'='*55}")


if __name__ == "__main__":
    main()