"""
SCRIPT: Export Tableau CSVs
============================
Exports 8 clean CSVs from isekai_vs_cultivation.db
ready for Tableau Public.

HOW TO RUN:
  python export_tableau_csvs.py
"""

import sqlite3
import pandas as pd
import os

DB_PATH  = r'C:\Users\Coding\Desktop\Isekai_vs_Cultivation_Analysis\isekai_vs_cultivation.db'
OUT_DIR  = r'C:\Users\Coding\Desktop\Isekai_vs_Cultivation_Analysis\tableau_data'

os.makedirs(OUT_DIR, exist_ok=True)
conn = sqlite3.connect(DB_PATH)

EXPORTS = {

    "timeline_data.csv": """
        SELECT
            n.year,
            COUNT(DISTINCT n.id)  AS novel_count,
            AVG(n.rating)         AS avg_novel_rating
        FROM novels n
        WHERE n.year BETWEEN 2010 AND 2025
          AND n.vote_count >= 5
        GROUP BY n.year
        ORDER BY n.year
    """,

    "anime_timeline.csv": """
        SELECT
            CAST(SUBSTR(a.start_date, 1, 4) AS INTEGER) AS year,
            COUNT(DISTINCT a.id)                         AS anime_count,
            AVG(a.mean)                                  AS avg_anime_score
        FROM anime a
        WHERE CAST(SUBSTR(a.start_date, 1, 4) AS INTEGER) BETWEEN 2010 AND 2025
        GROUP BY year
        ORDER BY year
    """,

    "genre_comparison.csv": """
        SELECT g.name AS genre, 'Novel' AS medium,
               COUNT(DISTINCT ng.novel_id) AS count,
               ROUND(COUNT(DISTINCT ng.novel_id) * 100.0 /
                   (SELECT COUNT(*) FROM novels WHERE vote_count >= 5), 2) AS pct
        FROM novel_genres ng
        JOIN genres  g ON ng.genre_id  = g.id
        JOIN novels  n ON ng.novel_id  = n.id
        WHERE n.vote_count >= 5
        GROUP BY g.name

        UNION ALL

        SELECT g.name AS genre, 'Anime' AS medium,
               COUNT(DISTINCT ag.anime_id) AS count,
               ROUND(COUNT(DISTINCT ag.anime_id) * 100.0 /
                   (SELECT COUNT(*) FROM anime), 2) AS pct
        FROM anime_genres ag
        JOIN genres g ON ag.genre_id = g.id
        GROUP BY g.name
        ORDER BY genre
    """,

    "tag_quality.csv": """
        SELECT
            t.name          AS tag,
            AVG(n.rating)   AS avg_rating,
            COUNT(DISTINCT n.id) AS novel_count,
            CASE
                WHEN AVG(n.rating) >= 4.0 THEN 'High'
                WHEN AVG(n.rating) <= 3.2 THEN 'Low'
                ELSE 'Mid'
            END AS quality_tier
        FROM novel_tags nt
        JOIN tags   t ON nt.tag_id   = t.id
        JOIN novels n ON nt.novel_id = n.id
        WHERE n.vote_count >= 5
        GROUP BY t.name
        HAVING COUNT(DISTINCT n.id) >= 50
        ORDER BY avg_rating DESC
    """,

    "reincarnation_boom.csv": """
        SELECT n.year, COUNT(DISTINCT n.id) AS count, 'Novel' AS medium
        FROM novels n
        JOIN novel_tags nt ON n.id       = nt.novel_id
        JOIN tags       t  ON nt.tag_id  = t.id
        WHERE t.name IN ('Transmigration','Reincarnation','Second Chance','Time Travel')
          AND n.year BETWEEN 2010 AND 2025
          AND n.vote_count >= 5
        GROUP BY n.year

        UNION ALL

        SELECT CAST(SUBSTR(a.start_date, 1, 4) AS INTEGER) AS year,
               COUNT(DISTINCT a.id) AS count, 'Anime' AS medium
        FROM anime a
        JOIN anime_themes at2 ON a.id        = at2.anime_id
        JOIN themes       t   ON at2.theme_id = t.id
        WHERE t.name = 'Reincarnation'
          AND CAST(SUBSTR(a.start_date, 1, 4) AS INTEGER) BETWEEN 2010 AND 2025
        GROUP BY year
        ORDER BY medium, year
    """,

    "studio_quality.csv": """
        SELECT
            s.name               AS studio,
            AVG(a.mean)          AS avg_score,
            COUNT(DISTINCT a.id) AS anime_count
        FROM anime_studios as2
        JOIN studios s ON as2.studio_id = s.id
        JOIN anime   a ON as2.anime_id  = a.id
        WHERE a.mean IS NOT NULL
        GROUP BY s.name
        HAVING COUNT(DISTINCT a.id) >= 3
        ORDER BY avg_score DESC
    """,

    "publisher_quality.csv": """
        SELECT
            p.name               AS publisher,
            AVG(n.rating)        AS avg_rating,
            COUNT(DISTINCT n.id) AS novel_count
        FROM novel_publishers np
        JOIN publishers p ON np.publisher_id = p.id
        JOIN novels     n ON np.novel_id     = n.id
        WHERE n.vote_count >= 5
        GROUP BY p.name
        HAVING COUNT(DISTINCT n.id) >= 5
        ORDER BY avg_rating DESC
    """,

    "clusters.csv": """
        SELECT
            id,
            title,
            rating,
            year,
            reading_list_count,
            status,
            language
        FROM novels
        WHERE vote_count >= 5
          AND rating IS NOT NULL
    """,
}

print(f"\n{'='*50}")
print(f"  Exporting Tableau CSVs")
print(f"  Output folder: {OUT_DIR}")
print(f"{'='*50}\n")

for filename, query in EXPORTS.items():
    try:
        df = pd.read_sql_query(query, conn)
        out_path = os.path.join(OUT_DIR, filename)
        df.to_csv(out_path, index=False, encoding='utf-8-sig')
        print(f"  ✓  {filename:<35} {len(df):>6} rows")
    except Exception as e:
        print(f"  ✗  {filename:<35} ERROR: {e}")

conn.close()
print(f"\n  All done! Open the tableau_data folder and connect in Tableau.")
print(f"{'='*50}\n")