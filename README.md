# Isekai vs Cultivation — Cross-Media Genre Analysis

A data analytics project comparing two of the world's fastest-growing fiction genres: **Japanese isekai anime** and **Chinese cultivation web novels**. Built from scratch using custom web scrapers, a normalized SQL database, exploratory data analysis, machine learning models, and an interactive Tableau dashboard.

---

## 📊 Live Dashboard

**[View on Tableau Public →](https://public.tableau.com/app/profile/monroe.solis/viz/IsekaivsCultivationAnalysis/QualityDrivers?publish=yes)**

---

## 🔍 Research Question

> Did the isekai anime genre and Chinese cultivation web novel genre undergo similar patterns of boom, saturation, and quality decline? Did these trends happen simultaneously or did one precede the other?

---

## 📁 Project Structure

```
isekai-vs-cultivation-analysis/
├── scripts/
│   ├── 02_scraper.py              # NovelUpdates web scraper (nodriver + BeautifulSoup)
│   ├── mal_scraper.py             # MyAnimeList scraper (genre page scraping + MAL API v2)
│   ├── 03_build_database.py       # Normalized SQLite database builder
│   ├── export_tableau_csvs.py     # Exports clean CSVs for Tableau
│   └── fix_cluster_chart_v2.py    # Cluster visualization fix script
├── notebooks/
│   ├── 04_eda_v2.ipynb            # Exploratory data analysis (16 charts)
│   └── 05_ml.ipynb                # Machine learning models (3 models)
├── charts/
│   ├── eda/                       # All EDA output charts
│   └── ml/                        # All ML output charts
└── README.md
```

---

## 🗃️ Data Sources

| Dataset | Source | Method | Size |
|---|---|---|---|
| Cultivation Novels | [NovelUpdates](https://www.novelupdates.com) | Custom web scraper (nodriver + BeautifulSoup) | ~3,800 novels |
| Isekai / Martial Arts Anime | [MyAnimeList](https://myanimelist.net) | Genre page scraping + MAL API v2 | ~1,100 anime |

### Fields Collected

**Cultivation Novels:** Title, Type, Language, Year, Status, Chapters, Translated, Publisher, Release Frequency, Authors, Genres, Tags, Rating, Vote Count, Reading List Count, Activity Ranks, Description

**Isekai Anime:** MAL ID, Title (EN/JP), Synonyms, Media Type, Status, Episodes, Start Date, Score, Rank, Popularity, Scoring Users, Members, Source, Rating, Studios, Genres, Themes, Synopsis

---

## 🕷️ Scraper Architecture

### NovelUpdates Scraper (`02_scraper.py`)
Uses **nodriver** (headless Chrome) to bypass Cloudflare protection on NovelUpdates. Visits each novel page, parses structured HTML fields using BeautifulSoup, and appends rows to a CSV immediately. Features a checkpoint system that logs completed URLs so the scraper can resume from any interruption — essential for a 3,800 novel dataset that takes 3-4 hours to scrape.

### MyAnimeList Scraper (`mal_scraper.py`)
Uses a two-phase approach necessitated by a MAL API v2 limitation — the API does not support filtering anime by genre/theme ID on its list endpoint:

- **Phase 1** — Scrapes the public MAL genre browse pages (`myanimelist.net/anime/genre/{id}`) for theme IDs 62 (Isekai), 72 (Reincarnation), and 17 (Martial Arts) using urllib + BeautifulSoup, collecting all anime IDs across paginated results. Results are cached to JSON so Phase 1 is skipped on resume.
- **Phase 2** — Calls the official MAL API v2 `/anime/{id}` endpoint for each collected ID to retrieve clean structured JSON data. Deduplicates anime that appear in multiple themes, storing all matching themes pipe-separated in a single row.

---

## 🏗️ Database Schema

Normalized SQLite database (`isekai_vs_cultivation.db`) with 15 tables:

**Core tables:** `novels`, `anime`

**Lookup tables:** `genres`, `tags`, `authors`, `publishers`, `themes`, `studios`

**Junction tables:** `novel_genres`, `novel_tags`, `novel_authors`, `novel_publishers`, `anime_genres`, `anime_themes`, `anime_studios`

Authors are stored with a `name_type` field (`original` for CJK characters, `romanized` for Latin script) to handle Chinese authors who publish under multiple name formats.

---

## 📈 Key Findings

### 1. Volume Without Saturation
Both genres are still growing in 2025 with no peak detected. Cultivation novels reached 355 new entries in 2024; isekai anime reached 130. Neither genre is declining in production volume.

### 2. Opposite Quality Trajectories ⭐ Headline Finding
Novel quality shows a **statistically significant decline** of -0.0168 points per year (p=0.000). Anime quality shows a **slight rise** of +0.0159 points per year (p=0.102). Two genres with nearly identical themes are moving in opposite directions — novels declining as the market floods, anime maintaining quality through production filters.

### 3. The Transmigration Explosion
The Transmigration tag in cultivation novels tripled post-2022, becoming the dominant trope by a wide margin. This is the saturation marker — volume keeps rising but the genre is doubling down on its most formulaic premise.

### 4. Independent Production Cycles
Year-over-year growth rate correlation: **r = 0.279 (p = 0.31)**. The two genres expand independently despite thematic overlap, suggesting each is driven by its own internal market forces rather than a shared global trend.

### 5. Popularity Predicts Ratings More Than Content
The most important feature for predicting novel ratings is **reader count** (log-transformed), accounting for 40% of Random Forest importance — nearly 3× the next feature. This suggests a cold start problem where obscure novels are systematically underrated regardless of actual quality.

### 6. Five Novel Archetypes Identified via Clustering

| Cluster | Archetype | Avg Rating | Avg Year | Avg Readers |
|---|---|---|---|---|
| C3 | Romance-forward cultivation | 4.00 ⭐ | 2018.8 | 3,688 |
| C4 | Classic xianxia epics | 3.64 | 2014.7 | 4,290 |
| C0 | Female protagonist transmigration | 3.60 | 2021.6 | 495 |
| C1 | Generic male power fantasy | 3.35 | 2021.4 | 341 |
| C2 | Speed cultivation factory | 3.31 ❌ | 2022.5 | 578 |

Cluster 2 — the newest, most formulaic archetype — is the lowest rated. Cluster 3 — romance-driven emotional storytelling — is the highest rated.

### 7. Quality Classifier at 80% Accuracy
A Random Forest classifier predicts whether a novel will be high or low quality from metadata alone with **80% accuracy** (5-fold CV: 0.7938 ± 0.0549). Key signals: reader count, protagonist gender tag, release year, and Harem/Xianxia genre flags.

---

## 🤖 Machine Learning Models

| Model | Algorithm | Target | Result |
|---|---|---|---|
| Rating Predictor | Random Forest Regressor | Novel rating (continuous) | R² = 0.32, MAE = 0.33 |
| Quality Classifier | Random Forest Classifier | High vs Low quality (binary) | Accuracy = 80% |
| Archetype Finder | KMeans Clustering (k=5) | Novel archetypes | 17.6% PCA variance explained |

**Feature engineering:** 57 features including binary tag/genre indicators, log-transformed engagement metrics, publication year, completion status, and language flags.

---

## 🛠️ Tech Stack

| Category | Tools |
|---|---|
| **Data Collection** | Python, nodriver, BeautifulSoup, Requests, MAL API v2 |
| **Data Storage** | SQLite, DB Browser for SQLite |
| **Analysis** | Python, Pandas, NumPy, SciPy |
| **Visualization** | Matplotlib, Seaborn, Tableau Public |
| **Machine Learning** | scikit-learn (Random Forest, KMeans, PCA, Ridge Regression) |
| **Environment** | Jupyter Notebook |

---

## 🚀 How to Run

### Requirements
```bash
pip install nodriver beautifulsoup4 pandas numpy matplotlib seaborn scikit-learn scipy joblib
```

### Steps
1. **Scrape NovelUpdates** — run `scripts/02_scraper.py` (requires Chrome)
2. **Scrape MyAnimeList** — run `scripts/mal_scraper.py` (requires MAL Client ID)
3. **Build database** — run `scripts/03_build_database.py` with your CSVs in the same folder
4. **Export Tableau data** — run `scripts/export_tableau_csvs.py`
5. **Run EDA** — open `notebooks/04_eda_v2.ipynb` in Jupyter
6. **Run ML** — open `notebooks/05_ml.ipynb` in Jupyter

> **Note:** Both scrapers require setup before running:
> - `02_scraper.py` requires Google Chrome installed and a `urls.txt` file of NovelUpdates URLs
> - `mal_scraper.py` uses a two-phase approach: Phase 1 scrapes MAL genre pages (IDs 62, 72, 17) to collect anime IDs, Phase 2 calls the official MAL API v2 per ID to retrieve full structured data. Requires a free [MyAnimeList API client ID](https://myanimelist.net/apiconfig).
> - The database file and raw CSVs are not included in this repo due to size — run the scrapers to generate them.

---

## 📊 Dashboard Preview

The Tableau dashboard consists of 3 interactive views:

- **The Timeline** — Annual release volume + quality over time with trend lines
- **The Saturation Story** — Reincarnation/transmigration boom + shared genre comparison
- **Quality Drivers** — Tag quality rankings, popularity vs rating scatter, studio and publisher rankings

**[Explore the full dashboard →](https://public.tableau.com/app/profile/monroe.solis/viz/IsekaivsCultivationAnalysis/QualityDrivers?publish=yes)**

---

## 👤 Author

**Monroe Solis**
[GitHub](https://github.com/monroesolisdata/isekai-vs-cultivation-analysis)

---

## 📄 License

This project is for educational and portfolio purposes. Data sourced from NovelUpdates and MyAnimeList — all rights belong to their respective owners.
