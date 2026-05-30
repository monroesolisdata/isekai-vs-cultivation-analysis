<div align="center">

# Isekai vs Cultivation — Cross-Media Genre Analysis

### Web Scraping · EDA · Machine Learning · Tableau Dashboard

*Comparing two of the world's fastest-growing fiction genres across 4,900+ titles using custom scrapers, SQL, and ML*

<br/>

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)
![pandas](https://img.shields.io/badge/pandas-2.x-150458?style=flat-square&logo=pandas&logoColor=white)
![scikit-learn](https://img.shields.io/badge/scikit--learn-1.x-F7931E?style=flat-square&logo=scikit-learn&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-3-003B57?style=flat-square&logo=sqlite&logoColor=white)
![Tableau](https://img.shields.io/badge/Tableau-Public-E97627?style=flat-square&logo=tableau&logoColor=white)
![License](https://img.shields.io/badge/License-Educational-green?style=flat-square)

</div>

---

## Research Question

> Did the isekai anime genre and Chinese cultivation web novel genre undergo similar patterns of boom, saturation, and quality decline — and did these trends happen simultaneously or did one precede the other?

---

## Live Dashboard

**[View on Tableau Public →](https://public.tableau.com/app/profile/monroe.solis/viz/IsekaivsCultivationAnalysis/QualityDrivers?publish=yes)**

---

## What It Does

```
NovelUpdates (3,800 novels)  ──┐
                                ├──▶ Normalized SQLite DB (15 tables)
MyAnimeList API (1,100 anime) ──┘         │
                                           ├──▶ EDA (16 charts)
                                           ├──▶ ML Models (3 models)
                                           └──▶ Tableau Dashboard
```

---

## Key Findings

| # | Finding | Detail |
|---|---|---|
| 1 | **Volume without saturation** | Both genres still growing in 2025 — cultivation reached 355 new entries in 2024, isekai reached 130 |
| 2 | **Opposite quality trajectories** ⭐ | Novel quality declining −0.017pts/yr (p<0.001); anime quality rising +0.016pts/yr — same themes, opposite directions |
| 3 | **Transmigration explosion** | Transmigration tags in cultivation novels tripled post-2022 — the saturation fingerprint |
| 4 | **Independent growth cycles** | YoY growth correlation r=0.279 (p=0.31) — genres expand independently despite thematic overlap |
| 5 | **Popularity predicts ratings** | Reader count (log) accounts for 40% of Random Forest importance — a cold-start problem |
| 6 | **Five novel archetypes via clustering** | Romance-forward (4.00★) vs Speed-cultivation factory (3.31★) — newest archetype is worst-rated |
| 7 | **80% quality classifier accuracy** | RF classifier predicts high/low quality from metadata alone (5-fold CV: 0.7938 ± 0.0549) |

---

## Machine Learning Models

| Model | Algorithm | Target | Result |
|---|---|---|---|
| Rating Predictor | Random Forest Regressor | Novel rating (continuous) | R² = 0.32, MAE = 0.33 |
| Quality Classifier | Random Forest Classifier | High vs Low quality (binary) | **Accuracy = 80%** |
| Archetype Finder | KMeans Clustering (k=5) | Novel archetypes | 17.6% PCA variance explained |

**57 engineered features** — binary tag/genre indicators, log-transformed engagement metrics, publication year, completion status, and language flags.

---

## Data Sources

| Dataset | Source | Method | Size |
|---|---|---|---|
| Cultivation Novels | NovelUpdates | nodriver (headless Chrome) + BeautifulSoup | ~3,800 novels |
| Isekai / Martial Arts Anime | MyAnimeList | Genre page scraping + MAL API v2 | ~1,100 anime |

---

## Project Structure

```
isekai-vs-cultivation-analysis/
├── scripts/
│   ├── 02_scraper.py          # NovelUpdates scraper (nodriver + BeautifulSoup)
│   ├── mal_scraper.py         # MyAnimeList two-phase scraper + MAL API v2
│   ├── 03_build_database.py   # Normalized SQLite DB builder (15 tables)
│   └── export_tableau_csvs.py # Exports clean CSVs for Tableau
├── notebooks/
│   ├── 04_eda_v2.ipynb        # Exploratory data analysis — 16 charts
│   └── 05_ml.ipynb            # ML pipeline — regression, classifier, clustering
├── charts/
│   ├── eda/                   # All 16 EDA output charts
│   └── ml/                    # All 6 ML output charts
└── README.md
```

---

## Tech Stack

| Layer | Tools |
|---|---|
| Data Collection | Python · nodriver · BeautifulSoup · MAL API v2 |
| Storage | SQLite (15-table normalized schema) |
| Analysis | pandas · NumPy · SciPy |
| Machine Learning | scikit-learn (Random Forest · KMeans · PCA · Ridge) |
| Visualisation | matplotlib · seaborn · Tableau Public |

---

## Quick Start

```bash
pip install nodriver beautifulsoup4 pandas numpy matplotlib seaborn scikit-learn scipy

# 1. Scrape NovelUpdates (requires Chrome + urls.txt)
python scripts/02_scraper.py

# 2. Scrape MyAnimeList (requires free MAL Client ID)
python scripts/mal_scraper.py

# 3. Build normalized SQLite database
python scripts/03_build_database.py

# 4. Export CSVs for Tableau
python scripts/export_tableau_csvs.py

# 5. Open notebooks in Jupyter
jupyter notebook notebooks/04_eda_v2.ipynb
```

---

## What I Learned

- **Multi-source scraper design** — Cloudflare bypass with nodriver; two-phase MAL API approach to work around genre-filter limitations
- **Normalized schema design** — 15-table SQLite schema with junction tables for many-to-many relationships (genres, tags, authors)
- **Cross-domain EDA** — comparing two datasets with different scales, languages, and rating systems requires careful normalisation
- **Feature engineering at scale** — 57 features from raw text fields (tag binarisation, log transforms, date features)
- **ML interpretability** — permutation importance and PCA reveal which signals actually drive quality vs which are noise

---

*Data sourced from NovelUpdates and MyAnimeList for educational and portfolio purposes.*