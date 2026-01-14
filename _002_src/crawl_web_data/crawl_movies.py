import os, sys
current_dir = os.path.dirname(__file__)
config_path = os.path.join(current_dir, '..')
config_path = os.path.abspath(config_path)
sys.path.insert(0, config_path)
from data_pipeline._01_config.jar_paths import *
from data_pipeline._02_utils.utils import *
import requests
import time
import pandas as pd

BASE_URL = "https://rophimapi.net/v1/movie/filterV2"

params = {
    "q": "",
    "countries": "",
    "genres": "",
    "years": "",
    "custom_year": "",
    "quality": "",
    "type": "",
    "status": "",
    "exclude_status": "Upcoming",
    "versions": "",
    "rating": "",
    "networks": "",
    "productions": "",
    "sort": "release_date",
    "page": 1
}

headers = {
    "User-Agent": "Mozilla/5.0"
}



def fetch_page(page: int):
    params["page"] = page
    r = requests.get(BASE_URL, params=params, headers=headers, timeout=15)
    r.raise_for_status()
    return r.json()


all_movies = []
seen_ids = set()
page = 1

while True:
    data = fetch_page(page)
    items = data.get("result", {}).get("items", [])

    if not items:
        print(f"Hết dữ liệu ở page {page}, dừng.")
        break

    print(f"Page {page}: {len(items)} phim")

    for mv in items:
        mid = mv.get("_id")
        if mid in seen_ids:
            continue
        seen_ids.add(mid)

        genres = mv.get("genres", [])
        genre_names = ", ".join(g.get("name", "") for g in genres)
        countries = ", ".join(mv.get("origin_country", []))
        overview = (mv.get("overview", "") or "").replace("\n", " ").strip()

        all_movies.append({
            "_id": mid,
            "title": mv.get("title", ""),
            "slug": mv.get("slug", ""),
            "original_title": mv.get("original_title", ""),
            "release_date": mv.get("release_date", ""),
            "status": mv.get("status", ""),
            "quality": mv.get("quality", ""),
            "rating": mv.get("rating", ""),
            "runtime": mv.get("runtime", ""),
            "overview": overview,
            "origin_country": countries,
            "genres": genre_names
        })

    page += 1
    time.sleep(0.3)
    
OUTPUT = f"{S3_DATALAKE_PATH}/crawl_data/rophim_all_movie_movies"


# Convert sang DataFrame
df = pd.DataFrame(all_movies)

# các cột text đảm bảo là string
text_cols = [
    "title", "slug", "original_title",
    "overview", "origin_country", "genres",
    "status", "quality"
]

for col in df.columns:
    if df[col].dtype == "object":
        df[col] = df[col].astype(str)

try:
    df_old = pd.read_parquet(OUTPUT, engine="pyarrow")
    df = pd.concat([df_old, df], ignore_index=True)
    df = df.drop_duplicates(subset="_id", keep="last")
except Exception:
    # First run or no existing parquet
    pass

# Ghi parquet (OVERWRITE)
df.to_parquet(
    OUTPUT,
    engine="pyarrow",
    index=False
)

print(f"✅ Đã lưu {len(df)} phim vào {OUTPUT}")

