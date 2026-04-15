"""
Download BTS FAF5 freight data and filter to 2022-2024.
Usage: python scripts/download_data.py
"""
import io
import zipfile
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

RAW_DIR = Path("data/raw")
FAF5_URL = "https://faf.ornl.gov/faf5/data/faf5_4_dot1.zip"
YEARS = {2022, 2023, 2024}


def download_faf5(url: str) -> pd.DataFrame:
    print(f"Downloading FAF5 from {url}")
    resp = requests.get(url, stream=True, timeout=180)
    resp.raise_for_status()
    total = int(resp.headers.get("content-length", 0))

    buf = io.BytesIO()
    with tqdm(total=total, unit="B", unit_scale=True, desc="FAF5") as pbar:
        for chunk in resp.iter_content(chunk_size=8192):
            buf.write(chunk)
            pbar.update(len(chunk))

    buf.seek(0)
    with zipfile.ZipFile(buf) as zf:
        csv_name = next(n for n in zf.namelist() if n.endswith(".csv"))
        print(f"Reading {csv_name} from zip...")
        df = pd.read_csv(zf.open(csv_name), low_memory=False)

    return df


def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    df = download_faf5(FAF5_URL)
    print(f"Full dataset: {len(df):,} rows | columns: {list(df.columns)}")

    # FAF5 year column
    year_col = next((c for c in df.columns if c.lower() == "year"), None)
    if year_col is None:
        raise ValueError(f"No 'year' column found. Available: {list(df.columns)}")

    df_filtered = df[df[year_col].isin(YEARS)].copy()
    print(f"Filtered to {YEARS}: {len(df_filtered):,} rows")
    print(f"Mode distribution:\n{df_filtered['dms_mode'].value_counts()}")

    out_path = RAW_DIR / "faf5_2022_2024.csv"
    df_filtered.to_csv(out_path, index=False)
    print(f"Saved to {out_path}")


if __name__ == "__main__":
    main()
