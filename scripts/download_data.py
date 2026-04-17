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
FAF5_URL = "https://faf.ornl.gov/faf5/Data/Download_Files/FAF5.7.1_2018-2024.zip"
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

    # FAF5.7.1 uses wide format — years are columns (tons_2022, tons_2023 etc.), not rows.
    # generate_synthetic.py only needs fr_orig, fr_dest, dms_mode — save the full file.
    print(f"Mode distribution:\n{df['dms_mode'].value_counts()}")

    out_path = RAW_DIR / "faf5_2022_2024.csv"
    df.to_csv(out_path, index=False)
    print(f"Saved to {out_path} ({len(df):,} rows)")


if __name__ == "__main__":
    main()
