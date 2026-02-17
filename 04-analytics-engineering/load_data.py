import os
import sys
import glob
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
from tqdm import tqdm
import time

"""
Load green and yellow taxi data (2019, 2020) from GitHub, convert to Parquet, upload to GCS.
"""

BUCKET_NAME = os.environ.get("GCP_GCS_BUCKET", "dtc-de-course-485215-ny-taxi-data")
INIT_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
YEARS = ["2019", "2020"]
SERVICES = ["green", "yellow"]
CHUNK_SIZE = 8 * 1024 * 1024

CREDENTIALS_FILE = os.path.join("..", "keys", "service_credentials.json")
if os.path.exists(CREDENTIALS_FILE):
    client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
else:
    client = storage.Client(project="dtc-de-course-485215")

bucket = client.bucket(BUCKET_NAME)


def create_bucket_if_not_exists(bucket_name):
    try:
        b = client.get_bucket(bucket_name)
        project_bucket_ids = [x.id for x in client.list_buckets()]
        if bucket_name in project_bucket_ids:
            print(f"Bucket '{bucket_name}' exists.")
        else:
            print(f"Bucket '{bucket_name}' exists but not in your project.")
            sys.exit(1)
    except NotFound:
        try:
            client.create_bucket(bucket_name)
            print(f"Created bucket '{bucket_name}'")
        except Exception as e:
            print(f"Failed to create bucket: {e}")
            sys.exit(1)
    except Forbidden:
        print(f"Bucket '{bucket_name}' not accessible.")
        sys.exit(1)


def _normalize_dtypes(df):
    """Normalize dtypes for BigQuery consistency."""
    if "passenger_count" in df.columns:
        df["passenger_count"] = pd.to_numeric(df["passenger_count"], errors="coerce").astype("float64")
    if "trip_type" in df.columns:
        df["trip_type"] = pd.to_numeric(df["trip_type"], errors="coerce").astype("float64")
    return df


def _align_to_schema(df, schema):
    """Force df to have same columns and dtypes as reference schema (from {type}_2019-01).
    Integer columns use nullable Int64 so NA in other months is allowed.
    """
    cols = schema["columns"]
    dtypes = schema["dtypes"]
    df = df.reindex(columns=cols, fill_value=pd.NA)
    for c in cols:
        target_dtype = dtypes.get(c)
        if target_dtype is None or c not in df.columns:
            continue
        try:
            if pd.api.types.is_datetime64_any_dtype(target_dtype):
                df[c] = pd.to_datetime(df[c], errors="coerce")
            elif pd.api.types.is_integer_dtype(target_dtype) and not pd.api.types.is_extension_array_dtype(target_dtype):
                # Numpy int (int64 etc.) cannot hold NA; use nullable Int64
                df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
            else:
                df[c] = df[c].astype(target_dtype)
        except (TypeError, ValueError):
            df[c] = pd.to_numeric(df[c], errors="coerce").astype(target_dtype)
    return df


def download_and_convert(service, year, month, reference_schema=None):
    """
    Download CSV.gz, convert to Parquet. If reference_schema is set (from {service}_2019-01),
    align columns and dtypes to that schema.
    """
    month_str = f"{month:02d}"
    csv_gz = f"{service}_tripdata_{year}-{month_str}.csv.gz"
    parquet_file = f"{service}_tripdata_{year}-{month_str}.parquet"
    url = f"{INIT_URL}{service}/{csv_gz}"
    try:
        r = requests.get(url, stream=True, timeout=60)
        r.raise_for_status()
        total_size = int(r.headers.get("content-length", 0))
        with open(csv_gz, "wb") as f:
            if total_size > 0:
                with tqdm(total=total_size, unit="B", unit_scale=True, leave=False, desc=f"{service} {year}-{month_str}") as pbar:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
            else:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        df = pd.read_csv(csv_gz, compression="gzip", low_memory=False)
        df = _normalize_dtypes(df)
        if reference_schema is not None:
            df = _align_to_schema(df, reference_schema)
        df.to_parquet(parquet_file, engine="pyarrow")
        os.remove(csv_gz)
        return (service, year, month_str, parquet_file)
    except Exception as e:
        if os.path.exists(csv_gz):
            os.remove(csv_gz)
        print(f"Error {service} {year}-{month_str}: {e}")
        return None


def upload_to_gcs(service, parquet_file, max_retries=3):
    blob_name = f"{service}/{parquet_file}"
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE
    for attempt in range(max_retries):
        try:
            blob.upload_from_filename(parquet_file)
            return True
        except Exception as e:
            print(f"Upload failed {blob_name}: {e}")
            time.sleep(5)
    return False


def cleanup_local_files():
    """Remove any leftover downloaded/converted files (parquet or csv.gz) from this run."""
    for pattern in (
        "green_tripdata_*.parquet",
        "yellow_tripdata_*.parquet",
        "green_tripdata_*.csv.gz",
        "yellow_tripdata_*.csv.gz",
    ):
        for path in glob.glob(pattern):
            try:
                os.remove(path)
                print(f"Removed: {path}")
            except OSError as e:
                print(f"Could not remove {path}: {e}")


if __name__ == "__main__":
    try:
        create_bucket_if_not_exists(BUCKET_NAME)

        # Phase 1: build reference schema from {type}_tripdata_2019-01; upload and remove immediately
        schema_by_service = {}
        results = []
        for service in SERVICES:
            out = download_and_convert(service, 2019, 1, reference_schema=None)
            if out is not None:
                _, _, _, parquet_path = out
                ref_df = pd.read_parquet(parquet_path)
                dtypes = ref_df.dtypes.to_dict()
                # Use nullable Int64 for integer columns so other months with NA align without error
                for c, dt in list(dtypes.items()):
                    if pd.api.types.is_integer_dtype(dt) and not pd.api.types.is_extension_array_dtype(dt):
                        dtypes[c] = pd.Int64Dtype()
                schema_by_service[service] = {
                    "columns": list(ref_df.columns),
                    "dtypes": dtypes,
                }
                upload_to_gcs(service, parquet_path)
                if os.path.exists(parquet_path):
                    os.remove(parquet_path)
                print(f"Reference schema for {service}: {len(ref_df.columns)} columns (from 2019-01), uploaded")

        # Phase 2: all other (type, year, month) aligned to reference schema
        tasks = [
            (s, y, m)
            for s in SERVICES
            for y in YEARS
            for m in range(1, 13)
            if (y, m) != (2019, 1)
        ]
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(download_and_convert, s, y, m, schema_by_service.get(s)): (s, y, m)
                for s, y, m in tasks
            }
            for fut in tqdm(as_completed(futures), total=len(futures), desc="Download+Convert"):
                r = fut.result()
                if r is not None:
                    results.append(r)

        for service, year, month_str, parquet_file in tqdm(results, desc="Upload to GCS"):
            upload_to_gcs(service, parquet_file)
            if os.path.exists(parquet_file):
                os.remove(parquet_file)
    finally:
        cleanup_local_files()

    print("Done. Green and yellow 2019–2020 loaded to GCS (schema aligned to 2019-01 per type).")