import os
import sys
import requests
import pandas as pd
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
from tqdm import tqdm

"""
Pre-reqs: 
1. Install dependencies: pandas, pyarrow, google-cloud-storage, requests
2. Credentials: Place service_credentials.json in ../keys/ or set GOOGLE_APPLICATION_CREDENTIALS
3. Set GCP_GCS_BUCKET environment variable or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc-de-course-485215-ny-taxi-data")


# Initialize client - try service account file first, then fall back to default credentials
CREDENTIALS_FILE = os.path.join('..', 'keys', 'service_credentials.json')
if os.path.exists(CREDENTIALS_FILE):
    client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
else:
    # If no service account file, use default credentials (requires gcloud auth application-default login)
    # or GOOGLE_APPLICATION_CREDENTIALS environment variable
    client = storage.Client()


def create_bucket_if_not_exists(bucket_name):
    """
    Check if bucket exists, create it if it doesn't exist.
    """
    try:
        # Try to get the bucket
        bucket = client.get_bucket(bucket_name)
        # Check if the bucket belongs to the current project
        project_bucket_ids = [bckt.id for bckt in client.list_buckets()]
        if bucket_name in project_bucket_ids:
            print(f"Bucket '{bucket_name}' exists and belongs to your project.")
        else:
            print(
                f"A bucket with the name '{bucket_name}' already exists, but it does not belong to your project."
            )
            sys.exit(1)
    except NotFound:
        # If the bucket doesn't exist, create it
        try:
            bucket = client.create_bucket(bucket_name)
            print(f"Created bucket '{bucket_name}'")
        except Exception as e:
            print(f"Failed to create bucket '{bucket_name}': {e}")
            print("Please create the bucket manually or check your permissions.")
            sys.exit(1)
    except Forbidden:
        # If the request is forbidden, it means the bucket exists but you don't have access
        print(
            f"A bucket with the name '{bucket_name}' exists, but it is not accessible. "
            f"Bucket name might be taken or you don't have permissions."
        )
        sys.exit(1)


def upload_to_gcs(bucket_name, object_name, local_file):
    """
    Upload file to Google Cloud Storage.
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    """
    Download CSV.gz files, convert to Parquet, and upload to GCS.
    """
    total_months = 12
    pbar = tqdm(range(1, 13), desc=f"{service} {year}", unit="month")
    
    for month in pbar:
        month_str = f"{month:02d}"
        
        # csv file_name
        csv_gz_file = f"{service}_tripdata_{year}-{month_str}.csv.gz"
        parquet_file = f"{service}_tripdata_{year}-{month_str}.parquet"

        try:
            # download it using requests
            request_url = f"{init_url}{service}/{csv_gz_file}"
            pbar.set_postfix_str(f"Downloading {month_str}...")
            r = requests.get(request_url, stream=True)
            r.raise_for_status()
            
            # Get file size for progress bar
            total_size = int(r.headers.get('content-length', 0))
            
            with open(csv_gz_file, 'wb') as f:
                if total_size > 0:
                    with tqdm(total=total_size, unit='B', unit_scale=True, 
                             desc=f"  {csv_gz_file}", leave=False) as dl_pbar:
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                dl_pbar.update(len(chunk))
                else:
                    # No content-length header, download without size progress
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

            # read it back into a parquet file
            pbar.set_postfix_str(f"Converting {month_str} to Parquet...")
            df = pd.read_csv(csv_gz_file, compression='gzip')
            df.to_parquet(parquet_file, engine='pyarrow')

            # upload it to gcs 
            gcs_path = f"{service}/{parquet_file}"
            pbar.set_postfix_str(f"Uploading {month_str} to GCS...")
            upload_to_gcs(BUCKET, gcs_path, parquet_file)
            
            # Clean up local files
            os.remove(csv_gz_file)
            os.remove(parquet_file)
            pbar.set_postfix_str(f"Completed {month_str}")
            
        except requests.exceptions.RequestException as e:
            pbar.write(f"Error downloading {csv_gz_file}: {e}")
            continue
        except NotFound as e:
            pbar.write(f"Bucket '{BUCKET}' not found. Please create it first or check the bucket name.")
            # Clean up partial files
            for f in [csv_gz_file, parquet_file]:
                if os.path.exists(f):
                    os.remove(f)
            continue
        except Forbidden as e:
            pbar.write(f"Permission denied accessing bucket '{BUCKET}'. Check your credentials and permissions.")
            # Clean up partial files
            for f in [csv_gz_file, parquet_file]:
                if os.path.exists(f):
                    os.remove(f)
            continue
        except Exception as e:
            pbar.write(f"Error processing {csv_gz_file}: {e}")
            # Clean up partial files
            for f in [csv_gz_file, parquet_file]:
                if os.path.exists(f):
                    os.remove(f)
            continue
    
    pbar.close()


if __name__ == "__main__":
    print('Starting web_to_gcs...')
    # Verify bucket exists before starting
    print(f"Checking bucket '{BUCKET}'...")
    create_bucket_if_not_exists(BUCKET)
    print()
    
    web_to_gcs('2019', 'green')
    web_to_gcs('2020', 'green')
    web_to_gcs('2019', 'yellow')
    web_to_gcs('2020', 'yellow')
