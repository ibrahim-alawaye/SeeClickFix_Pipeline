import os
import requests
import json
from dotenv import load_dotenv
from airflow.hooks.base import BaseHook
from minio import Minio
from io import BytesIO

# Load environment variables
path = '/workspaces/SeeClickFix_Pipeline/.env'
load_dotenv()

base_url = os.getenv("BASE_URL")
max_pages = int(os.getenv("MAX_PAGES", 5))
Bucket_name = os.getenv('BUCKET_NAME')


#minio_conn = BaseHook.get_connection('minio_api')


minio_endpoint = 'minio:9000'
minio_access_key = 'minio'
minio_secret_key = 'minio456'

# Function to create a MinIO client
def minio_client():
    client = Minio(
        endpoint=minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False
    )
    return client

def get_scf_data(base_url, max_pages):
    all_data = []
    first_page = f"{base_url}"
    res = requests.get(first_page)

    if res.status_code == 200:
        data = res.json()
        all_data.extend(data['issues'])
        print("First data fetched successfully")
    else:
        print(f"Failed to fetch first page: {res.status_code}")
        return all_data

    page = 2

    while page <= max_pages:
        next_page_url = f"{base_url}?page={page}"
        res = requests.get(next_page_url)

        if res.status_code != 200:
            print(f"Failed to fetch data for page: {page}, Status Code: {res.status_code}")
            break

        data = res.json()

        if not data['issues']:
            print("No more data to fetch")
            break

        all_data.extend(data['issues'])
        print(f"Data fetched successfully for page: {page}")

        page += 1


    client = minio_client()

    if not client.bucket_exists(Bucket_name):
        client.make_bucket(Bucket_name)
        print(f"Bucket '{Bucket_name}' created successfully")

    scf_data = json.loads(json.dumps(all_data, ensure_ascii=False))
    data = json.dumps(scf_data, ensure_ascii=False).encode('utf8')

    objw = client.put_object(
        bucket_name=Bucket_name,
        object_name='SeeClickFix_Raw/issues.json',
        data=BytesIO(data),
        length=len(data)
    )
    print(f"Data uploaded successfully to MinIO as '{objw.object_name}'")
    return f'{objw.object_name}'
