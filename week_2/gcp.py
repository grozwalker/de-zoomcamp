from google.cloud import storage
import requests

bucket_name = 'de-zoomcamp2024-us'

client = storage.Client()
bucket = client.get_bucket(bucket_name)

for month in range(1, 13):

  url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-{"{:0>2}".format(month)}.csv.gz'
  source_file_path = f'fhvcsv/fhv_tripdata_2019-{"{:0>2}".format(month)}.csv.gz'

  blob = bucket.blob(source_file_path)

  with requests.get(url, stream=True) as response:
      with blob.open(mode='wb') as f:
          for batch in response.iter_content(1024 * 1024 * 24): # 24 Mb batch
              f.write(batch)
              print('.', end='')
      print()
      print(f'file: {source_file_path} uploaded to bucket: {bucket_name} successfully')
