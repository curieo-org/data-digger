# Import the required libraries
import requests
import ssl
import zipfile
from urllib.request import urlretrieve
from datetime import date
import os
import shutil
from bs4 import BeautifulSoup



# Download the latest AACT database HTML page
BASE_URL = 'https://aact.ctti-clinicaltrials.org/snapshots'
r = requests.get(BASE_URL)
soup = BeautifulSoup(r.content, 'html.parser')



# Find the latest database file
current_date = date.today().strftime('%Y%m%d')
download_url = ''
download_file_name = ''
unzipped_folder_name = ''

for tag in soup.find_all('a'):
    if current_date in tag.text:
        download_url = tag.get('href').strip()
        download_file_name = tag.text.strip()
        unzipped_folder_name = download_url.split('/')[-1].strip()
        break



# Download the file using requests (streaming mode)
# with requests.get(download_url, stream=True) as r:
#     r.raise_for_status()
#     with open(download_file_name, 'wb') as f:
#         for chunk in r.iter_content(chunk_size=8192):
#             f.write(chunk)
    
# Download the file using urllib
print('Downloading file:', download_file_name, 'from:', download_url)

ssl._create_default_https_context = ssl._create_stdlib_context
urlretrieve(download_url, download_file_name)

print('Downloaded file:', download_file_name)
print('Downloaded from:', download_url)



# Unzip the file
print('Unzipping file:', download_file_name)

with zipfile.ZipFile(download_file_name, 'r') as zip_ref:
    zip_ref.extractall(unzipped_folder_name)

print('Unzipped file:', download_file_name)



# Copy the database file to the root folder
database_file = 'postgres.dmp'
shutil.copy(os.path.join(unzipped_folder_name, database_file), database_file)