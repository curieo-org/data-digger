# Import the required libraries
import requests
import ssl
import zipfile
from urllib.request import urlretrieve
from datetime import date
import os
import shutil
from bs4 import BeautifulSoup
import tarfile

# Download the latest AACT database HTML page
BASE_URL = 'http://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/latest/'
r = requests.get(BASE_URL)
soup = BeautifulSoup(r.content, 'html.parser')

# Find all the necessary URLs
postgres_url = ''
h5_url = ''
postgres_file_name = ''
h5_file_name = ''

for tag in soup.find_all('a'):
    if 'postgresql' in tag.get('href'):
        postgres_url = BASE_URL + tag.get('href').strip()
        postgres_file_name = tag.get('href').strip()

    if '.h5' in tag.get('href'):
        h5_url = BASE_URL + tag.get('href').strip()
        h5_file_name = tag.get('href').strip()

print('Postgres URL:', postgres_url)
print('H5 URL:', h5_url)

# Download the file using urllib
ssl._create_default_https_context = ssl._create_stdlib_context

print('Downloading file:', postgres_url)
urlretrieve(postgres_url, postgres_file_name)

print('Downloading file:', h5_url)
urlretrieve(h5_url, h5_file_name)

print('Downloaded files:', postgres_file_name, h5_file_name)

# Unzip the file
print('Unzipping file:', postgres_file_name)
with tarfile.open(postgres_file_name, 'r:gz') as tar_ref:
    tar_ref.extractall('postgres')


# Move the postgres dump file to the root folder
dump_file_path = 'chembl_postgres.dmp'
h5_file_path = 'chembl.h5'
unzipped_dump_file_path = 'postgres/' + os.listdir('postgres')[0] + '/' + os.listdir('postgres/' + os.listdir('postgres')[0])[0] + '/' + postgres_file_name.split('.')[0] + '.dmp'

shutil.move(unzipped_dump_file_path, dump_file_path)
shutil.copyfile(h5_file_name, h5_file_path)

