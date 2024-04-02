# Import the required libraries
import requests
import ssl
import zipfile
from urllib.request import urlretrieve
from datetime import date
import os
import shutil
from bs4 import BeautifulSoup


async def scrape_clinical_trial_database():
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
    target_file = 'assets/postgres.dmp'
    shutil.copy(os.path.join(unzipped_folder_name, database_file), target_file)

    # Remove the zip file and the unzipped folder
    os.remove(download_file_name)
    shutil.rmtree(unzipped_folder_name)


async def setup_local_database():
    # Set up the database
    print('Setting up the database in docker postgres container')

    os.system('chmod +x assets/local_database_setup.sh')
    exit_code = os.system('./assets/local_database_setup.sh')

    if exit_code != 0:
        raise Exception('Local database setup failed')
    print('Database setup completed')


async def remove_local_database():
    # Remove the database
    print('Removing the database in docker postgres container')

    os.system('chmod +x assets/local_database_remove.sh')
    exit_code = os.system('./assets/local_database_remove.sh')

    if exit_code != 0:
        raise Exception('Local database removal failed')
    print('Database removal completed')