#!/bin/bash
set -eo pipefail

aact_url="https://aact.ctti-clinicaltrials.org/snapshots"

available_data_urls=$(wget $aact_url -q -O - | sed -n 's/.*"\(https:\/\/ctti-aact.nyc3.digitaloceanspaces.com[^"]*\).*/\1/p')

if [ -z "$available_data_urls" ]; then
  echo "No urls found"
  exit 1
fi

# Download the first url
latest_url=$(echo $available_data_urls | awk '{print $1}')
latest_filename=$(echo $latest_url | sed 's/.*\///')

mkdir -p aact_data || true
cd aact_data
echo "Downloading $latest_filename from $latest_url"

wget $latest_url
unzip $latest_filename

# Environment variables
# HOST=
# PORT=
# USERNAME=
# PASSWORD=

export PGPASSWORD=$PASSWORD

psql -h $HOST -p $PORT -U $USERNAME -c "DROP DATABASE aact;" || true
psql -h $HOST -p $PORT -U $USERNAME -c "CREATE DATABASE aact;"
pg_restore --no-owner --no-privileges -h $HOST -d aact -U $USERNAME postgres.dmp
psql -h $HOST -p $PORT -U $USERNAME -d aact -f ../queries.sql

cd ..
rm -r aact_data