#!/bin/bash

mkdir pubmed-fulltext || true
cd pubmed-fulltext

# Environment Variables
# HOST=
# PORT=
# USERNAME=
# PASSWORD=
# DATABASE=

Queued=0
InProgress=1
Completed=2
Failed=3
Unavailable=4

export PGPASSWORD=$PASSWORD

process_ftp_files() {
    ftp_files="$1"
    job_name="$2"
    
    for file in $ftp_files; do
        file_name=$(echo $file | awk -F'/' '{print $NF}')

        file_exists=$(psql -h $HOST -p $PORT -U $USERNAME -d $DATABASE -c "SELECT EXISTS (SELECT 1 FROM pmctasks WHERE name = '$file_name' AND job = '$job_name' AND state = $Completed);" | sed -n 3p | sed 's/ //g')
        
        if [ "$file_exists" == "f" ]; then
            echo "Downloading File: $file"

            query="INSERT INTO pmctasks (name, state, job, timestamp) VALUES ('$file_name', $InProgress, '$job_name', now()) ON CONFLICT (name, job) DO UPDATE SET state = $InProgress, timestamp = now();"
            psql -h $HOST -p $PORT -U $USERNAME -d $DATABASE -c "$query"

            wget --ftp-user='anonymous' --ftp-password='anonymous' --continue $file

            aws s3 cp $file_name s3://pubmed-fulltext/bulk/
            tar -zxf $file_name
            rm $file_name
            aws s3 cp ./ s3://pubmed-fulltext/bulk/ --recursive
            rm -r *

            query="UPDATE pmctasks SET state = $Completed, timestamp = now() WHERE name = '$file_name' AND job = '$job_name';"
            psql -h $HOST -p $PORT -U $USERNAME -d $DATABASE -c "$query"
        fi
    done
}

# Process Comm Folder
comm_ftp_files=$(wget -q -O - ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_comm/xml/ | grep -Eo "href=\".*.tar.gz\"" | sed 's/href="//g' | sed 's/"//g')
process_ftp_files "$comm_ftp_files" "comm"

# Process Noncomm Folder
noncomm_ftp_files=$(wget -q -O - ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_noncomm/xml/ | grep -Eo "href=\".*.tar.gz\"" | sed 's/href="//g' | sed 's/"//g')
process_ftp_files "$noncomm_ftp_files" "noncomm"

cd ../
rm -r pubmed-fulltext