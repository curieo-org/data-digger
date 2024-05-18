apk update
apk upgrade
apk add wget
apk add aws-cli
apk add postgresql

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

process_ftp_files() {
    ftp_files="$1"
    job_name="$2"
    files_to_be_updates=()
    
    for file in $ftp_files; do
        file_name=$(echo $file | awk -F'/' '{print $NF}')

        file_exists=$(psql -h $HOST -p $PORT -U $USERNAME -d $DATABASE -c "SELECT EXISTS (SELECT 1 FROM pmctasks WHERE name = '$file_name' AND job = '$job_name' AND state = $Completed);" | sed -n 3p | sed 's/ //g')
        
        if [ "$file_exists" == "f" ]; then
            echo "Downloading File: $file"

            query="INSERT INTO pmctasks (name, state, job, timestamp) VALUES ('$file_name', $Completed, '$job_name', now()) ON CONFLICT (name, job) DO UPDATE SET state = $InProgress, timestamp = now();"
            psql -h $HOST -p $PORT -U $USERNAME -d $DATABASE -c "$query"

            files_to_be_updates+=("$file_name")

            wget --ftp-user='anonymous' --ftp-password='anonymous' --continue $file
        fi
    done

    aws s3 cp ./ s3://pubmed-fulltext/bulk/ --recursive

    for file in *.tar.gz; do tar -zxf "$file"; done
    rm *.tar.gz
    aws s3 cp ./ s3://pubmed-fulltext/bulk/ --recursive

    rm -r *

    for file in "${files_to_be_updates[@]}"; do
        query="UPDATE pmctasks SET state = $Completed, timestamp = now() WHERE name = '$file' AND job = '$job_name';"
        psql -h $HOST -p $PORT -U $USERNAME -d $DATABASE -c "$query"
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