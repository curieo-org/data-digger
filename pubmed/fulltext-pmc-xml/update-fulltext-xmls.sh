apk update
apk upgrade
apk add wget
apk add aws-cli

mkdir pubmed-fulltext || true
cd pubmed-fulltext

process_ftp_files() {
    ftp_files="$1"
    
    for file in $ftp_files; do
        last_x_days=3

        for days_before in $(seq 0 $last_x_days); do
            current_date=$(date +%s)
            date_before=$(date -d @$((current_date - (days_before * 86400))) "+%Y-%m-%d")
            if [[ "$file" == *"$date_before"* ]]; then
                echo "Downloading File: $file"
                echo "Downloading Date: $date_before"
                wget --ftp-user='anonymous' --ftp-password='anonymous' --continue $file
            fi
        done
    done

    aws s3 cp ./ s3://pubmed-fulltext/bulk/ --recursive

    for file in *.tar.gz; do tar -zxf "$file"; done
    rm *.tar.gz
    aws s3 cp ./ s3://pubmed-fulltext/bulk/ --recursive

    rm -r *
}

# Process Comm Folder
comm_ftp_files=$(wget -q -O - ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_comm/xml/ | grep -Eo "href=\".*.tar.gz\"" | sed 's/href="//g' | sed 's/"//g')
process_ftp_files "$comm_ftp_files"

# Process Noncomm Folder
noncomm_ftp_files=$(wget -q -O - ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_noncomm/xml/ | grep -Eo "href=\".*.tar.gz\"" | sed 's/href="//g' | sed 's/"//g')
process_ftp_files "$noncomm_ftp_files"

cd ../
rm -r pubmed-fulltext