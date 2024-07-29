# Update Full Text PMC XML

## Table of Contents
- [Update Full Text PMC XML](#update-full-text-pmc-xml)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Steps to update the full text PMC XML](#steps-to-update-the-full-text-pmc-xml)
  - [FTP Folders](#ftp-folders)
  - [Usage](#usage)
  - [Deployment](#deployment)
  - [Future Scope](#future-scope)

## Introduction
In the [Pubmed FTP](https://ftp.ncbi.nlm.nih.gov/pub/pmc/), there are a large volume of full text articles in XML format. These articles storage has three parts:
1.` *.tar.gz` files: Here are the group of full text articles in XML format. These files are updated daily.
2. `*.filelist.csv` files: These files contain the pubmed ids, pmc ids, article location in the tar.gz files, and few other properties.
3. `*.filelist.txt` files: These files are same as *.filelist.csv files, but in a text format.

As `*.tar.gz` file size is large, it is not feasible to sync them with the help of code. So, we are syncing them with the help of shell script and bulk download from the FTP server.

## Steps to update the full text PMC XML
1. Process the `*.filelist.txt` files from [code](../data-digger-etl/README.md)
2. Fetch the list of files to be downloaded from the FTP server filtered last 3 days.
3. Download the files from the FTP server.
4. Untar the files and store them in the respective directories.
5. Transfer the files to the S3 bucket.

## FTP Folders
1. [Commercial](https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_comm/xml/)
2. [Non Commercial](https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_noncomm/xml/)
3. [Other](https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_other/xml/)

## Usage
```bash
brew install aws-cli
# Setup the aws credentials in the ~/.aws/credentials file

# Run the script
./update-fulltext-xmls.sh

# Configure the following line for the last `X` days if required
last_x_days=
```

## Deployment
```bash
# Update the following line in the Makefile for the TAG
TAG = 

# Build and upload the image to the ECR
make

# Change the TAG in the deployment/update_fulltext_xml_job.yaml file in the root directory
image: 698471419283.dkr.ecr.eu-central-1.amazonaws.com/data-digger-pubmed-fulltext-xml:<TAG>

# Deploy a Cronjob in the Kubernetes from the root directory
kubectl apply -f deployments/update_fulltext_xml_job.yaml -n $NAMESPACE
```

## Future Scope
1. Research about licensing and terms of the [other](https://www.ncbi.nlm.nih.gov/pmc/tools/ftp/) folder. And if suitable, add the other folder to the script.
2. Add loggings, error handling, and auto retry mechanism in the script.
3. In place of only last `X` days, check for the files that are not synced and sync them.