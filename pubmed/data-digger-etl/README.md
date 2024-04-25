# data-digger-java

This repository takes care of parts of the content data feed that powers curieo baseline search.

There is a number of readers here, all of which must be run in live mode to keep the indexes up to date.

(c) 2024 Curieo Technologies BV  


## Setting up

## Configuration
Copy the `.env.template` file to `.env` in the root folder and configure the necessary environment variables. 

```sh
cp .env.template .env
```

### With Docker

If you are using local postgresql, use `host.docker.internal` in place of `127.0.0.1` in the postgres credentials.

```sh
# Create Docker container
docker build -t data-digger-pubmed .
docker run --name data-digger-pubmed -d data-digger-pubmed

# Baseline
docker exec -it data-digger-pubmed /bin/bash -c "./data-digger-etl/scripts/load-pubmed.sh pubmed-baseline-2-postgres"

# Updates
docker exec -it data-digger-pubmed /bin/bash -c "./data-digger-etl/scripts/load-pubmed.sh pubmed-updates-2-postgres"
```

### Without Docker

* Make sure you have a [Java JDK](https://jdk.java.net/21/) installed.
* For local build, make sure you have [Maven](https://maven.apache.org/install.html) installed.
_(Note that leaving variable values_ empty _in this file resets the value, so if you want to take the value from the system environment, comment out these lines rather than assign to empty.)_ 

* Building locally involves two steps:

```sh
# in the root folder (data-digger-java)
mvn clean install -DskipTests
# in the ETL folder (data-digger-java/data-digger-etl)
mvn package assembly:single -DskipTests
```

The last step will build a 50MB jar that is referenced in the `load-pubmed.sh` script.

## Running
From the root folder, run the `load-pubmed.sh` script. This script will scrape the FTP server for new files, parse the files, map the data, and store it in the database.

# Run the script
./data-digger-etl/scripts/load-pubmed.sh <OPTION>
```

Options:
1. `pubmed-baseline-2-postgres`
2. `pubmed-updates-2-postgres`
3. `pubmedcentral-test`
4. `pubmed-updates-2-postgres-20-1000`

## General Overview
The general purpose of this module is to retrieve data from any data source, map it to the right format, and then store it into data stores that are fit for downstream purposes.

![General Overview](../flow.png).

In this process, all components are designed to be interchangeable, although of course this is not true: not all sources are eligible to be stored in all downstream data storages. But technically, the architecture is consistent.


### Scraping
We currently support scraping from FTP sources through the `FTPProcessing` class. Filtering by file extensions is done and we keep track in a local tracker file of which files we have seen on the remote location. On the _file_ level, this avoids loading a file twice. If you want to start with a clean slate, deleting the local tracker file will do that. 

### Parsing
Currently only pubmed parsing is supported. All records are supposedly implementing the "Record" interface.

### Mapping
We map `PubmedRecord` to `StandardRecord` for serialization purposes.

We map `PubmedRecord` to `Authorship` to be able to store authorships per record.

### Storage
Storage is encapsulated in `Sink` classes, which are an extension to `Consumer`, adding some extra admin handles. On _types_ of storage, see [Data Storage](#data_storage).


## Configuration
The script will scrape a remote handle to import data into the specified database.

See the [load-pubmed.sh](./scripts/load-pubmed.sh) script for the example.

## Content Sources
Now represented are:

- Pubmed

Under development are:
- Patent data (USPTO)


Data is normalized to the effect that all data fit into a single database.
We're aiming to first outdo [ChemSpider](https://www.chemspider.com/).


### Pubmed

Pubmed data is drawn from [FTP](https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/).
There is an annual baseline that must be augmented by daily updates.
This process needs daily runs to stay up-to-date.

### USPTO
Data from [https://bulkdata.uspto.gov/](https://bulkdata.uspto.gov/).

# Data Storage
<a id="data_storage"></a>
Into PostgreSQL currently.

## PostgreSQL

## S3
Downloading full-text is quite another story from the bulk-files that contain title-abstract-references records. 
Full-text downloads are different on at least two accounts:

1. **[1B1]** Full-text items are typically downloaded one-by-one (though there are exceptions, notably PLOS)
2. **[VOL]** Full-text items cannot reasonably be stored locally due to volume

How do we store and keep track of full-text records?
Each full text record has at least two properties of note:

1. type [JATS/PDF/...]
1. location (on S3)

The other attributes are _peripheral_ in the sense that one file can have multiple identifiers.
If we are downloading a PMC file it can possible be identified through DOI, pubmedid and PMCID. 

In storing in a database (postgresql) we keep the job-tracking and the storage of the files together, such that we can move both data and tracking around as a unit. With S3-downloads this is no longer feasible due to **[1B1]** - updating the S3 index every single download doubles the S3-access. Also, in S3-index-files, we _probably_ want to  decouple the identifier-file links, because _some_ files will have a PMC-identifier and _not_ a DOI-identifier _and_ the other way around. So the obvious solution to that is to have multiple index files stored in the S3-bucket, which now gets the following structure:

* ROOT
	* indexes
		* pubmed-index.csv
		* doi-index.csv
		* pmc-index.csv
	* data
		* 2024
			* ...
		* 2023
			* ...

Note that there _will_ be further structuring under data folders (below 2024 etc) as there will be _unfeasible_ numbers of files under these directories.


## Job tracking

Practical tracking of jobs and progress is done through a local Postgres database. This database can be synchronized with any and all remote indexes at any point. 

The job definition is a simple table of records with YEAR,IDENTIFIER,STATUS,LOCATION supplied to the algorithm that _knows_ how to deal with the identifier-type at hand (how to retrieve the file), where to store it and so on.

The synchronization of this table with the remote (S3) table is a separate job.





