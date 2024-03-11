# data-digger-java

This repository takes care of parts of the content data feed that powers curieo baseline search.

There is a number of readers here, all of which must be run in live mode to keep the indexes up to date.

(c) 2024 MD for Curieo Technologies BV  



## Setting up

* Make sure you have a [Java JDK](https://jdk.java.net/21/) installed.
* For local build, make sure you have [Maven](https://maven.apache.org/install.html) installed.
* Building locally involves two steps:
	
```sh
# in the root folder (data-digger-java)
mvn clean install -DskipTests
# in the ETL folder (data-digger-java/data-digger-etl)
mvn package assembly:single
```

The last step will build a 50MB jar that is referenced in the `load-pubmed.sh` script.


## General Overview
The general purpose of this module is to retrieve data from any data source, map it to the right format, and then store it into data stores that are fit for downstream purposes.

![General Overview](./flow.png).

In this process, all components are designed to be interchangeable, although of course this is not true: not all sources are eligible to be stored in all downstream data storages. But technically, the architecture is consistent.


### Scraping
We currently support scraping from FTP sources through the `FTPProcessing` class. Filtering by file extensions is done and we keep track in a local tracker file of which files we have seen on the remote location. On the _file_ level, this avoids loading a file twice. If you want to start with a clean slate, deleting the local tracker file will do that. 

### Parsing
Currently only pubmed parsing is supported. All records are supposedly implementing the "Record" interface.

### Mapping
We map `PubmedRecord` to `StandardRecord` for serialization purposes.
We map `PubmedRecord` to `Authorship` to be able to store authorships per record.
We map `StandardRecord` to `Embeddings` through an embeddingsservice to store embeddings.

### Storage
Storage is encapsulated in `Consumer` classes (extended to `Sink` for some extra admin tasks).

* SQLConsumer : storing in database
* ElasticConsumer : storing in elastic search


## Configuration

* If you want to Set up and configure an Elastic Search Database, e.g. in a local docker or on a remote server.
* Credentials for all services call must be stored in a hidden `json` file in your root folder, `.credentials.json`. This file has the following format (e.g. for ElasticSearch connectivity)


```json
{
  "elastic" : {
    "url" : "http://127.0.0.1:9200",
    "server": "127.0.0.1",
    "port" : "9200",
    "apiKey" : "<SECRET>",
    "fingerprint": "<SECRET>",
    "user": "elastic",
    "password": "<SECRET>"
  },
  "pubmed": {
    "server" : "ftp.ncbi.nlm.nih.gov",
    "remotepath": "/pubmed/baseline/",
    "user" : "anonymous",
    "password" : "does not matter"
  }
}

```

The application will search for these credentials to access Elastic.

The script will scrape a remote handle to import data into the specified database.

See the [load-pubmed.sh](./scripts/load-pubmed.sh) script for the example.

The script maintains a status file that records progress on the overall scraping process.



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

# Data Feed
Into Elastic - currently.
