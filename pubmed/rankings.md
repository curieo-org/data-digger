# Ranking articles by citation rate, and plotting citation rates per year

The postgres database contains two tables after the full data import for pubmed.

* records
* referencetable

After the process of computing citationcounts and percentiles, we have two extra tables:

* records
* referencetable
* citationcountswithoutyear
* citationcounts
* pubmed_percentiles

## Computing citation ranking

For the script, see [citations.ipynb](ranking/notebooks/citations.ipynb).

For determining the reputation / trustworthiness of an article, we compute the number of citations for each article.
We are proposing that articles with a certain citation rate that puts them above the _n_th percentile in 'popularity', are those articles that are most relevant.

By joining the above tables we can create a table that contains

<table><th><td>pubmedid</td><td>year</td><td>citationcount</td></th></table>

```sql
/* 
    datadigger.referencetable  has 366K records.
    in order process these in a reasonable time, we're first grouping them by cited pubmed id.

    (Query returned successfully in 6 min 26 secs.)
*/
SELECT pubmed AS PubmedId, COUNT(pubmed) AS citationcount
INTO datadigger.citationcountswithoutyear
FROM datadigger.referencetable 
GROUP BY pubmed

/* 
   Then we create the table citationcounts 
   The glitch is that the Records identifier is pubmedid + '00' - based on the original
   thinking that we would allow multiple data sources (which so far has not materialized).

   The `LEFT JOIN` combined with the `COALESCE` accounts for the fact that some articles are _never_ cited 
   and thus get citationcount = 0.
*/
SELECT SUBSTRING(r.identifier, 1, LENGTH(r.identifier)-2) AS PubmedId, 
COALESCE(rt.citationcount, 0) citationcount, Year
INTO datadigger.citationcounts
FROM datadigger.records r LEFT JOIN datadigger.citationcountswithoutyear rt
ON SUBSTRING(r.identifier, 1, LENGTH(r.identifier)-2)=rt.pubmedid
GROUP BY SUBSTRING(r.identifier, 1, LENGTH(r.identifier)-2), 
COALESCE(rt.citationcount, 0), Year
```

Aggregating this table on another level we can create a curve of citation count vs. year.
We are interested to see the citation count of articles that are in the top _n_ of that year.\
For that reason we want to compute the citation count that corresponds to the _percentiles_ for each year.
The computation of these percentiles _could be_ done with the below SQL statement.

```sql 

ALTER TABLE datadigger.citationcounts ADD COLUMN YearRank INT

WITH RankedCitationCounts AS 
(SELECT
    pubmedid,
    citationcount,
    ROUND(
        PERCENT_RANK() OVER (
			PARTITION BY Year
            ORDER BY citationcount
        ) 
	) percentile_rank
FROM
    datadigger.citationcounts)

UPDATE datadigger.citationcounts  
SET yearrank = rcc.percentile_rank
FROM datadigger.citationcounts cc JOIN RankedCitationCounts rcc
ON cc.pubmedid = rcc.pubmedid
```

The problem with this statement is that this statement does not complete within a reasonable time frame (after a few hours I gave up).
For this reason the program 'ranking' was written in rust. Run it as 

```sh
./ranking --credentials path-to-creds --database postgres-datadigger
```

And it will compute a table `pubmed_percentiles`.

With this table, we can split the documents of each year into percentiles and choose what cut-off to use for each year.

For example, to pick documents above the 15% percentile we do the following query.

```sql
SELECT r.* FROM datadigger.Records r JOIN 
    datadigger.citationcounts cc ON CONCAT(cc.pubmedid,'00') = r.identifier JOIN 
    datadigger.pubmed_percentiles p ON p.citationcount <= cc.citationcount
    WHERE p.percentile = 15
```

We can of course pick any percentile we want.