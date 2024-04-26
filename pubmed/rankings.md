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

Now, the Pubmed References are used for ranking.
The total sum of Pubmed references as of 2024 amounts to some 366K records, stored in the table `referencetable`.
In order to process these in a reasonable time, we're first grouping them by cited pubmed id.

This [query](./data-digger-etl/scripts/sql/fill-citation-counts.sql), executes (on M2, 2024):  successfully in approx. 7 minutes.

After that, we pull in the YEAR information, into the table `citationcounts` by combining the `records` table (which contains the year) with the raw citation counts with this [query](./data-digger-etl/scripts/sql/aggregate-citation-counts.sql). The other critical aspect of this query is that records that are *never* cited are *included* -- of course these records also contribute to the ranking.

   The `LEFT JOIN` combined with the `COALESCE` accounts for the fact that some articles are _never_ cited and thus get `citationcount = 0`.


Aggregating this table on yet another level we can create a curve of citation count vs. year.
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
For this reason the program 'ranking' was written in rust. It has all expectations about the database hard-coded, but reads configuration parameters from `.env`. Run it as 

```sh
./ranking
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