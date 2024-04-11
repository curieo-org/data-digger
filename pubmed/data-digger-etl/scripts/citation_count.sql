

/* 
    datadigger.referencetable  has 366K records.
    in order process these in a reasonable time, we're first grouping them by cited pubmed id
*/
SELECT pubmed AS PubmedId, COUNT(pubmed) AS citationcount
INTO datadigger.citationcountswithoutyear
FROM datadigger.referencetable 
GROUP BY pubmed

/* Query returned successfully in 6 min 26 secs. */
SELECT SUBSTRING(r.identifier, 1, LENGTH(r.identifier)-2) AS PubmedId, 
COALESCE(rt.citationcount, 0) citationcount, Year
INTO datadigger.citationcounts
FROM datadigger.records r LEFT JOIN datadigger.citationcountswithoutyear rt
ON SUBSTRING(r.identifier, 1, LENGTH(r.identifier)-2)=rt.pubmedid
GROUP BY SUBSTRING(r.identifier, 1, LENGTH(r.identifier)-2), 
COALESCE(rt.citationcount, 0), Year

/* Query returned successfully in 5 min 42 secs. */

SELECT SUBSTRING(r.identifier, 1, LENGTH(r.identifier)-2) AS PubmedId, Year
INTO datadigger.yeardata
FROM datadigger.records r 

CREATE INDEX idxYearData ON datadigger.yeardata (pubmedid)



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

