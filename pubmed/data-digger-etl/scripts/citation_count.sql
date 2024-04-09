

SELECT pubmed, count(pubmed) AS citationcount
INTO datadigger.citationcounts
FROM datadigger.referencetable
GROUP BY pubmed
/* Query returned successfully in 10 min 17 secs. */

SELECT SUBSTRING(r.identifier, 1, LENGTH(r.identifier)-1) AS PubmedId, COUNT(pubmed) AS citationcount, Year
INTO datadigger.citationcounts
FROM datadigger.records r LEFT JOIN datadigger.referencetable rt
ON SUBSTRING(r.identifier, 1, LENGTH(r.identifier)-1)=rt.pubmed
GROUP BY SUBSTRING(r.identifier, 1, LENGTH(r.identifier)-1), Year

/* Query returned successfully in 40 min 16 secs. */

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

