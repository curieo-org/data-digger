
SELECT pubmed AS PubmedId, COUNT(pubmed) AS citationcount
INTO citationcountswithoutyear
FROM referencetable 
GROUP BY pubmed
