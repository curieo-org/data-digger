
SELECT reference AS PubmedId, COUNT(reference) AS citationcount
INTO citationcountswithoutyear
FROM referencetable
where reference_type == 0
GROUP BY reference
