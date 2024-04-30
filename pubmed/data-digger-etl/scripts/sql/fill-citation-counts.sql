
SELECT reference, reference_type, COUNT(reference) AS citationcount
INTO citationcountswithoutyear
FROM referencetable
GROUP BY reference, reference_type
