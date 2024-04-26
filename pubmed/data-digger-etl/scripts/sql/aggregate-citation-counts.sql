
SELECT CAST(r.identifier AS VARCHAR(30)) AS PubmedId, 
COALESCE(rt.citationcount, 0) citationcount, Year
INTO citationcounts
FROM records r LEFT JOIN citationcountswithoutyear rt
ON CAST(r.identifier AS VARCHAR(30))=rt.pubmedid
GROUP BY r.identifier, 
COALESCE(rt.citationcount, 0), Year
