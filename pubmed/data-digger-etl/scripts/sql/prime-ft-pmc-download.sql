-- this query loads the jobs table for full text downloads with NEW records from the link-table.
INSERT INTO fulltextdownloads (identifier, location, year, state, timestamp)
SELECT l.pmc, null, MAX(r.year), 0, MAX(l.timestamp) FROM 
linktable l JOIN records r ON r.identifier = l.pubmed
LEFT JOIN fulltextdownloads ft ON ft.identifier = l.pmc
WHERE ft.identifier IS NULL
GROUP BY l.pmc;
