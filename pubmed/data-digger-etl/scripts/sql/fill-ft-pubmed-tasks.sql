
INSERT INTO fulltextdownloads_pm (identifier, location, year, state, timestamp)
SELECT DISTINCT lt.pubmed, location, year, state, ft.timestamp FROM fulltextdownloads ft
JOIN linktable lt ON ft.identifier = lt.pmc
WHERE ft.state=2
