CREATE TABLE IF NOT EXISTS citationcountswithoutyear (
    id SERIAL PRIMARY KEY,
    reference VARCHAR(255),
    reference_type VARCHAR(255),
    citationcount INTEGER
);

INSERT INTO citationcountswithoutyear (reference, reference_type, citationcount) 
SELECT reference, reference_type, COUNT(reference) AS citationcount FROM referencetable GROUP BY reference, reference_type;

CREATE TABLE IF NOT EXISTS citationcounts (
    id SERIAL PRIMARY KEY,
    identifier BIGINT,
    citationcount INTEGER,
    year INTEGER
);

INSERT INTO citationcounts (identifier, citationcount, year)
select r.identifier as identifier,
coalesce(rt.citationcount, 0) citationcount, year
from records r left join citationcountswithoutyear rt
on cast(r.identifier as varchar(30)) = rt.reference
group by r.identifier,
coalesce(rt.citationcount, 0), year;