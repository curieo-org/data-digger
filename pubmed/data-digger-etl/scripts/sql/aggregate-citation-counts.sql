
select cast(r.identifier as varchar(30)) as identifier,
coalesce(rt.citationcount, 0) citationcount, year
into citationcounts
from records r left join citationcountswithoutyear rt
on cast(r.identifier as varchar(30)) = rt.reference
group by r.identifier,
coalesce(rt.citationcount, 0), year
