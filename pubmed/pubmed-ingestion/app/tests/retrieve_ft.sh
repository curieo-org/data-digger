
# The file 'locations.txt' was created locally by the following query on the datadigger database:

# SELECT pubmed, pmc, CONCAT('s3://pubmed-fulltext/bulk/PMC', 
# 			  SUBSTRING(LPAD(SUBSTRING(pmc, 4, 10), 9, '0'), 1, 3), 
# 			  'xxxxxx/', pmc, '.xml') AS FullTextLocation  FROM datadigger.linktable lt 
# JOIN datadigger.records r on lt.pubmed = r.identifier
# WHERE Year>2022 AND pmc LIKE 'PMC%'
# ORDER BY YEAR DESC
# LIMIT 200
#
# the output was exported to a text file (and \x0d characters removed) into 'remote_locations.csv'

awk -F, '{print $3}' remote_locations.csv | xargs -I echo > locations.txt

# all of the full-texts go into the /full_text/ folder
mkdir -p full_text 
cat locations.txt | while read line ; do aws s3 cp $line ./full_text/; done

# rm $TEMP_FILE
