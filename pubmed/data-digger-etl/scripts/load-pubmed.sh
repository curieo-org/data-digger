
set -x

# get the paths to the relevant jars
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )" # https://stackoverflow.com/questions/59895/getting-the-source-directory-of-a-bash-script-from-within
JAR="$(ls $DIR/../target/data-digger-*-jar-with-dependencies.jar)"

# compose the command for pubmed loading
# If PROFILER_LIB is defined we add it as an agent.
if [[ -z "${PROFILER_LIB}" ]]; then
  CMD="java -cp $JAR -Xmx64G org.curieo.driver.DataLoader"
else
  CMD="java -cp $JAR -Xmx64G org.curieo.driver.DataLoader -agentpath:$PROFILER_LIB=start,event=cpu,file=profile.html"
fi

ARGS="-f 2018 -d pubmed-baseline"

case $1 in
    pubmed-baseline-2-postgres)
        echo "Pubmed baseline (full records) to postgres"
        STORE_LINKS="--link-table LinkTable:pubmed=pmc PubmedDOI:pubmed=doi"
        ARGS="-d pubmed-baseline --full-records --references pubmed --batch-size 100 --use-keys $STORE_LINKS"
        $CMD $ARGS
    ;;

    pubmed-updates-2-postgres)
        echo "Pubmed updates (full records) to postgres"
        STORE_LINKS="--link-table pubmed=pmc pubmed=doi"
        STORE_LINKS="--link-table LinkTable:pubmed=pmc PubmedDOI:pubmed=doi"
        ARGS="-d pubmed-updates -x pubmed-baseline --full-records --references pubmed --batch-size 100 --use-keys $STORE_LINKS"
        $CMD $ARGS
    ;;

    # testing with full-text
    pubmedcentral-test)
        echo "Pubmed Central Test"
        CMD="java -cp $JAR -Xmx64G org.curieo.driver.DataLoaderPMC"
        QUERY="--query SELECT PMC FROM linktable LIMIT 10"
        ARGS="$QUERY --table-name PMCFullText --use-keys "
        $CMD $ARGS
    ;;

    # PMC bulk comm folder
    pubmedcentral-bulk-comm)
        echo "Pubmed Central Bulk"
        CMD="java -cp $JAR -Xmx64G org.curieo.driver.DataLoaderPMC"
        ARGS="--bulk-processing comm --job-table-name pmctasks"
        $CMD $ARGS
    ;;

    # PMC bulk noncomm folder
    pubmedcentral-bulk-noncomm)
        echo "Pubmed Central Bulk"
        CMD="java -cp $JAR -Xmx64G org.curieo.driver.DataLoaderPMC"
        ARGS="--bulk-processing noncomm --job-table-name pmctasks"
        $CMD $ARGS
    ;;

    # PMC bulk other folder
    pubmedcentral-bulk-other)
        echo "Pubmed Central Bulk"
        CMD="java -cp $JAR -Xmx64G org.curieo.driver.DataLoaderPMC"
        ARGS="--bulk-processing other --job-table-name pmctasks"
        $CMD $ARGS
    ;;

    # retrieve pubmed central full text, seeded by the "linktable" produced during a Pubmed Run
    # the seed queries can (and must) change
    pubmedcentral-s3-seed)
        echo "Pubmed Central to S3 storage"
        # SEED_QUERY=$DIR/sql/prime-ft-pmc-download.sql
        CMD="java -cp $JAR -Xmx64G org.curieo.driver.DataLoaderPMC"
        # QUERY="--execute-query $SEED_QUERY"
        PREPROCESS=$DIR/sql/fulltext-pubmed-preprocessing.sql
        POSTPROCESS=$DIR/sql/fulltext-pubmed-postprocessing.sql
        QUERY="-u $PREPROCESS -v $POSTPROCESS"
        ARGS="$QUERY --job-table-name fulltextdownloads -x pubmed-updates --use-aws "
        $CMD $ARGS
    ;;

    # synchronize the status of pubmed central full text, uploads with the remote path
    pubmedcentral-s3-synchronize) 
        PM_QUERIES="$DIR/sql/drop-ft-pubmed-tasks.sql $DIR/sql/create-ft-pubmed-tasks.sql $DIR/sql/fill-ft-pubmed-tasks.sql"
        echo "Pubmed Central to S3 storage synchronization"
        CMD="java -cp $JAR -Xmx64G org.curieo.driver.DataLoaderPMC"
        QUERY="--execute-query $PM_QUERIES"
        # SYNCHRONIZE="--synchronize data/indexes/pmc-index.tsv"
        # ARGS="$SYNCHRONIZE --job-table-name fulltextdownloads"
        SYNCHRONIZE="--synchronize data/indexes/pubmed-index.tsv"
        ARGS="$SYNCHRONIZE $QUERY --job-table-name fulltextdownloads_pm -x fulltextdownloads"
        $CMD $ARGS
    ;;

    # testing with different batch sizes
    pubmed-updates-2-postgres-20-1000)
        echo "Pubmed updates (full records) to postgres"
        ARGS="-d pubmed-updates --full-records --maximum-files 20 --batch-size 1000"
        $CMD $ARGS
    ;;
    
    # compute citation count-based ranking
    pubmed-citation-counts)
        echo "Pubmed Citation Count Aggregation"
        SQL=$DIR/sql
        PM_QUERIES="$SQL/drop-citation-counts.sql $SQL/drop-citation-counts-without-year.sql $SQL/fill-citation-counts.sql $SQL/aggregate-citation-counts.sql"
        CMD="java -cp $JAR -Xmx64G org.curieo.driver.DataLoaderPMC"
        ARGS="--execute-query $PM_QUERIES"
        $CMD $ARGS
        $DIR/../../ranking/target/release/ranking
    ;;

    *)
        echo "No operation selected."
    ;;
esac

echo "Process completed!"
