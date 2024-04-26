
set -x

# get the paths to the relevant jars
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )" # https://stackoverflow.com/questions/59895/getting-the-source-directory-of-a-bash-script-from-within
JAR="$(ls $DIR/../target/data-digger-*-jar-with-dependencies.jar)"

# compose the command for pubmed loading
CMD="java -cp $JAR -Xmx64G org.curieo.driver.DataLoader"
ARGS="-f 2018 -d pubmed-baseline"

case $1 in
    pubmed-baseline-2-postgres)
        echo "Pubmed baseline (full records) to postgres"
        ARGS="-d pubmed-baseline --full-records --references pubmed --batch-size 100"
        $CMD $ARGS
    ;;

    pubmed-updates-2-postgres)
        echo "Pubmed updates (full records) to postgres"
        STORE_LINKS="--link-table pubmed=pmc pubmed=doi"
        STORE_LINKS="--link-table LinkTable:pubmed=pmc PubmedDOI:pubmed=doi"
        ARGS="-d pubmed-updates --full-records --references pubmed --batch-size 100 --use-keys $STORE_LINKS"
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

    # retrieve pubmed central full text, seeded by the "linktable" produced during a Pubmed Run
    # the seed queries can (and must) change
    pubmedcentral-s3-seed)
        echo "Pubmed Central to S3 storage"
        SEED_QUERY=$DIR/prime-ft-pmc-download.sql
        CMD="java -cp $JAR -Xmx64G org.curieo.driver.DataLoaderPMC"
        QUERY="--execute-query $SEED_QUERY"
        ARGS="$QUERY --job-table-name fulltextdownloads --use-aws "
        $CMD $ARGS
    ;;

    # synchronize the status of pubmed central full text, uploads with the remote path
    pubmedcentral-s3-synchronize) 
        PM_QUERIES="$DIR/create-ft-pubmed-tasks.sql $DIR/fill-ft-pubmed-tasks.sql"
        echo "Pubmed Central to S3 storage synchronization"
        CMD="java -cp $JAR -Xmx64G org.curieo.driver.DataLoaderPMC"
        QUERY="--execute-query $PM_QUERIES"
        SYNCHRONIZE="--synchronize data/indexes/pmc-index.tsv"
        ARGS="$SYNCHRONIZE --job-table-name fulltextdownloads"
        SYNCHRONIZE="--synchronize data/indexes/pubmed-index.tsv"
        ARGS="$SYNCHRONIZE $QUERY --job-table-name fulltextdownloads_pm"
        $CMD $ARGS
    ;;

    # testing with different batch sizes
    pubmed-updates-2-postgres-20-1000)
        echo "Pubmed updates (full records) to postgres"
        ARGS="-d pubmed-updates --full-records --maximum-files 20 --batch-size 1000"
        $CMD $ARGS
    ;;
    
    *)
        echo "No operation selected."
    ;;
esac

echo "Process completed!"
