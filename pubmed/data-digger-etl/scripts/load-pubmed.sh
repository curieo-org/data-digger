
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
        ARGS="-d updates --full-records --references pubmed --batch-size 100 --use-keys $STORE_LINKS"
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

    # testing with different batch sizes
    pubmed-updates-2-postgres-20-1000)
        echo "Pubmed updates (full records) to postgres"
        ARGS="-d updates --full-records --maximum-files 20 --batch-size 1000"
        $CMD $ARGS
    ;;
    
    *)
        echo "No operation selected."
    ;;
esac

echo "Process completed!"
