
set -x

# get the current directory
CONFIGDIR=$(pwd)/config

# get the paths to the relevant jars
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )" # https://stackoverflow.com/questions/59895/getting-the-source-directory-of-a-bash-script-from-within
LOGJAR=$CONFIGDIR/slf4j-simple-2.0.11.jar
JAR="$(ls $DIR/../target/data-digger-*-jar-with-dependencies.jar)"

# compose the command for pubmed loading
CMD="java -cp $JAR:$LOGJAR -Xmx64G org.curieo.driver.DataLoader"
CREDS=$CONFIGDIR/credentials.json
STATUS=$CONFIGDIR/status.json
ARGS="-c $CREDS -f 2018 -i search-curieo -d pubmed -t $STATUS -e http://127.0.0.1:5000/embed"


# This is a very serious hack that will fix the logger.
# what happens is that an out-of-date logging configuration creeps into the jar and 
# is mistaken for the actual configuration
# if we remove it, it is fixed.
zip -d $JAR META-INF/org/apache/logging/log4j/core/config/plugins/Log4j2Plugins.dat

case $1 in
	pubmed-2-elastic)
        # execute
        $CMD $ARGS
    ;;

    pubmed-updates-2-elastic)
        echo "Pubmed updates to Elastic"
    ;;

    pubmed-baseline-2-postgres)
        echo "Pubmed baseline (full records) to postgres"
        STATUS=$CONFIGDIR/baseline-status.json
        POSTGRESUSER=datadigger
        ARGS="-c $CREDS -d pubmed -t $STATUS -p $POSTGRESUSER --full-records --references pubmed  --batch-size 100"
        $CMD $ARGS
    ;;

    pubmed-updates-2-postgres)
        echo "Pubmed updates (full records) to postgres"
        STATUS=$CONFIGDIR/updates-status.json
        POSTGRESUSER=datadigger
        STORE_LINKS="--link-table pubmed=pmc pubmed=doi"
        $CMD $ARGS
    ;;

    # testing with different batch sizes
    pubmed-updates-2-postgres-20-100)
        echo "Pubmed updates (full records) to postgres"
        STATUS=$CONFIGDIR/updates-status.json
        POSTGRESUSER=datadigger
        ARGS="-c $CREDS -d pubmed-updates -t $STATUS -p $POSTGRESUSER --full-records --maximum-files 20 --batch-size 100"
        $CMD $ARGS
    ;;

    # testing with different batch sizes
    pubmed-updates-2-postgres-20-1000)
        echo "Pubmed updates (full records) to postgres"
        STATUS=$CONFIGDIR/updates-status.json
        POSTGRESUSER=datadigger
        ARGS="-c $CREDS -d pubmed-updates -t $STATUS -p $POSTGRESUSER --full-records --maximum-files 20 --batch-size 1000"
        $CMD $ARGS
    ;;

    pubmed-updates-2-both)
        echo "Pubmed updates to Elastic and postgres"
        STATUS=$CONFIGDIR/updates-status.json
        POSTGRESUSER=datadigger
        ARGS="-c $CREDS -f 2018 -i search-curieo -d pubmed-updates -t $STATUS -e http://127.0.0.1:5000/embed -p $POSTGRESUSER"
        $CMD $ARGS
    ;;
    
    *)
        echo "No operation selected."
    ;;
esac
