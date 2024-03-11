
set -x

# get the paths to the relevant jars
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )" # https://stackoverflow.com/questions/59895/getting-the-source-directory-of-a-bash-script-from-within
LOGJAR=$DIR/slf4j-simple-2.0.11.jar
JAR="$(ls $DIR/../target/data-digger-*-jar-with-dependencies.jar)"

# compose the command for pubmed loading
CMD="java -cp $JAR:$LOGJAR -Xmx16G org.curieo.driver.DataLoader"
CREDS=~/.credentials.json
STATUS=~/Documents/corpora/pubmed/status.json
ARGS="-c $CREDS -f 2018 -i search-curieo -d pubmed -t $STATUS -e http://127.0.0.1:5000/embed"


case $1 in
	pubmed-2-elastic)
        # execute
        $CMD $ARGS
    ;;

    pubmed-updates-2-elastic)
        echo "Pubmed updates to Elastic"
    ;;

    pubmed-updates-2-both)
        echo "Pubmed updates to Elastic and postgres"
        STATUS=~/Documents/corpora/pubmed/updates-status.json
        POSTGRESUSER=datadigger
        ARGS="-c $CREDS -f 2018 -i search-curieo -d pubmed-updates -t $STATUS -e http://127.0.0.1:5000/embed -p $POSTGRESUSER"
        $CMD $ARGS
    ;;
    
    *)
        echo "No operation selected."
    ;;
esac
