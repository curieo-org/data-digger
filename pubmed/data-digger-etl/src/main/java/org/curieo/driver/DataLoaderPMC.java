package org.curieo.driver;

import static org.curieo.driver.DataLoader.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Set;
import javax.xml.stream.XMLStreamException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.curieo.consumer.AsyncSink;
import org.curieo.consumer.PostgreSQLClient;
import org.curieo.consumer.SQLSinkFactory;
import org.curieo.consumer.Sink;
import org.curieo.model.FullTextRecord;
import org.curieo.model.Record;
import org.curieo.sources.pubmedcentral.FullText;
import org.curieo.utils.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * We need to: - keep track of what we downloaded - download some more - and then upload into the
 * search
 *
 * @param sourceType you can specify a year range that you want loaded.
 */
public record DataLoaderPMC(String sourceType, Sink<Record> sink) {
  public static final int LOGGING_INTERVAL = 1000;
  private static final Logger LOGGER = LoggerFactory.getLogger(DataLoaderPMC.class);
  private static final String OAI_SERVICE = "https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi";

  static Option queryOption() {
    return Option.builder()
        .option("q")
        .longOpt("query")
        .hasArgs()
        .desc("query for retrieving the PMCs that you want to download")
        .build();
  }

  static Option oaiOption() {
    return Option.builder().option("o").longOpt("oai-service").hasArg().desc("oai-service").build();
  }

  static Option tableNameOption() {
    return Option.builder()
        .option("t")
        .longOpt("table-name")
        .hasArg()
        .desc("table name for storing full text")
        .build();
  }

  public static void main(String[] args)
      throws ParseException, IOException, SQLException, XMLStreamException, URISyntaxException {
    Option postgresuserOpt = postgresUser();
    Option credentialsOpt = credentialsOption();
    Option batchSizeOption = batchSizeOption();
    Option oaiOption = oaiOption();
    Option useKeys = useKeysOption();
    Option queryOpt = queryOption();
    Option tableNameOpt = tableNameOption();
    Options options =
        new Options()
            .addOption(batchSizeOption)
            .addOption(oaiOption)
            .addOption(queryOpt)
            .addOption(tableNameOpt)
            .addOption(useKeys);
    CommandLineParser parser = new DefaultParser();
    CommandLine parse = parser.parse(options, args);
    Config config = new Config();
    int batchSize = getIntOption(parse, batchSizeOption).orElse(SQLSinkFactory.DEFAULT_BATCH_SIZE);

    FullText ft = new FullText(parse.getOptionValue(oaiOption, OAI_SERVICE));
    PostgreSQLClient postgreSQLClient = PostgreSQLClient.getPostgreSQLClient(config);
    SQLSinkFactory sqlSinkFactory =
        new SQLSinkFactory(postgreSQLClient, batchSize, parse.hasOption(useKeys));
    if (!parse.hasOption(queryOpt)) {
      LOGGER.error("You must specify the --query option");
      System.exit(1);
    }
    String query = String.join(" ", parse.getOptionValues("query"));
    Set<String> todo =
        PostgreSQLClient.retrieveSetOfStrings(postgreSQLClient.getConnection(), query);
    LOGGER.info(query);
    String tableName = parse.getOptionValue(tableNameOpt, "FullText");
    final Sink<FullTextRecord> sink = new AsyncSink<>(sqlSinkFactory.createPMCSink(tableName));
    for (String pmc : todo) {
      String content = ft.getJats(pmc);
      if (content != null) {
        sink.accept(new FullTextRecord(pmc, content));
      }
    }

    sink.finalCall();
    LOGGER.info(
        "Stored {} records, updated {} records", sink.getTotalCount(), sink.getUpdatedCount());

    postgreSQLClient.close();
    System.exit(0);
  }
}
