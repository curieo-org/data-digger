package org.curieo.driver;

import static org.curieo.driver.OptionDefinitions.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import javax.xml.stream.XMLStreamException;
import lombok.Generated;
import lombok.Value;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.curieo.consumer.AWSStorageSink;
import org.curieo.consumer.AsyncSink;
import org.curieo.consumer.PostgreSQLClient;
import org.curieo.consumer.SQLSinkFactory;
import org.curieo.consumer.Sink;
import org.curieo.model.FullTextJob;
import org.curieo.model.FullTextRecord;
import org.curieo.model.Job;
import org.curieo.model.Record;
import org.curieo.model.TS;
import org.curieo.sources.pubmedcentral.FullText;
import org.curieo.utils.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * We need to: - keep track of what we downloaded - download some more - and then upload into the
 * search
 */
@Generated
@Value
public class DataLoaderPMC {
  public static final int LOGGING_INTERVAL = 1000;
  private static final Logger LOGGER = LoggerFactory.getLogger(DataLoaderPMC.class);

  // you can specify a source type
  String sourceType;
  Sink<Record> sink;

  static Option queryOption =
      Option.builder()
          .option("q")
          .longOpt("query")
          .hasArgs()
          .desc(
              "query for seeding the jobs -- if not specified, it is reading from the job-table name")
          .build();

  static Option oaiOption =
      Option.builder().option("o").longOpt("oai-service").hasArg().desc("oai-service").build();

  static Option tableNameOption =
      Option.builder()
          .option("t")
          .longOpt("table-name")
          .hasArg()
          .desc("table name for storing full text")
          .build();

  static Option jobTableOption =
      Option.builder()
          .option("j")
          .longOpt("job-table-name")
          .hasArg()
          .desc("table name for storing job information")
          .build();

  public static void main(String[] args)
      throws ParseException, IOException, SQLException, XMLStreamException, URISyntaxException {
    Options options =
        new Options()
            .addOption(batchSizeOption)
            .addOption(oaiOption)
            .addOption(queryOption)
            .addOption(jobTableOption)
            .addOption(awsStorageOption)
            .addOption(tableNameOption)
            .addOption(useKeysOption);
    CommandLineParser parser = new DefaultParser();
    CommandLine parse = parser.parse(options, args);
    int batchSize = getIntOption(parse, batchSizeOption).orElse(SQLSinkFactory.DEFAULT_BATCH_SIZE);
    Config config = new Config();

    FullText ft = new FullText(parse.getOptionValue(oaiOption, FullText.OAI_SERVICE));
    try (PostgreSQLClient postgreSQLClient = PostgreSQLClient.getPostgreSQLClient(config)) {

      SQLSinkFactory sqlSinkFactory =
          new SQLSinkFactory(postgreSQLClient, batchSize, parse.hasOption(useKeysOption));
      String query = null;
      if (!parse.hasOption(queryOption)) {
        LOGGER.error(
            "You did not specify the --query option -- for seeding the job, will resort to --job-table-name option");
      } else {
        query = String.join(" ", parse.getOptionValues(queryOption));
      }

      if (!parse.hasOption(jobTableOption)) {
        LOGGER.error("You did not specify the --job-table-name option -- sorry, need that");
      }

      if (query == null) {
        query = String.format(FULL_TEXT_JOB_QUERY_TEMPLATE, parse.getOptionValue(jobTableOption));
      }

      Map<String, TS<FullTextJob>> todo =
          PostgreSQLClient.retrieveItems(
              postgreSQLClient.getConnection(),
              query,
              DataLoaderPMC::mapFullTextJob,
              j -> j.value().getIdentifier());
      LOGGER.info(query);
      Sink<TS<FullTextJob>> jobsSink =
          sqlSinkFactory.createFullTextJobsSink(parse.getOptionValue(jobTableOption));
      Sink<FullTextRecord> sink = null;
      if (parse.hasOption(tableNameOption)) {
        String tableName = parse.getOptionValue(tableNameOption, "FullText");
        sink = new AsyncSink<>(sqlSinkFactory.createPMCSink(tableName));
      }
      if (parse.hasOption(awsStorageOption)) {
        if (sink != null) sink = sink.concatenate(new AsyncSink<>(new AWSStorageSink(config)));
        else sink = new AsyncSink<>(new AWSStorageSink(config));
      }
      if (sink == null) {
        throw new RuntimeException("Define at least 1 sink with --use-aws or --table-name ");
      }

      processAllRecords(todo, jobsSink, sink, ft);

      sink.finalCall();
      LOGGER.info(
          "Stored {} records, updated {} records", sink.getTotalCount(), sink.getUpdatedCount());
    }

    System.exit(0);
  }

  private static void processAllRecords(
      Map<String, TS<FullTextJob>> jobs,
      Sink<TS<FullTextJob>> jobsSink,
      Sink<FullTextRecord> sink,
      FullText ft)
      throws IOException, XMLStreamException, URISyntaxException {
    AtomicInteger done = new AtomicInteger(0);
    AtomicInteger filesSeen = new AtomicInteger(0);
    Predicate<Map.Entry<String, TS<FullTextJob>>> needsWork =
        (entry) -> {
          Job.State state = entry.getValue().value().getJobState();
          return (state == Job.State.Queued || state == Job.State.Failed);
        };

    for (Map.Entry<String, TS<FullTextJob>> pmc : jobs.entrySet()) {
      if (needsWork.test(pmc)) {
        String content = ft.getJats(pmc.getKey());
        Integer year = pmc.getValue().value().getYear();
        if (content != null) {
          sink.accept(new FullTextRecord(pmc.getKey(), year, content));
        }
      }
    }

    final int jobsSize = jobs.size();
    Executor executor = Executors.newFixedThreadPool(10);
    final List<CompletableFuture<Void>> futures =
        jobs.entrySet().stream()
            .filter(needsWork)
            .map(
                entry -> {
                  TS<FullTextJob> ts = entry.getValue();
                  Timestamp timestamp = ts.timestamp();

                  return CompletableFuture.supplyAsync(
                          () -> supplyJats(ft, entry.getKey()), executor)
                      .thenAcceptAsync(
                          jats ->
                              processJats(
                                  jats,
                                  entry.getKey(),
                                  ts,
                                  timestamp,
                                  filesSeen,
                                  done,
                                  jobsSize,
                                  jobsSink,
                                  sink));
                })
            .toList();

    futures.forEach(CompletableFuture::join);
  }

  private static String supplyJats(FullText ft, String key) {
    try {
      return ft.getJats(key);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void processJats(
      String jats,
      String key,
      TS<FullTextJob> ts,
      Timestamp timestamp,
      AtomicInteger filesSeen,
      AtomicInteger done,
      int jobsSize,
      Sink<TS<FullTextJob>> jobsSink,
      Sink<FullTextRecord> sink) {
    {
      if (jats == null) {
        LOGGER.error("Cannot retrieve file {}", key);
        jobsSink.accept(TS.of(ts.value().failed(), timestamp));
      } else {
        FullTextRecord ftr = new FullTextRecord(key, ts.value().getYear(), jats);
        String location = ftr.computeLocation();
        sink.accept(ftr);
        FullTextJob completed = ts.value().completed(location);
        jobsSink.accept(TS.of(completed, timestamp));
        LOGGER.info("Processed {}: state = {}", key, ts);
        filesSeen.getAndIncrement();
      }

      int currentDone = done.incrementAndGet();
      LOGGER.info(
          String.format(
              "Done %d/%d, at %.1f%%",
              currentDone, jobsSize, (float) 100 * currentDone / jobsSize));
    }
  }

  private static TS<FullTextJob> mapFullTextJob(ResultSet rs) throws SQLException {
    FullTextJob job =
        new FullTextJob(
            // name, location, year, state, timestamp
            rs.getString(1), rs.getString(2), rs.getInt(3), Job.State.fromInt(rs.getInt(4)));
    return new TS<>(job, rs.getTimestamp(5));
  }

  private static final String FULL_TEXT_JOB_QUERY_TEMPLATE =
      "SELECT identifier, state, year, location, timestamp FROM %s";
}
