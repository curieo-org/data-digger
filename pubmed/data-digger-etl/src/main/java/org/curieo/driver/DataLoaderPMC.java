package org.curieo.driver;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.curieo.driver.OptionDefinitions.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.sql.ResultSet;
import java.sql.SQLException;
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
import org.curieo.model.*;
import org.curieo.model.Record;
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
              "query for seeding the tasks -- if not specified, it is reading from the job-table name")
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

  static Option taskTableOption =
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
            .addOption(taskTableOption)
            .addOption(synchronizeOption)
            .addOption(awsStorageOption)
            .addOption(tableNameOption)
            .addOption(executeQueryOption)
            .addOption(useKeysOption);
    CommandLineParser parser = new DefaultParser();
    CommandLine parse = parser.parse(options, args);
    int batchSize = getIntOption(parse, batchSizeOption).orElse(SQLSinkFactory.DEFAULT_BATCH_SIZE);
    Config config = new Config();

    FullText ft = new FullText(parse.getOptionValue(oaiOption, FullText.OAI_SERVICE));
    try (PostgreSQLClient postgreSQLClient = PostgreSQLClient.getPostgreSQLClient(config)) {

      // do what needs to be done
      if (parse.hasOption(executeQueryOption)) {
        for (String q : parse.getOptionValues(executeQueryOption)) {
          postgreSQLClient.execute(new String(Files.readAllBytes(new File(q).toPath()), UTF_8));
        }
      }
      SQLSinkFactory sqlSinkFactory =
          new SQLSinkFactory(postgreSQLClient, batchSize, parse.hasOption(useKeysOption));
      String query = null;
      if (!parse.hasOption(queryOption)) {
        LOGGER.error(
            "You did not specify the --query option -- for seeding the job, will resort to --job-table-name option");
      } else {
        query = String.join(" ", parse.getOptionValues(queryOption));
      }

      if (!parse.hasOption(taskTableOption)) {
        LOGGER.error("You did not specify the --job-table-name option -- sorry, need that");
        System.exit(1);
      }

      Sink<TS<FullTextTask>> tasksSink =
          sqlSinkFactory.createFullTextTasksSink(parse.getOptionValue(taskTableOption));

      Sink<FullTextRecord> sink = null;
      if (parse.hasOption(tableNameOption)) {
        String tableName = parse.getOptionValue(tableNameOption, "FullText");
        sink = new AsyncSink<>(sqlSinkFactory.createPMCSink(tableName));
      }
      if (parse.hasOption(awsStorageOption)) {
        if (sink != null) sink = sink.concatenate(new AsyncSink<>(new AWSStorageSink(config)));
        else sink = new AsyncSink<>(new AWSStorageSink(config));
      }
      if (sink != null) {
        if (query == null) {
          query =
              String.format(FULL_TEXT_JOB_QUERY_TEMPLATE, parse.getOptionValue(taskTableOption));
        }

        Map<String, TS<FullTextTask>> todo =
            PostgreSQLClient.retrieveItems(
                postgreSQLClient.getConnection(),
                query,
                DataLoaderPMC::mapFullTextJob,
                j -> j.value().getIdentifier());
        LOGGER.info(query);

        processAllRecords(todo, tasksSink, sink, ft);

        sink.finalCall();
        LOGGER.info(
            "Stored {} records, updated {} records", sink.getTotalCount(), sink.getUpdatedCount());
      }

      // synchronize
      if (parse.hasOption(synchronizeOption)) {
        query =
            String.format(
                FULL_TEXT_COMPLETED_QUERY_TEMPLATE,
                parse.getOptionValue(taskTableOption),
                TaskState.State.Completed.ordinal());
        String remotePath = parse.getOptionValue(synchronizeOption);
        Synchronize.S3 s3 = new Synchronize.S3(config);
        s3.synchronizeFullTextTableWithRemote(remotePath, tasksSink);
        s3.synchronizeRemoteWithFullTextTable(postgreSQLClient.getConnection(), query, remotePath);
      } else if (sink == null) {
        throw new RuntimeException(
            "Either use --synchronize, or define at least 1 sink with --use-aws or --table-name ");
      }
    }

    System.exit(0);
  }

  private static void processAllRecords(
      Map<String, TS<FullTextTask>> tasks,
      Sink<TS<FullTextTask>> tasksSink,
      Sink<FullTextRecord> sink,
      FullText ft) {
    AtomicInteger done = new AtomicInteger(0);
    AtomicInteger filesSeen = new AtomicInteger(0);
    Predicate<Map.Entry<String, TS<FullTextTask>>> needsWork =
        (entry) -> {
          TaskState.State state = entry.getValue().value().getTaskState();
          return (state == TaskState.State.Queued || state == TaskState.State.Failed);
        };

    final int tasksSize = tasks.size();
    Executor executor = Executors.newFixedThreadPool(1);
    final List<CompletableFuture<Void>> futures =
        tasks.entrySet().stream()
            .filter(needsWork)
            .map(
                entry -> {
                  return CompletableFuture.supplyAsync(
                          () -> supplyJats(ft, entry.getKey()), executor)
                      .thenAcceptAsync(
                          jats ->
                              processJats(
                                  jats,
                                  entry.getKey(),
                                  entry.getValue(),
                                  filesSeen,
                                  done,
                                  tasksSize,
                                  tasksSink,
                                  sink));
                })
            .toList();

    futures.forEach(CompletableFuture::join);
  }

  private static Response<String> supplyJats(FullText ft, String key) {
    try {
      return ft.getJats(key);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void processJats(
      Response<String> jats,
      String key,
      TS<FullTextTask> ts,
      AtomicInteger filesSeen,
      AtomicInteger done,
      int tasksSize,
      Sink<TS<FullTextTask>> tasksSink,
      Sink<FullTextRecord> sink) {
    {
      if (!jats.ok()) {
        LOGGER.error("Cannot retrieve file {}", key);
        tasksSink.accept(TS.of(ts.value().update(jats.state()), ts.timestamp()));
      } else {
        FullTextRecord ftr = new FullTextRecord(key, ts.value().getYear(), jats.value());
        String location = ftr.computeLocation();
        sink.accept(ftr);
        FullTextTask completed = ts.value().completed(location);
        tasksSink.accept(TS.of(completed, ts.timestamp()));
        LOGGER.info("Processed {}: state = {}", key, ts);
        filesSeen.getAndIncrement();
      }

      int currentDone = done.incrementAndGet();
      LOGGER.info(
          String.format(
              "Done %d/%d, at %.1f%%",
              currentDone, tasksSize, (float) 100 * currentDone / tasksSize));
    }
  }

  private static TS<FullTextTask> mapFullTextJob(ResultSet rs) throws SQLException {
    FullTextTask job =
        new FullTextTask(
            // name, location, year, state, timestamp
            rs.getString(1), rs.getString(2), rs.getInt(3), TaskState.State.fromInt(rs.getInt(4)));
    return new TS<>(job, rs.getTimestamp(5));
  }

  private static final String FULL_TEXT_JOB_QUERY_TEMPLATE =
      "SELECT identifier, location, year, state, timestamp FROM %s";
  private static final String FULL_TEXT_COMPLETED_QUERY_TEMPLATE =
      "SELECT identifier, location, year, state, timestamp FROM %s WHERE state = %d";
}
