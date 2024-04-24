package org.curieo.driver;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.curieo.consumer.*;
import org.curieo.model.*;
import org.curieo.model.Record;
import org.curieo.retrieve.ftp.FTPProcessing;
import org.curieo.retrieve.ftp.FTPProcessingFilter;
import org.curieo.sources.SourceReader;
import org.curieo.utils.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * We need to: - keep track of what we downloaded - download some more - and then upload into the
 * search
 *
 * @param firstYear you can specify a year range that you want loaded.
 */
public record DataLoader(
    Integer firstYear, Integer lastYear, String sourceType, Sink<Record> sink) {
  public static final int LOGGING_INTERVAL = 1000;
  private static final Logger LOGGER = LoggerFactory.getLogger(DataLoader.class);

  static Option batchSizeOption() {
    return Option.builder()
        .option("b")
        .longOpt("batch-size")
        .hasArg()
        .desc("the size of batches to be submitted to the SQL database")
        .type(Integer.class)
        .build();
  }

  static Option useKeysOption() {
    return Option.builder().option("k").longOpt("use-keys").required(false).build();
  }

  public static void main(String[] args) throws ParseException, IOException, SQLException {
    Option batchSizeOption = batchSizeOption();
    Option useKeys = useKeysOption();
    Option maxFiles =
        Option.builder()
            .option("m")
            .longOpt("maximum-files")
            .hasArg()
            .desc("maximum number of records to process")
            .type(Integer.class)
            .build();
    Option firstYearOption =
        Option.builder()
            .option("f")
            .longOpt("first-year")
            .hasArg()
            .desc("first year (inclusive)")
            .type(Integer.class)
            .build();
    Option lastYearOption =
        Option.builder()
            .option("l")
            .longOpt("last-year")
            .hasArg()
            .desc("last year (inclusive)")
            .type(Integer.class)
            .build();
    Option references =
        Option.builder()
            .option("r")
            .longOpt("references")
            .desc("references to sql database (specify types, e.g. \"pubmed\")")
            .required(false)
            .hasArgs()
            .build();
    Option linkTable =
        Option.builder()
            .option("l")
            .longOpt("link-table")
            .desc("create a link table for links between x=y")
            .required(false)
            .hasArgs()
            .build();

    Options options =
        new Options()
            .addOption(firstYearOption)
            .addOption(lastYearOption)
            .addOption(batchSizeOption)
            .addOption(
                new Option(
                    "y", "source type", true, "source type - can currently only be \"pubmed\""))
            .addOption(
                new Option("d", "data-set", true, "data set to load (defined in credentials)"))
            .addOption(maxFiles)
            .addOption(new Option("f", "full-records", false, "full records to sql database"))
            .addOption(new Option("a", "authors", false, "authors to sql database"))
            .addOption(references)
            .addOption(linkTable)
            .addOption(useKeys);
    CommandLineParser parser = new DefaultParser();
    CommandLine parse = parser.parse(options, args);
    Config config = new Config();
    String application = parse.getOptionValue('d', "baseline");
    String sourceType = parse.getOptionValue('y', SourceReader.PUBMED);
    int maximumNumberOfRecords = getIntOption(parse, maxFiles).orElse(Integer.MAX_VALUE);
    int batchSize = getIntOption(parse, batchSizeOption).orElse(SQLSinkFactory.DEFAULT_BATCH_SIZE);

    Sink<TS<Job>> jobsSink = new Sink.Noop<>();
    Sink<Record> tsink = new Sink.Noop<>();

    PostgreSQLClient postgreSQLClient = PostgreSQLClient.getPostgreSQLClient(config);
    SQLSinkFactory sqlSinkFactory =
        new SQLSinkFactory(postgreSQLClient, batchSize, parse.hasOption(useKeys));

    jobsSink = sqlSinkFactory.createJobsSink();
    // store authorships
    if (parse.hasOption('a')) {
      Sink<Record> asink =
          new MapSink<>(Record::toAuthorships, sqlSinkFactory.createAuthorshipSink());
      tsink = tsink.concatenate(asink);
    }
    // store references
    if (parse.hasOption(references)) {
      Sink<Record> asink =
          new MapSink<>(
              Record::toReferences,
              sqlSinkFactory.createReferenceSink(parse.getOptionValues(references)));
      tsink = tsink.concatenate(asink);
    }
    // store full records
    if (parse.hasOption("full-records")) {
      Sink<Record> asink = new MapSink<>(StandardRecord::copy, sqlSinkFactory.createRecordSink());
      tsink = tsink.concatenate(asink);
    }

    // store link table
    if (parse.hasOption(linkTable)) {
      String[] sourceTargets = parse.getOptionValues(linkTable);
      for (String sourceTarget : sourceTargets) {
        String[] st = sourceTarget.split("=");
        if (st.length != 2) {
          LOGGER.warn("Arguments to {} need to be of the shape A=B", linkTable.getLongOpt());
          System.exit(1);
        }
        Sink<Record> asink =
            new MapSink<>(
                r -> r.toLinks(st[0], st[1]), sqlSinkFactory.createLinkTable(st[0], st[1]));
        tsink = tsink.concatenate(asink);
      }
    }

    final Sink<Record> sink = new AsyncSink<>(tsink);
    DataLoader loader =
        new DataLoader(
            getIntOption(parse, firstYearOption).orElse(1500),
            getIntOption(parse, lastYearOption).orElse(3000),
            sourceType,
            sink);

    String remotePath = null;
    if (application.equals("baseline")) {
      remotePath = config.baseline_remote_path;
    } else if (application.equals("updates")) {
      remotePath = config.updates_remote_path;
    } else if (application.equals("commons")) {
      remotePath = config.commons_remote_path;
    } else {
      LOGGER.error("Cannot find application {} in environment", application);
      System.exit(1);
    }

    try (FTPProcessing ftpProcessing = new FTPProcessing(config)) {
      assert postgreSQLClient != null;
      Map<String, TS<Job>> jobs = PostgreSQLClient.retrieveJobs(postgreSQLClient.getConnection());

      ftpProcessing.processRemoteDirectory(
          remotePath,
          jobs,
          jobsSink,
          FTPProcessingFilter.ValidExtension(".xml.gz"),
          loader::processFile,
          maximumNumberOfRecords);
    }
    sink.finalCall();
    LOGGER.info(
        "Stored {} records, updated {} records", sink.getTotalCount(), sink.getUpdatedCount());

    postgreSQLClient.close();
    System.exit(0);
  }

  public FTPProcessing.Status processFile(File file, String name) {
    String path = file.getAbsolutePath();
    if (path.toLowerCase().endsWith(".xml.gz")) {
      AtomicInteger count = new AtomicInteger();
      AtomicInteger countRejected = new AtomicInteger();
      long startTimeInMillis = System.currentTimeMillis();

      try {
        final Iterable<Record> reader = SourceReader.getReader(sourceType).read(file, name);
        reader.forEach(
            r -> {
              count.getAndIncrement();
              if (checkYear(r)) {
                sink.accept(r);
              } else {
                countRejected.getAndIncrement();
              }
            });

        LOGGER.info("Seen {} records - rejected {} by year filter", count, countRejected);

        long endTimeInMillis = System.currentTimeMillis();
        LOGGER.info(
            "Processed file {}, {} records; in {} seconds averaging {} milliseconds/record",
            path,
            count,
            String.format(Locale.US, "%.1f", (float) (endTimeInMillis - startTimeInMillis) / 1000),
            String.format(
                Locale.US, "%.2f", (float) (endTimeInMillis - startTimeInMillis) / count.get()));

        return FTPProcessing.Status.Success;
      } catch (Exception e) {
        LOGGER.error(String.format("Failed to process file %s", path), e);
        return FTPProcessing.Status.Error;
      }
    }

    return FTPProcessing.Status.Seen;
  }

  private boolean checkYear(Record sr) {
    Integer year = sr.getYear();
    if (year == null) return false;
    return !((firstYear != null && year < firstYear) || (lastYear != null && year > lastYear));
  }

  static Optional<Integer> getIntOption(CommandLine cmd, Option option) {
    if (!cmd.hasOption(option)) return Optional.empty();
    try {
      return Optional.of(Integer.parseInt(cmd.getOptionValue(option)));
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }
}
