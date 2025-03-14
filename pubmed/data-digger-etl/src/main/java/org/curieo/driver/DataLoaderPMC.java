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
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.curieo.consumer.AWSStorageSink;
import org.curieo.consumer.AsyncSink;
import org.curieo.consumer.PostgreSQLClient;
import org.curieo.consumer.S3Helpers;
import org.curieo.consumer.SQLSinkFactory;
import org.curieo.consumer.Sink;
import org.curieo.model.*;
import org.curieo.retrieve.ftp.FTPProcessing;
import org.curieo.retrieve.ftp.FTPProcessingFilter;
import org.curieo.sources.pubmedcentral.BulkFileHandler;
import org.curieo.sources.pubmedcentral.FullText;
import org.curieo.utils.Config;
import org.curieo.utils.TaskUtil;
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

  Sink<TS<FullTextTask>> tasksSink;
  Sink<FullTextRecord> sink;
  FullText fullTextHandler;
  AtomicInteger done;
  AtomicInteger filesSeen;
  int threadPoolSize;

  public static void main(String[] args)
      throws ParseException, IOException, SQLException, XMLStreamException, URISyntaxException {
    Options options =
        new Options()
            .addOption(batchSizeOption)
            .addOption(oaiOption)
            .addOption(queryOption)
            .addOption(taskTableOption)
            .addOption(previousJobOption)
            .addOption(synchronizeOption)
            .addOption(bulkProcessOption)
            .addOption(awsStorageOption)
            .addOption(tableNameOption)
            .addOption(executeQueryOption)
            .addOption(preprocessQueryOption)
            .addOption(postprocessQueryOption)
            .addOption(useKeysOption);
    CommandLineParser parser = new DefaultParser();
    CommandLine parse = parser.parse(options, args);
    int batchSize = getIntOption(parse, batchSizeOption).orElse(SQLSinkFactory.DEFAULT_BATCH_SIZE);
    Config config = new Config();

    try (PostgreSQLClient postgreSQLClient = PostgreSQLClient.getPostgreSQLClient(config)) {
      String previousJob = parse.getOptionValue(previousJobOption);

      if (previousJob != null) {
        boolean isCompleted = TaskUtil.checkPreviousJob(previousJob, postgreSQLClient);
        if (isCompleted) {
          LOGGER.info("Previous Job {} is completed", previousJob);
        } else {
          LOGGER.error("Previous Job {} is not completed", previousJob);
          System.exit(1);
        }
      }

      // do what needs to be done
      if (parse.hasOption(preprocessQueryOption)) {
        for (String filePath : parse.getOptionValues(preprocessQueryOption)) {
          String queries = new String(Files.readAllBytes(new File(filePath).toPath()), UTF_8);
          for (String q : queries.split(";")) {
            postgreSQLClient.execute(q);
          }
        }
      }
      if (parse.hasOption(executeQueryOption)) {
        for (String q : parse.getOptionValues(executeQueryOption)) {
          postgreSQLClient.execute(new String(Files.readAllBytes(new File(q).toPath()), UTF_8));
        }
      }
      SQLSinkFactory sqlSinkFactory =
          new SQLSinkFactory(postgreSQLClient, batchSize, parse.hasOption(useKeysOption));
      String query = null;
      if (parse.hasOption(queryOption)) {
        // LOGGER.error(
        //     "You did not specify the --query option -- for seeding the job, will resort to
        // --job-table-name option");
        query = String.join(" ", parse.getOptionValues(queryOption));
      }

      if (!parse.hasOption(taskTableOption)) {
        LOGGER.error("You did not specify the --job-table-name option -- sorry, need that");
        System.exit(1);
      }

      if (parse.getOptionValue(taskTableOption).equals("pmctasks")) {
        if (!parse.hasOption(bulkProcessOption)) {
          LOGGER.error("You did not specify the --bulk-process option -- sorry, need that");
          System.exit(1);
        }
        String job = "comm";
        String server = config.pubmed_ftp_server;
        String serverPath = config.pmc_comm_folder_path;

        switch (parse.getOptionValue(bulkProcessOption)) {
          case "comm":
            job = "comm";
            serverPath = config.pmc_comm_folder_path;
            break;
          case "noncomm":
            job = "noncomm";
            serverPath = config.pmc_noncomm_folder_path;
            break;
          case "other":
            job = "other";
            serverPath = config.pmc_other_folder_path;
            break;
          default:
            LOGGER.error("Unknown bulk process option");
            System.exit(1);
            break;
        }

        String tasksTable = parse.getOptionValue(taskTableOption);
        Sink<TS<PubmedTask>> tasksSink = sqlSinkFactory.createTasksSink(tasksTable);
        Map<String, TS<PubmedTask>> tasks =
            PostgreSQLClient.retrieveJobTasks(postgreSQLClient.getConnection(), tasksTable, job);
        Sink<PMCLocation> origin = sqlSinkFactory.createPMCRecordSink("pmc_location");

        BulkFileHandler fh =
            new BulkFileHandler(S3Helpers.getS3Client(config), config.aws_storage_bucket, origin);

        // copy TAR.GZ to S3 and track progress
        // populate pmc_origin table reading CSV from remote origin
        try (FTPProcessing ftpProcessing = new FTPProcessing(config, server)) {
          ftpProcessing.processRemoteDirectory(
              job,
              serverPath,
              tasks,
              tasksSink,
              FTPProcessingFilter.ValidExtensions(".txt"),
              fh::processBulkFile,
              Integer.MAX_VALUE);
        }

        // Calculate the location of the full-text files
        String[] queries = {
          "ALTER TABLE linktable ADD COLUMN IF NOT EXISTS location VARCHAR(300);",
          "UPDATE linktable lt SET location = pml.articlefile, timestamp = GREATEST(pml.lastupdate, lt.timestamp) from (SELECT p1.pmcid, p1.lastupdate, p2.articlefile FROM (SELECT pmcid, MAX(lastupdate) AS lastupdate FROM pmc_location GROUP BY pmcid) p1 INNER JOIN pmc_location p2 ON p1.pmcid = p2.pmcid AND p1.lastupdate = p2.lastupdate) as pml where pml.pmcid = lt.pmc;"
        };
        for (String q : queries) {
          postgreSQLClient.execute(q);
        }

        // figure out which pmc files in the pmc_origin table HAVE not yet been uploaded
        // these go into the pmc_tasks table as 'todo'. This is the bulk-extract task
        // read full-text-location and copy FT files one-by-one
        // go by the pmc_tasks that are 'extract job
        // read pmcthe pmc_origin table, order by container
        // if the container has not been downloaded, download
        // extract file
        // upload to S3
      } else {
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
          FullText ft = new FullText(parse.getOptionValue(oaiOption, FullText.OAI_SERVICE));
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

          new DataLoaderPMC(
                  tasksSink,
                  sink,
                  ft,
                  new AtomicInteger(0),
                  new AtomicInteger(0),
                  config.thread_pool_size)
              .processAllRecords(todo);

          sink.finalCall();
          LOGGER.info(
              "Stored {} records, updated {} records",
              sink.getTotalCount(),
              sink.getUpdatedCount());
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
          s3.synchronizeRemoteWithFullTextTable(
              postgreSQLClient.getConnection(), query, remotePath);
        } else if (sink == null) {
          throw new RuntimeException(
              "Either use --synchronize, or define at least 1 sink with --use-aws or --table-name ");
        }
      }

      if (parse.hasOption(postprocessQueryOption)) {
        for (String filePath : parse.getOptionValues(postprocessQueryOption)) {
          String queries = new String(Files.readAllBytes(new File(filePath).toPath()), UTF_8);
          for (String q : queries.split(";")) {
            postgreSQLClient.execute(q);
          }
        }
      }
    }

    System.exit(0);
  }

  private void processAllRecords(Map<String, TS<FullTextTask>> tasks) {
    Predicate<Map.Entry<String, TS<FullTextTask>>> needsWork =
        (entry) -> {
          TaskState.State state = entry.getValue().value().getTaskState();
          return (state == TaskState.State.Queued || state == TaskState.State.Failed);
        };

    final int tasksSize = tasks.size();
    Executor executor = Executors.newFixedThreadPool(threadPoolSize);
    final List<CompletableFuture<Void>> futures =
        tasks.entrySet().stream()
            .filter(needsWork)
            .map(
                entry -> {
                  return CompletableFuture.supplyAsync(() -> supplyJats(entry.getKey()), executor)
                      .thenAcceptAsync(
                          jats -> processJats(jats, entry.getKey(), entry.getValue(), tasksSize));
                })
            .toList();

    futures.forEach(CompletableFuture::join);
  }

  private void processJats(Response<String> jats, String key, TS<FullTextTask> ts, int tasksSize) {
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

  private Response<String> supplyJats(String key) {
    try {
      return fullTextHandler.getJats(key);
    } catch (Exception e) {
      throw new RuntimeException(e);
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
