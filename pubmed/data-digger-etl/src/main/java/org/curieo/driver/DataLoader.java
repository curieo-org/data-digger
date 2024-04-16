package org.curieo.driver;

import static org.curieo.retrieve.ftp.FTPProcessing.skipExtensions;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Result;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Locale;
import java.util.function.BiFunction;
import java.util.function.Function;
import lombok.Generated;
import lombok.Value;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.curieo.consumer.AsynchSink;
import org.curieo.consumer.CountingConsumer;
import org.curieo.consumer.MapperSink;
import org.curieo.consumer.MultiSink;
import org.curieo.consumer.PostgreSQLClient;
import org.curieo.consumer.SQLSinkFactory;
import org.curieo.consumer.Sink;
import org.curieo.elastic.Client;
import org.curieo.elastic.ElasticConsumer;
import org.curieo.embed.EmbeddingService;
import org.curieo.embed.SentenceEmbeddingService;
import org.curieo.model.Record;
import org.curieo.model.StandardRecord;
import org.curieo.model.StandardRecordWithEmbeddings;
import org.curieo.retrieve.ftp.FTPProcessing;
import org.curieo.sources.SourceReader;
import org.curieo.utils.Credentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * We need to: - keep track of what we downloaded - download some more - and then upload into the
 * search
 */
@Generated
@Value
public class DataLoader {
  public static final int LOGGING_INTERVAL = 1000;
  private static final Logger LOGGER = LoggerFactory.getLogger(DataLoader.class);

  // you can specify a year range that you want loaded.
  Integer firstYear;
  Integer lastYear;
  String sourceType;
  Sink<Record> sink;

  static Option postgresUser() {
    return Option.builder()
        .option("p")
        .longOpt("postgres-user")
        .hasArg()
        .desc("postgresql user")
        .build();
  }

  static Option credentialsOption() {
    return new Option("c", "credentials", true, "Path to credentials file");
  }

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

  public static void main(String[] args)
      throws ParseException, JsonProcessingException, IOException, SQLException {
    Option postgresuserOpt = postgresUser();
    Option credentialsOpt = credentialsOption();
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
            .addOption(credentialsOpt)
            .addOption(firstYearOption)
            .addOption(lastYearOption)
            .addOption(batchSizeOption)
            .addOption(new Option("i", "index", true, "index in elastic"))
            .addOption(new Option("e", "embeddings", true, "embeddings server URL"))
            .addOption(
                new Option(
                    "y", "source type", true, "source type - can currently only be \"pubmed\""))
            .addOption(
                new Option("d", "data-set", true, "data set to load (defined in credentials)"))
            .addOption(postgresuserOpt)
            .addOption(maxFiles)
            .addOption(new Option("f", "full-records", false, "full records to sql database"))
            .addOption(new Option("a", "authors", false, "authors to sql database"))
            .addOption(references)
            .addOption(linkTable)
            .addOption(useKeys)
            .addOption(new Option("t", "status-tracker", true, "path to file that tracks status"));
    CommandLineParser parser = new DefaultParser();
    CommandLine parse = parser.parse(options, args);
    String credpath =
        parse.getOptionValue(credentialsOpt, System.getenv("HOME") + "/.credentials.json");
    Credentials credentials = Credentials.read(new File(credpath));
    String application = parse.getOptionValue('d', "pubmed");
    String sourceType = parse.getOptionValue('y', SourceReader.PUBMED);
    String index = parse.getOptionValue('i');
    int maximumNumberOfRecords =
        parse.hasOption(maxFiles) ? getIntOption(parse, maxFiles) : Integer.MAX_VALUE;
    int batchSize =
        parse.hasOption(batchSizeOption)
            ? getIntOption(parse, batchSizeOption)
            : SQLSinkFactory.DEFAULT_BATCH_SIZE;

    SentenceEmbeddingService sentenceEmbeddingService = null;
    String embeddingsServiceUrl = parse.getOptionValue('e');
    if (embeddingsServiceUrl != null) {
      EmbeddingService embeddingService = new EmbeddingService(embeddingsServiceUrl, 1024);
      if (embeddingService.embed("Test sentence.") == null) {
        LOGGER.error("Embedding service not live at {}", embeddingsServiceUrl);
        System.exit(1);
      }
      sentenceEmbeddingService = new SentenceEmbeddingService(embeddingService);
    }

    Sink<Record> tsink = null;
    if (index != null) {
      Sink<Record> esink = getElasticConsumer(credentials, index, sentenceEmbeddingService);
      tsink = esink;
    }

    String postgresuser = parse.getOptionValue(postgresuserOpt, "datadigger");

    PostgreSQLClient postgreSQLClient = null;
    SQLSinkFactory sqlSinkFactory = null;
    if (postgresuser != null) {
      postgreSQLClient = PostgreSQLClient.getPostgreSQLClient(credentials, postgresuser);
      sqlSinkFactory =
          new SQLSinkFactory(postgreSQLClient.getConnection(), batchSize, parse.hasOption(useKeys));

      // store authorships
      if (parse.hasOption('a')) {
        Sink<Record> asink =
            new MultiSink<>(Record::toAuthorships, sqlSinkFactory.createAuthorshipSink());
        tsink = tsink == null ? asink : tsink.concatenate(asink);
      }
      // store references
      if (parse.hasOption(references)) {
        Sink<Record> asink =
            new MultiSink<>(
                Record::toReferences,
                sqlSinkFactory.createReferenceSink(parse.getOptionValues(references)));
        tsink = tsink == null ? asink : tsink.concatenate(asink);
      }
      // store full records
      if (parse.hasOption("full-records")) {
        Sink<Record> asink =
            new MapperSink<>(StandardRecord::copy, sqlSinkFactory.createRecordSink());
        tsink = tsink == null ? asink : tsink.concatenate(asink);
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
              new MapperSink<>(
                  r -> r.toLinks(st[0], st[1]), sqlSinkFactory.createLinkoutTable(st[0], st[1]));
          tsink = tsink == null ? asink : tsink.concatenate(asink);
        }
      }
    }

    final Sink<Record> sink = new AsynchSink<>(tsink);
    DataLoader loader =
        new DataLoader(
            getIntOption(parse, firstYearOption),
            getIntOption(parse, lastYearOption),
            sourceType,
            sink);

    if (!credentials.hasApplication(application)) {
      LOGGER.error("Cannot find application {} in {}", application, credpath);
      System.exit(1);
    }

    try (FTPProcessing ftpProcessing = new FTPProcessing(credentials, application)) {
      if (parse.getOptionValue('t') == null) {
        LOGGER.error("You must specify a status tracker file with option --status-tracker");
        System.exit(1);
      }
      File statusTracker = new File(parse.getOptionValue('t'));

      ftpProcessing.processRemoteDirectory(
          credentials.get(application, "remotepath"),
          statusTracker,
          skipExtensions("md5"),
          loader::processFile,
          maximumNumberOfRecords);
    }
    sink.finalCall();
    LOGGER.info(
        "Stored {} records, updated {} records", sink.getTotalCount(), sink.getUpdatedCount());

    if (postgreSQLClient != null) {
      postgreSQLClient.close();
    }
    System.exit(0);
  }

  public FTPProcessing.Status processFile(File file) {
    String path = file.getAbsolutePath();
    if (path.toLowerCase().endsWith(".xml.gz")) {
      try {
        long startTimeInMillis = System.currentTimeMillis();
        int count = 0, countRejected = 0;
        for (Record r : SourceReader.getReader(sourceType).read(file)) {
          count++;
          if (checkYear(r)) {
            sink.accept(r);
          } else {
            countRejected++;
          }
        }
        LOGGER.info("Seen {} records - rejected {} by year filter", count, countRejected);

        long endTimeInMillis = System.currentTimeMillis();
        LOGGER.info(
            "Processed file {}, {} records; in {} seconds averaging {} milliseconds/record",
            path,
            count,
            String.format(Locale.US, "%.1f", (float) (endTimeInMillis - startTimeInMillis) / 1000),
            String.format(
                Locale.US, "%.2f", (float) (endTimeInMillis - startTimeInMillis) / count));
        return FTPProcessing.Status.Success;
      } catch (Exception e) {
        LOGGER.error(String.format("Processed file %s", path), e);
        return FTPProcessing.Status.Error;
      }
    } else {
      return FTPProcessing.Status.Seen;
    }
  }

  public static Sink<Record> getElasticConsumer(
      Credentials credentials, String index, SentenceEmbeddingService sentenceEmbeddingService) {
    ElasticsearchClient client =
        new Client(
                credentials.get("elastic", "server"),
                Integer.parseInt(credentials.get("elastic", "port")),
                credentials.get("elastic", "fingerprint"),
                credentials.get("elastic", "user"),
                credentials.get("elastic", "password"))
            .getClient();

    Function<Record, StandardRecord> baseOperation = StandardRecord::copy;
    Function<Record, Result> elasticSink;

    if (sentenceEmbeddingService != null) {
      ElasticConsumer<StandardRecordWithEmbeddings> ec =
          new ElasticConsumer<>(client, index, StandardRecordWithEmbeddings::getIdentifier);
      elasticSink = baseOperation.andThen(sentenceEmbeddingService.andThen(ec));
    } else {
      elasticSink =
          baseOperation.andThen(new ElasticConsumer<>(client, index, Record::getIdentifier));
    }

    BiFunction<Result, Integer, String> formatter =
        (result, c) -> String.format("Uploaded %d items with result %s", c, result.name());
    return new CountingConsumer<>(LOGGING_INTERVAL, elasticSink, formatter);
  }

  private boolean checkYear(Record sr) {
    Integer year = sr.getYear();
    if (year == null) return false;
    return !((firstYear != null && year < firstYear) || (lastYear != null && year > lastYear));
  }

  static Integer getIntOption(CommandLine cmd, Option option) {
    if (!cmd.hasOption(option)) return null;
    return Integer.parseInt(cmd.getOptionValue(option));
  }
}
