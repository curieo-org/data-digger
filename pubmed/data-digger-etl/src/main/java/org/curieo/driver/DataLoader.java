package org.curieo.driver;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.xml.stream.XMLStreamException;

import org.curieo.sources.SourceReader;
import org.curieo.utils.Credentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.curieo.consumer.CountingConsumer;
import org.curieo.consumer.MultiSink;
import org.curieo.consumer.PostgreSQLClient;
import org.curieo.consumer.PostgreSQLSink;
import org.curieo.consumer.Sink;
import org.curieo.elastic.Client;
import org.curieo.elastic.ElasticConsumer;
import org.curieo.embed.EmbeddingService;
import org.curieo.embed.SentenceEmbeddingService;
import org.curieo.model.Record;
import org.curieo.model.StandardRecord;
import org.curieo.model.StandardRecordWithEmbeddings;
import org.curieo.retrieve.ftp.FTPProcessing;
import static org.curieo.retrieve.ftp.FTPProcessing.skipExtensions;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Result;
import lombok.Builder;
import lombok.Generated;
import lombok.Value;

/**
 * We need to:
 * - keep track of what we downloaded
 * - download some more
 * - and then upload into the search
 */
@Generated @Value @Builder
public class DataLoader 
{
	public static final int LOGGING_INTERVAL = 1000;
    private static final Logger LOGGER = LoggerFactory.getLogger(DataLoader.class);
	
	// you can specify a year range that you want loaded.
	Integer firstYear;
	Integer lastYear;
    
	public static void main(String[] args) throws ParseException, JsonProcessingException, IOException, SQLException {
		Options options = new Options()
				.addOption(new Option("c", "credentials", true, "Path to credentials file"))
				.addOption(Option.builder().option("f")
										   .longOpt("first-year")
										   .hasArg().desc("first year (inclusive)")
										   .type(Integer.class).build())
				.addOption(Option.builder().option("l")
						   .longOpt("last-year")
						   .hasArg().desc("last year (inclusive)")
						   .type(Integer.class).build())
				.addOption(new Option("i", "index", true, "index in elastic"))
				.addOption(new Option("e", "embeddings", true, "embeddings server URL"))
				.addOption(new Option("y", "source type", true, "source type - can currently only be \"pubmed\""))
				.addOption(new Option("d", "data-set", true, "data set to load (defined in credentials)"))
				.addOption(new Option("p", "postgres-user", true, "postgresql user"))
				.addOption(new Option("t", "status-tracker", true, "path to file that tracks status"));
		CommandLineParser parser = new DefaultParser();
		CommandLine parse = parser.parse(options, args);
		String credpath = parse.getOptionValue('c', System.getenv("HOME") + "/.credentials.json");
		Credentials credentials = Credentials.read(new File(credpath));
		String application = parse.getOptionValue('d');
		String sourceType = parse.getOptionValue('y', SourceReader.PUBMED);
		String index = parse.getOptionValue('i', null);
		SentenceEmbeddingService sentenceEmbeddingService = null;
		String embeddingsServiceUrl = parse.getOptionValue('e');
		if (embeddingsServiceUrl != null) {
			EmbeddingService embeddingService = new EmbeddingService(embeddingsServiceUrl, 768);
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

		String postgresuser = parse.getOptionValue('p', null);
		PostgreSQLClient postgreSQLClient = null;
		if (postgresuser != null) {
			postgreSQLClient = getPostgreSQLClient(credentials, postgresuser);
			Sink<Record> asink = new MultiSink<>(Record::toAuthorships, 
					PostgreSQLSink.createAuthorshipSink(postgreSQLClient.getConnection()));
			tsink = tsink == null ? asink : tsink.concatenate(asink);
		}
		
		DataLoader loader = DataLoader.builder()
				.firstYear(getIntOption(parse, 'f'))
				.lastYear(getIntOption(parse, 'l')).build();
		
		if (!credentials.hasApplication(application)) {
			LOGGER.error("Cannot find application {} in {}", application, credpath);
			System.exit(1);
		}
		
		final Sink<Record> sink = tsink;		
		try (FTPProcessing ftpProcessing = new FTPProcessing(credentials, application)) {
			File statusTracker = new File(parse.getOptionValue('t'));
			ftpProcessing.processRemoteDirectory(credentials.get(application, "remotepath"), 
					statusTracker, skipExtensions("md5"),
					file -> {
				String path = file.getAbsolutePath();
				if (path.toLowerCase().endsWith(".xml.gz")) {
					int count = 0;
					try {
						count = loader.loadData(path, sourceType, sink);
						LOGGER.info("Processed file {}, {} records", path, count);
						return FTPProcessing.Status.Success;
					}
					catch (Exception e) {
						LOGGER.error(String.format("Processed file %s, exception %s", path), e);
						return FTPProcessing.Status.Error;
					}
				}
				else {
					return FTPProcessing.Status.Seen;
				}
			});
		}
		
		if (postgreSQLClient != null) {
			postgreSQLClient.close();
		}
		System.exit(0);
	}
	
    public int loadData(String path, String sourceType, Sink<Record> sink) throws IOException, XMLStreamException {
    	int count = 0, countRejected = 0;
    	for (Record r : SourceReader.getReader(sourceType).read(new File(path))) {
    		count++;
    		if (checkYear(r)) {
    			sink.accept(r);
    		}
    		else {
    			countRejected++;
    		}
    	}
    	sink.finalCall();
    	LOGGER.info("Seen {} records - rejected {} by year filter", count, countRejected);
    	return count;
    }
    
    public static PostgreSQLClient getPostgreSQLClient(Credentials credentials, String postgresuser) throws SQLException {
		postgresuser = "postgres-" + postgresuser;
		String user = credentials.need(postgresuser, "user");
		String database = credentials.need(postgresuser, "database");
		String password = credentials.need(postgresuser, "password");
		return new PostgreSQLClient(database, user, password);
    }
    
    public static Sink<Record> getElasticConsumer(
    		Credentials credentials, 
    		String index, 
    		SentenceEmbeddingService sentenceEmbeddingService) {
    	ElasticsearchClient client = 
    			new Client(credentials.get("elastic", "server"), 
    					   Integer.parseInt(credentials.get("elastic", "port")), 
    					   credentials.get("elastic", "fingerprint"), 
    					   credentials.get("elastic", "user"),
    					   credentials.get("elastic", "password")).getClient();

    	Function<Record, StandardRecord> baseOperation = StandardRecord::copy;
    	Function<Record, Result> elasticSink;

		if (sentenceEmbeddingService != null) {
			ElasticConsumer<StandardRecordWithEmbeddings> ec = new ElasticConsumer<>(client, index, StandardRecordWithEmbeddings::getIdentifier);
			elasticSink = baseOperation.andThen(sentenceEmbeddingService.andThen(ec));
		}
		else {
			elasticSink = baseOperation.andThen(new ElasticConsumer<>(client, index, Record::getIdentifier));
		}
  
    	BiFunction<Result, Integer, String> formatter = (result, c) -> String.format("Uploaded %d items with result %s", c, result.name());
    	return new CountingConsumer<>(LOGGING_INTERVAL, elasticSink, formatter);
    }
    
	private boolean checkYear(Record sr) {
		Integer year = sr.getYear();
		if (year == null) return false;
		return ! ((firstYear != null && year < firstYear) || 
				  (lastYear != null && year > lastYear));
	}
	
	private static Integer getIntOption(CommandLine cmd, char option) {
		if (!cmd.hasOption(option)) return null;
		return Integer.parseInt(cmd.getOptionValue(option));
	}
}
