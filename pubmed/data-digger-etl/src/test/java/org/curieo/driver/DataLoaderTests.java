package org.curieo.driver;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import javax.xml.stream.XMLStreamException;

import org.curieo.consumer.MultiSink;
import org.curieo.consumer.PostgreSQLClient;
import org.curieo.consumer.SQLSinkFactory;
import org.curieo.consumer.Sink;
import org.curieo.model.Record;
import org.curieo.utils.Credentials;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

class DataLoaderTests {

	@Test @Disabled
	void test() throws IOException, XMLStreamException {
		Credentials credentials = Credentials.read(new File(System.getenv("HOME") + "/.credentials.json"));
		String path = System.getenv("HOME") + "/Documents/corpora/pubmed/pubmed24n0003.xml.gz";
		Sink<Record> esink = DataLoader.getElasticConsumer(credentials, "search-curieo", null);
		DataLoader dataLoader = new DataLoader();
		int loaded = dataLoader.loadData(path, "pubmed", esink);
		System.out.printf("Loaded: %d items", loaded);
	}
	
	@Test
	void testPostgres() throws JsonProcessingException, IOException, XMLStreamException, SQLException {
		// CREATE DATABASE pubmed
		// CREATE SCHEMA IF NOT EXISTS datadigger AUTHORIZATION datadigger;
		
		Credentials credentials = Credentials.read(new File(System.getenv("HOME") + "/.credentials.json"));
		String path = System.getenv("HOME") + "/Documents/corpora/pubmed/pubmed24n1223.xml";
		PostgreSQLClient client = DataLoader.getPostgreSQLClient(credentials, "datadigger");
		Sink<Record> sink = new MultiSink<>(Record::toAuthorships, 
				SQLSinkFactory.createAuthorshipSink(client.getConnection()));
		DataLoader dataLoader = new DataLoader();
		int loaded = dataLoader.loadData(path, "pubmed", sink);
		System.out.printf("Loaded: %d items", loaded);
	}
}
