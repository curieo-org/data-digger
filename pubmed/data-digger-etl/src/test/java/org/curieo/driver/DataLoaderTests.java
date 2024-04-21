package org.curieo.driver;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import org.curieo.consumer.MultiSink;
import org.curieo.consumer.PostgreSQLClient;
import org.curieo.consumer.SQLSinkFactory;
import org.curieo.consumer.Sink;
import org.curieo.model.Record;
import org.curieo.utils.Config;
import org.curieo.utils.Credentials;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class DataLoaderTests {

  @Test
  @Disabled
  void test() throws IOException {
    Credentials credentials = Credentials.defaults();
    String path = Config.CORPORA_FOLDER + "pubmed24n1307.xml.gz";
    Sink<Record> esink = DataLoader.getElasticConsumer(credentials, "search-curieo", null);
    DataLoader dataLoader = new DataLoader(0, 3000, "pubmed", esink);
    dataLoader.processFile(new File(path), "pubmed24n1307.xml.gz");
  }

  @Test
  void testPostgres() throws IOException, SQLException {
    // CREATE DATABASE pubmed
    // CREATE SCHEMA IF NOT EXISTS datadigger AUTHORIZATION datadigger;

    Credentials credentials = Credentials.defaults();
    String path = Config.CORPORA_FOLDER + "pubmed24n1307.xml.gz";

    PostgreSQLClient client = PostgreSQLClient.getPostgreSQLClient(credentials, "datadigger");
    SQLSinkFactory sqlSinkFactory = new SQLSinkFactory(client.getConnection(), 100, false);
    Sink<Record> sink =
        new MultiSink<>(Record::toAuthorships, sqlSinkFactory.createAuthorshipSink());
    DataLoader dataLoader = new DataLoader(0, 3000, "pubmed", sink);
    dataLoader.processFile(new File(path), "pubmed24n1307.xml.gz");
  }
}
