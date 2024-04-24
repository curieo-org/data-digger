package org.curieo.driver;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import org.curieo.consumer.*;
import org.curieo.model.Record;
import org.curieo.utils.Config;
import org.junit.jupiter.api.Test;

class DataLoaderTests {

  @Test
  void testPostgres() throws IOException, SQLException {
    // CREATE DATABASE pubmed
    // CREATE SCHEMA IF NOT EXISTS datadigger AUTHORIZATION datadigger;

    Config config = new Config();
    String path = config.corpora_folder_path + "pubmed24n1307.xml.gz";

    PostgreSQLClient client = PostgreSQLClient.getPostgreSQLClient(config);
    SQLSinkFactory sqlSinkFactory = new SQLSinkFactory(client, 100, false);
    Sink<Record> sink = new MapSink<>(Record::toAuthorships, sqlSinkFactory.createAuthorshipSink());
    DataLoader dataLoader = new DataLoader(0, 3000, "pubmed", sink);
    dataLoader.processFile(new File(path), "pubmed24n1307.xml.gz");
  }
}
