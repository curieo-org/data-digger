package org.curieo.sources.pubmedcentral;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import org.curieo.model.PMCRecord;
import org.junit.jupiter.api.Test;

class BulkTest {
  private static final String SAMPLE =
      "Article File,Article Citation,AccessionID,LastUpdated (YYYY-MM-DD HH:MM:SS),PMID,License,Retracted\n"
          + "PMC006xxxxxx/PMC6908519.xml,\"Front Oncol. 2019 Dec 6; 9:1306\",PMC6908519,\"2024-02-07 23:35:31\",31867269,\"CC BY\",no\n"
          + "PMC007xxxxxx/PMC7615599.xml,\"Animat Pract Process Prod. 2022 Jun 20; 11(1):81-107\",PMC7615599,\"2024-02-07 23:54:52\",38323037,\"CC BY\",no\n"
          + "PMC007xxxxxx/PMC7615600.xml,\"Neurobiol Dis. 2024 Jan 1; 190:106363\",PMC7615600,\"2024-02-07 23:54:54\",37996040,\"CC BY\",no\n"
          + "PMC007xxxxxx/PMC7615601.xml,\"J Vis Exp. 2023 Dec 8;(202):10.3791/66315\",PMC7615601,\"2024-02-07 23:54:54\",38145377,\"CC BY\",no\n"
          + "PMC007xxxxxx/PMC7615602.xml,\"J Vis Exp. 2023 Mar 24;(193):10.3791/65176\",PMC7615602,\"2024-02-07 23:54:54\",37036224,\"CC BY\",no";

  @Test
  void testCsv() throws FileNotFoundException, IOException {
    int numberOfRecords = 0;
    try (ByteArrayInputStream fis = new ByteArrayInputStream(SAMPLE.getBytes());
        Reader reader = new InputStreamReader(fis);
        CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1).build()) {
      for (String[] line : csvReader) {
        PMCRecord.fromCSV(null, line);
        numberOfRecords++;
      }
    }

    assertEquals(5, numberOfRecords);
  }
}
