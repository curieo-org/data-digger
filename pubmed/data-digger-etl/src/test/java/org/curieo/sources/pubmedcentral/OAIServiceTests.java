package org.curieo.sources.pubmedcentral;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.stream.XMLStreamException;
import org.curieo.model.Response;
import org.curieo.model.Response.Status;
import org.curieo.sources.TarExtractor;
import org.curieo.sources.pubmedcentral.FullText.Record;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class OAIServiceTests {

  @Test
  void testOAIService() throws IOException, XMLStreamException, URISyntaxException {
    FullText ft = new FullText(FullText.OAI_SERVICE);
    String pmcId = "PMC5334499";
    assertTrue(ft.getRecord(pmcId).ok());
    Response<File> tar = ft.getFullText(pmcId, "tgz");
    assertTrue(tar.ok());
    File desired =
        TarExtractor.getSingleFileOutOfTar(
            tar.value(),
            tar.value().getAbsolutePath().toLowerCase().endsWith("gz"),
            f -> f.getAbsolutePath().toLowerCase().endsWith("xml"));
    assertNotNull(desired);
    tar.value().delete();
    assertTrue(desired.exists());
    desired.delete();
    pmcId = "PMC4034166";
    assertEquals(Status.Unavailable, ft.getRecord(pmcId).status());
  }

  @Test
  @Disabled("This takes a while to run")
  void runStatistics() throws IOException, XMLStreamException, CsvException {
    List<String[]> lines = null;
    try (Reader reader =
            new InputStreamReader(this.getClass().getResourceAsStream("/random-pmc.csv"));
        CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1).build()) {
      lines = csvReader.readAll();
    }

    FullText ft = new FullText(FullText.OAI_SERVICE);
    Map<String, Integer> licenseCount = new HashMap<>();
    Map<String, Integer> linkCount = new HashMap<>();
    Map<String, Integer> formats = new HashMap<>();
    Map<String, Integer> found = new HashMap<>();
    int counter = 0;
    for (String[] line : lines) {
      String pmcId = line[0];
      counter += 1;
      if (counter % 100 == 0) System.out.printf("At record %d/%d%n", counter, lines.size());
      Response<Record> record = ft.getRecord(pmcId);
      if (!record.ok()) {
        found.merge("not found", 1, (a, b) -> a + b);
        continue;
      }
      found.merge("found", 1, (a, b) -> a + b);
      licenseCount.merge(record.value().getLicense(), 1, (a, b) -> a + b);
      /*
      for (FullText.Link link : record.getLinks()) {
        linkCount.merge(link.getFormat(), 1, (a, b) -> a + b);
        if (link.getFormat().equals(FullText.GZIPPED_TAR_FORMAT)) {
          File tar = ft.retrieveFile(record.getId(), link);
          for (File file : TarExtractor.untarAndDelete(tar, true)) {
            String extension = "unknown";
            int ext = file.getName().lastIndexOf('.');
            if (ext != -1) {
              extension = file.getName().substring(ext + 1);
            }
            formats.merge(extension, 1, (a, b) -> a + b);
          }
        }
      }*/
    }
    printCounts("Found", found);
    printCounts("License", licenseCount);
    printCounts("Link   ", linkCount);
    printCounts("Format ", formats);
  }

  static void printCounts(String type, Map<String, Integer> counts) {

    for (Map.Entry<String, Integer> count : counts.entrySet()) {
      System.out.printf("%s %s : %d%n", type, count.getKey(), count.getValue());
    }
  }
}
