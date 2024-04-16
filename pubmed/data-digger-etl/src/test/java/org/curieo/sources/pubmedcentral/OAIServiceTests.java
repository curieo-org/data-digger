package org.curieo.sources.pubmedcentral;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import javax.xml.stream.XMLStreamException;
import org.curieo.sources.TarExtractor;
import org.junit.jupiter.api.Test;

class OAIServiceTests {

  @Test
  void testOAIService() throws IOException, XMLStreamException {
    FullText ft = new FullText("https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi");
    String pmcId = "PMC5334499";
    assertNotNull(ft.getRecord(pmcId));
    File tar = ft.getFullText(pmcId, "tgz");
    File desired =
        TarExtractor.getSingleFileOutOfTar(
            tar,
            tar.getAbsolutePath().toLowerCase().endsWith("gz"),
            f -> f.getAbsolutePath().toLowerCase().endsWith("xml"));
    assertNotNull(tar);
    assertNotNull(desired);
    tar.delete();
    assertTrue(desired.exists());
    desired.delete();
  }
}
