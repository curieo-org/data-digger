package org.curieo.sources.pubmedcentral;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import lombok.Generated;
import lombok.Value;
import org.curieo.retrieve.ftp.FTPProcessing;
import org.curieo.sources.TarExtractor;
import org.curieo.utils.URIHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record FullText(String oaiService) {
  public static final String GZIPPED_TAR_FORMAT = "tgz";
  public static final String XML_EXTENSION = "xml";
  public static final String OAI_SERVICE = "https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi";

  private static final Logger LOGGER = LoggerFactory.getLogger(FullText.class);
  private static XMLInputFactory XMLINPUTFACTORY = XMLInputFactory.newInstance();
  private static final QName ID_ATTRIBUTE = new QName("id");
  private static final QName CITATION_ATTRIBUTE = new QName("citation");
  private static final QName LICENSE_ATTRIBUTE = new QName("license");
  private static final QName RETRACTED_ATTRIBUTE = new QName("retracted");
  private static final QName FORMAT_ATTRIBUTE = new QName("format");
  private static final QName UPDATED_ATTRIBUTE = new QName("updated");
  private static final QName HREF_ATTRIBUTE = new QName("href");

  /**
   * Retrieve the contents of the first file with an extension ending in "xml" in a package
   * corresponding to a pubmed central identifier
   *
   * @param pmcId
   * @return full file contents read into a UTF_8 string
   * @throws IOException
   * @throws XMLStreamException
   */
  public String getJats(String pmcId) throws IOException, XMLStreamException, URISyntaxException {
    File file = getFullText(pmcId, GZIPPED_TAR_FORMAT);
    if (file == null) {
      LOGGER.warn("Cannot retrieve {}", pmcId);
      return null;
    }
    File target =
        TarExtractor.getSingleFileOutOfTar(
            file, true, a -> a.getAbsolutePath().toLowerCase().endsWith(XML_EXTENSION));
    file.delete();
    assert target != null;
    String content = Files.readString(target.toPath());
    target.delete();
    return content;
  }

  /**
   * Retrieve the file that corresponds to the link of the format requested ("tgz" or "pdf",
   * mostly).
   *
   * @param pmcId
   * @param format
   * @return a file object pointing to the downloaded file.
   * @throws IOException
   * @throws XMLStreamException
   */
  public File getFullText(String pmcId, String format)
      throws IOException, XMLStreamException, URISyntaxException {
    Record record = getRecord(pmcId);
    if (record == null) {
      return null;
    }
    Link link =
        record.links.stream().filter(l -> l.getFormat().equals(format)).findFirst().orElse(null);
    return retrieveFile(pmcId, link);
  }

  public File retrieveFile(String pmcId, Link link)
      throws IOException, XMLStreamException, URISyntaxException {
    File file = File.createTempFile(pmcId, link.getFormat());
    if (link.getHref().startsWith("ftp://")) {
      if (!FTPProcessing.retrieve(link.getHref(), file)) {
        LOGGER.warn("Could not download {} not available for PMC {}", link.getHref(), pmcId);
      }
    } else {
      URIHandler.writeHTTPURL(link.getHref(), file);
    }
    return file;
  }

  /**
   * get the informational record on a certain Pubmed Central identifier through the OAI API of the
   * NIH.
   *
   * @param pmcId
   * @return the information record (including links) about the PMC ID.
   * @throws IOException
   * @throws XMLStreamException
   */
  public Record getRecord(String pmcId) throws IOException, XMLStreamException {
    URL url = URI.create(oaiService).toURL(); // + "?id=" + pmcId);

    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod("GET");
    Map<String, String> parameters = new HashMap<>();
    parameters.put("id", pmcId);

    con.setDoOutput(true);
    try (DataOutputStream out = new DataOutputStream(con.getOutputStream())) {
      out.writeBytes(getParamsString(parameters));
    }

    // con.setRequestProperty("Content-Type", "application/json");
    int status = con.getResponseCode();
    if (status != 200) {
      LOGGER.warn("No response for PMC {}", pmcId);
      return null;
    }
    XMLEventReader reader = XMLINPUTFACTORY.createXMLEventReader(con.getInputStream());
    Record record = null;
    while (reader.hasNext()) {
      XMLEvent event = reader.nextEvent();
      if (event.isStartElement()) {
        StartElement startElement = event.asStartElement();
        switch (startElement.getName().getLocalPart()) {
          case "record":
            record =
                new Record(
                    getAttribute(startElement, ID_ATTRIBUTE),
                    getAttribute(startElement, CITATION_ATTRIBUTE),
                    getAttribute(startElement, LICENSE_ATTRIBUTE),
                    getAttribute(startElement, RETRACTED_ATTRIBUTE),
                    new ArrayList<>());
            break;
          case "link":
            if (record == null) {
              throw new IllegalArgumentException("Format of OAI record is corrupted");
            }
            record
                .getLinks()
                .add(
                    new Link(
                        getAttribute(startElement, FORMAT_ATTRIBUTE),
                        getAttribute(startElement, UPDATED_ATTRIBUTE),
                        getAttribute(startElement, HREF_ATTRIBUTE)));
            break;
          default:
            break;
        }
      }
    }
    con.disconnect();
    return record;
  }

  public static String getParamsString(Map<String, String> params) {
    StringBuilder result = new StringBuilder();

    for (Map.Entry<String, String> entry : params.entrySet()) {
      result.append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8));
      result.append("=");
      result.append(URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8));
      result.append("&");
    }

    String resultString = result.toString();
    return !resultString.isEmpty()
        ? resultString.substring(0, resultString.length() - 1)
        : resultString;
  }

  @Generated
  @Value
  public static class Record {
    String id;
    String citation;
    String license;
    String retracted;
    List<Link> links;
  }

  @Generated
  @Value
  public static class Link {
    String format;
    String updated;
    String href;
  }

  private static String getAttribute(StartElement startElement, QName attribute) {
    Attribute attr = startElement.getAttributeByName(attribute);
    return attr == null ? null : attr.getValue();
  }
}
