package org.curieo.rdf;

import static javax.xml.stream.events.XMLEvent.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import lombok.Generated;
import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 * Trix Handler -- reading Rdf Xml, returning a Store -- a taxonomy-to-triple store loader
 *
 * <p>cf. https://docs.oracle.com/javase/tutorial/jaxp/sax/parsing.html
 *
 * @author doornenbalm
 */
public class TrixHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(TrixHandler.class);

  private Store rdf = new Store(new NamespaceServiceImpl());
  private Map<String, Integer> tags;
  private int counter = 0;
  private boolean verbose;

  // current state members
  private StringBuilder charbuf = new StringBuilder();
  private String datatype = null;
  private String language = null;

  public static final String TRIX = "http://www.w3.org/2004/03/trix/trix-1/";

  private List<RdfItem> rdfItems;

  /**
   * Reads Trix XML from files (possibly multiple RDF/XML files) or from a single file (RDF/XML).
   *
   * @param file path to either a dictionary, zip file or single (RDF/XML) file.
   * @return
   * @throws IOException
   * @throws XMLStreamException
   */
  public static Store read(File file) throws IOException, XMLStreamException {
    Store rdf;
    try (FileInputStream fis = new FileInputStream(file)) {
      rdf = read(fis);
    }
    return rdf;
  }

  public static Store read(InputStream inputStream) throws XMLStreamException {
    TrixHandler sh = new TrixHandler();
    XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
    XMLEventReader xmlReader = xmlInputFactory.createXMLEventReader(inputStream);
    while (xmlReader.hasNext()) {
      XMLEvent xe = xmlReader.nextEvent();
      switch (xe.getEventType()) {
        case START_ELEMENT:
          sh.startElement(xe.asStartElement());
          break;
        case END_ELEMENT:
          sh.endElement(xe.asEndElement());
          break;
        case START_DOCUMENT:
          sh.startDocument();
          break;
        case END_DOCUMENT:
          sh.endDocument();
          break;
        case CHARACTERS:
          sh.characters(xe.asCharacters());
        default:
          break;
          /*
           * @see #
           * @see #
           * @see #ATTRIBUTE
           * @see #NAMESPACE
           * @see #PROCESSING_INSTRUCTION
           * @see #COMMENT
           * @see #
           * @see #
           * @see #DTD
           */
      }
    }
    return sh.rdf;
  }

  /** overrides DefaultHandler.characters() */
  void characters(Characters characters) {
    charbuf.append(characters.getData());
  }

  public void startDocument() {
    tags = new HashMap<>();
    counter = 0;
  }

  private void startElement(StartElement se) {
    counter++;
    if (counter % 100000 == 0) {
      LOGGER.info("Seen {} xml nodes", counter);
    }

    String nodeName = se.getName().getNamespaceURI() + se.getName().getLocalPart();
    tags.merge(nodeName, 1, (a, b) -> a + b);

    datatype = getAttribute(se, Constants.RDF, "datatype");

    if (datatype == null) {
      datatype = getAttribute(se, TRIX, "datatype");
    }
    language = getAttribute(se, "http://www.w3.org/XML/1998/namespace", "lang");

    if (nodeName.equals(TRIX + "triple")) {
      this.rdfItems = new ArrayList<>();
    }
    charbuf.setLength(0);
  }

  private static String getAttribute(StartElement se, String ns, String local) {
    Attribute a = se.getAttributeByName(new QName(ns, local));
    if (a == null) {
      return null;
    }
    return a.getValue();
  }

  public void endElement(EndElement ee) {

    String nodeName = ee.getName().getNamespaceURI() + ee.getName().getLocalPart();

    switch (nodeName) {
      case TRIX + "triple":
        if (rdfItems.size() != 3) {
          LOGGER.warn("A triple of length {}?", rdfItems.size());
        }
        if (rdfItems.get(2).isLiteral()) {
          rdf.assertTriple(
              rdfItems.get(0).getString(),
              rdfItems.get(1).getString(),
              rdfItems.get(2).getLiteral());
        } else {
          rdf.assertTriple(
              rdfItems.get(0).getString(),
              rdfItems.get(1).getString(),
              rdfItems.get(2).getString());
        }
        break;
      case TRIX + "uri":
        rdfItems.add(new RdfItem(charbuf.toString()));
        break;
      case TRIX + "typedLiteral":
        rdfItems.add(new RdfItem(new Literal(charbuf.toString(), language, datatype)));
        break;
      default:
        break;
    }
  }

  public void setVerbose(boolean v) {
    this.verbose = v;
  }

  /**
   * Do the things that you have to do by the end of the document. If you assigned
   * DUMMY_LABEL_RESOURCE then ALL of these must be replaced by real resource IDs.
   */
  void endDocument() {
    if (verbose) {
      for (Map.Entry<String, Integer> entry : tags.entrySet()) {
        LOGGER.info("Local Name \"{}\" occurs {} times", entry.getKey(), entry.getValue());
      }
    }
  }

  @Generated
  @Value
  private static class SaxErrorHandler implements ErrorHandler {
    String name;

    @Override
    public void warning(SAXParseException exception) throws SAXException {}

    @Override
    public void error(SAXParseException exception) throws SAXException {}

    @Override
    public void fatalError(SAXParseException exception) throws SAXException {}
  }
}
