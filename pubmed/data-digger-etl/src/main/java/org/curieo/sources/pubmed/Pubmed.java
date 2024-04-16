package org.curieo.sources.pubmed;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

public class Pubmed {

  public static Iterable<PubmedRecord> read(File file) throws IOException, XMLStreamException {
    return new PubmedReader(file);
  }

  private static class PubmedReader implements Iterable<PubmedRecord> {
    XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
    XMLEventReader reader;

    public PubmedReader(File file) throws IOException, XMLStreamException {
      FileInputStream bais = new FileInputStream(file);
      InputStream stream;
      if (file.getAbsolutePath().toLowerCase().endsWith(".gz")) {
        stream = new GZIPInputStream(bais);
      } else {
        stream = bais;
      }
      reader = xmlInputFactory.createXMLEventReader(stream);
    }

    @Override
    public Iterator<PubmedRecord> iterator() {
      return new PubmedIterator();
    }

    private class PubmedIterator implements Iterator<PubmedRecord> {
      PubmedRecord next = null;

      private PubmedRecord readRecord() throws XMLStreamException {

        // Now that the XMLEventReader is ready, we move forward through the stream with
        // nextEvent():

        while (reader.hasNext()) {
          XMLEvent nextEvent = reader.nextEvent();
          // Next, we need to find our desired start tag first:

          if (nextEvent.isStartElement()) {
            StartElement startElement = nextEvent.asStartElement();
            if (startElement.getName().getLocalPart().equals(PubmedRecord.RECORD_TAG)) {
              return PubmedRecord.read(reader, nextEvent);
            }
          }
        }
        return null;
      }

      @Override
      public boolean hasNext() {
        try {
          if (next == null) {
            next = readRecord();
          }
          return next != null;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public PubmedRecord next() {
        if (hasNext()) {
          PubmedRecord retval = next;
          next = null;
          return retval;
        }
        return null;
      }
    }
  }
}
