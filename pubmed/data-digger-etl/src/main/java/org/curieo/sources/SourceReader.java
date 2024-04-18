package org.curieo.sources;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import javax.xml.stream.XMLStreamException;
import org.curieo.model.Record;
import org.curieo.sources.pubmed.Pubmed;

/** Standard source reader */
public interface SourceReader {
  String PUBMED = "pubmed";

  Iterable<Record> read(File path) throws IOException, XMLStreamException;

  static SourceReader getReader(String type) {
    if (type.equals(PUBMED)) {
      return path -> new Mapper<>(Pubmed.read(path));
    }
    throw new IllegalArgumentException(String.format("Do not know input type %s", type));
  }

  record Mapper<T extends Record>(Iterable<T> source) implements Iterable<Record> {
    @Override
    public Iterator<Record> iterator() {
      return new Iterator<>() {
        final Iterator<T> it = source.iterator();

        @Override
        public boolean hasNext() {
          return it.hasNext();
        }

        @Override
        public Record next() {
          return it.next();
        }
      };
    }
  }
}
