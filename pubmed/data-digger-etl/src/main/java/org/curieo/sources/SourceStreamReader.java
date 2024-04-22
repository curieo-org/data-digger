package org.curieo.sources;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.xml.stream.XMLStreamException;
import org.curieo.model.Record;
import org.curieo.sources.pubmed.Pubmed;

/** Standard source reader */
public interface SourceStreamReader {
  String PUBMED = "pubmed";

  Stream<Record> stream(File path, String jobName) throws IOException, XMLStreamException;

  static SourceStreamReader get(String type) {
    if (type.equals(PUBMED)) {
      return (path, jobName) -> {
        Iterable<Record> iterator = new Mapper<>(Pubmed.read(path, jobName));
        return StreamSupport.stream(iterator.spliterator(), true);
      };
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
