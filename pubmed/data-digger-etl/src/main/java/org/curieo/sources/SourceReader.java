package org.curieo.sources;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import javax.xml.stream.XMLStreamException;

import org.curieo.model.Record;
import org.curieo.sources.pubmed.Pubmed;

import lombok.Generated;
import lombok.Value;

/**
 * Standard source reader
 */
public interface SourceReader {
	static final String PUBMED = "pubmed";
	
	Iterable<Record> read(File path) throws IOException, XMLStreamException;
	
	static SourceReader getReader(String type) {
		switch (type) {
		case PUBMED:
			return new SourceReader() {
				@Override
				public Iterable<Record> read(File path) throws IOException, XMLStreamException {
					return new Mapper<>(Pubmed.read(path));
				}
			};
		default:
			throw new IllegalArgumentException(String.format("Do not know input type %s", type));
		}
	}
	
	@Generated @Value
	static class Mapper<T extends Record> implements Iterable<Record> {
		Iterable<T> source;
		@Override
		public Iterator<Record> iterator() {
			return new Iterator<Record>() {
				Iterator<T> it = source.iterator();
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
