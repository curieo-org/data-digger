package org.curieo.sources.pubmed;

import static org.curieo.sources.pubmed.PubmedRecord.readPublicationDate;
import static org.curieo.sources.pubmed.PubmedRecord.readText;

import java.util.Date;
import java.util.List;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.curieo.model.Metadata;
import org.curieo.sources.Source;
import lombok.Builder;
import lombok.Generated;
import lombok.Singular;
import lombok.Value;

@Generated @Value @Builder
public class Journal {
	public static final String ISSN = "ISSN";
	public static final String ISOABBREVIATION = "ISOAbbreviation";
	public static final String JOURNAL_TAG = "Journal";
	public static final String ISSUE_TAG = "Issue";
	public static final String ISSN_TAG = "ISSN";
	public static final String VOLUME_TAG = "Volume";
	public static final String TITLE_TAG = "Title";
	public static final String PUBDATE_TAG = "PubDate";
	public static final String ISO_TAG = "ISOAbbreviation";
	
	@Singular
	List<Metadata> identifiers;
	String volume;
	String issue;
	Date publicationDate;
	String title;

	public static Journal read(XMLEventReader reader, XMLEvent current) throws XMLStreamException {
		// read the record
		JournalBuilder builder = Journal.builder();
		while (reader.hasNext()) {
		    XMLEvent event = reader.nextEvent();
		    if (event.isStartElement()) {
		        StartElement startElement = event.asStartElement();
		        switch (startElement.getName().getLocalPart()) {
		            case TITLE_TAG:
		               builder = builder.title(readText(reader, TITLE_TAG));
		                break;
		            case PUBDATE_TAG:
		        		builder = builder.publicationDate(readPublicationDate(reader, PUBDATE_TAG));
		                break;
		            case ISO_TAG:
		            	builder = builder.identifier(new Metadata(ISOABBREVIATION, readText(reader, ISO_TAG)));
		                break;
		            case ISSN_TAG:
		            	builder = builder.identifier(new Metadata(ISSN, readText(reader, ISSN_TAG)));
		                break;
	                case ISSUE_TAG:
	                	builder = builder.issue(readText(reader, ISSUE_TAG));
		                break;
	                case VOLUME_TAG:
	                	builder = builder.volume(readText(reader, VOLUME_TAG));
		                break;
		            default:
		            	break;
		        }
		    }
		    if (event.isEndElement() && event.asEndElement().getName().getLocalPart().equals(JOURNAL_TAG)) {
	            return builder.build();
	        }
		}
		return builder.build();
	}

	public Source toSource() {
		return Source.builder().type(title).identifiers(this.identifiers).build();
	}
}
