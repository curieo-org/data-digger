package org.curieo.sources.pubmed;

import static org.curieo.sources.pubmed.PubmedRecord.readText;

import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.curieo.model.Metadata;

import lombok.Builder;
import lombok.Data;
import lombok.Generated;
import lombok.Singular;

@Generated @Data @Builder
public class PubmedReference {
	public static final String REFERENCE_TAG = "Reference";
	String citation;
	@Singular
	List<Metadata> articleIds;

	@Override
	public String toString() {
		return citation;
	}
	
	public static List<PubmedReference> readReferenceList(XMLEventReader reader) throws XMLStreamException {
		// read the record
		List<PubmedReference> references = new ArrayList<>();
		PubmedReferenceBuilder builder = PubmedReference.builder();
		while (reader.hasNext()) {
		    XMLEvent event = reader.nextEvent();
		    if (event.isStartElement()) {
		        StartElement startElement = event.asStartElement();
		        switch (startElement.getName().getLocalPart()) {
	            case "Citation":
	               builder = builder.citation(readText(reader, "Citation"));
	               break;
	            case PubmedRecord.ARTICLEID_TAG:
	                builder = builder.articleId(PubmedRecord.readArticleId(reader, startElement));
	               break;
	            default:
	            	break;
		        }
		    }
		    if (event.isEndElement()) {
		        EndElement endElement = event.asEndElement();
		        switch (endElement.getName().getLocalPart()) {
		        case REFERENCE_TAG:
		        	references.add(builder.build());
		        	builder = PubmedReference.builder();
		        	break;
		        case PubmedRecord.REFERENCELIST_TAG:
		            return references;
		        }
		    }
		}
		return references;
	}
}
