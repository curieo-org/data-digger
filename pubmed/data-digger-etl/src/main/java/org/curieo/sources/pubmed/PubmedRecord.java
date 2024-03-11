package org.curieo.sources.pubmed;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.curieo.model.Authorship;
import org.curieo.model.Metadata;
import org.curieo.model.Record;
import org.curieo.model.Text;
import org.curieo.sources.IdProvider;
import org.curieo.sources.Source;
import org.curieo.sources.SourceReader;

import lombok.Builder;
import lombok.Generated;
import lombok.Singular;
import lombok.Value;

@Generated @Value @Builder
public class PubmedRecord implements Record {
	public static final String RECORD_TAG = "PubmedArticle";
	public static final String TITLE_TAG = "ArticleTitle";
	public static final String ABSTRACT_TAG = "AbstractText";
	public static final String ARTICLEIDLIST_TAG = "ArticleIdList";
	public static final String REFERENCELIST_TAG = "ReferenceList";
	public static final String ARTICLEID_TAG = "ArticleId";
	public static final String AUTHORLIST_TAG = "AuthorList";
	public static final String MESHHEADINGLIST_TAG = "MeshHeadingList";
	private static final QName IDTYPE = new QName("IdType");
	
	@Singular("abstractTex")
	List<Text> abstractText;
	@Singular
	List<Text> titles;
	@Singular("metadatum")
	List<Metadata> metadata;
	@Singular
	List<Metadata> identifiers;
	@Singular
	List<Source> sources;
	
	List<MeshHeading> meshHeadings;
	
	Journal journal;
	List<PubmedAuthor> pubmedAuthors;
	List<PubmedReference> pubmedReferences;
	Date publicationDate;

	@Override
	public List<String> getAuthors() {
		return CollectionUtils.emptyIfNull(getPubmedAuthors()).stream().map(PubmedAuthor::toString).toList();
	}
	@Override
	public List<String> getReferences() {
		return CollectionUtils.emptyIfNull(getPubmedReferences()).stream().map(PubmedReference::toString).toList();
	}

	@Override
	public List<Authorship> toAuthorships() {
		List<Authorship> list = new ArrayList<>();
		int year;
		if (publicationDate == null) {
			year = 0;
		}
		else {
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(publicationDate);
			year = calendar.get(Calendar.YEAR);
		}
		
		for (int ordinal = 0; ordinal < ListUtils.emptyIfNull(pubmedAuthors).size(); ordinal++) {
			list.add(pubmedAuthors.get(ordinal).toAuthorship(ordinal, year, getIdentifier()));
		}
		return list;
	}
	
	/**
	 * We're computing a unique identifier by suffixing the PubmedId with a code from the IdProvider
	 */
	@Override
	public String getIdentifier() {
		return IdProvider.identifier(this.getIdentifier("pubmed"), SourceReader.PUBMED);
	}
	
	public String getIdentifier(String type) {
		for (Metadata id : identifiers) {
			if (id.getKey().equals(type)) {
				return id.getValue();
			}
		}
		return null;
	}

	public static PubmedRecord read(XMLEventReader reader, XMLEvent current) throws XMLStreamException {
		// read the record
		PubmedRecordBuilder builder = PubmedRecord.builder();
		while (reader.hasNext()) {
		    XMLEvent event = reader.nextEvent();
		    if (event.isStartElement()) {
		        StartElement startElement = event.asStartElement();
		        switch (startElement.getName().getLocalPart()) {
	            case TITLE_TAG:
	               builder = builder.title(new Text(readText(reader, TITLE_TAG), null));
	                break;
	            case AUTHORLIST_TAG:
	               builder = builder.pubmedAuthors(PubmedAuthor.readAuthorList(reader));
	                break;
	            case ABSTRACT_TAG:
	            	builder = builder.abstractTex(new Text(readText(reader, ABSTRACT_TAG), null));
	                break;
	            case Journal.JOURNAL_TAG:
	            	Journal journal = Journal.read(reader, current);
	            	builder = builder.journal(journal).source(journal.toSource()).publicationDate(journal.getPublicationDate());
	                break;
	            case REFERENCELIST_TAG:
	            	builder = builder.pubmedReferences(PubmedReference.readReferenceList(reader));
	                break;
                case ARTICLEID_TAG:
	                builder = builder.identifier(readArticleId(reader, startElement));
	                break;
                case MESHHEADINGLIST_TAG:
                	builder = builder.meshHeadings(MeshHeading.readHeadings(reader));
                	break;
	            default:
	            	break;
		        }
		    }
		    if (event.isEndElement()) {
		        EndElement endElement = event.asEndElement();
		        switch (endElement.getName().getLocalPart()) {
		        case RECORD_TAG:
		            return builder.build();
		        case ARTICLEIDLIST_TAG:
		        	break;
		        }
		    }
		}
		return builder.build();
	}

	static Metadata readArticleId(XMLEventReader reader, StartElement startElement) throws XMLStreamException {
        String type = startElement.getAttributeByName(IDTYPE).getValue();
        return new Metadata(type, readText(reader, ARTICLEID_TAG));
	}
	
	static String readText(XMLEventReader reader, String tag) throws XMLStreamException {
		StringBuilder text = new StringBuilder();
		while (reader.hasNext()) {
	        XMLEvent nextEvent = reader.nextEvent();
		    if (nextEvent.isEndElement()) {
		    	EndElement end = nextEvent.asEndElement();
		    	if (end.getName().getLocalPart().equals(tag))
		    		return text.toString();
		    }
		    if (nextEvent.isCharacters())
	        	text.append(nextEvent.asCharacters().getData());
		}
		return text.toString();
	}
}
