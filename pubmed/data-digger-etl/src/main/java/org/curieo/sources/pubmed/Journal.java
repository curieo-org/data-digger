package org.curieo.sources.pubmed;

import static org.curieo.sources.pubmed.PubmedRecord.readText;
import java.text.DateFormatSymbols;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.curieo.model.Metadata;
import org.curieo.sources.Source;
import org.curieo.utils.Trie;

import lombok.Builder;
import lombok.Generated;
import lombok.Singular;
import lombok.Value;

@Generated @Value @Builder
public class Journal {
	
	private static final Trie<Integer> MONTHS = new Trie<>();
	static {
		String[] mo = new DateFormatSymbols(Locale.US).getMonths();
		for (int i = 0; i < mo.length; i++) {
			MONTHS.put(mo[i].toLowerCase(), i);
		}
		MONTHS.markUnique();
	}
	
	public static final String ISSN = "ISSN";
	public static final String ISOABBREVIATION = "ISOAbbreviation";
	public static final String JOURNAL_TAG = "Journal";
	public static final String ISSUE_TAG = "Issue";
	public static final String ISSN_TAG = "ISSN";
	public static final String YEAR_TAG = "Year";
	public static final String VOLUME_TAG = "Volume";
	public static final String MONTH_TAG = "Month";
	public static final String DAY_TAG = "Day";
	public static final String TITLE_TAG = "Title";
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
		Calendar calendar = Calendar.getInstance();
		int dateData = 0;
		while (reader.hasNext()) {
		    XMLEvent event = reader.nextEvent();
		    if (event.isStartElement()) {
		        StartElement startElement = event.asStartElement();
		        switch (startElement.getName().getLocalPart()) {
		            case TITLE_TAG:
		               builder = builder.title(readText(reader, TITLE_TAG));
		                break;
		            case DAY_TAG:
		            	String day = readText(reader, startElement.getName().getLocalPart());
		            	if (day.length() <= 2 && day.chars().allMatch(Character::isDigit)) {
		            		calendar.set(Calendar.DAY_OF_MONTH, Integer.parseInt(day));
		            		dateData++;
		            	}
		            case MONTH_TAG:
		            	Integer month = MONTHS.get(readText(reader, startElement.getName().getLocalPart()).toLowerCase());
		            	if (month != null) {
		            		calendar.set(Calendar.MONTH, month);
		            		dateData++;
		            	}
		                break;
		            case YEAR_TAG:
		            	String year = readText(reader, startElement.getName().getLocalPart());
		            	if (year.length() <= 5 && year.chars().allMatch(Character::isDigit)) {
			            	calendar.set(Calendar.YEAR, Integer.parseInt(year));
		            		dateData++;
		            	}
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
		    if (event.isEndElement()) {
		        EndElement endElement = event.asEndElement();
		        switch (endElement.getName().getLocalPart()) {
		        case JOURNAL_TAG:
		        	if (dateData != 0) {
		        		builder = builder.publicationDate(calendar.getTime());
		        	}
		            return builder.build();
		        }
		    }
		}
		return builder.build();
	}

	public Source toSource() {
		return Source.builder().type(title).identifiers(this.identifiers).build();
	}
}
