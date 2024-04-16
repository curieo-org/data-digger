package org.curieo.sources.pubmed;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import lombok.Builder;
import lombok.Data;
import lombok.Generated;
import lombok.Singular;
import lombok.Value;

@Generated @Data @Builder
public class MeshHeading {
	private static final QName QNAME_UI = new QName("UI");
	private static final QName QNAME_MajorTopicYN = new QName("MajorTopicYN");
	
	@Generated @Value
	public static class Heading {
		String UI;
		boolean majorTopic;
		
		public String toString() {
			return String.format("%s (%s)", UI, majorTopic? "Y" : "N");
		}
	}
	public static final String AUTHOR_TAG = "Author";
	Heading descriptorName;
	@Singular
	List<Heading> qualifierNames;

	@Override
	public String toString() {
		return String.format("Descriptor %s, Qualifiers %s.", 
				descriptorName.toString(),
                qualifierNames.stream().map(Object::toString).collect(Collectors.joining(", ")));
	}

	/**
	 * Parse this
	 * <MeshHeadingList>
        <MeshHeading>
          <DescriptorName UI="D001419" MajorTopicYN="N">Bacteria</DescriptorName>
          <QualifierName UI="Q000235" MajorTopicYN="N">genetics</QualifierName>
          <QualifierName UI="Q000302" MajorTopicYN="N">isolation &amp; purification</QualifierName>
          <QualifierName UI="Q000378" MajorTopicYN="N">metabolism</QualifierName>
        </MeshHeading>
	 */
	public static List<MeshHeading> readHeadings(XMLEventReader reader) throws XMLStreamException {
		// read the record
		List<MeshHeading> headings = new ArrayList<>();
		MeshHeadingBuilder builder = MeshHeading.builder();
		while (reader.hasNext()) {
		    XMLEvent event = reader.nextEvent();
		    if (event.isStartElement()) {
		        StartElement startElement = event.asStartElement();
		        switch (startElement.getName().getLocalPart()) {
	            case "DescriptorName":
	               builder = builder.descriptorName(readHeading(startElement, "DescriptorName"));
	               break;
	            case "QualifierName":
	               builder = builder.qualifierName(readHeading(startElement, "QualifierName"));
	            default:
	            	break;
		        }
		    }
		    if (event.isEndElement()) {
		        EndElement endElement = event.asEndElement();
		        switch (endElement.getName().getLocalPart()) {
		        case "MeshHeading":
		        	headings.add(builder.build());
		        	builder = MeshHeading.builder();
		        	break;
		        case PubmedRecord.MESHHEADINGLIST_TAG:
		            return headings;
		        }
		    }
		}
		return headings;
	}

	private static Heading readHeading(StartElement startElement, String string) {
        String ui = startElement.getAttributeByName(QNAME_UI).getValue();
        String major = startElement.getAttributeByName(QNAME_MajorTopicYN).getValue();
        return new Heading(ui, major.equals("Y"));
	}
}
