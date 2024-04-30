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
import lombok.Builder;
import lombok.Generated;
import lombok.Singular;
import lombok.Value;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.curieo.model.Authorship;
import org.curieo.model.LinkedField;
import org.curieo.model.Metadata;
import org.curieo.model.Record;
import org.curieo.model.Reference;
import org.curieo.model.Text;
import org.curieo.sources.Source;

@Generated
@Value
@Builder
public class PubmedRecord implements Record {
  public static final String RECORD_TAG = "PubmedArticle";
  public static final String TITLE_TAG = "ArticleTitle";
  public static final String ABSTRACT_TAG = "AbstractText";
  public static final String ARTICLEIDLIST_TAG = "ArticleIdList";
  public static final String REFERENCELIST_TAG = "ReferenceList";
  public static final String REFERENCE_TAG = "Reference";
  public static final String ARTICLEID_TAG = "ArticleId";
  public static final String DATECOMPLETED_TAG = "DateCompleted";
  public static final String ARTICLEDATE_TAG = "ArticleDate";
  public static final String AUTHORLIST_TAG = "AuthorList";
  public static final String MESHHEADINGLIST_TAG = "MeshHeadingList";
  private static final QName IDTYPE = new QName("IdType");

  public static final String MONTH_TAG = "Month";
  public static final String DAY_TAG = "Day";
  public static final String YEAR_TAG = "Year";

  String origin;

  @Singular("abstractTex")
  List<Text> abstractText;

  @Singular List<Text> titles;

  @Singular("metadatum")
  List<Metadata> metadata;

  @Singular List<Metadata> identifiers;
  @Singular List<Source> sources;

  List<MeshHeading> meshHeadings;

  Journal journal;
  List<PubmedAuthor> pubmedAuthors;
  List<Reference> references;
  Date dateCompleted;
  Date articleDate;

  @Override
  public String getPublicationDate() {
    if (articleDate != null) return Record.formatDate(articleDate);
    if (dateCompleted != null) return Record.formatDate(dateCompleted);
    if (journal != null && journal.getPublicationDate() != null)
      return Record.formatDate(journal.getPublicationDate());
    return null;
  }

  @Override
  public String getOrigin() {
    return origin;
  }

  @Override
  public List<String> getAuthors() {
    return CollectionUtils.emptyIfNull(getPubmedAuthors()).stream()
        .map(PubmedAuthor::toString)
        .toList();
  }

  @Override
  public List<LinkedField<Authorship>> toAuthorships() {
    List<LinkedField<Authorship>> list = new ArrayList<>();
    Integer year = getYear();
    for (int ordinal = 0; ordinal < ListUtils.emptyIfNull(pubmedAuthors).size(); ordinal++) {
      list.add(
          new LinkedField<>(
              ordinal,
              getNumericIdentifier(),
              pubmedAuthors.get(ordinal).toAuthorship(year == null ? 0 : year)));
    }
    return list;
  }

  /** Retrieve unique pubmed identifier */
  @Override
  public String getIdentifier() {
    return this.getIdentifier("pubmed");
  }

  public String getIdentifier(String type) {
    for (Metadata id : identifiers) {
      if (id.key().equals(type)) {
        return id.value();
      }
    }
    return null;
  }

  public static PubmedRecord read(String filename, XMLEventReader reader, XMLEvent current)
      throws XMLStreamException {
    // read the record
    PubmedRecordBuilder builder = PubmedRecord.builder().origin(filename);
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
          case ARTICLEDATE_TAG:
            builder = builder.articleDate(readPublicationDate(reader, ARTICLEDATE_TAG));
            break;
          case DATECOMPLETED_TAG:
            builder = builder.dateCompleted(readPublicationDate(reader, DATECOMPLETED_TAG));
            break;
          case Journal.JOURNAL_TAG:
            Journal journal = Journal.read(reader, current);
            builder = builder.journal(journal).source(journal.toSource());
            break;
          case REFERENCELIST_TAG:
            builder = builder.references(readReferenceList(reader));
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

  static Date readPublicationDate(XMLEventReader reader, String endTag) throws XMLStreamException {
    // read the date
    Calendar calendar = Calendar.getInstance();
    int dateData = 0;
    while (reader.hasNext()) {
      XMLEvent event = reader.nextEvent();
      if (event.isStartElement()) {
        StartElement startElement = event.asStartElement();
        switch (startElement.getName().getLocalPart()) {
          case DAY_TAG:
            String day = readText(reader, startElement.getName().getLocalPart());
            if (day.length() <= 2 && day.chars().allMatch(Character::isDigit)) {
              calendar.set(Calendar.DAY_OF_MONTH, Integer.parseInt(day));
              dateData++;
            }
            break;
          case MONTH_TAG:
            Integer month =
                org.curieo.utils.Months.get(
                    readText(reader, startElement.getName().getLocalPart()).toLowerCase());
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
          default:
            break;
        }
      }
      if (event.isEndElement() && event.asEndElement().getName().getLocalPart().equals(endTag)) {
        if (dateData != 0) {
          return calendar.getTime();
        }
        return null;
      }
    }
    return null;
  }

  static Metadata readArticleId(XMLEventReader reader, StartElement startElement)
      throws XMLStreamException {
    String type = startElement.getAttributeByName(IDTYPE).getValue();
    return new Metadata(type, readText(reader, ARTICLEID_TAG));
  }

  static String readText(XMLEventReader reader, String tag) throws XMLStreamException {
    StringBuilder text = new StringBuilder();
    while (reader.hasNext()) {
      XMLEvent nextEvent = reader.nextEvent();
      if (nextEvent.isEndElement()) {
        EndElement end = nextEvent.asEndElement();
        if (end.getName().getLocalPart().equals(tag)) return text.toString();
      }
      if (nextEvent.isCharacters()) text.append(nextEvent.asCharacters().getData());
    }
    return text.toString();
  }

  public static List<Reference> readReferenceList(XMLEventReader reader) throws XMLStreamException {
    // read the record
    List<Reference> references = new ArrayList<>();
    String citation = null;
    List<Metadata> identifiers = new ArrayList<>();
    while (reader.hasNext()) {
      XMLEvent event = reader.nextEvent();
      if (event.isStartElement()) {
        StartElement startElement = event.asStartElement();
        switch (startElement.getName().getLocalPart()) {
          case "Citation":
            citation = readText(reader, "Citation");
            break;
          case "MedlineCitation":
            citation = readText(reader, "MedlineCitation");
            break;
          case PubmedRecord.ARTICLEID_TAG:
            identifiers.add(PubmedRecord.readArticleId(reader, startElement));
            break;
          default:
            break;
        }
      }
      if (event.isEndElement()) {
        EndElement endElement = event.asEndElement();
        switch (endElement.getName().getLocalPart()) {
          case REFERENCE_TAG:
            references.add(new Reference(citation, new ArrayList<>(identifiers)));
            citation = null;
            identifiers.clear();
            break;
          case PubmedRecord.REFERENCELIST_TAG:
            return references;
        }
      }
    }
    return references;
  }
}
