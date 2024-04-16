package org.curieo.sources.pubmed;

import static org.curieo.sources.pubmed.PubmedRecord.readText;

import java.util.ArrayList;
import java.util.List;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.XMLEvent;
import lombok.Builder;
import lombok.Data;
import lombok.Generated;
import lombok.Singular;
import org.curieo.model.Authorship;
import org.curieo.utils.StringUtils;

@Generated
@Data
@Builder
public class PubmedAuthor {
  public static final String AUTHOR_TAG = "Author";
  public static final String AFFILIATIONINFO_TAG = "AffiliationInfo";
  public static final String AFFILIATION_TAG = "AffiliationInfo";
  String foreName;
  String lastName;
  String initials;

  @Singular List<String> affiliations;

  @Override
  public String toString() {
    if (foreName == null) {
      if (initials == null) {
        return lastName;
      }
      return String.format("%s %s", initials, lastName);
    }
    if (initials == null) {
      return String.format("%s %s", foreName, lastName);
    }
    if (foreName.replace(" ", "").equals(initials)) {
      return String.format("%s %s", initials, lastName);
    }
    return String.format("%s %s", foreName, lastName);
  }

  public static List<PubmedAuthor> readAuthorList(XMLEventReader reader) throws XMLStreamException {
    // read the record
    List<PubmedAuthor> authors = new ArrayList<>();
    PubmedAuthorBuilder builder = PubmedAuthor.builder();
    while (reader.hasNext()) {
      XMLEvent event = reader.nextEvent();
      if (event.isStartElement()) {
        String field = event.asStartElement().getName().getLocalPart();
        switch (field) {
          case "LastName":
            builder = builder.lastName(readText(reader, field));
            break;
          case "ForeName":
            builder = builder.foreName(readText(reader, field));
            break;
          case "Initials":
            builder = builder.initials(readText(reader, field));
            break;
          case "Affiliation":
            builder = builder.affiliation(readText(reader, field));
          default:
            break;
        }
      }
      if (event.isEndElement()) {
        EndElement endElement = event.asEndElement();
        switch (endElement.getName().getLocalPart()) {
          case AUTHOR_TAG:
            authors.add(builder.build());
            builder = PubmedAuthor.builder();
            break;
          case PubmedRecord.AUTHORLIST_TAG:
            return authors;
        }
      }
    }
    return authors;
  }

  public Authorship toAuthorship(int year) {
    List<String> affiliationProcessed = new ArrayList<>();
    String email = null;
    for (String affiliation : affiliations) {
      List<String> emails = StringUtils.extractEmails(affiliation);
      affiliationProcessed.add(emails.get(0));
      if (emails.size() > 1) {
        email = emails.get(1); // discarding all other email addresses mentioned
      }
    }

    return Authorship.builder()
        .foreName(foreName)
        .lastName(lastName)
        .initials(initials)
        .affiliations(affiliationProcessed)
        .yearActive(year)
        .emailAddress(email)
        .build();
  }
}
