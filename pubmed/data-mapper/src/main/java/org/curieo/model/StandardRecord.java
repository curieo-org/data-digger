package org.curieo.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.Generated;

/**
 * This is the standardized record as we file it in third party stores (e.g. Elasticsearch.) Use
 * this record, because JSON serialization works on actual fields and not interfaces. (I am sure
 * that this can be tweaked but this seems a straightforward way).
 */
@Generated
@Data
@Builder
public class StandardRecord implements Record {
  static final ObjectWriter OBJECT_WRITER = new ObjectMapper().writerFor(StandardRecord.class);
  String origin;
  List<Text> abstractText;
  List<Text> titles;
  List<String> authors;
  List<Reference> references;
  List<Metadata> metadata;
  List<Metadata> identifiers;
  String identifier;
  String publicationDate;

  public static StandardRecord copy(Record record) {
    return new StandardRecordBuilder()
        .origin(record.getOrigin())
        .abstractText(record.getAbstractText())
        .titles(record.getTitles())
        .authors(record.getAuthors())
        .metadata(record.getMetadata())
        .publicationDate(record.getPublicationDate())
        .references(record.getReferences())
        .identifiers(record.getIdentifiers())
        .identifier(record.getIdentifier())
        .build();
  }

  public String toJson() {
    try {
      return OBJECT_WRITER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getOrigin() {
    return origin;
  }
}
