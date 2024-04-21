package org.curieo.model;

import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.Generated;

/**
 * This is the standardized record as we file it in third party stores (e.g. Elastic Search.) Use
 * this record, because JSON serialization works on actual fields and not interfaces. (I am sure
 * that this can be tweaked but this seems a straightforward way).
 */
@Generated
@Data
@Builder
public class StandardRecordWithEmbeddings implements Record {
  String origin;
  List<Text> abstractText;
  List<Text> titles;
  List<String> authors;
  List<Reference> references;
  List<Metadata> metadata;
  String identifier;
  List<Metadata> identifiers;
  String publicationDate;
  double[] embeddings;

  public static StandardRecordWithEmbeddings copy(Record record, double[] embeddings) {
    return new StandardRecordWithEmbeddings.StandardRecordWithEmbeddingsBuilder()
        .origin(record.getOrigin())
        .abstractText(record.getAbstractText())
        .titles(record.getTitles())
        .authors(record.getAuthors())
        .metadata(record.getMetadata())
        .publicationDate(record.getPublicationDate())
        .references(record.getReferences())
        .identifiers(record.getIdentifiers())
        .identifier(record.getIdentifier())
        .embeddings(embeddings)
        .build();
  }

  @Override
  public String getOrigin() {
    return origin;
  }
}
