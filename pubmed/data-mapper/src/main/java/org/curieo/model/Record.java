package org.curieo.model;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Optional;
import org.apache.commons.collections4.ListUtils;

public interface Record {
  List<Text> getAbstractText();

  List<Text> getTitles();

  List<String> getAuthors();

  List<Reference> getReferences();

  List<Metadata> getMetadata();

  /**
   * @return publication date formatted as YYYY-MM-DD
   */
  String getPublicationDate();

  /** This identifier must be unique *across* sources */
  String getIdentifier();

  List<Metadata> getIdentifiers();

  default List<LinkedField<Authorship>> toAuthorships() {
    throw new UnsupportedOperationException("Not defined for standard records.");
  }

  default Integer getYear() {
    if (getPublicationDate() == null) {
      return null;
    }
    return Integer.parseInt(getPublicationDate().substring(0, 4));
  }

  default List<LinkedField<Reference>> toReferences() {
    List<LinkedField<Reference>> list = new ArrayList<>();

    for (int ordinal = 0; ordinal < ListUtils.emptyIfNull(getReferences()).size(); ordinal++) {
      list.add(new LinkedField<>(ordinal, getIdentifier(), getReferences().get(ordinal)));
    }
    return list;
  }

  default List<Metadata> toLinks(String source, String target) {
    Optional<String> sourceOpt =
        this.getIdentifiers().stream()
            .filter(m -> m.getKey().equals(source))
            .map(Metadata::getValue)
            .findFirst();
    Optional<String> targetOpt =
        this.getIdentifiers().stream()
            .filter(m -> m.getKey().equals(target))
            .map(Metadata::getValue)
            .findFirst();

    if (sourceOpt.isPresent() && targetOpt.isPresent()) {
      return Collections.singletonList(new Metadata(sourceOpt.get(), targetOpt.get()));
    }
    return Collections.emptyList();
  }

  static String formatDate(Date date) {
    Calendar calendar = GregorianCalendar.getInstance();
    calendar.setTime(date);
    return String.format(
        "%04d-%02d-%02d",
        calendar.get(Calendar.YEAR),
        calendar.get(Calendar.MONTH) + 1,
        calendar.get(Calendar.DAY_OF_MONTH));
  }
}
