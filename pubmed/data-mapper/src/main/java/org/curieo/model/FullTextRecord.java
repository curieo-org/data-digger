package org.curieo.model;

import lombok.Generated;
import lombok.Value;

@Generated
@Value
public class FullTextRecord {
  String identifier;
  Integer year;
  String content;

  public String computeLocation() {
    String location = "data/" + Integer.toString(year);
    String name = getIdentifier();
    int breaks = 2;
    while (breaks > 0
        && name.length() > 4
        && Character.isLetterOrDigit(name.charAt(0))
        && Character.isLetterOrDigit(name.charAt(1))) {
      location = location + "/" + name.substring(0, 2);
      name = name.substring(2);
      breaks--;
    }
    return location + "/" + getIdentifier();
  }
}
