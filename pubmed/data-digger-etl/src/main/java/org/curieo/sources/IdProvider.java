package org.curieo.sources;

import java.util.HashMap;
import java.util.Map;

public class IdProvider {
  private static final Map<String, String> SUFFIXES = new HashMap<>();

  private IdProvider() {}

  static {
    SUFFIXES.put(SourceReader.PUBMED, "00");
  }

  public static String identifier(String sourceId, String source) {
    String suffix = SUFFIXES.get(source.toLowerCase());
    if (suffix == null) {
      throw new IllegalArgumentException("source is not known");
    }
    return sourceId + suffix;
  }
}
