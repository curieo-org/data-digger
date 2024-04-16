package org.curieo.utils;

import java.text.DateFormatSymbols;
import java.util.Locale;

public class Months {

  private static final Trie<Integer> MONTHS = new Trie<>();

  static {
    String[] mo = new DateFormatSymbols(Locale.US).getMonths();
    for (int i = 0; i < mo.length; i++) {
      MONTHS.put(mo[i].toLowerCase(), i);
    }
    MONTHS.markUnique();
  }

  public static Integer get(String month) {
    return MONTHS.get(month);
  }
}
