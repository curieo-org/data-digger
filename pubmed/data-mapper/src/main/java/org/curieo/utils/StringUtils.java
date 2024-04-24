package org.curieo.utils;

import java.util.*;
import org.apache.commons.collections4.SetUtils;
import org.curieo.data.Range;

public class StringUtils {
  private static final boolean[] NAME_CHARACTERS = new boolean[128];
  private static final boolean[] ALNUM_CHARACTERS = new boolean[128];
  private static final Set<String> EMAIL_PREFIXES =
      SetUtils.hashSet("E-mail:", "Email:", "Electronic address:", "email:");

  public static final List<String> ENUMERATORS =
      Arrays.asList(", and ", "; and ", " and ", ",", ";");

  static {
    for (char c : "!#$%&'*+/=?^_`{|}~-".toCharArray()) {
      NAME_CHARACTERS[c] = true;
    }
    for (int c = 'A'; c <= 'Z'; c++) {
      NAME_CHARACTERS[c] = true;
      ALNUM_CHARACTERS[c] = true;
    }
    for (int c = 'a'; c <= 'z'; c++) {
      ALNUM_CHARACTERS[c] = true;
      NAME_CHARACTERS[c] = true;
    }
    for (int c = '0'; c <= '9'; c++) {
      NAME_CHARACTERS[c] = true;
      ALNUM_CHARACTERS[c] = true;
    }
  }

  private StringUtils() {}

  public static List<String> splitEnumerations(String s) {
    List<String> retval = new ArrayList<>();
    int pos = 0;
    while (pos != s.length() && Character.isWhitespace(s.charAt(pos))) pos++;
    int start = pos;
    while (pos != s.length()) {
      int cur = pos;
      Optional<String> sep = ENUMERATORS.stream().filter(e -> s.startsWith(e, cur)).findFirst();
      if (sep.isPresent()) {
        retval.add(s.substring(start, pos).trim());
        pos += sep.get().length();
        start = pos;
      } else {
        pos++;
      }
    }

    if (start != pos) {
      retval.add(s.substring(start, pos).trim());
    }
    return retval;
  }

  public static int[] findAll(Range range, String text, Character c) {
    return range.stream().filter(pos -> text.charAt(pos) == c).toArray();
  }

  /**
   * Get out all email addresses from this string.
   *
   * @param s
   * @return a list of strings. The first element is the original string, with all email addresses
   *     removed. The other elements are the removed email addresses.
   */
  public static List<String> extractEmails(String s) {
    List<Range> emails = findEmails(s);
    if (emails.isEmpty()) {
      return Collections.singletonList(s);
    }
    StringBuilder sb = new StringBuilder();
    int pos = 0;
    for (Range email : emails) {
      Range toRemove = new Range(prefix(EMAIL_PREFIXES, s, email.getStart()), email.getEnd());
      if (bracketed(toRemove, s)) {
        toRemove.setStart(toRemove.getStart() - 1);
        toRemove.setEnd(toRemove.getEnd() + 1);
      }
      toRemove.setStart(doubleDot(toRemove, s));
      sb.append(s.subSequence(pos, toRemove.getStart()));
      pos = toRemove.getEnd();
    }
    sb.append(s.substring(pos));
    List<String> retval = new ArrayList<>();
    retval.add(sb.toString().trim());
    emails.forEach(e -> retval.add(e.getString(s)));
    return retval;
  }

  /**
   * If there are double dots, remove the first.
   *
   * @param toRemove
   * @param s
   * @return new start position
   */
  private static int doubleDot(Range toRemove, String s) {
    int end = toRemove.getEnd();
    while (end < s.length() && Character.isWhitespace(s.charAt(end))) {
      end++;
    }
    if (end == s.length() || s.charAt(end) != '.') return toRemove.getStart();
    int start = toRemove.getStart();
    while (start > 0 && Character.isWhitespace(s.charAt(start - 1))) {
      start--;
    }
    if (start > 0
        && (s.charAt(start - 1) == '.'
            || s.charAt(start - 1) == ';'
            || s.charAt(start - 1) == ',')) {
      start--;
    }
    return start;
  }

  /**
   * Is this range bracketed?
   *
   * @param range
   * @param s
   * @return true if so
   */
  private static boolean bracketed(Range range, String s) {
    return range.getStart() > 0
        && range.getEnd() < s.length()
        && s.charAt(range.getEnd()) == ')'
        && s.charAt(range.getStart() - 1) == '(';
  }

  /**
   * Is there a prefix in [prefixes] that ends the string s before start
   *
   * @param prefixes possible prefixes
   * @param s
   * @param start
   * @return start if none found, offset of prefix if found.
   */
  private static int prefix(Set<String> prefixes, String s, int start) {
    int org = start;
    while (start > 0 && Character.isWhitespace(s.charAt(start - 1))) {
      start--;
    }
    String preceding = s.substring(0, start);
    for (String p : prefixes) {
      if (preceding.endsWith(p)) return start - p.length();
    }
    return org;
  }

  public static List<Range> findEmails(String s) {
    List<Range> emails = new ArrayList<>();

    int offset = 0;
    while (offset < s.length()) {
      int nameEnd = name(s, offset);
      if (offset < nameEnd) {
        if (nameEnd < s.length() && s.charAt(nameEnd) == '@') {
          int[] domainEnd = domain(s, nameEnd + 1);
          if (domainEnd[1] != -1) {
            emails.add(new Range(offset, domainEnd[1]));
          }
          offset = domainEnd[0];
        } else {
          offset = nameEnd;
        }
      } else {
        offset++;
      }
    }

    return emails;
  }

  private static int name(String text, int offset) {
    while (offset != text.length()
        && ((text.charAt(offset) == '.')
            || ((text.charAt(offset) < (char) 128) && NAME_CHARACTERS[text.charAt(offset)]))) {
      offset++;
    }
    return offset;
  }

  private static int[] domain(String text, int offset) {
    int recover = offset; // fall-back position if nothing found
    int laststop = 0; // possible endpoint of an email address.
    boolean onlyAlphanumericalSinceLastDot = false;
    while ((offset != text.length())) {
      if (text.charAt(offset) == '.') {
        if (onlyAlphanumericalSinceLastDot) {
          laststop = offset;
        }
        onlyAlphanumericalSinceLastDot = true;
      } else if (text.charAt(offset) < (char) 128) {
        if (!NAME_CHARACTERS[text.charAt(offset)]) {
          break;
        }
        if (!ALNUM_CHARACTERS[text.charAt(offset)]) {
          onlyAlphanumericalSinceLastDot = false;
        }
      } else {
        break;
      }
      offset++;
    }

    if (onlyAlphanumericalSinceLastDot) {
      if (laststop + 1 == offset) { // borderline case - a dot-terminated email address
        return new int[] {laststop, laststop};
      } else {
        return new int[] {offset, offset};
      }
    } else if (laststop != 0) {
      return new int[] {laststop, laststop};
    } else {
      return new int[] {recover, -1};
    }
  }

  /**
   * Checks that the specified string is both not {@code null} and not empty. This method is
   * designed primarily for doing parameter validation in methods and constructors, as demonstrated
   * below:
   *
   * <blockquote>
   *
   * <pre>
   * public Foo(String bar) {
   *     this.bar = Objects.requireNonEmpty(bar);
   * }
   * </pre>
   *
   * </blockquote>
   *
   * @param str the object reference to check for nullity
   * @return {@code str} if not {@code null} and not empty
   * @throws NullPointerException if {@code str} is {@code null}
   * @throws NullPointerException if {@code str} is emtpy.
   */
  public static String requireNonEmpty(String str) {
    if (Objects.requireNonNull(str).isEmpty()) {
      throw new IllegalArgumentException("String is empty");
    }
    return str;
  }
}
