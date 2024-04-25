package org.curieo.utils;

import lombok.Generated;
import lombok.Value;

public abstract class ParseParameters {

  private char singleCommentChar;
  protected int pos;
  protected int line;
  protected char newLineChar = '\n';

  public static final CharPredicate NonWhite = c -> !Character.isWhitespace(c);

  public abstract boolean done();

  public boolean skipWhite() {
    boolean done;
    do {
      done = true;
      while (!done() && (next() <= ' ' || Character.isSpaceChar(next()))) {
        eat();
      }
      if (!done() && next() == singleCommentChar) {
        done = false;
        while (!done() && next() != newLineChar) {
          eat();
        }
      }
    } while (!done);

    return done();
  }

  public abstract char eat();

  public String parseToken(CharPredicate pred) {
    StringBuilder sb = new StringBuilder();
    while (!done() && pred.test(next())) {
      sb.append(eat());
    }
    return sb.toString();
  }

  public String parseString(char open, char close, char escape) {
    if (done() || next() != open) {
      return null;
    }
    eat();
    StringBuilder sb = new StringBuilder();
    while (!done()) {
      char c = eat();
      if (c == close) {
        return sb.toString();
      }
      if (c == escape) {
        c = eat();
      }
      sb.append(c);
    }
    // unfinished string
    return sb.toString();
  }

  public abstract char next();

  public abstract RuntimeException exception(String exception);

  public interface Text {
    char charAt(int pos);

    int length();
  }

  public static interface CharPredicate {
    boolean test(char c);
  }

  @Generated
  @Value
  public static class StringText implements Text {
    String text;

    public char charAt(int pos) {
      return text.charAt(pos);
    }

    public int length() {
      return text.length();
    }
  }

  public ParseParameters withSingleCommentChar(char c) {
    singleCommentChar = c;
    return this;
  }

  public ParseParameters withLineSeparator(char c) {
    newLineChar = c;
    return this;
  }

  public void notEnd() {
    if (done()) {
      throw exception("Premature end of input");
    }
  }

  public abstract char peek(int ahead);

  public abstract void uneat(char c);

  public void expect(char c) {
    expect(c, "Expecting character %c at this point");
  }

  public void expect(char c, String message) {
    if (done() || next() != c) {
      throw exception(String.format(message, c));
    }
  }
}
