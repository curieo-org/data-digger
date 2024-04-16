package org.curieo.utils;

public class ParseParametersString extends ParseParameters {
  private final Text text;
  private int bufpos = 0;

  public ParseParametersString(String text) {
    this.text = new StringText(text);
  }

  @Override
  public char eat() {
    char next = next();
    pos++;
    bufpos++;
    if (next == newLineChar) {
      line++;
      pos = 0;
    }
    return next;
  }

  @Override
  public boolean done() {
    return bufpos >= text.length();
  }

  @Override
  public char next() {
    return text.charAt(bufpos);
  }

  @Override
  public RuntimeException exception(String exception) {
    return new RuntimeException(
        String.format("Exception %s at position %d, line %d", exception, pos, line));
  }

  @Override
  public char peek(int ahead) {
    if (bufpos + ahead <= text.length()) {
      return text.charAt(pos + ahead);
    }
    throw exception("Cannot look that far ahead");
  }

  @Override
  public void uneat(char c) {
    bufpos--;
    if (c == '\n') {
      line--;
    }
  }
}
