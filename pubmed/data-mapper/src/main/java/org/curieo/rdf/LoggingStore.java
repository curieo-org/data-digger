package org.curieo.rdf;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;

/** A triple store that logs every assertion */
public class LoggingStore implements TripleStore, Closeable {
  private String origin;
  private final FileOutputStream outputStream;
  private final TripleStore embedded;

  public LoggingStore(String origin, File output, TripleStore embedded) {
    try {
      this.origin = origin;
      outputStream = new FileOutputStream(output);
      this.embedded = embedded;
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Cannot open file " + output, e);
    }
  }

  /**
   * Origin of assertions
   *
   * @param origin
   */
  public void setOrigin(String origin) {
    this.origin = origin;
  }

  @Override
  public RdfTriple assertTriple(String uri, String subject, String verb, String object) {
    write(uri, subject, verb, object);
    return embedded.assertTriple(uri, subject, verb, object);
  }

  @Override
  public RdfTriple assertTriple(String uri, String subject, String verb, Literal object) {
    write(uri, subject, verb, object.toString());
    return embedded.assertTriple(uri, subject, verb, object);
  }

  @Override
  public RdfTriple assertAndNameTriple(String uri, String subject, String verb, String object) {
    write(null, subject, verb, object);
    return embedded.assertAndNameTriple(uri, subject, verb, object);
  }

  @Override
  public RdfTriple accept(RdfTriple triple) {
    write(
        triple.getUri(),
        triple.getSubject(),
        triple.getVerb(),
        triple.isSimple() ? triple.getObject() : triple.getLiteralObject().toString());
    return embedded.accept(triple);
  }

  private synchronized void write(String u, String s, String v, String o) {
    try {
      s =
          String.format(
              "%s\t%s\t%s\t%s\t%s\t%n", origin, StringUtils.defaultIfEmpty(u, ""), s, v, o);
      outputStream.write(s.getBytes(UTF_8));
    } catch (IOException e) {
      throw new RuntimeException("Cannot write log trace", e);
    }
  }

  @Override
  public void close() throws IOException {
    outputStream.flush();
    outputStream.close();
  }
}
