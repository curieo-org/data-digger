package org.curieo.rdf;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import lombok.Generated;
import lombok.Value;
import org.curieo.utils.ParseParameters;
import org.curieo.utils.ParseParameters.CharPredicate;
import org.curieo.utils.ParseParametersFile;
import org.curieo.utils.ParseParametersString;

/**
 * This class is a reader for Turtle and Trig files. These files are read using the same method
 * {@link read} which returns a {@code Map<>} from graph identifier to graphs. If there are no named
 * graphs, the triples read are under the {@code DEFAULT} graph, so for turtle (non-Trig) files, the
 * read operation could read {@code turtle.read(file).get(DEFAULT);}. The Turtle reader needs an
 * {@code Store} supplier - if you have default namespace services or abbreviation optimizations
 * (e.g. for Skos thesauri) then this makes reading cheaper.
 */
public class Turtle {
  public static final String DEFAULT = "Anonymous";
  private static final Non NONCOLON = new Non(':');
  private static final String REIFICATION_BASE = "REIFICATION%08d";
  private static final AtomicInteger REIFICATION_COUNTER = new AtomicInteger();
  private final Map<String, AsyncTripleSink<Store>> graphs = new HashMap<>();
  private final Supplier<Store> storeSupplier;

  public Turtle(Supplier<Store> supplier) {
    storeSupplier = supplier;
  }

  public Map<String, Store> read(File file) throws IOException {
    if (file.getName().toLowerCase().endsWith(".zip")) {
      try (ZipFile zipFile = new ZipFile(file)) {
        ZipEntry entry = zipFile.entries().nextElement();
        try (InputStream is = zipFile.getInputStream(entry)) {
          return read(is, file.getName());
        }
      }
    } else if (file.getName().toLowerCase().endsWith(".gz")) {
      try (FileInputStream fis = new FileInputStream(file);
          GZIPInputStream is = new GZIPInputStream(fis)) {
        return read(is, file.getName());
      }
    } else {
      try (FileInputStream fis = new FileInputStream(file)) {
        return read(fis, file.getName());
      }
    }
  }

  public Map<String, Store> read(InputStream is, String source) throws IOException {
    try (ParseParametersFile pf = new ParseParametersFile(is, source)) {
      return parseTurtle(pf);
    }
  }

  public Map<String, Store> readFromString(String string) {
    return parseTurtle(new ParseParametersString(string));
  }

  private Map<String, Store> parseTurtle(ParseParameters pf) {
    parse(pf.withSingleCommentChar('#').withLineSeparator('\n'));
    return graphs.entrySet().stream()
        .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().get()));
  }

  private void parse(ParseParameters pf) {
    try {
      Store store = storeSupplier.get();
      NamespaceService ns = store.getNamespaceService();
      graphs.put(DEFAULT, new AsyncTripleSink<>(store));
      String currentGraph = DEFAULT;
      while (!pf.done()) {
        pf.skipWhite();
        String token = pf.parseToken(ParseParameters.NonWhite);
        if (token.equals("@prefix")) {
          pf.skipWhite();
          String prefix = pf.parseToken(NONCOLON);
          pf.skipWhite();
          if (pf.eat() != ':') {
            throw pf.exception("Expecting ':'");
          }

          pf.skipWhite();
          String full = pf.parseString('<', '>', (char) 0);
          ns.put(prefix, full);
          pf.skipWhite();
          if (pf.eat() != '.') {
            throw pf.exception("Expecting '.'");
          }
        } else if (token.length() != 0) {
          // we may be seeing the end of a trig graph here - if we see '}'
          if (token.equals("}")) {
            currentGraph = DEFAULT;
            continue;
          }
          RdfItem subject = new RdfItem(expandPrefix(token, ns));
          if (currentGraph.equals(DEFAULT)) {
            // we may be defining a trig graph here - if we get '{'
            pf.skipWhite();
            if (pf.peek(0) == '{') {
              currentGraph = subject.getString();
              graphs.computeIfAbsent(
                  currentGraph,
                  g -> {
                    Store t = storeSupplier.get();
                    ns.knownPrefixes()
                        .forEach(p -> t.getNamespaceService().put(p, ns.getFullForm(p)));
                    return new AsyncTripleSink<>(t);
                  });
              pf.eat();
              continue;
            }
          }
          RdfItem verb = parseRdfThing(pf, ns).expandVerb();
          RdfItem object = parseRdfThing(pf, ns);
          String[] reification = reification(subject.getString());
          if (reification != null) {
            String id = String.format(REIFICATION_BASE, REIFICATION_COUNTER.incrementAndGet());
            graphs
                .get(currentGraph)
                .assertAndNameTriple(id, reification[0], reification[1], reification[2]);
            subject = new RdfItem(id);
            assertTriple(graphs.get(currentGraph), subject, verb, object);
          } else {
            assertTriple(graphs.get(currentGraph), subject, verb, object);
          }

          boolean stop = false;
          do {
            pf.skipWhite();
            switch (pf.eat()) {
              case '.':
                stop = true;
                break;
              case ',':
                object = parseRdfThing(pf, ns);
                assertTriple(graphs.get(currentGraph), subject, verb, object);
                break;
              case ';':
                verb = parseRdfThing(pf, ns).expandVerb();
                object = parseRdfThing(pf, ns);
                assertTriple(graphs.get(currentGraph), subject, verb, object);
                break;
              default:
                stop = true;
                break;
            }
          } while (!stop);
        }
      }
    } catch (NullPointerException e) {
      throw new RuntimeException("Bad processing of ttl : " + e.getMessage(), e);
    }
  }

  private static void assertTriple(TripleStore rdf, RdfItem sr, RdfItem vr, RdfItem o) {
    String s = sr.getString();
    String v = vr.getString();
    if (s == null)
      throw new NullPointerException("Subject cannot be null (v = " + v + "), (o = " + o + ")");
    if (v == null)
      throw new NullPointerException("Verb cannot be null (s = " + s + "), (o = " + o + ")");
    if (o == null)
      throw new NullPointerException("Object cannot be null (s = " + s + "), (v = " + v + ")");
    if (o.isLiteral()) {
      rdf.assertTriple(s, v, o.getLiteral());
    } else {
      if (o.getString() == null)
        throw new NullPointerException(
            "Object value cannot be null (s = " + s + "), (v = " + v + ")");
      rdf.assertTriple(s, v, o.getString());
    }
  }

  private static RdfItem parseRdfThing(ParseParameters pf, NamespaceService ns) {
    pf.skipWhite();
    if (pf.next() == '"') {
      String s = pf.parseString('"', '"', '\\');
      switch (pf.next()) {
        case '"':
          // triple quoted string
          if (s.length() != 0) {
            return new RdfItem(new Literal(s, null, null)); // good luck with that
          }
          return new RdfItem(
              new Literal(parseTripleQuotedString(pf, '"'), optionalLanguage(pf), null));
        case '^':
          pf.eat();
          if (pf.eat() != '^') {
            throw pf.exception("Expecting \"^^\" - not a single \"^\"");
          }
          String type = chopComma(pf.parseToken(ParseParameters.NonWhite), pf);
          type = expandPrefix(type, ns);
          return new RdfItem(new Literal(s, null, type));
        case '@':
        default:
          return new RdfItem(new Literal(s, optionalLanguage(pf), null));
      }
    } else if (pf.next() == '<') {
      String s = pf.parseString('<', '>', (char) 0);
      return new RdfItem(expandPrefix(s, ns));
    } else {
      String s = pf.parseToken(ParseParameters.NonWhite);
      s = chopComma(s, pf);

      if (s.length() == 1 && (s.equals(".") || s.equals(";") || s.equals(","))) {
        throw pf.exception("Either \".,;\" are not Rdf Things!");
      }
      return new RdfItem(expandPrefix(s, ns));
    }
  }

  private static String optionalLanguage(ParseParameters pf) {
    if (pf.next() == '@') {
      pf.eat();
      return chopComma(pf.parseToken(ParseParameters.NonWhite), pf);
    }
    return null;
  }

  private static String parseTripleQuotedString(ParseParameters pf, char quote) {
    // we're at a quote now
    pf.eat();
    pf.notEnd();

    StringBuilder result = new StringBuilder();
    boolean done = false;
    while (!done) {
      pf.notEnd();
      if (pf.next() == quote) {
        pf.eat();
        if (pf.next() == quote) {
          pf.eat();
          if (pf.next() == quote) {
            pf.eat();
            done = true;
          } else {
            result.append(quote);
            result.append(quote);
          }
        } else {
          result.append(quote);
        }
      } else {
        result.append(pf.eat());
      }
    }

    return result.toString();
  }

  private static String chopComma(String s, ParseParameters pf) {
    // this is a horrible hack but a consequence of TopBraid NOT separated comma's by a space
    if (s.length() > 1) {
      if (s.endsWith(",")) {
        pf.uneat(',');
        s = s.substring(0, s.length() - 1);
      } else if (s.endsWith(";")) {
        pf.uneat(';');
        s = s.substring(0, s.length() - 1);
      }
    }
    return s;
  }

  private static String expandPrefix(String s, NamespaceService ns) {
    if (s.startsWith("<") && s.endsWith(">")) {
      s = s.substring(1, s.length() - 1);
    }
    return ns.decodeUri(s);
  }

  // http://datashapes.org/reification.html
  private static final Pattern REIFICATION_PATTERN =
      Pattern.compile("urn:triple:([^:]+):([^:]+):([^:]+)");

  private static String[] reification(String s) {
    // <urn:triple:${enc(S)}:${enc(P)}:${enc(O)}>
    Matcher m = REIFICATION_PATTERN.matcher(s);
    if (!m.matches()) {
      return null;
    }
    return new String[] {decode(m.group(1)), decode(m.group(2)), decode(m.group(3))};
  }

  private static String decode(String string) {
    String decoded = URLDecoder.decode(string, UTF_8);
    if (decoded.startsWith("<") && decoded.endsWith(">")) {
      return decoded.substring(1, decoded.length() - 1);
    }
    return decoded;
  }

  @Value
  @Generated
  private static class Non implements CharPredicate {
    private char doNotMatch;

    @Override
    public boolean test(char c) {
      return !Character.isWhitespace(c) && c != doNotMatch;
    }
  }
}
