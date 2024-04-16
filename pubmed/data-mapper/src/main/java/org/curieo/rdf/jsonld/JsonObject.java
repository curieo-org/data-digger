package org.curieo.rdf.jsonld;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.curieo.utils.ParseParameters;

public interface JsonObject {

  enum Type {
    INTEGER,
    FLOAT,
    STRING,
    MAP,
    LIST
  }

  Type getType();

  /**
   * @return as a specific value - never null
   */
  default Map<String, JsonObject> asMap() {
    return Collections.emptyMap();
  }

  default Integer asInteger() {
    return Integer.MAX_VALUE;
  }

  default Double asFloat() {
    return Double.NaN;
  }

  default String asString() {
    return "";
  }

  default List<JsonObject> asList() {
    return Collections.emptyList();
  }

  /**
   * @return query type
   */
  default boolean isMap() {
    return this.getType() == Type.MAP;
  }

  default boolean isList() {
    return this.getType() == Type.LIST;
  }

  /**
   * Parse a Json Object
   *
   * @param pp
   * @return a json object - if any
   */
  public static JsonObject parse(ParseParameters pp) {
    pp.skipWhite();
    if (pp.done()) return null;
    switch (pp.next()) {
      case '"':
      case '\'':
        return new JsonString(pp.parseString(pp.next(), pp.next(), '\\'));
      case '[':
        List<JsonObject> list = new ArrayList<>();
        pp.eat();
        pp.skipWhite();
        while (!pp.done() && pp.next() != ']') {
          JsonObject n = parse(pp);
          if (n == null) {
            throw pp.exception("Expection another object");
          }
          list.add(n);
          pp.skipWhite();
          if (!pp.done() && pp.next() == ',') {
            pp.eat();
          }
        }
        pp.expect(']');
        pp.eat();
        return new JsonList(list);
      case '{':
        Map<String, JsonObject> map = new HashMap<>();
        pp.eat();
        pp.skipWhite();
        while (!pp.done() && pp.next() != '}') {
          JsonObject key = parse(pp);
          if (key == null) {
            throw pp.exception("Expecting a key");
          }
          if (key.getType() != JsonObject.Type.STRING) {
            throw pp.exception("Expecting a STRING key");
          }
          pp.skipWhite();
          pp.expect(':');
          pp.eat();
          pp.skipWhite();
          JsonObject value = parse(pp);
          if (value == null) {
            throw pp.exception("Expecting a value");
          }
          pp.skipWhite();
          if (!pp.done() && pp.next() == ',') {
            pp.eat();
            pp.skipWhite();
          }
          map.put(key.asString(), value);
        }
        if (pp.done()) {
          throw pp.exception("Expecting '}'");
        }
        pp.eat();
        return new JsonMap(map);
      default:
        String number = pp.parseToken(ch -> Character.isDigit(ch) || ch == '.');
        try {
          if (number.contains(".")) {
            return new JsonFloat(Double.parseDouble(number));
          } else {
            return new JsonInteger(Integer.parseInt(number));
          }
        } catch (NumberFormatException nfe) {
          throw pp.exception("Not finding json object here.");
        }
    }
  }
}
