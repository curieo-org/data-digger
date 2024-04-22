package org.curieo.consumer;

import java.sql.Timestamp;
import java.util.List;
import java.util.function.Function;

record Extract<T>(
    FieldSpec spec,
    Function<T, List<String>> explode,
    Function<T, String> stringExtract,
    Function<T, Integer> intExtract,
    Function<T, Long> longExtract,
    Function<T, Timestamp> timestampExtract) {
  String getAsString(T t) {
    return switch (spec.getType()) {
      case String -> stringExtract.apply(t);
      case Integer, SmallInt -> Integer.toString(intExtract.apply(t));
      case BigInteger -> Long.toString(longExtract.apply(t));
      default ->
          throw new IllegalArgumentException(
              "KEY fields must have be either INT or STRING specified");
    };
  }

  public Integer getInteger(T t) {
    return intExtract.apply(t);
  }

  public Long getLong(T t) {
    return longExtract.apply(t);
  }

  public String getString(T t) {
    return stringExtract.apply(t);
  }

  public Timestamp getTimestamp(T t) {
    return timestampExtract.apply(t);
  }
}
