package org.curieo.consumer;

import java.util.List;
import java.util.function.Function;

record Extract<T>(
    StorageSpec spec,
    Function<T, List<String>> explode,
    Function<T, String> stringExtract,
    Function<T, Integer> intExtract) {
  String getAsString(T t) {
    return switch (spec.getType()) {
      case String -> stringExtract.apply(t);
      case Integer, SmallInt -> Integer.toString(intExtract.apply(t));
      default ->
          throw new IllegalArgumentException(
              "KEY fields must have be either INT or STRING specified");
    };
  }

  public int getInteger(T t) {
    return intExtract.apply(t);
  }

  public String getString(T t) {
    return stringExtract.apply(t);
  }
}
