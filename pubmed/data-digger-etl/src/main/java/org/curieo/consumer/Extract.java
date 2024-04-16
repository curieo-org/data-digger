package org.curieo.consumer;

import java.util.List;
import java.util.function.Function;
import lombok.Generated;
import lombok.Value;

@Generated
@Value
class Extract<T> {
  StorageSpec spec;
  Function<T, List<String>> explode;
  Function<T, String> stringExtract;
  Function<T, Integer> intExtract;

  String getAsString(T t) {
    switch (spec.getType()) {
      case String:
        return stringExtract.apply(t);
      case Integer:
      case SmallInt:
        return Integer.toString(intExtract.apply(t));
      default:
        throw new IllegalArgumentException(
            "KEY fields must have be either INT or STRING specified");
    }
  }

  public int getInteger(T t) {
    return intExtract.apply(t);
  }

  public String getString(T t) {
    return stringExtract.apply(t);
  }
}
