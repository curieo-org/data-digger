package org.curieo.model;

import java.sql.Timestamp;
import java.time.Instant;

/** Timestamped value wrapper * */
public record TS<T>(T value, Timestamp timestamp) {

  public static <T> TS<T> of(T v, Timestamp timestamp) {
    return new TS<>(v, timestamp);
  }

  public static <T> TS<T> now(T v) {
    return new TS<>(v, Timestamp.from(Instant.now()));
  }

  public static <T> TS<T> epoch(T v) {
    return new TS<>(v, Timestamp.from(Instant.EPOCH));
  }
}
