package org.curieo.consumer;

/**
 * Useful as a starting point when you are concatenating several sinks.
 *
 * @param <T>
 */
public record NoopSink<T>() implements Sink<T> {

  @Override
  public void finalCall() {}

  @Override
  public int getTotalCount() {
    return 0;
  }

  @Override
  public int getUpdatedCount() {
    return 0;
  }

  @Override
  public void accept(T t) {}
}
