package org.curieo.consumer;

import java.util.List;

public record ListSink<T>(AbstractSink<T> sink) implements Sink<List<T>> {
  @Override
  public void accept(List<T> t) {
    t.forEach(sink);
  }

  public void finalCall() {
    sink.finalCall();
  }

  public int getTotalCount() {
    return sink.getTotalCount();
  }

  public int getUpdatedCount() {
    return sink.getUpdatedCount();
  }
}
