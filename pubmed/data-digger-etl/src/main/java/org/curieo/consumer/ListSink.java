package org.curieo.consumer;

import java.util.List;
import java.util.Set;
import org.curieo.rdf.HashSet;

public record ListSink<T>(AbstractSink<T> sink) implements Sink<List<T>> {
  @Override
  public void accept(List<T> t) {
    if (sink.getKeyExtractor() != null) {
      Set<String> keys = new HashSet<>();
      for (T item : t) {
        keys.add(sink.getKeyExtractor().getAsString(item));
      }
      sink.guaranteeUniqueKeys(keys);
    }
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
