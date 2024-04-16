package org.curieo.consumer;

import java.util.List;
import java.util.Set;
import org.curieo.rdf.HashSet;

class ListSink<T> implements Sink<List<T>> {
  AbstractSink<T> sink;

  ListSink(AbstractSink<T> sink) {
    this.sink = sink;
  }

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
