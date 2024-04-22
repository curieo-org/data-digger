package org.curieo.consumer;

record GenericSink<T>(AbstractSink<T> sink, boolean doNotDownloadDuplicates) implements Sink<T> {
  @Override
  public void accept(T t) {
    sink.accept(t);
  }

  @Override
  public void finalCall() {
    sink.finalCall();
  }

  @Override
  public int getTotalCount() {
    return sink.getTotalCount();
  }

  @Override
  public int getUpdatedCount() {
    return sink.getUpdatedCount();
  }
}
