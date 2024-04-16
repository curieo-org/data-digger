package org.curieo.consumer;

import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class AsynchSink<T> implements Sink<T> {
  BlockingQueue<Optional<T>> queue = new ArrayBlockingQueue<>(1000);
  Sink<T> embedded;
  StorageThread storageThread;

  public AsynchSink(Sink<T> sink) {
    this.embedded = sink;
    storageThread = new StorageThread();
    storageThread.start();
  }

  @Override
  public void accept(T t) {
    try {
      queue.put(Optional.of(t));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void finalCall() {
    try {
      queue.put(Optional.empty());
      storageThread.join();
      embedded.finalCall();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public int getTotalCount() {
    return embedded.getTotalCount();
  }

  public int getUpdatedCount() {
    return embedded.getUpdatedCount();
  }

  private class StorageThread extends Thread {
    @Override
    public void run() {
      Optional<T> recordOpt;
      try {
        while ((recordOpt = queue.take()).isPresent()) {
          T record = recordOpt.get();
          embedded.accept(record);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
  }
}
