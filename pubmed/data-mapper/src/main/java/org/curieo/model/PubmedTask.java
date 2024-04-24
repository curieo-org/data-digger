package org.curieo.model;

import java.util.Objects;
import org.curieo.utils.StringUtils;

/** Tracks the progression of a task */
public record PubmedTask(String name, State state, String job) implements TaskState {
  public PubmedTask {
    StringUtils.requireNonEmpty(name);
    StringUtils.requireNonEmpty(job);
    Objects.requireNonNull(state);
  }

  public static PubmedTask queue(String name, String job) {
    return new PubmedTask(name, State.Queued, job);
  }

  public static PubmedTask inProgress(String name, String job) {
    return new PubmedTask(name, State.InProgress, job);
  }

  public static PubmedTask completed(String name, String job) {
    return new PubmedTask(name, State.Completed, job);
  }

  public static PubmedTask failed(String name, String job) {
    return new PubmedTask(name, State.Failed, job);
  }

  @Override
  public TaskState.State getTaskState() {
    return state;
  }

  @Override
  public String toString() {
    return String.format("%s: %s (%s)", job, name, state);
  }
}
