package org.curieo.model;

import java.util.Objects;
import lombok.Getter;
import org.curieo.utils.StringUtils;

/** Tracks the progression of a task */
public record Task(String name, State state, String job) {
  public Task {
    StringUtils.requireNonEmpty(name);
    StringUtils.requireNonEmpty(job);
    Objects.requireNonNull(state);
  }

  public static Task queue(String name, String job) {
    return new Task(name, State.Queued, job);
  }

  public static Task inProgress(String name, String job) {
    return new Task(name, State.InProgress, job);
  }

  public static Task completed(String name, String job) {
    return new Task(name, State.Completed, job);
  }

  public static Task failed(String name, String job) {
    return new Task(name, State.Failed, job);
  }

  @Getter
  public enum State {
    Queued(0),
    InProgress(1),
    Completed(2),
    Failed(3);

    private final int inner;

    State(int inner) {
      this.inner = inner;
    }

    public static State fromInt(int i) throws IllegalArgumentException {
      return switch (i) {
        case 0 -> Queued;
        case 1 -> InProgress;
        case 2 -> Completed;
        case 3 -> Failed;
        default -> throw new IllegalArgumentException("Invalid state: " + i);
      };
    }
  }

  @Override
  public String toString() {
    return String.format("%s: %s (%s)", job, name, state);
  }
}
