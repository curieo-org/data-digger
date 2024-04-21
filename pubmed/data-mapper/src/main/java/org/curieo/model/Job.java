package org.curieo.model;

import lombok.Builder;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

/** The SyncProgress model keeps track of progress made syncing with a source */
@Data
@Builder
public class Job {
  // position in sequence of authors of publication
  String name;
  State jobState;

  public Job(String name, State state) {
    this.name = name;
    this.jobState = state;
  }

  public static Job queue(String name) {
    return new Job(name, State.Queued);
  }

  public static Job inProgress(String name) {
    return new Job(name, State.InProgress);
  }

  public static Job completed(String name) {
    return new Job(name, State.Completed);
  }

  public static Job failed(String name) {
    return new Job(name, State.Failed);
  }

  public Job copy() {
    return Job.builder().name(name).jobState(jobState).build();
  }

  public int getJobStateInner() {
    return this.getJobState().inner;
  }

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
    return String.format("%s (%s)", StringUtils.defaultIfEmpty(name, ""), jobState.inner);
  }
}
