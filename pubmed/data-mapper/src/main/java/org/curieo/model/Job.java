package org.curieo.model;

import org.apache.commons.lang3.StringUtils;

/** The SyncProgress model keeps track of progress made syncing with a source */
public class Job implements JobState {
  // position in sequence of authors of publication
  final String name;
  final State jobState;

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

  public String getName() {
    return name;
  }

  @Override
  public State getJobState() {
    return jobState;
  }

  @Override
  public String toString() {
    return String.format("%s (%s)", StringUtils.defaultIfEmpty(name, ""), jobState.getInner());
  }
}
