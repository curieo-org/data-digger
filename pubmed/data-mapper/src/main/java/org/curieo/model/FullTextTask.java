package org.curieo.model;

import org.apache.commons.lang3.StringUtils;

/** The SyncProgress model keeps track of progress made syncing with a source */
public class FullTextTask implements TaskState {
  // position in sequence of authors of publication
  String identifier;
  String location;
  Integer year;
  State jobState;

  public FullTextTask(String identifier, String location, Integer year, State state) {
    this.identifier = identifier;
    this.location = location;
    this.year = year;
    this.jobState = state;
  }

  public FullTextTask failed() {
    return new FullTextTask(this.identifier, this.location, this.year, State.Failed);
  }

  public FullTextTask completed(String location) {
    return new FullTextTask(this.identifier, location, this.year, State.Completed);
  }

  @Override
  public String toString() {
    return String.format("%s (%s)", StringUtils.defaultIfEmpty(identifier, ""), jobState);
  }

  public String getIdentifier() {
    return identifier;
  }

  public String getLocation() {
    return location;
  }

  public Integer getYear() {
    return year;
  }

  @Override
  public State getTaskState() {
    return jobState;
  }
}
