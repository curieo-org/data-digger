package org.curieo.model;

import org.apache.commons.lang3.StringUtils;

/** The SyncProgress model keeps track of progress made syncing with a source */
public class FullTextJob implements JobState {
  // position in sequence of authors of publication
  String identifier;
  String location;
  Integer year;
  State jobState;

  public FullTextJob(String identifier, String location, Integer year, State state) {
    this.identifier = identifier;
    this.location = location;
    this.year = year;
    this.jobState = state;
  }

  public FullTextJob failed() {
    return new FullTextJob(this.identifier, this.location, this.year, State.Failed);
  }

  public FullTextJob completed(String location) {
    return new FullTextJob(this.identifier, location, this.year, State.Completed);
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
  public State getJobState() {
    return jobState;
  }
}
