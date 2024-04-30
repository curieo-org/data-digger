package org.curieo.model;

public interface TaskState {
  State[] STATES = State.values();

  State getTaskState();

  enum State {
    Queued,
    InProgress,
    Completed,
    Failed,
    Unavailable;

    public static State fromInt(int i) {
      return STATES[i];
    }
  }
}
