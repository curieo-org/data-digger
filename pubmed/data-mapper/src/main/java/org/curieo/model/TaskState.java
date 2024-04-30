package org.curieo.model;

import org.curieo.utils.EnumUtils;

public interface TaskState {
  State getTaskState();

  enum State {
    Queued,
    InProgress,
    Completed,
    Failed,
    Unavailable;

    public static State fromInt(int i) {
      return EnumUtils.get(State.class, i);
    }
  }
}
