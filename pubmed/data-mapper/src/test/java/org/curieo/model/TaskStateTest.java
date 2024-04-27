package org.curieo.model;

import static org.curieo.model.TaskState.State.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class TaskStateTest {

  @Test
  void stateOrdinalGuard() {
    // If this test fails you've goofed up the State enum. No worries though, this test caught it :)
    // It is very important that the enum matches the expected integer value, as
    // this is how it is stored in our DB.
    // If you add a new enum value, make sure you also add a check for the correct ordinal.
    validateState(Queued, 0);
    validateState(InProgress, 1);
    validateState(Completed, 2);
    validateState(Failed, 3);
    validateState(Unavailable, 4);
  }

  private void validateState(TaskState.State expectedState, int ordinal) {
    // This is function is written this way by design:
    // If a new enum is added this switch will complain at compile time.
    switch (TaskState.State.fromInt(ordinal)) {
      case Queued -> assertEquals(Queued, expectedState);
      case InProgress -> assertEquals(InProgress, expectedState);
      case Completed -> assertEquals(Completed, expectedState);
      case Failed -> assertEquals(Failed, expectedState);
      case Unavailable -> assertEquals(Unavailable, expectedState);
    }
  }
}
