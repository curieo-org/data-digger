package org.curieo.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class BackwardCompatibilityTests {

  /**
   * For backward compatibility with existing databases we fix the ordering of the {@code
   * TaskState.State} enum.
   */
  @Test
  void testConstancyInOrderingOfStateEnum() {
    assertEquals(0, TaskState.State.Queued.ordinal());
    assertEquals(1, TaskState.State.InProgress.ordinal());
    assertEquals(2, TaskState.State.Completed.ordinal());
    assertEquals(3, TaskState.State.Failed.ordinal());
    assertEquals(4, TaskState.State.Unavailable.ordinal());
  }
}
