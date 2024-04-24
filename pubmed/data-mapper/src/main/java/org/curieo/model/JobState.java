package org.curieo.model;

public interface JobState {

  State getJobState();

  public enum State {
    Queued(0),
    InProgress(1),
    Completed(2),
    Failed(3);

    private final int inner;

    State(int inner) {
      this.inner = inner;
    }

    public int getInner() {
      return inner;
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
}
