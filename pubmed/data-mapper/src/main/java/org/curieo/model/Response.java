package org.curieo.model;

public record Response<T>(T value, Status status) {
  public static enum Status {
    Ok,
    Unavailable,
    Fail
  }

  public boolean ok() {
    return status == Status.Ok;
  }

  public TaskState.State state() {
    return switch (status) {
      case Ok -> TaskState.State.Completed;
      case Unavailable -> TaskState.State.Unavailable;
      default -> TaskState.State.Failed;
    };
  }

  public <Y> Response<Y> map(Y value) {
    return new Response<Y>(value, status);
  }

  public static <T> Response<T> fail(T value) {
    return new Response<T>(value, Status.Fail);
  }

  public static <T> Response<T> ok(T value) {
    return new Response<T>(value, Status.Ok);
  }
}
