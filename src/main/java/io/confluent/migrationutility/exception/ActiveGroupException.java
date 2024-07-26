package io.confluent.migrationutility.exception;

public class ActiveGroupException extends RuntimeException {
  public ActiveGroupException(String message) {
    super(message);
  }
}
