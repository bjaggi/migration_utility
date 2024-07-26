package io.confluent.migrationutility.exception;

public class
InvalidClusterIdException extends RuntimeException {


  public InvalidClusterIdException(Throwable cause) {
    super(cause);
  }

  public InvalidClusterIdException(String message) {
    super(message);
  }
}
