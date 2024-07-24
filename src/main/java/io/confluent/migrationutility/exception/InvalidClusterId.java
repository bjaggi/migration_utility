package io.confluent.migrationutility.exception;

public class InvalidClusterId extends RuntimeException {

  public InvalidClusterId(String message) {
    super(message);
  }
}
