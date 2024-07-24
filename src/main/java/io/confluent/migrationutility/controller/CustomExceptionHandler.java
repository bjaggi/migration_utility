package io.confluent.migrationutility.controller;

import io.confluent.migrationutility.exception.InvalidClusterId;
import io.confluent.migrationutility.model.ErrorResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;

@Slf4j
@ControllerAdvice
public class CustomExceptionHandler {

  @ExceptionHandler(Exception.class)
  public final ResponseEntity<ErrorResponse> handleAllExceptions(Exception ex, WebRequest request) {
    log.error("{}", request);
    final ErrorResponse internalServerError = new ErrorResponse("INTERNAL_SERVER_ERROR", ex.getLocalizedMessage());
    return new ResponseEntity<>(internalServerError, HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @ExceptionHandler(InvalidClusterId.class)
  public final ResponseEntity<ErrorResponse> handleInvalidClusterIdException(InvalidClusterId ex, WebRequest request) {
    log.error("{}", request);
    final ErrorResponse internalServerError = new ErrorResponse("BAD_REQUEST", ex.getLocalizedMessage());
    return new ResponseEntity<>(internalServerError, HttpStatus.BAD_REQUEST);
  }

  
}
