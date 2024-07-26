package io.confluent.migrationutility.controller;

import io.confluent.migrationutility.exception.ActiveGroupException;
import io.confluent.migrationutility.exception.AdminClientException;
import io.confluent.migrationutility.exception.GroupServiceException;
import io.confluent.migrationutility.exception.InvalidClusterIdException;
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
    log.error("Error caught from request : {}", request);
    final ErrorResponse internalServerError = new ErrorResponse("INTERNAL_SERVER_ERROR", ex.getLocalizedMessage());
    return new ResponseEntity<>(internalServerError, HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @ExceptionHandler(GroupServiceException.class)
  public final ResponseEntity<ErrorResponse> handleGroupServiceException(GroupServiceException ex, WebRequest request) {
    log.error("GroupService exception caught from request : {}", request);
    final ErrorResponse error = new ErrorResponse("GROUP_SERVICE_ERROR", ex.getLocalizedMessage());
    return new ResponseEntity<>(error, HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @ExceptionHandler(AdminClientException.class)
  public final ResponseEntity<ErrorResponse> handleAdminClientException(AdminClientException ex, WebRequest request) {
    log.error("AdminClientException caught from request : {}", request);
    final ErrorResponse error = new ErrorResponse("ADMIN_CLIENT_ERROR", ex.getLocalizedMessage());
    return new ResponseEntity<>(error, HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @ExceptionHandler(ActiveGroupException.class)
  public final ResponseEntity<ErrorResponse> handleActiveGroupException(ActiveGroupException ex, WebRequest request) {
    log.error("ActiveGroupException caught from request : {}", request);
    final ErrorResponse error = new ErrorResponse("USER_ERROR", ex.getLocalizedMessage());
    return new ResponseEntity<>(error, HttpStatus.NOT_ACCEPTABLE);
  }



  @ExceptionHandler(InvalidClusterIdException.class)
  public final ResponseEntity<ErrorResponse> handleInvalidClusterIdException(InvalidClusterIdException ex, WebRequest request) {
    log.error("InvalidClusterIdException caught from request : {}", request);
    final ErrorResponse internalServerError = new ErrorResponse("BAD_REQUEST", ex.getLocalizedMessage());
    return new ResponseEntity<>(internalServerError, HttpStatus.BAD_REQUEST);
  }

  
}
