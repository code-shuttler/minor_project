package com.backend.exceptions;

public class TimetableGenerationTimeoutException extends RuntimeException {
    public TimetableGenerationTimeoutException(String message) {
        super(message);
    }
}
