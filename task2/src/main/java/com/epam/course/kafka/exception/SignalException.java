package com.epam.course.kafka.exception;

public class SignalException extends RuntimeException {

    public SignalException() {
        super();
    }

    public SignalException(String message) {
        super(message);
    }
}
