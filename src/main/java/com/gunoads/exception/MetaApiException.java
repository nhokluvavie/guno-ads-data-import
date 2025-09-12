package com.gunoads.exception;

public class MetaApiException extends RuntimeException {

    private final int errorCode;
    private final String errorType;
    private final String facebookTraceId;

    public MetaApiException(String message) {
        super(message);
        this.errorCode = 0;
        this.errorType = "UNKNOWN";
        this.facebookTraceId = null;
    }

    public MetaApiException(String message, Throwable cause) {
        super(message, cause);
        this.errorCode = 0;
        this.errorType = "UNKNOWN";
        this.facebookTraceId = null;
    }

    public MetaApiException(String message, int errorCode, String errorType) {
        super(message);
        this.errorCode = errorCode;
        this.errorType = errorType;
        this.facebookTraceId = null;
    }

    public MetaApiException(String message, int errorCode, String errorType, String facebookTraceId) {
        super(message);
        this.errorCode = errorCode;
        this.errorType = errorType;
        this.facebookTraceId = facebookTraceId;
    }

    public int getErrorCode() { return errorCode; }
    public String getErrorType() { return errorType; }
    public String getFacebookTraceId() { return facebookTraceId; }

    public boolean isRateLimitError() {
        return errorCode == 4 || errorCode == 17 || errorCode == 32;
    }

    public boolean isAuthenticationError() {
        return errorCode == 190 || errorCode == 102;
    }

    public boolean isPermissionError() {
        return errorCode == 200 || errorCode == 10;
    }

    @Override
    public String toString() {
        return String.format("MetaApiException{code=%d, type='%s', message='%s', traceId='%s'}",
                errorCode, errorType, getMessage(), facebookTraceId);
    }
}