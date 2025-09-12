package com.gunoads.model.enums;

public enum AccountStatus {
    ACTIVE("ACTIVE"),
    DISABLED("DISABLED"),
    UNSETTLED("UNSETTLED"),
    PENDING_RISK_REVIEW("PENDING_RISK_REVIEW"),
    PENDING_SETTLEMENT("PENDING_SETTLEMENT"),
    IN_GRACE_PERIOD("IN_GRACE_PERIOD"),
    PENDING_CLOSURE("PENDING_CLOSURE"),
    CLOSED("CLOSED"),
    ANY_ACTIVE("ANY_ACTIVE"),
    ANY_CLOSED("ANY_CLOSED");

    private final String status;

    AccountStatus(String status) {
        this.status = status;
    }

    public String getStatus() { return status; }

    public static AccountStatus fromString(String status) {
        for (AccountStatus s : values()) {
            if (s.status.equals(status)) {
                return s;
            }
        }
        return null;
    }
}