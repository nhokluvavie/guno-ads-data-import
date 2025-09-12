package com.gunoads.model.enums;

public enum AdStatus {
    ACTIVE("ACTIVE"),
    PAUSED("PAUSED"),
    DELETED("DELETED"),
    ARCHIVED("ARCHIVED"),
    PENDING_REVIEW("PENDING_REVIEW"),
    DISAPPROVED("DISAPPROVED"),
    PREAPPROVED("PREAPPROVED"),
    CAMPAIGN_PAUSED("CAMPAIGN_PAUSED"),
    ADSET_PAUSED("ADSET_PAUSED"),
    IN_PROCESS("IN_PROCESS"),
    WITH_ISSUES("WITH_ISSUES");

    private final String status;

    AdStatus(String status) {
        this.status = status;
    }

    public String getStatus() { return status; }

    public static AdStatus fromString(String status) {
        for (AdStatus s : values()) {
            if (s.status.equals(status)) {
                return s;
            }
        }
        return null;
    }
}