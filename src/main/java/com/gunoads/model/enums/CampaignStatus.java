package com.gunoads.model.enums;

public enum CampaignStatus {
    ACTIVE("ACTIVE"),
    PAUSED("PAUSED"),
    DELETED("DELETED"),
    ARCHIVED("ARCHIVED"),
    PENDING_REVIEW("PENDING_REVIEW"),
    DISAPPROVED("DISAPPROVED"),
    PREAPPROVED("PREAPPROVED"),
    PENDING_BILLING_INFO("PENDING_BILLING_INFO"),
    CAMPAIGN_PAUSED("CAMPAIGN_PAUSED"),
    IN_PROCESS("IN_PROCESS"),
    WITH_ISSUES("WITH_ISSUES");

    private final String status;

    CampaignStatus(String status) {
        this.status = status;
    }

    public String getStatus() { return status; }

    public static CampaignStatus fromString(String status) {
        for (CampaignStatus s : values()) {
            if (s.status.equals(status)) {
                return s;
            }
        }
        return null;
    }
}