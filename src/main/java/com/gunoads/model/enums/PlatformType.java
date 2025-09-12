package com.gunoads.model.enums;

public enum PlatformType {
    META("META", "Meta Ads"),
    GOOGLE("GOOGLE", "Google Ads"),
    TIKTOK("TIKTOK", "TikTok Ads"),
    TWITTER("TWITTER", "Twitter Ads"),
    LINKEDIN("LINKEDIN", "LinkedIn Ads");

    private final String code;
    private final String displayName;

    PlatformType(String code, String displayName) {
        this.code = code;
        this.displayName = displayName;
    }

    public String getCode() { return code; }
    public String getDisplayName() { return displayName; }

    public static PlatformType fromCode(String code) {
        for (PlatformType type : values()) {
            if (type.code.equals(code)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown platform code: " + code);
    }
}