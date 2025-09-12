package com.gunoads.model.enums;

public enum BuyingType {
    AUCTION("AUCTION"),
    REACH_AND_FREQUENCY("REACH_AND_FREQUENCY"),
    TARGET_FREQUENCY("TARGET_FREQUENCY"),
    RESERVED("RESERVED");

    private final String type;

    BuyingType(String type) {
        this.type = type;
    }

    public String getType() { return type; }

    public static BuyingType fromString(String type) {
        for (BuyingType bt : values()) {
            if (bt.type.equals(type)) {
                return bt;
            }
        }
        return null;
    }
}