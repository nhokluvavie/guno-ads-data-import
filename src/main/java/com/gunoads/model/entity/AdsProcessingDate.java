package com.gunoads.model.entity;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AdsProcessingDate {

    @NotBlank
    private String fullDate;

    @NotNull
    private Integer dayOfWeek;

    private String dayOfWeekName;

    @NotNull
    private Integer dayOfMonth;

    @NotNull
    private Integer dayOfYear;

    @NotNull
    private Integer weekOfYear;

    @NotNull
    private Integer monthOfYear;

    private String monthName;

    @NotNull
    private Integer quarter;

    @NotNull
    private Integer year;

    @NotNull
    private Boolean isWeekend;

    @NotNull
    private Boolean isHoliday;

    private String holidayName;

    @NotNull
    private Integer fiscalYear;

    @NotNull
    private Integer fiscalQuarter;

    public AdsProcessingDate(String fullDate) {
        this.fullDate = fullDate;
    }
}