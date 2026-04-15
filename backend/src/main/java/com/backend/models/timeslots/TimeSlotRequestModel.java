package com.backend.models.timeslots;

import java.time.LocalTime;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class TimeSlotRequestModel {

    @NotBlank(message = "Day is required")
    @Pattern(
            regexp = "(?i)^(monday|tuesday|wednesday|thursday|friday|saturday|sunday)$",
            message = "Day must be a valid weekday")
    private String day;

    @NotNull(message = "Start time is required")
    @JsonProperty("start_time")
    private LocalTime startTime;

    @NotNull(message = "End time is required")
    @JsonProperty("end_time")
    private LocalTime endTime;
}
