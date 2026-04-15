package com.backend.models.timetable;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class TimetableRequestModel {

    @NotNull(message = "Batch id is required")
    @JsonProperty("batch_id")
    @JsonAlias("batchId")
    private Long batchId;

    @NotNull(message = "Subject id is required")
    @JsonProperty("subject_id")
    @JsonAlias("subjectId")
    private Long subjectId;

    @NotNull(message = "Teacher id is required")
    @JsonProperty("teacher_id")
    @JsonAlias("teacherId")
    private Long teacherId;

    @NotNull(message = "Time slot id is required")
    @JsonProperty("time_slot_id")
    @JsonAlias("timeSlotId")
    private Long timeSlotId;

    @NotNull(message = "Room id is required")
    @JsonProperty("room_id")
    @JsonAlias("roomId")
    private Long roomId;
}