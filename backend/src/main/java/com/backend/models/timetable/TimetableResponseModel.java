package com.backend.models.timetable;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class TimetableResponseModel {

    private Long id;

    @JsonProperty("batch_id")
    private Long batchId;

    @JsonProperty("subject_id")
    private Long subjectId;

    @JsonProperty("teacher_id")
    private Long teacherId;

    @JsonProperty("time_slot_id")
    private Long timeSlotId;

    @JsonProperty("room_id")
    private Long roomId;
}