package com.backend.models.timetable;

import java.util.Set;

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
public class TimetableAutoGenerateRequestModel {

    @NotNull(message = "Batch ids are required")
    @JsonProperty("batches_id")
    @JsonAlias("batchIds")
    private Set<Long> batchIds;

}