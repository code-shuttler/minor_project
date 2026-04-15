package com.backend.models.batch;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class BatchRequestModel {

    @NotBlank(message = "Batch name is required")
    @Size(max = 120, message = "Batch name must be at most 120 characters")
    private String name;

    @NotNull(message = "Semester is required")
    @Min(value = 1, message = "Semester must be greater than or equal to 1")
    private Integer semester;

    @NotNull(message = "Department id is required")
    @JsonProperty("department_id")
    @JsonAlias("departmentId")
    private Long departmentId;

    @JsonProperty("subject_ids")
    @JsonAlias("subjectIds")
    private Set<Long> subjectIds;
}
