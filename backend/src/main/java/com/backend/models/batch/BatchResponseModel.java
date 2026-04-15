package com.backend.models.batch;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class BatchResponseModel {

    private Long id;

    private String name;

    private Integer semester;

    @JsonProperty("department_id")
    private Long departmentId;

    @JsonProperty("subject_ids")
    private Set<Long> subjectIds;
}
