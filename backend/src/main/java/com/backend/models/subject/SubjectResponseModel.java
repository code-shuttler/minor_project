package com.backend.models.subject;

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
public class SubjectResponseModel {
    private Long id;
    private String name;

    @JsonProperty("department_ids")
    private Set<Long> departmentIds;

    private Integer lecture;
    private Integer tutorial;
    private Integer practical;
}
