package com.backend.models.subject;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
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
public class SubjectRequestModel {

    @NotBlank(message = "Subject name is required")
    @Size(max = 120, message = "Subject name must be at most 120 characters")
    private String name;

    @NotEmpty(message = "At least one department id is required")
    @JsonProperty("department_ids")
    @JsonAlias("departmentIds")
    private Set<Long> departmentIds;

    @NotNull(message = "Lecture count is required")
    @Min(value = 0, message = "Lecture count must be zero or greater")
    private Integer lecture;

    @NotNull(message = "Tutorial count is required")
    @Min(value = 0, message = "Tutorial count must be zero or greater")
    private Integer tutorial;

    @NotNull(message = "Practical count is required")
    @Min(value = 0, message = "Practical count must be zero or greater")
    private Integer practical;
}
