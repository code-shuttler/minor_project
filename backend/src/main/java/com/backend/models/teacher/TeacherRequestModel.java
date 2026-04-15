package com.backend.models.teacher;

import java.util.Set;

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
public class TeacherRequestModel {

    @NotBlank(message = "Teacher name is required")
    @Size(max = 120, message = "Teacher name must be at most 120 characters")
    private String name;

    @NotBlank(message = "Teacher abbreviation is required")
    @Size(max = 20, message = "Teacher abbreviation must be at most 20 characters")
    private String abbreviation;

    @NotEmpty(message = "At least one specialization subject id is required")
    private Set<Long> specializationIds;

    @NotNull(message = "Department id is required")
    private Long departmentId;
}
