package com.backend.models.department;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class DepartmentRequestModel {

    @NotBlank(message = "Department name is required")
    @Size(max = 120, message = "Department name must be at most 120 characters")
    private String name;

    @NotBlank(message = "Department abbreviation is required")
    @Size(max = 20, message = "Department abbreviation must be at most 20 characters")
    private String abbreviation;
}
