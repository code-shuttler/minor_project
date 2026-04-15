package com.backend.models.teacher;

import java.util.Set;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class TeacherResponseModel {
    private Long id;
    private String name;
    private String abbreviation;
    private Set<Long> specializationIds;
    private Long departmentId;
}
