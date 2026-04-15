package com.backend.models.room;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class RoomResponseModel {
    private Long id;
    private String name;

    @JsonProperty("isLab")
    @JsonAlias({ "islab" })
    private Boolean isLab;

    @JsonProperty("department_id")
    @JsonAlias("departmentId")
    private Long departmentId;
}